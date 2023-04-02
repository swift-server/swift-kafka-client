//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka
import Logging

/// Base class for ``KafkaProducer`` and ``KafkaConsumer``,
/// which is used to handle the connection to the Kafka ecosystem.
final class KafkaClient {
    // Default size for Strings returned from C API
    static let stringSize = 1024

    /// A logger.
    private let logger: Logger

    /// A client is either a `.producer` or a `.consumer`.
    private let clientType: rd_kafka_type_t
    /// The configuration object of the client.
    private let config: KafkaConfig
    /// Handle for the C library's Kafka instance.
    private let kafkaHandle: OpaquePointer
    /// References the opaque object passed to the config to ensure ARC retains it as long as the config exists.
    private var opaque: CapturedClosure?

    /// Determines if client is a producer or a consumer.
    enum `Type` {
        case producer
        case consumer
    }

    // TODO: remove?
    init(type: Type, config: KafkaConfig, logger: Logger) throws {
        self.logger = logger
        self.clientType = type == .producer ? RD_KAFKA_PRODUCER : RD_KAFKA_CONSUMER
        self.config = config

        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
        defer { errorChars.deallocate() }

        self.kafkaHandle = try self.config.withDuplicatePointer { [clientType] duplicateConfig in
            // Duplicate because rd_kafka_new takes ownership of the pointer and frees it upon success.
            guard let handle = rd_kafka_new(
                clientType,
                duplicateConfig,
                errorChars,
                KafkaClient.stringSize
            ) else {
                // rd_kafka_new only frees the duplicate pointer upon success.
                rd_kafka_conf_destroy(duplicateConfig)

                let errorString = String(cString: errorChars)
                throw KafkaError.client(reason: errorString)
            }

            return handle
        }
    }

    init(
        type: Type,
        configDictionary: [String: String],
        callback: ((UnsafePointer<rd_kafka_message_t>?) -> Void)? = nil,
        logger: Logger
    ) throws {
        self.logger = logger
        self.clientType = type == .producer ? RD_KAFKA_PRODUCER : RD_KAFKA_CONSUMER
        self.config = KafkaConfig() // TODO: get rid / keep no reference

        // TODO: delete comment
        // create rd_kafka_conf directly
        let rdConfig: OpaquePointer = rd_kafka_conf_new()
        try configDictionary.forEach { key, value in
            try Self.set(value, forKey: key, configPointer: rdConfig)
        }

        // TODO: refactor
        // TODO: is KafkaClient the right place for this?
        if let callback {
            let capturedClosure = CapturedClosure(callback)

            // Pass the captured closure to the C closure as an opaque object
            let opaquePointer: UnsafeMutableRawPointer? = Unmanaged.passUnretained(capturedClosure).toOpaque()
            rd_kafka_conf_set_opaque(
                rdConfig,
                opaquePointer
            )

            // Create a C closure that calls the captured closure
            let callbackWrapper: (
                @convention(c) (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, UnsafeMutableRawPointer?) -> Void
            ) = { _, messagePointer, opaquePointer in

                guard let opaquePointer = opaquePointer else {
                    fatalError("Could not resolve reference to KafkaProducer instance")
                }
                let opaque = Unmanaged<CapturedClosure>.fromOpaque(opaquePointer).takeUnretainedValue()

                let actualCallback = opaque.closure
                actualCallback(messagePointer)
            }

            rd_kafka_conf_set_dr_msg_cb(
                rdConfig,
                callbackWrapper
            )

            // Retain captured closure in this config
            // This shall only happen after rd_kafka_conf_set_dr_msg_cb to avoid potential race-conditions
            self.opaque = capturedClosure
            // TODO: end setDeliveryReportCallback
        }

        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
        defer { errorChars.deallocate() }

        guard let handle = rd_kafka_new(
            clientType,
            rdConfig,
            errorChars,
            KafkaClient.stringSize
        ) else {
            // rd_kafka_new only frees the duplicate pointer upon success.
            rd_kafka_conf_destroy(rdConfig)

            let errorString = String(cString: errorChars)
            throw KafkaError.client(reason: errorString)
        }
        self.kafkaHandle = handle
    }

    // TODO: refactor
    static func set(_ value: String, forKey key: String, configPointer: OpaquePointer) throws {
        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
        defer { errorChars.deallocate() }

        let configResult = rd_kafka_conf_set(
            configPointer,
            key,
            value,
            errorChars,
            KafkaClient.stringSize
        )

        if configResult != RD_KAFKA_CONF_OK {
            let errorString = String(cString: errorChars)
            throw KafkaError.config(reason: errorString)
        }
    }

    // TODO: refactor
    private final class CapturedClosure {
        typealias Closure = (UnsafePointer<rd_kafka_message_t>?) -> Void
        let closure: Closure

        init(_ closure: @escaping Closure) {
            self.closure = closure
        }
    }

    deinit {
        rd_kafka_destroy(kafkaHandle)
    }

    /// Scoped accessor that enables safe access to the pointer of the client's Kafka handle.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the Kafka handle pointer.
    @discardableResult
    func withKafkaHandlePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        return try body(self.kafkaHandle)
    }
}
