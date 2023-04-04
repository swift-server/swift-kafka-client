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

/// A collection of helper functions wrapping common `rd_kafka_conf_*` functions in Swift.
struct RDKafkaConfig {
    /// Wraps a Swift closure inside of a class to be able to pass it to `librdkafka` as an `OpaquePointer`.
    final class CapturedClosure {
        typealias Closure = (UnsafePointer<rd_kafka_message_t>?) -> Void
        let closure: Closure

        init(_ closure: @escaping Closure) {
            self.closure = closure
        }
    }

    /// Create a new `rd_kafka_conf_t` object in memory and initialize it with the given configuration properties.
    /// - Parameter configDictionary: A dictionary containing the Kafka client configurations.
    /// - Returns: An `OpaquePointer` pointing to the newly created `rd_kafka_conf_t` object in memory.
    /// - Throws: A ``KafkaError`` if setting a config value failed.
    static func createFrom(configDictionary: [String: String]) throws -> OpaquePointer {
        let configPointer: OpaquePointer = rd_kafka_conf_new()
        try configDictionary.forEach { key, value in
            try Self.set(configPointer: configPointer, key: key, value: value)
        }

        return configPointer
    }

    /// A Swift wrapper for `rd_kafka_conf_set`.
    /// - Parameter configPointer: An `OpaquePointer` pointing to the `rd_kafka_conf_t` object in memory.
    /// - Parameter key: The configuration property to be changed.
    /// - Parameter value: The new value of the configuration property to be changed.
    /// - Throws: A ``KafkaError`` if setting the value failed.
    static func set(configPointer: OpaquePointer, key: String, value: String) throws {
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

    /// A Swift wrapper for `rd_kafka_conf_set_dr_msg_cb`.
    /// Defines a function that is called upon every message acknowledgement.
    /// - Parameter configPointer: An `OpaquePointer` pointing to the `rd_kafka_conf_t` object in memory.
    /// - Parameter callback: A closure that is invoked upon message acknowledgement.
    /// - Returns: A ``CapturedClosure`` object that must me retained by the caller as long as acknowledgements are received.
    static func setDeliveryReportCallback(
        configPointer: OpaquePointer,
        _ callback: @escaping ((UnsafePointer<rd_kafka_message_t>?) -> Void)
    ) -> CapturedClosure {
        let capturedClosure = CapturedClosure(callback)
        // Pass the captured closure to the C closure as an opaque object
        let opaquePointer: UnsafeMutableRawPointer? = Unmanaged.passUnretained(capturedClosure).toOpaque()
        rd_kafka_conf_set_opaque(
            configPointer,
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
            configPointer,
            callbackWrapper
        )

        return capturedClosure
    }
}
