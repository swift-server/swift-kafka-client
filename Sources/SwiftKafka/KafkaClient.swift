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

    /// Determines if client is a producer or a consumer.
    enum `Type` {
        case producer
        case consumer
    }

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
                throw KafkaError.anyError(description: errorString)
            }

            return handle
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
