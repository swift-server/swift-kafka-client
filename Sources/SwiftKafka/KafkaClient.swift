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

    /// Handle for the C library's Kafka instance.
    private let kafkaHandle: OpaquePointer
    /// References the opaque object passed to the config to ensure ARC retains it as long as the client exists.
    private let opaque: RDKafkaConfig.CapturedClosures
    /// A logger.
    private let logger: Logger

    init(
        kafkaHandle: OpaquePointer,
        opaque: RDKafkaConfig.CapturedClosures,
        logger: Logger
    ) {
        self.kafkaHandle = kafkaHandle
        self.opaque = opaque
        self.logger = logger
    }

    deinit {
        rd_kafka_destroy(kafkaHandle)
    }

    /// Polls the Kafka client for events.
    ///
    /// Events will cause application-provided callbacks to be called.
    ///
    /// - Parameter timeout: Specifies the maximum amount of time
    /// (in milliseconds) that the call will block waiting for events.
    /// For non-blocking calls, provide 0 as `timeout`.
    /// To wait indefinitely for an event, provide -1.
    /// - Returns: The number of events served.
    @discardableResult
    func poll(timeout: Int32) -> Int32 {
        return rd_kafka_poll(self.kafkaHandle, timeout)
    }

    /// Redirect the main ``KafkaClient/poll(timeout:)`` queue to the `KafkaConsumer`'s
    /// queue (``KafkaClient/consumerPoll``).
    ///
    /// Events that would be triggered by ``KafkaClient/poll(timeout:)``
    /// are now triggered by ``KafkaClient/consumerPoll``.
    ///
    /// - Warning: It is not allowed to call ``KafkaClient/poll(timeout:)`` after ``KafkaClient/pollSetConsumer``.
    func pollSetConsumer() throws {
        let result = rd_kafka_poll_set_consumer(self.kafkaHandle)
        if result != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: result)
        }
    }

    /// Request a new message from the Kafka cluster.
    ///
    /// - Important: This method should only be invoked from ``KafkaConsumer``.
    ///
    /// - Returns: A ``KafkaConsumerMessage`` or `nil` if there are no new messages.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
    func consumerPoll() throws -> KafkaConsumerMessage? {
        guard let messagePointer = rd_kafka_consumer_poll(self.kafkaHandle, 0) else {
            // No error, there might be no more messages
            return nil
        }

        defer {
            // Destroy message otherwise poll() will block forever
            rd_kafka_message_destroy(messagePointer)
        }

        // Reached the end of the topic+partition queue on the broker
        if messagePointer.pointee.err == RD_KAFKA_RESP_ERR__PARTITION_EOF {
            return nil
        }

        let message = try KafkaConsumerMessage(messagePointer: messagePointer)
        return message
    }

    /// Subscribe to topic set using balanced consumer groups.
    /// - Parameter topicPartitionList: Pointer to a list of topics + partition pairs.
    func subscribe(topicPartitionList: RDKafkaTopicPartitionList) throws {
        try topicPartitionList.withListPointer { pointer in
            let result = rd_kafka_subscribe(self.kafkaHandle, pointer)
            if result != RD_KAFKA_RESP_ERR_NO_ERROR {
                throw KafkaError.rdKafkaError(wrapping: result)
            }
        }
    }

    /// Atomic assignment of partitions to consume.
    /// - Parameter topicPartitionList: Pointer to a list of topics + partition pairs.
    func assign(topicPartitionList: RDKafkaTopicPartitionList) throws {
        try topicPartitionList.withListPointer { pointer in
            let result = rd_kafka_assign(self.kafkaHandle, pointer)
            if result != RD_KAFKA_RESP_ERR_NO_ERROR {
                throw KafkaError.rdKafkaError(wrapping: result)
            }
        }
    }

    /// Close the consumer.
    func consumerClose() throws {
        let result = rd_kafka_consumer_close(self.kafkaHandle)
        if result != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: result)
        }
    }

    /// Scoped accessor that enables safe access to the pointer of the client's Kafka handle.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the Kafka handle pointer.
    @discardableResult
    func withKafkaHandlePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        return try body(self.kafkaHandle)
    }
}
