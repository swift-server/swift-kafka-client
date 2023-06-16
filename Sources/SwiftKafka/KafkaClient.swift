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
    private let opaque: RDKafkaConfig.CapturedClosure?
    /// A logger.
    private let logger: Logger

    init(
        kafkaHandle: OpaquePointer,
        opaque: RDKafkaConfig.CapturedClosure?,
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

    /// Request a new message from the Kafka cluster.
    ///
    /// This method blocks for a maximum of `timeout` milliseconds.
    ///
    /// - Important: This method should only be invoked from ``KafkaConsumer``.
    ///
    /// - Parameter timeout: Maximum amount of milliseconds this method waits for a new message.
    /// - Returns: A ``KafkaConsumerMessage`` or `nil` if there are no new messages.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
    func consumerPoll(timeout: Int32 = 100) async throws -> KafkaConsumerMessage? {
        try await withCheckedThrowingContinuation { continuation in
            guard let messagePointer = rd_kafka_consumer_poll(self.kafkaHandle, timeout) else {
                // No error, there might be no more messages
                continuation.resume(returning: nil)
                return
            }

            defer {
                // Destroy message otherwise poll() will block forever
                rd_kafka_message_destroy(messagePointer)
            }

            // Reached the end of the topic+partition queue on the broker
            if messagePointer.pointee.err == RD_KAFKA_RESP_ERR__PARTITION_EOF {
                continuation.resume(returning: nil)
                return
            }

            do {
                let message = try KafkaConsumerMessage(messagePointer: messagePointer)
                continuation.resume(returning: message)
            } catch {
                continuation.resume(throwing: error)
            }
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
