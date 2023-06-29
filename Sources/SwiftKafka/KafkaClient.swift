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

    /// Produce a message to the Kafka cluster.
    ///
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    /// - Parameter newMessageID: ID that was assigned to the `message`.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter topicHandles: Topic handles that this client uses to produce new messages
    func produce(
        message: KafkaProducerMessage,
        newMessageID: UInt,
        topicConfig: KafkaTopicConfiguration,
        topicHandles: RDKafkaTopicHandles
    ) throws {
        let keyBytes: [UInt8]?
        if var key = message.key {
            keyBytes = key.readBytes(length: key.readableBytes)
        } else {
            keyBytes = nil
        }

        let responseCode = try message.value.withUnsafeReadableBytes { valueBuffer in
            return try topicHandles.withTopicHandlePointer(topic: message.topic, topicConfig: topicConfig) { topicHandle in
                // Pass message over to librdkafka where it will be queued and sent to the Kafka Cluster.
                // Returns 0 on success, error code otherwise.
                return rd_kafka_produce(
                    topicHandle,
                    message.partition.rawValue,
                    RD_KAFKA_MSG_F_COPY,
                    UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress),
                    valueBuffer.count,
                    keyBytes,
                    keyBytes?.count ?? 0,
                    UnsafeMutableRawPointer(bitPattern: newMessageID)
                )
            }
        }

        guard responseCode == 0 else {
            throw KafkaError.rdKafkaError(wrapping: rd_kafka_last_error())
        }
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

    /// Wraps a Swift closure inside of a class to be able to pass it to `librdkafka` as an `OpaquePointer`.
    /// This is specifically used to pass a Swift closure as a commit callback for the ``KafkaConsumer``.
    final class CapturedCommitCallback {
        typealias Closure = (Result<Void, KafkaError>) -> Void
        let closure: Closure

        init(_ closure: @escaping Closure) {
            self.closure = closure
        }
    }

    /// Non-blocking commit of a the `message`'s offset to Kafka.
    ///
    /// - Parameter message: Last received message that shall be marked as read.
    func commitSync(_ message: KafkaConsumerMessage) async throws {
        // Declare captured closure outside of withCheckedContinuation.
        // We do that because do an unretained pass of the captured closure to
        // librdkafka which means we have to keep a reference to the closure
        // ourselves to make sure it does not get deallocated before
        // commitSync returns.
        var capturedClosure: CapturedCommitCallback!
        try await withCheckedThrowingContinuation { continuation in
            capturedClosure = CapturedCommitCallback { result in
                continuation.resume(with: result)
            }

            // The offset committed is always the offset of the next requested message.
            // Thus, we increase the offset of the current message by one before committing it.
            // See: https://github.com/edenhill/librdkafka/issues/2745#issuecomment-598067945
            let changesList = RDKafkaTopicPartitionList()
            changesList.setOffset(
                topic: message.topic,
                partition: message.partition,
                offset: Int64(message.offset + 1)
            )

            // Unretained pass because the reference that librdkafka holds to capturedClosure
            // should not be counted in ARC as this can lead to memory leaks.
            let opaquePointer: UnsafeMutableRawPointer? = Unmanaged.passUnretained(capturedClosure).toOpaque()

            let consumerQueue = rd_kafka_queue_get_consumer(self.kafkaHandle)

            // Create a C closure that calls the captured closure
            let callbackWrapper: (
                @convention(c) (
                    OpaquePointer?,
                    rd_kafka_resp_err_t,
                    UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?,
                    UnsafeMutableRawPointer?
                ) -> Void
            ) = { _, error, _, opaquePointer in

                guard let opaquePointer = opaquePointer else {
                    fatalError("Could not resolve reference to catpured Swift callback instance")
                }
                let opaque = Unmanaged<CapturedCommitCallback>.fromOpaque(opaquePointer).takeUnretainedValue()

                let actualCallback = opaque.closure

                if error == RD_KAFKA_RESP_ERR_NO_ERROR {
                    actualCallback(.success(()))
                } else {
                    let kafkaError = KafkaError.rdKafkaError(wrapping: error)
                    actualCallback(.failure(kafkaError))
                }
            }

            changesList.withListPointer { listPointer in
                rd_kafka_commit_queue(
                    self.kafkaHandle,
                    listPointer,
                    consumerQueue,
                    callbackWrapper,
                    opaquePointer
                )
            }
        }
    }

    /// Close the consumer asynchronously. This means revoking its assignemnt, committing offsets to broker and
    /// leaving the consumer group (if applicable).
    ///
    /// Make sure to run poll loop until ``KafkaClient/consumerIsClosed`` returns `true`.
    func consumerClose() throws {
        let consumerQueue = rd_kafka_queue_get_consumer(self.kafkaHandle)
        let result = rd_kafka_consumer_close_queue(self.kafkaHandle, consumerQueue)
        let kafkaError = rd_kafka_error_code(result)
        if kafkaError != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: kafkaError)
        }
    }

    var isConsumerClosed: Bool {
        rd_kafka_consumer_closed(self.kafkaHandle) == 1
    }

    /// Scoped accessor that enables safe access to the pointer of the client's Kafka handle.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the Kafka handle pointer.
    @discardableResult
    func withKafkaHandlePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        return try body(self.kafkaHandle)
    }
}
