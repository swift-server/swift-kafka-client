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
final class KafkaClient: Sendable {
    // Default size for Strings returned from C API
    static let stringSize = 1024

    /// Handle for the C library's Kafka instance.
    private let kafkaHandle: OpaquePointer
    /// A logger.
    private let logger: Logger

    /// `librdkafka`'s main `rd_kafka_queue_t`.
    private let mainQueue: OpaquePointer

    init(
        kafkaHandle: OpaquePointer,
        logger: Logger
    ) {
        self.kafkaHandle = kafkaHandle
        self.logger = logger

        self.mainQueue = rd_kafka_queue_get_main(self.kafkaHandle)

        rd_kafka_set_log_queue(self.kafkaHandle, self.mainQueue)
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

    /// Swift wrapper for events from `librdkafka`'s event queue.
    enum KafkaEvent {
        case deliveryReport(results: [Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>])
    }

    /// Poll the event `rd_kafka_queue_t` for new events.
    ///
    /// - Parameter maxEvents:Maximum number of events to serve in one invocation.
    func eventPoll(maxEvents: Int = 100) -> [KafkaEvent] {
        var events = [KafkaEvent]()
        events.reserveCapacity(maxEvents)

        for _ in 0..<maxEvents {
            let event = rd_kafka_queue_poll(self.mainQueue, 0)
            defer { rd_kafka_event_destroy(event) }

            let rdEventType = rd_kafka_event_type(event)
            guard let eventType = RDKafkaEvent(rawValue: rdEventType) else {
                fatalError("Unsupported event type: \(rdEventType)")
            }

            switch eventType {
            case .deliveryReport:
                let forwardEvent = self.handleDeliveryReportEvent(event)
                events.append(forwardEvent) // Return KafkaEvent.deliveryReport as part of this method
            case .log:
                self.handleLogEvent(event)
            case .none:
                // Finished reading events, return early
                return events
            default:
                break // Ignored Event
            }
        }

        return events
    }

    /// Handle event of type `RDKafkaEvent.deliveryReport`.
    ///
    /// - Parameter event: Pointer to underlying `rd_kafka_event_t`.
    /// - Returns: `KafkaEvent` to be returned as part of ``KafkaClient.eventPoll()`.
    private func handleDeliveryReportEvent(_ event: OpaquePointer?) -> KafkaEvent {
        let deliveryReportCount = rd_kafka_event_message_count(event)
        var deliveryReportResults = [Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>]()
        deliveryReportResults.reserveCapacity(deliveryReportCount)

        while let messagePointer = rd_kafka_event_message_next(event) {
            guard let message = Self.convertMessageToAcknowledgementResult(messagePointer: messagePointer) else {
                continue
            }
            deliveryReportResults.append(message)
        }

        return .deliveryReport(results: deliveryReportResults)
    }

    /// Handle event of type `RDKafkaEvent.log`.
    ///
    /// - Parameter event: Pointer to underlying `rd_kafka_event_t`.
    private func handleLogEvent(_ event: OpaquePointer?) {
        var faculty: UnsafePointer<CChar>?
        var buffer: UnsafePointer<CChar>?
        var level: Int32 = 0
        if rd_kafka_event_log(event, &faculty, &buffer, &level) == 0 {
            if let faculty, let buffer {
                // Mapping according to https://en.wikipedia.org/wiki/Syslog
                switch level {
                case 0...2: /* Emergency, Alert, Critical */
                    self.logger.critical(
                        Logger.Message(stringLiteral: String(cString: buffer)), source: String(cString: faculty)
                    )
                case 3: /* Error */
                    self.logger.error(
                        Logger.Message(stringLiteral: String(cString: buffer)), source: String(cString: faculty)
                    )
                case 4: /* Warning */
                    self.logger.warning(
                        Logger.Message(stringLiteral: String(cString: buffer)), source: String(cString: faculty)
                    )
                case 5: /* Notice */
                    self.logger.notice(
                        Logger.Message(stringLiteral: String(cString: buffer)), source: String(cString: faculty)
                    )
                case 6: /* Informational */
                    self.logger.info(
                        Logger.Message(stringLiteral: String(cString: buffer)), source: String(cString: faculty)
                    )
                default: /* Debug */
                    self.logger.debug(
                        Logger.Message(stringLiteral: String(cString: buffer)), source: String(cString: faculty)
                    )
                }
            }
        }
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

    /// Returns `true` if the underlying `librdkafka` consumer is closed.
    var isConsumerClosed: Bool {
        rd_kafka_consumer_closed(self.kafkaHandle) == 1
    }

    /// Returns the current out queue length.
    ///
    /// This means the number of producer messages that wait to be sent + the number of any
    /// callbacks that are waiting to be executed by invoking `rd_kafka_poll`.
    var outgoingQueueSize: Int32 {
        return rd_kafka_outq_len(self.kafkaHandle)
    }

    /// Scoped accessor that enables safe access to the pointer of the client's Kafka handle.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the Kafka handle pointer.
    @discardableResult
    func withKafkaHandlePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        return try body(self.kafkaHandle)
    }

    /// Convert an unsafe`rd_kafka_message_t` object to a safe ``KafkaAcknowledgementResult``.
    /// - Parameter messagePointer: An `UnsafePointer` pointing to the `rd_kafka_message_t` object in memory.
    /// - Returns: A ``KafkaAcknowledgementResult``.
    private static func convertMessageToAcknowledgementResult(
        messagePointer: UnsafePointer<rd_kafka_message_t>?
    ) -> Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>? {
        guard let messagePointer else {
            return nil
        }

        let messageID = KafkaProducerMessageID(rawValue: UInt(bitPattern: messagePointer.pointee._private))

        let messageResult: Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>
        do {
            let message = try KafkaAcknowledgedMessage(messagePointer: messagePointer, id: messageID)
            messageResult = .success(message)
        } catch {
            guard let error = error as? KafkaAcknowledgedMessageError else {
                fatalError("Caught error that is not of type \(KafkaAcknowledgedMessageError.self)")
            }
            messageResult = .failure(error)
        }

        return messageResult
    }
}
