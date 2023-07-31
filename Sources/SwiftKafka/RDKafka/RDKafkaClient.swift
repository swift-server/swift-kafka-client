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
import Dispatch
import Logging

/// Base class for ``KafkaProducer`` and ``KafkaConsumer``,
/// which is used to handle the connection to the Kafka ecosystem.
final class RDKafkaClient: Sendable {
    /// Determines if client is a producer or a consumer.
    enum ClientType {
        case producer
        case consumer
    }

    // Default size for Strings returned from C API
    static let stringSize = 1024

    /// Handle for the C library's Kafka instance.
    private let kafkaHandle: OpaquePointer
    /// A logger.
    private let logger: Logger

    /// `librdkafka`'s main `rd_kafka_queue_t`.
    private let mainQueue: OpaquePointer

    // Use factory method to initialize
    private init(
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

    /// Factory method creating a new instance of a ``RDKafkaClient``.
    static func makeClient(
        type: ClientType,
        configDictionary: [String: String],
        events: [RDKafkaEvent],
        logger: Logger
    ) throws -> RDKafkaClient {
        let clientType = type == .producer ? RD_KAFKA_PRODUCER : RD_KAFKA_CONSUMER

        let rdConfig = try RDKafkaConfig.createFrom(configDictionary: configDictionary)
        // Manually override some of the configuration options
        // Handle logs in event queue
        try RDKafkaConfig.set(configPointer: rdConfig, key: "log.queue", value: "true")
        // KafkaConsumer is manually storing read offsets
        if type == .consumer {
            try RDKafkaConfig.set(configPointer: rdConfig, key: "enable.auto.offset.store", value: "false")
        }
        RDKafkaConfig.setEvents(configPointer: rdConfig, events: events)

        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: RDKafkaClient.stringSize)
        defer { errorChars.deallocate() }

        guard let handle = rd_kafka_new(
            clientType,
            rdConfig,
            errorChars,
            RDKafkaClient.stringSize
        ) else {
            // rd_kafka_new only frees the rd_kafka_conf_t upon success
            rd_kafka_conf_destroy(rdConfig)

            let errorString = String(cString: errorChars)
            throw KafkaError.client(reason: errorString)
        }

        return RDKafkaClient(kafkaHandle: handle, logger: logger)
    }

    /// Produce a message to the Kafka cluster.
    ///
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    /// - Parameter newMessageID: ID that was assigned to the `message`.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter topicHandles: Topic handles that this client uses to produce new messages
    func produce<Key, Value>(
        message: KafkaProducerMessage<Key, Value>,
        newMessageID: UInt,
        topicConfig: KafkaTopicConfiguration,
        topicHandles: RDKafkaTopicHandles
    ) throws {
        let responseCode = try message.value.withUnsafeBytes { valueBuffer in
            try topicHandles.withTopicHandlePointer(topic: message.topic, topicConfig: topicConfig) { topicHandle in
                if let key = message.key {
                    // Key available, we can use scoped accessor to safely access its rawBufferPointer.
                    // Pass message over to librdkafka where it will be queued and sent to the Kafka Cluster.
                    // Returns 0 on success, error code otherwise.
                    return key.withUnsafeBytes { keyBuffer in
                        return rd_kafka_produce(
                            topicHandle,
                            message.partition.rawValue,
                            RD_KAFKA_MSG_F_COPY,
                            UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress),
                            valueBuffer.count,
                            keyBuffer.baseAddress,
                            keyBuffer.count,
                            UnsafeMutableRawPointer(bitPattern: newMessageID)
                        )
                    }
                } else {
                    // No key set.
                    // Pass message over to librdkafka where it will be queued and sent to the Kafka Cluster.
                    // Returns 0 on success, error code otherwise.
                    return rd_kafka_produce(
                        topicHandle,
                        message.partition.rawValue,
                        RD_KAFKA_MSG_F_COPY,
                        UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress),
                        valueBuffer.count,
                        nil,
                        0,
                        UnsafeMutableRawPointer(bitPattern: newMessageID)
                    )
                }
            }
        }

        guard responseCode == 0 else {
            throw KafkaError.rdKafkaError(wrapping: rd_kafka_last_error())
        }
    }

    /// Swift wrapper for events from `librdkafka`'s event queue.
    enum KafkaEvent {
        case deliveryReport(results: [KafkaDeliveryReport])
        case consumerMessages(result: Result<KafkaConsumerMessage, Error>)
        case statistics(KafkaStatistics)
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
                events.append(forwardEvent)
            case .fetch:
                if let forwardEvent = self.handleFetchEvent(event) {
                    events.append(forwardEvent)
                }
            case .log:
                self.handleLogEvent(event)
            case .offsetCommit:
                self.handleOffsetCommitEvent(event)
            case .statistics:
                events.append(self.handleStatistics(event))
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
    /// - Returns: `KafkaEvent` to be returned as part of ``RDKafkaClient.eventPoll()`.
    private func handleDeliveryReportEvent(_ event: OpaquePointer?) -> KafkaEvent {
        let deliveryReportCount = rd_kafka_event_message_count(event)
        var deliveryReportResults = [KafkaDeliveryReport]()
        deliveryReportResults.reserveCapacity(deliveryReportCount)

        while let messagePointer = rd_kafka_event_message_next(event) {
            guard let messageStatus = KafkaDeliveryReport(messagePointer: messagePointer) else {
                continue
            }
            deliveryReportResults.append(messageStatus)
        }

        // The returned message(s) MUST NOT be freed with rd_kafka_message_destroy().
        return .deliveryReport(results: deliveryReportResults)
    }

    /// Handle event of type `RDKafkaEvent.fetch`.
    ///
    /// - Parameter event: Pointer to underlying `rd_kafka_event_t`.
    /// - Returns: `KafkaEvent` to be returned as part of ``KafkaClient.eventPoll()`.
    private func handleFetchEvent(_ event: OpaquePointer?) -> KafkaEvent? {
        do {
            // RD_KAFKA_EVENT_FETCH only returns a single message:
            // https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#a3a855eb7bdf17f5797d4911362a5fc7c
            if let messagePointer = rd_kafka_event_message_next(event) {
                let message = try KafkaConsumerMessage(messagePointer: messagePointer)
                return .consumerMessages(result: .success(message))
            } else {
                return nil
            }
        } catch {
            return .consumerMessages(result: .failure(error))
        }
        // The returned message(s) MUST NOT be freed with rd_kafka_message_destroy().
    }

    private func handleStatistics(_ event: OpaquePointer?) -> KafkaEvent {
        let jsonStr = String(cString: rd_kafka_event_stats(event))
        return .statistics(KafkaStatistics(jsonString: jsonStr))
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

    /// Handle event of type `RDKafkaEvent.offsetCommit`.
    ///
    /// - Parameter event: Pointer to underlying `rd_kafka_event_t`.
    private func handleOffsetCommitEvent(_ event: OpaquePointer?) {
        guard let opaquePointer = rd_kafka_event_opaque(event) else {
            fatalError("Could not resolve reference to catpured Swift callback instance")
        }
        let opaque = Unmanaged<CapturedCommitCallback>.fromOpaque(opaquePointer).takeUnretainedValue()
        let actualCallback = opaque.closure

        let error = rd_kafka_event_error(event)
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            let kafkaError = KafkaError.rdKafkaError(wrapping: error)
            actualCallback(.failure(kafkaError))
            return
        }
        actualCallback(.success(()))
    }

    /// Redirect the main ``RDKafkaClient/poll(timeout:)`` queue to the `KafkaConsumer`'s
    /// queue (``RDKafkaClient/consumerPoll``).
    ///
    /// Events that would be triggered by ``RDKafkaClient/poll(timeout:)``
    /// are now triggered by ``RDKafkaClient/consumerPoll``.
    ///
    /// - Warning: It is not allowed to call ``RDKafkaClient/poll(timeout:)`` after ``RDKafkaClient/pollSetConsumer``.
    func pollSetConsumer() throws {
        let result = rd_kafka_poll_set_consumer(self.kafkaHandle)
        if result != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: result)
        }
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

    /// Store `message`'s offset for next auto-commit.
    ///
    /// - Important: `enable.auto.offset.store` must be set to `false` when using this API.
    func storeMessageOffset(_ message: KafkaConsumerMessage) throws {
        // The offset committed is always the offset of the next requested message.
        // Thus, we increase the offset of the current message by one before committing it.
        // See: https://github.com/edenhill/librdkafka/issues/2745#issuecomment-598067945
        let changesList = RDKafkaTopicPartitionList()
        changesList.setOffset(
            topic: message.topic,
            partition: message.partition,
            offset: Int64(message.offset + 1)
        )

        let error = changesList.withListPointer { listPointer in
            rd_kafka_offsets_store(
                self.kafkaHandle,
                listPointer
            )
        }

        if error != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: error)
        }
    }

    /// Non-blocking **awaitable** commit of a the `message`'s offset to Kafka.
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

            changesList.withListPointer { listPointer in
                rd_kafka_commit_queue(
                    self.kafkaHandle,
                    listPointer,
                    self.mainQueue,
                    nil,
                    opaquePointer
                )
            }
        }
    }

    /// Flush any outstanding produce requests.
    ///
    /// Parameters:
    ///     - timeoutMilliseconds: Maximum time to wait for outstanding messages to be flushed.
    func flush(timeoutMilliseconds: Int32) async throws {
        // rd_kafka_flush is blocking and there is no convenient way to make it non-blocking.
        // We therefore execute rd_kafka_flush on a DispatchQueue to ensure it gets executed
        // on a separate thread that is not part of Swift Concurrency's cooperative thread pool.
        let queue = DispatchQueue(label: "com.swift-server.swift-kafka.flush")
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            queue.async {
                let error = rd_kafka_flush(self.kafkaHandle, timeoutMilliseconds)
                if error != RD_KAFKA_RESP_ERR_NO_ERROR {
                    continuation.resume(throwing: KafkaError.rdKafkaError(wrapping: error))
                } else {
                    continuation.resume()
                }
            }
        }
    }

    /// Close the consumer asynchronously. This means revoking its assignemnt, committing offsets to broker and
    /// leaving the consumer group (if applicable).
    ///
    /// Make sure to run poll loop until ``RDKafkaClient/consumerIsClosed`` returns `true`.
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

    /// Scoped accessor that enables safe access to the pointer of the client's Kafka handle.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the Kafka handle pointer.
    @discardableResult
    func withKafkaHandlePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        return try body(self.kafkaHandle)
    }
}
