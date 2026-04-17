//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka
import Dispatch
import Logging
import NIOCore

import class Foundation.JSONDecoder

/// Base class for ``KafkaProducer`` and ``KafkaConsumer``,
/// which is used to handle the connection to the Kafka ecosystem.
@_spi(Internal)
public final class RDKafkaClient: Sendable {
    // Default size for Strings returned from C API
    static let stringSize = 1024

    /// Shared concurrent queue for offloading blocking C calls (e.g., `rd_kafka_committed`,
    /// `rd_kafka_seek_partitions`, `rd_kafka_flush`) so they don't block Swift Concurrency's
    /// cooperative thread pool. Concurrent so independent operations can run in parallel.
    private static let blockingQueue = DispatchQueue(
        label: "com.swift-server.swift-kafka.blocking",
        attributes: .concurrent
    )

    /// Determines if client is a producer or a consumer.
    enum ClientType {
        case producer
        case consumer
    }

    /// Handle for the C library's Kafka instance.
    private let kafkaHandle: SendableOpaquePointer
    /// A logger.
    private let logger: Logger

    /// `librdkafka`'s `rd_kafka_queue_t` that events are received on.
    private let queueHandle: SendableOpaquePointer

    // Use factory method to initialize
    private init(
        type: ClientType,
        kafkaHandle: SendableOpaquePointer,
        logger: Logger
    ) {
        self.kafkaHandle = kafkaHandle
        self.logger = logger
        self.queueHandle = .init(rd_kafka_queue_get_main(self.kafkaHandle.pointer))

        rd_kafka_set_log_queue(self.kafkaHandle.pointer, self.queueHandle.pointer)
    }

    deinit {
        // Loose reference to librdkafka's event queue
        rd_kafka_queue_destroy(self.queueHandle.pointer)
        rd_kafka_destroy(kafkaHandle.pointer)
    }

    /// Factory method creating a new instance of a ``RDKafkaClient``.
    static func makeClient(
        type: ClientType,
        configDictionary: [String: String],
        events: [RDKafkaEvent],
        logger: Logger,
        rebalanceContext: RebalanceContext? = nil
    ) throws -> RDKafkaClient {
        let rdConfig = try RDKafkaConfig.createFrom(configDictionary: configDictionary)
        // Manually override some of the configuration options
        // Handle logs in event queue
        try RDKafkaConfig.set(configPointer: rdConfig, key: "log.queue", value: "true")
        RDKafkaConfig.setEvents(configPointer: rdConfig, events: events)

        // Register rebalance callback for consumer clients.
        // This handles rebalance events synchronously inside rd_kafka_consumer_poll(),
        // which is the correct queue for these events (consumer group queue, not main queue).
        if let rebalanceContext {
            RDKafkaConfig.setRebalanceCallback(
                configPointer: rdConfig,
                context: rebalanceContext
            )
        }

        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: RDKafkaClient.stringSize)
        defer { errorChars.deallocate() }

        let clientType = type == .producer ? RD_KAFKA_PRODUCER : RD_KAFKA_CONSUMER
        guard
            let handle = rd_kafka_new(
                clientType,
                rdConfig,
                errorChars,
                RDKafkaClient.stringSize
            )
        else {
            // rd_kafka_new only frees the rd_kafka_conf_t upon success
            rd_kafka_conf_destroy(rdConfig)

            let errorString = String(cString: errorChars)
            throw KafkaError.client(reason: errorString)
        }

        let kafkaHandle = SendableOpaquePointer(handle)
        return RDKafkaClient(type: type, kafkaHandle: kafkaHandle, logger: logger)
    }

    /// Produce a message to the Kafka cluster.
    ///
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    /// - Parameter newMessageID: ID that was assigned to the `message`.
    /// - Parameter topicHandles: Topic handles that this client uses to produce new messages
    func produce<Key, Value>(
        message: KafkaProducerMessage<Key, Value>,
        newMessageID: UInt,
        topicHandles: RDKafkaTopicHandles
    ) throws {
        precondition(
            0...Int(Int32.max) ~= message.partition.rawValue || message.partition == .unassigned,
            "Partition ID outside of valid range \(0...Int32.max)"
        )

        // Pass message over to librdkafka where it will be queued and sent to the Kafka Cluster.
        // Returns 0 on success, error code otherwise.
        let error = try topicHandles.withTopicHandlePointer(
            topic: message.topic
        ) { topicHandle in
            try Self.withMessageKeyAndValueBuffer(for: message) { keyBuffer, valueBuffer in
                if message.headers.isEmpty {
                    // No message headers set, normal produce method can be used.
                    rd_kafka_produce(
                        topicHandle,
                        Int32(message.partition.rawValue),
                        RD_KAFKA_MSG_F_COPY,
                        UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress),
                        valueBuffer.count,
                        keyBuffer?.baseAddress,
                        keyBuffer?.count ?? 0,
                        UnsafeMutableRawPointer(bitPattern: newMessageID)
                    )
                    return rd_kafka_last_error()
                } else {
                    let errorPointer = try Self.withKafkaCHeaders(for: message.headers) { cHeaders in
                        // Setting message headers only works with `rd_kafka_produceva` (variadic arguments).
                        try self._produceVariadic(
                            topicHandle: topicHandle,
                            partition: Int32(message.partition.rawValue),
                            messageFlags: RD_KAFKA_MSG_F_COPY,
                            key: keyBuffer,
                            value: valueBuffer,
                            opaque: UnsafeMutableRawPointer(bitPattern: newMessageID),
                            cHeaders: cHeaders
                        )
                    }
                    return rd_kafka_error_code(errorPointer)
                }
            }
        }

        if error != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: error)
        }
    }

    /// Wrapper for `rd_kafka_produceva`.
    /// (Message production with variadic options, required for sending message headers).
    ///
    /// This function should only be called from within a scoped pointer accessor
    /// to ensure the referenced memory is valid for the function's lifetime.
    ///
    /// - Returns: `nil` on success. An opaque pointer `*rd_kafka_resp_err_t` on error.
    private func _produceVariadic(
        topicHandle: OpaquePointer,
        partition: Int32,
        messageFlags: Int32,
        key: UnsafeRawBufferPointer?,
        value: UnsafeRawBufferPointer,
        opaque: UnsafeMutableRawPointer?,
        cHeaders: [(key: UnsafePointer<CChar>, value: UnsafeRawBufferPointer?)]
    ) throws -> OpaquePointer? {
        let sizeWithoutHeaders = (key != nil) ? 6 : 5
        let size = sizeWithoutHeaders + cHeaders.count
        var arguments = Array(repeating: rd_kafka_vu_t(), count: size)
        var index = 0

        arguments[index].vtype = RD_KAFKA_VTYPE_RKT
        arguments[index].u.rkt = topicHandle
        index += 1

        arguments[index].vtype = RD_KAFKA_VTYPE_PARTITION
        arguments[index].u.i32 = partition
        index += 1

        arguments[index].vtype = RD_KAFKA_VTYPE_MSGFLAGS
        arguments[index].u.i = messageFlags
        index += 1

        if let key {
            arguments[index].vtype = RD_KAFKA_VTYPE_KEY
            arguments[index].u.mem.ptr = UnsafeMutableRawPointer(mutating: key.baseAddress)
            arguments[index].u.mem.size = key.count
            index += 1
        }

        arguments[index].vtype = RD_KAFKA_VTYPE_VALUE
        arguments[index].u.mem.ptr = UnsafeMutableRawPointer(mutating: value.baseAddress)
        arguments[index].u.mem.size = value.count
        index += 1

        arguments[index].vtype = RD_KAFKA_VTYPE_OPAQUE
        arguments[index].u.ptr = opaque
        index += 1

        for cHeader in cHeaders {
            arguments[index].vtype = RD_KAFKA_VTYPE_HEADER

            arguments[index].u.header.name = cHeader.key
            arguments[index].u.header.val = cHeader.value?.baseAddress
            arguments[index].u.header.size = cHeader.value?.count ?? 0

            index += 1
        }

        assert(arguments.count == size)

        return rd_kafka_produceva(
            self.kafkaHandle.pointer,
            arguments,
            arguments.count
        )
    }

    /// Scoped accessor that enables safe access to a ``KafkaProducerMessage``'s key and value raw buffers.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the pointer.
    @discardableResult
    private static func withMessageKeyAndValueBuffer<T, Key, Value>(
        for message: KafkaProducerMessage<Key, Value>,
        _ body: (UnsafeRawBufferPointer?, UnsafeRawBufferPointer) throws -> T  // (keyBuffer, valueBuffer)
    ) rethrows -> T {
        try message.value.withUnsafeBytes { valueBuffer in
            if let key = message.key {
                return try key.withUnsafeBytes { keyBuffer in
                    try body(keyBuffer, valueBuffer)
                }
            } else {
                return try body(nil, valueBuffer)
            }
        }
    }

    /// Scoped accessor that enables safe access the underlying memory of an array of ``KafkaHeader``s.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the pointer.
    @discardableResult
    private static func withKafkaCHeaders<T>(
        for headers: [KafkaHeader],
        _ body: ([(key: UnsafePointer<CChar>, value: UnsafeRawBufferPointer?)]) throws -> T
    ) rethrows -> T {
        var headersMemory: [(key: UnsafePointer<CChar>, value: UnsafeRawBufferPointer?)] = []
        var headers: [KafkaHeader] = headers.reversed()
        return try self._withKafkaCHeadersRecursive(kafkaHeaders: &headers, cHeaders: &headersMemory, body)
    }

    /// Recursive helper function that enables safe access the underlying memory of an array of ``KafkaHeader``s.
    /// Reads through all `kafkaHeaders` and stores their corresponding pointers in `cHeaders`.
    private static func _withKafkaCHeadersRecursive<T>(
        kafkaHeaders: inout [KafkaHeader],
        cHeaders: inout [(key: UnsafePointer<CChar>, value: UnsafeRawBufferPointer?)],
        _ body: ([(key: UnsafePointer<CChar>, value: UnsafeRawBufferPointer?)]) throws -> T
    ) rethrows -> T {
        guard let kafkaHeader = kafkaHeaders.popLast() else {
            // Base case: we have read all kafkaHeaders and now invoke the accessor closure
            // that can safely access the pointers in cHeaders
            return try body(cHeaders)
        }

        // Access underlying memory of key and value with scoped accessor and to a
        // recursive call to _withKafkaCHeadersRecursive in the scoped accessor.
        // This allows us to build a chain of scoped accessors so that the body closure
        // can ultimately access all kafkaHeader underlying key/value bytes safely.
        return try kafkaHeader.key.withCString { keyCString in
            if let headerValue = kafkaHeader.value {
                return try headerValue.withUnsafeReadableBytes { valueBuffer in
                    let cHeader: (UnsafePointer<CChar>, UnsafeRawBufferPointer?) = (keyCString, valueBuffer)
                    cHeaders.append(cHeader)
                    return try self._withKafkaCHeadersRecursive(
                        kafkaHeaders: &kafkaHeaders,
                        cHeaders: &cHeaders,
                        body
                    )
                }
            } else {
                let cHeader: (UnsafePointer<CChar>, UnsafeRawBufferPointer?) = (keyCString, nil)
                cHeaders.append(cHeader)
                return try self._withKafkaCHeadersRecursive(
                    kafkaHeaders: &kafkaHeaders,
                    cHeaders: &cHeaders,
                    body
                )
            }
        }
    }

    /// Typed event returned by ``producerEventPoll(maxEvents:)``.
    /// Contains only events relevant to a Kafka producer.
    enum ProducerPollEvent {
        case deliveryReport(results: [KafkaDeliveryReport])
        case statistics(RDKafkaStatistics)
        case error(KafkaError)
    }

    /// Typed event returned by ``consumerEventPoll(maxEvents:)``.
    /// Contains only events relevant to a Kafka consumer.
    enum ConsumerPollEvent {
        case statistics(RDKafkaStatistics)
        case error(KafkaError)
    }

    /// Poll for producer-relevant events from the `librdkafka` event queue.
    ///
    /// Drains the queue, handling internal events (log, offset commit) and
    /// returning only events relevant to a producer.
    ///
    /// - Parameter maxEvents: Maximum number of events to serve in one invocation.
    func producerEventPoll(maxEvents: Int = 100) -> [ProducerPollEvent] {
        var events = [ProducerPollEvent]()
        events.reserveCapacity(maxEvents)

        for _ in 0..<maxEvents {
            let event = rd_kafka_queue_poll(self.queueHandle.pointer, 0)
            defer { rd_kafka_event_destroy(event) }

            let rdEventType = rd_kafka_event_type(event)
            guard let eventType = RDKafkaEvent(rawValue: rdEventType) else {
                fatalError("Unsupported event type: \(rdEventType)")
            }

            switch eventType {
            case .deliveryReport:
                let result = self.handleDeliveryReportEvent(event)
                events.append(.deliveryReport(results: result))
            case .statistics:
                if let statistics = self.handleStatistics(event) {
                    events.append(.statistics(statistics))
                }
            case .error:
                if let error = self.handleErrorEvent(event) {
                    events.append(.error(error))
                }
            case .log:
                self.handleLogEvent(event)
            case .offsetCommit:
                self.handleOffsetCommitEvent(event)
            case .none:
                return events
            default:
                break
            }
        }

        return events
    }

    /// Poll for consumer-relevant events from the `librdkafka` event queue.
    ///
    /// Drains the queue, handling internal events (log, offset commit) and
    /// returning only events relevant to a consumer. Producer-only events
    /// (e.g., delivery reports) are handled internally but never surfaced.
    ///
    /// - Parameter maxEvents: Maximum number of events to serve in one invocation.
    func consumerEventPoll(maxEvents: Int = 100) -> [ConsumerPollEvent] {
        var events = [ConsumerPollEvent]()
        events.reserveCapacity(maxEvents)

        for _ in 0..<maxEvents {
            let event = rd_kafka_queue_poll(self.queueHandle.pointer, 0)
            defer { rd_kafka_event_destroy(event) }

            let rdEventType = rd_kafka_event_type(event)
            guard let eventType = RDKafkaEvent(rawValue: rdEventType) else {
                fatalError("Unsupported event type: \(rdEventType)")
            }

            switch eventType {
            case .statistics:
                if let statistics = self.handleStatistics(event) {
                    events.append(.statistics(statistics))
                }
            case .error:
                if let error = self.handleErrorEvent(event) {
                    events.append(.error(error))
                }
            case .log:
                self.handleLogEvent(event)
            case .offsetCommit:
                self.handleOffsetCommitEvent(event)
            case .deliveryReport:
                // Should never occur on a consumer queue, but handle defensively.
                _ = self.handleDeliveryReportEvent(event)
            case .rebalance:
                // Rebalance events arrive here when polled from the main queue
                // (e.g., during shutdown after rd_kafka_consumer_close_queue forwards
                // the consumer queue to the main queue). Dispatch through the
                // rebalance callback so assign/unassign is performed and the cgrp
                // subsystem is acknowledged — otherwise the close process hangs.
                let err = rd_kafka_event_error(event)
                let tpl = rd_kafka_event_topic_partition_list(event)
                let opaque = rd_kafka_opaque(self.kafkaHandle.pointer)
                if let opaque {
                    let context = Unmanaged<RebalanceContext>.fromOpaque(opaque).takeUnretainedValue()
                    context.handleRebalance(
                        kafkaHandle: self.kafkaHandle.pointer,
                        error: err,
                        partitions: tpl
                    )
                }
            case .none:
                return events
            default:
                break
            }
        }

        return events
    }

    /// Handle event of type `RDKafkaEvent.deliveryReport`.
    ///
    /// - Parameter event: Pointer to underlying `rd_kafka_event_t`.
    /// - Returns: Delivery report results parsed from the event.
    private func handleDeliveryReportEvent(_ event: OpaquePointer?) -> [KafkaDeliveryReport] {
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
        return deliveryReportResults
    }

    /// Handle event of type `RDKafkaEvent.error`.
    ///
    /// - Parameter event: Pointer to underlying `rd_kafka_event_t`.
    /// - Returns: A ``KafkaError`` if the event carries a non-zero error code, or `nil` otherwise.
    private func handleErrorEvent(_ event: OpaquePointer?) -> KafkaError? {
        let code = rd_kafka_event_error(event)
        guard code != RD_KAFKA_RESP_ERR_NO_ERROR else {
            return nil
        }
        let reasonCString = rd_kafka_event_error_string(event)
        let reason =
            reasonCString.flatMap { String(cString: $0) }
            ?? String(cString: rd_kafka_err2str(code))
        return KafkaError.rdKafkaError(wrapping: code, reason: reason)
    }

    /// Handle event of type `RDKafkaEvent.statistics`.
    ///
    /// - Parameter event: Pointer to underlying `rd_kafka_event_t`.
    private func handleStatistics(_ event: OpaquePointer?) -> RDKafkaStatistics? {
        let jsonStr = String(cString: rd_kafka_event_stats(event))
        do {
            if let jsonData = jsonStr.data(using: .utf8) {
                let json = try JSONDecoder().decode(RDKafkaStatistics.self, from: jsonData)
                return json
            }
        } catch {
            assertionFailure("Error occurred when decoding JSON statistics: \(error) when decoding \(jsonStr)")
        }
        return nil
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
                case 0...2:  // Emergency, Alert, Critical
                    self.logger.critical(
                        Logger.Message(stringLiteral: String(cString: buffer)),
                        source: String(cString: faculty)
                    )
                case 3:  // Error
                    self.logger.error(
                        Logger.Message(stringLiteral: String(cString: buffer)),
                        source: String(cString: faculty)
                    )
                case 4:  // Warning
                    self.logger.warning(
                        Logger.Message(stringLiteral: String(cString: buffer)),
                        source: String(cString: faculty)
                    )
                case 5:  // Notice
                    self.logger.notice(
                        Logger.Message(stringLiteral: String(cString: buffer)),
                        source: String(cString: faculty)
                    )
                case 6:  // Informational
                    self.logger.info(
                        Logger.Message(stringLiteral: String(cString: buffer)),
                        source: String(cString: faculty)
                    )
                default:  // Debug
                    self.logger.debug(
                        Logger.Message(stringLiteral: String(cString: buffer)),
                        source: String(cString: faculty)
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
            // No opaque pointer means this is an auto-commit event (e.g. triggered by
            // rd_kafka_consumer_close_queue during shutdown) rather than a user-initiated
            // rd_kafka_commit_queue call. These don't have a callback to invoke.
            let error = rd_kafka_event_error(event)
            if error != RD_KAFKA_RESP_ERR_NO_ERROR {
                self.logger.info(
                    "Auto-commit failed during event processing",
                    metadata: ["error": "\(String(cString: rd_kafka_err2str(error)))"]
                )
            }
            return
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

    /// Request a new message from the Kafka cluster.
    ///
    /// - Important: This method should only be invoked from ``KafkaConsumer``.
    ///
    /// - Returns: A ``KafkaConsumerMessage`` or `nil` if there are no new messages.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
    func consumerPoll(for pollTimeoutMs: Int32 = 0) throws -> KafkaConsumerMessage? {
        guard let messagePointer = rd_kafka_consumer_poll(self.kafkaHandle.pointer, pollTimeoutMs) else {
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
            let result = rd_kafka_subscribe(self.kafkaHandle.pointer, pointer)
            if result != RD_KAFKA_RESP_ERR_NO_ERROR {
                throw KafkaError.rdKafkaError(wrapping: result)
            }
        }
    }

    /// Atomic assignment of partitions to consume.
    /// - Parameter topicPartitionList: Pointer to a list of topics + partition pairs.
    func assign(topicPartitionList: RDKafkaTopicPartitionList) throws {
        try topicPartitionList.withListPointer { pointer in
            let result = rd_kafka_assign(self.kafkaHandle.pointer, pointer)
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

    /// Store the offset of a consumed message in librdkafka's local offset store.
    ///
    /// This does **not** commit the offset to the broker. Instead, it marks the offset
    /// in the local in-memory store. When `enable.auto.commit` is `true` (the default),
    /// the auto-commit timer will periodically commit these stored offsets to the broker.
    ///
    /// This method is intended to be used with `enable.auto.offset.store` set to `false`,
    /// allowing the application to store offsets only **after** successful message processing.
    /// This is the recommended pattern for at-least-once delivery semantics.
    ///
    /// - Parameter message: The message whose offset should be stored.
    /// - Throws: A ``KafkaError`` if storing the offset failed.
    func storeOffset(_ message: KafkaConsumerMessage) throws {
        // rd_kafka_offsets_store expects the offset of the *next* message to consume,
        // which is the current message's offset + 1.
        // See: https://github.com/edenhill/librdkafka/issues/2745#issuecomment-598067945
        let changesList = RDKafkaTopicPartitionList()
        changesList.setOffset(
            topic: message.topic,
            partition: message.partition,
            offset: Int64(message.offset.rawValue + 1)
        )

        let error = changesList.withListPointer { listPointer in
            rd_kafka_offsets_store(
                self.kafkaHandle.pointer,
                listPointer
            )
        }

        if error != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: error)
        }
    }

    /// Non-blocking "fire-and-forget" commit of a `message`'s offset to Kafka.
    /// Schedules a commit and returns immediately.
    /// Any errors encountered after scheduling the commit will be discarded.
    ///
    /// - Parameter message: Last received message that shall be marked as read.
    /// - Throws: A ``KafkaError`` if scheduling the commit failed.
    func scheduleCommit(_ message: KafkaConsumerMessage) throws {
        // The offset committed is always the offset of the next requested message.
        // Thus, we increase the offset of the current message by one before committing it.
        // See: https://github.com/edenhill/librdkafka/issues/2745#issuecomment-598067945
        let changesList = RDKafkaTopicPartitionList()
        changesList.setOffset(
            topic: message.topic,
            partition: message.partition,
            offset: Int64(message.offset.rawValue + 1)
        )

        let error = changesList.withListPointer { listPointer in
            rd_kafka_commit(
                self.kafkaHandle.pointer,
                listPointer,
                1  // async = true
            )
        }

        if error != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: error)
        }
    }

    /// Non-blocking **awaitable** commit of a `message`'s offset to Kafka.
    ///
    /// - Parameter message: Last received message that shall be marked as read.
    /// - Throws: A ``KafkaError`` if the commit failed.
    func commit(_ message: KafkaConsumerMessage) async throws {
        // Declare captured closure outside of withCheckedContinuation.
        // We do that because do an unretained pass of the captured closure to
        // librdkafka which means we have to keep a reference to the closure
        // ourselves to make sure it does not get deallocated before
        // commit returns.
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
                offset: Int64(message.offset.rawValue + 1)
            )

            // Unretained pass because the reference that librdkafka holds to capturedClosure
            // should not be counted in ARC as this can lead to memory leaks.
            let opaquePointer: UnsafeMutableRawPointer? = Unmanaged.passUnretained(capturedClosure).toOpaque()

            changesList.withListPointer { listPointer in
                rd_kafka_commit_queue(
                    self.kafkaHandle.pointer,
                    listPointer,
                    self.queueHandle.pointer,
                    nil,
                    opaquePointer
                )
            }
        }
    }

    /// Flush any outstanding produce requests.
    ///
    /// - Parameters:
    ///     - timeoutMilliseconds: Maximum time to wait for outstanding messages to be flushed.
    func flush(timeoutMilliseconds: Int32) async throws {
        // rd_kafka_flush is blocking and there is no convenient way to make it non-blocking.
        // We therefore execute rd_kafka_flush on a DispatchQueue to ensure it gets executed
        // on a separate thread that is not part of Swift Concurrency's cooperative thread pool.
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            Self.blockingQueue.async {
                let error = rd_kafka_flush(self.kafkaHandle.pointer, timeoutMilliseconds)
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
        let result = rd_kafka_consumer_close_queue(self.kafkaHandle.pointer, self.queueHandle.pointer)
        let kafkaError = rd_kafka_error_code(result)
        if kafkaError != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: kafkaError)
        }
    }

    /// Returns `true` if the underlying `librdkafka` consumer is closed.
    var isConsumerClosed: Bool {
        rd_kafka_consumer_closed(self.kafkaHandle.pointer) == 1
    }

    /// Retrieve the last-committed offsets for the given topic+partition pairs from the broker.
    ///
    /// - Parameters:
    ///   - topicPartitions: An array of ``KafkaTopicPartition`` to query.
    ///   - timeoutMilliseconds: Maximum time to wait for broker response.
    /// - Returns: An array of ``KafkaTopicPartitionOffset``. Offset is `nil` if no committed offset exists.
    /// - Throws: A ``KafkaError`` if the query failed.
    func committed(
        topicPartitions: [KafkaTopicPartition],
        timeoutMilliseconds: Int32
    ) async throws -> [KafkaTopicPartitionOffset] {
        let tpl = RDKafkaTopicPartitionList(size: Int32(topicPartitions.count))
        for tp in topicPartitions {
            tpl.add(topic: tp.topic, partition: tp.partition)
        }

        // rd_kafka_committed is blocking — offload to a DispatchQueue so it does not
        // block Swift Concurrency's cooperative thread pool.
        let kafkaHandle = self.kafkaHandle
        return try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<[KafkaTopicPartitionOffset], Error>) in
            Self.blockingQueue.async {
                let error = tpl.withListPointer { listPointer in
                    rd_kafka_committed(
                        kafkaHandle.pointer,
                        listPointer,
                        timeoutMilliseconds
                    )
                }

                if error != RD_KAFKA_RESP_ERR_NO_ERROR {
                    continuation.resume(throwing: KafkaError.rdKafkaError(wrapping: error))
                } else {
                    let results = tpl.withListPointer { listPointer in
                        Self.extractOffsetsFromList(listPointer)
                    }
                    continuation.resume(returning: results)
                }
            }
        }
    }

    /// Retrieve the current positions (next offset to fetch) for the given topic+partition pairs.
    ///
    /// - Parameter topicPartitions: An array of ``KafkaTopicPartition`` to query.
    /// - Returns: An array of ``KafkaTopicPartitionOffset``. Offset is `nil` if no position is available.
    /// - Throws: A ``KafkaError`` if the query failed.
    func position(
        topicPartitions: [KafkaTopicPartition]
    ) throws -> [KafkaTopicPartitionOffset] {
        let tpl = RDKafkaTopicPartitionList(size: Int32(topicPartitions.count))
        for tp in topicPartitions {
            tpl.add(topic: tp.topic, partition: tp.partition)
        }

        let error = tpl.withListPointer { listPointer in
            rd_kafka_position(
                self.kafkaHandle.pointer,
                listPointer
            )
        }

        if error != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: error)
        }

        return tpl.withListPointer { listPointer in
            Self.extractOffsetsFromList(listPointer)
        }
    }

    /// Extract offset information from a `rd_kafka_topic_partition_list_t`.
    private static func extractOffsetsFromList(
        _ listPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>
    ) -> [KafkaTopicPartitionOffset] {
        var results: [KafkaTopicPartitionOffset] = []
        let count = Int(listPointer.pointee.cnt)
        results.reserveCapacity(count)

        for i in 0..<count {
            let element = listPointer.pointee.elems[i]
            let topic = String(cString: element.topic)
            let partition = KafkaPartition(rawValue: Int(element.partition))
            // RD_KAFKA_OFFSET_INVALID (-1001) means no offset is stored
            let offset: KafkaOffset? =
                element.offset == Int64(RD_KAFKA_OFFSET_INVALID)
                ? nil
                : KafkaOffset(rawValue: Int(element.offset))
            results.append(KafkaTopicPartitionOffset(topic: topic, partition: partition, offset: offset))
        }

        return results
    }

    /// Check if the current partition assignment has been lost involuntarily.
    ///
    /// This is only useful when reacting to a rebalance event or from within
    /// a rebalance callback. Partitions that have been lost may already be
    /// owned by other members in the group and therefore committing offsets
    /// may fail.
    ///
    /// Calling `rd_kafka_assign`, `rd_kafka_incremental_assign`, or
    /// `rd_kafka_incremental_unassign` resets this flag.
    ///
    /// - Returns: `true` if the current partition assignment is considered lost, `false` otherwise.
    var isAssignmentLost: Bool {
        rd_kafka_assignment_lost(self.kafkaHandle.pointer) == 1
    }

    /// Seek consumer for partitions to the per-partition offset.
    ///
    /// The offset may be either absolute (>= 0) or a logical offset
    /// (e.g., `.beginning`, `.end`, `.storedOffset`).
    ///
    /// This call will purge all pre-fetched messages for the given partitions.
    /// Repeated use of seek may lead to increased network usage as messages
    /// are re-fetched from the broker.
    ///
    /// - Important: Seek must only be performed for already assigned/consumed partitions.
    ///
    /// - Parameters:
    ///   - topicPartitionOffsets: An array of ``KafkaTopicPartitionOffset`` to seek to.
    ///   - timeoutMilliseconds: Maximum time to wait. Pass `0` for async (fire-and-forget).
    /// - Throws: A ``KafkaError`` if the seek operation failed.
    func seekPartitions(
        topicPartitionOffsets: [KafkaTopicPartitionOffset],
        timeoutMilliseconds: Int32
    ) async throws {
        let tpl = RDKafkaTopicPartitionList(size: Int32(topicPartitionOffsets.count))
        for tpo in topicPartitionOffsets {
            guard let offset = tpo.offset else {
                throw KafkaError.config(reason: "Seek requires a non-nil offset for \(tpo.topic):\(tpo.partition)")
            }
            tpl.setOffset(
                topic: tpo.topic,
                partition: tpo.partition,
                offset: Int64(offset.rawValue)
            )
        }

        // rd_kafka_seek_partitions is blocking — offload to a DispatchQueue so it does not
        // block Swift Concurrency's cooperative thread pool.
        let kafkaHandle = self.kafkaHandle
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            Self.blockingQueue.async {
                let error = tpl.withListPointer { listPointer in
                    rd_kafka_seek_partitions(
                        kafkaHandle.pointer,
                        listPointer,
                        timeoutMilliseconds
                    )
                }

                if let error {
                    let code = rd_kafka_error_code(error)
                    rd_kafka_error_destroy(error)
                    if code != RD_KAFKA_RESP_ERR_NO_ERROR {
                        continuation.resume(throwing: KafkaError.rdKafkaError(wrapping: code))
                    } else {
                        continuation.resume()
                    }
                } else {
                    continuation.resume()
                }
            }
        }
    }

    /// Scoped accessor that enables safe access to the pointer of the client's Kafka handle.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the Kafka handle pointer.
    @discardableResult
    func withKafkaHandlePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        try body(self.kafkaHandle.pointer)
    }

    /// Extract ``KafkaHeader``s from a `rd_kafka_message_t` pointer.
    ///
    /// - Parameters:
    ///    - for: Pointer to the `rd_kafka_message_t` object to extract the headers from.
    internal static func getHeaders(
        for messagePointer: UnsafePointer<rd_kafka_message_t>
    ) throws -> [KafkaHeader] {
        var result: [KafkaHeader] = []
        var headers: OpaquePointer?

        var readStatus = rd_kafka_message_headers(messagePointer, &headers)

        if readStatus == RD_KAFKA_RESP_ERR__NOENT {
            // No Header Entries
            return result
        }

        guard readStatus == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.rdKafkaError(wrapping: readStatus)
        }

        guard let headers else {
            return result
        }

        let headerCount = rd_kafka_header_cnt(headers)
        result.reserveCapacity(headerCount)

        var headerIndex = 0

        while readStatus != RD_KAFKA_RESP_ERR__NOENT && headerIndex < headerCount {
            var headerKeyPointer: UnsafePointer<CChar>?
            var headerValuePointer: UnsafeRawPointer?
            var headerValueSize = 0

            readStatus = rd_kafka_header_get_all(
                headers,
                headerIndex,
                &headerKeyPointer,
                &headerValuePointer,
                &headerValueSize
            )

            if readStatus == RD_KAFKA_RESP_ERR__NOENT {
                // No Header Entries
                return result
            }

            guard readStatus == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: readStatus)
            }

            guard let headerKeyPointer else {
                fatalError("Found null pointer when reading KafkaConsumerMessage header key")
            }
            let headerKey = String(cString: headerKeyPointer)

            var headerValue: ByteBuffer?
            if let headerValuePointer, headerValueSize > 0 {
                let headerValueBufferPointer = UnsafeRawBufferPointer(
                    start: headerValuePointer,
                    count: headerValueSize
                )
                headerValue = ByteBuffer(bytes: headerValueBufferPointer)
            }

            let newHeader = KafkaHeader(key: headerKey, value: headerValue)
            result.append(newHeader)

            headerIndex += 1
        }

        return result
    }
}
