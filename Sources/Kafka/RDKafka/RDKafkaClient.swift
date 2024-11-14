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

import class Foundation.JSONDecoder

/// Base class for ``KafkaProducer`` and ``KafkaConsumer``,
/// which is used to handle the connection to the Kafka ecosystem.
@_spi(Internal)
public final class RDKafkaClient: Sendable {
    // Default size for Strings returned from C API
    static let stringSize = 1024

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
        logger: Logger
    ) throws -> RDKafkaClient {
        let rdConfig = try RDKafkaConfig.createFrom(configDictionary: configDictionary)
        // Manually override some of the configuration options
        // Handle logs in event queue
        try RDKafkaConfig.set(configPointer: rdConfig, key: "log.queue", value: "true")
        RDKafkaConfig.setEvents(configPointer: rdConfig, events: events)

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
    /// - Parameter topicConfiguration: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter topicHandles: Topic handles that this client uses to produce new messages
    func produce<Key, Value>(
        message: KafkaProducerMessage<Key, Value>,
        newMessageID: UInt,
        topicConfiguration: KafkaTopicConfiguration,
        topicHandles: RDKafkaTopicHandles
    ) throws {
        precondition(
            0...Int(Int32.max) ~= message.partition.rawValue || message.partition == .unassigned,
            "Partition ID outside of valid range \(0...Int32.max)"
        )

        // Pass message over to librdkafka where it will be queued and sent to the Kafka Cluster.
        // Returns 0 on success, error code otherwise.
        let error = try topicHandles.withTopicHandlePointer(
            topic: message.topic,
            topicConfiguration: topicConfiguration
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

    /// Swift wrapper for events from `librdkafka`'s event queue.
    enum KafkaEvent {
        case deliveryReport(results: [KafkaDeliveryReport])
        case statistics(RDKafkaStatistics)
    }

    /// Poll the event `rd_kafka_queue_t` for new events.
    ///
    /// - Parameter maxEvents:Maximum number of events to serve in one invocation.
    func eventPoll(maxEvents: Int = 100) -> [KafkaEvent] {
        var events = [KafkaEvent]()
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
                let forwardEvent = self.handleDeliveryReportEvent(event)
                events.append(forwardEvent)
            case .log:
                self.handleLogEvent(event)
            case .offsetCommit:
                self.handleOffsetCommitEvent(event)
            case .statistics:
                if let forwardEvent = self.handleStatistics(event) {
                    events.append(forwardEvent)
                }
            case .none:
                // Finished reading events, return early
                return events
            default:
                break  // Ignored Event
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

    /// Handle event of type `RDKafkaEvent.statistics`.
    ///
    /// - Parameter event: Pointer to underlying `rd_kafka_event_t`.
    private func handleStatistics(_ event: OpaquePointer?) -> KafkaEvent? {
        let jsonStr = String(cString: rd_kafka_event_stats(event))
        do {
            if let jsonData = jsonStr.data(using: .utf8) {
                let json = try JSONDecoder().decode(RDKafkaStatistics.self, from: jsonData)
                return .statistics(json)
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

    /// Request a new message from the Kafka cluster.
    ///
    /// - Important: This method should only be invoked from ``KafkaConsumer``.
    ///
    /// - Returns: A ``KafkaConsumerMessage`` or `nil` if there are no new messages.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
    func consumerPoll() throws -> KafkaConsumerMessage? {
        guard let messagePointer = rd_kafka_consumer_poll(self.kafkaHandle.pointer, 0) else {
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
        let queue = DispatchQueue(label: "com.swift-server.swift-kafka.flush")
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            queue.async {
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

    /// Scoped accessor that enables safe access to the pointer of the client's Kafka handle.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the Kafka handle pointer.
    @discardableResult
    func withKafkaHandlePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        try body(self.kafkaHandle.pointer)
    }
}
