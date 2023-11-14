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

/// Base class for ``KafkaProducer`` and ``KafkaConsumer``,
/// which is used to handle the connection to the Kafka ecosystem.
final class RDKafkaClient: Sendable {
    // Default size for Strings returned from C API
    static let stringSize = 1024

    /// Determines if client is a producer or a consumer.
    enum ClientType {
        case producer
        case consumer
    }

    /// Handle for the C library's Kafka instance.
    private let kafkaHandle: OpaquePointer
    /// A logger.
    private let logger: Logger

    /// `librdkafka`'s `rd_kafka_queue_t` that events are received on.
    private let queue: OpaquePointer
    
    private let rebalanceCallBackStorage: RebalanceCallbackStorage?

    /// Queue for blocking calls outside of cooperative thread pool
    private var gcdQueue: DispatchQueue {
        // global concurrent queue
        .global(qos: .default) // FIXME: maybe DispatchQueue(label: "com.swift.kafka.queue")
    }

    // Use factory method to initialize
    private init(
        type: ClientType,
        kafkaHandle: OpaquePointer,
        logger: Logger,
        rebalanceCallBackStorage: RebalanceCallbackStorage? = nil
    ) {
        self.kafkaHandle = kafkaHandle
        self.logger = logger
        self.queue = rd_kafka_queue_get_main(self.kafkaHandle)
        self.rebalanceCallBackStorage = rebalanceCallBackStorage

        rd_kafka_set_log_queue(self.kafkaHandle, self.queue)
    }

    deinit {
        // Loose reference to librdkafka's event queue
        rd_kafka_queue_destroy(self.queue)
        rd_kafka_destroy(kafkaHandle)
    }

    typealias RebalanceCallback = @Sendable (KafkaEvent) -> ()
    final class RebalanceCallbackStorage: Sendable {
        let rebalanceCallback: RebalanceCallback
        
        init(rebalanceCallback: @escaping RebalanceCallback) {
            self.rebalanceCallback = rebalanceCallback
        }
    }
    
    /// Factory method creating a new instance of a ``RDKafkaClient``.
    static func makeClient(
        type: ClientType,
        configDictionary: [String: String],
        events: [RDKafkaEvent],
        logger: Logger,
        rebalanceCallBackStorage: RebalanceCallbackStorage? = nil
    ) throws -> RDKafkaClient {
        let rdConfig = try RDKafkaConfig.createFrom(configDictionary: configDictionary)
        // Manually override some of the configuration options
        // Handle logs in event queue
        try RDKafkaConfig.set(configPointer: rdConfig, key: "log.queue", value: "true")
        // KafkaConsumer is manually storing read offsets
        if type == .consumer {
            try RDKafkaConfig.set(configPointer: rdConfig, key: "enable.auto.offset.store", value: "false")
            try RDKafkaConfig.set(configPointer: rdConfig, key: "enable.partition.eof", value: "true")
            if let rebalanceCallBackStorage {
                let rebalanceCb = Unmanaged.passUnretained(rebalanceCallBackStorage)
                rd_kafka_conf_set_opaque(rdConfig, rebalanceCb.toOpaque())
                rd_kafka_conf_set_rebalance_cb(rdConfig) { handle, code, partitions, rebalanceOpaqueCb in
                    let protoStringDef = String(cString: rd_kafka_rebalance_protocol(handle))
                    let rebalanceProtocol = KafkaRebalanceProtocol.convert(from: protoStringDef)
                    guard let partitions else {
                        fatalError("No partitions in callback")
                    }
                    let list = KafkaTopicList(from: .init(from: partitions))
                    var event: KafkaEvent
                    switch code {
                    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                        event = .rebalance(.assign(rebalanceProtocol, list))
                    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                        event =  .rebalance(.revoke(rebalanceProtocol, list))
                    default:
                        event = .rebalance(.error(rebalanceProtocol, list, KafkaError.rdKafkaError(wrapping: code)))
                    }
                    if let rebalanceOpaqueCb {
                        let rebalanceCb = Unmanaged<RebalanceCallbackStorage>.fromOpaque(rebalanceOpaqueCb).takeUnretainedValue()
                        rebalanceCb.rebalanceCallback(event)
                    } else {
                        fatalError("Cannot find rebalance cb")
                    }
                }
            }
        }
        RDKafkaConfig.setEvents(configPointer: rdConfig, events: events)

        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: RDKafkaClient.stringSize)
        defer { errorChars.deallocate() }

        let clientType = type == .producer ? RD_KAFKA_PRODUCER : RD_KAFKA_CONSUMER
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

        return RDKafkaClient(type: type, kafkaHandle: handle, logger: logger, rebalanceCallBackStorage: rebalanceCallBackStorage)
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
            return try Self.withMessageKeyAndValueBuffer(for: message) { keyBuffer, valueBuffer in
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
            throw KafkaError.rdKafkaError(wrapping: rd_kafka_last_error())
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
            self.kafkaHandle,
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
        _ body: (UnsafeRawBufferPointer?, UnsafeRawBufferPointer) throws -> T // (keyBuffer, valueBuffer)
    ) rethrows -> T {
        return try message.value.withUnsafeBytes { valueBuffer in
            if let key = message.key {
                return try key.withUnsafeBytes { keyBuffer in
                    return try body(keyBuffer, valueBuffer)
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
        case statistics(KafkaStatistics)
        case rebalance(RebalanceAction)
    }

    /// Poll the event `rd_kafka_queue_t` for new events.
    ///
    /// - Parameter maxEvents:Maximum number of events to serve in one invocation.
    func eventPoll(events: inout [KafkaEvent], maxEvents: Int = 100) -> Bool /* should sleep */ {
//        var events = [KafkaEvent]()
        events.removeAll(keepingCapacity: true)
        events.reserveCapacity(maxEvents)
        
        var shouldSleep = true

        for _ in 0..<maxEvents {
            let event = rd_kafka_queue_poll(self.queue, 0)
            defer { rd_kafka_event_destroy(event) }

            let rdEventType = rd_kafka_event_type(event)
            guard let eventType = RDKafkaEvent(rawValue: rdEventType) else {
                fatalError("Unsupported event type: \(rdEventType)")
            }

            switch eventType {
            case .deliveryReport:
                let forwardEvent = self.handleDeliveryReportEvent(event)
                events.append(forwardEvent)
                shouldSleep = false
            case .log:
                self.handleLogEvent(event)
            case .offsetCommit:
                self.handleOffsetCommitEvent(event)
                shouldSleep = false
            case .statistics:
                events.append(self.handleStatistics(event))
            case .rebalance:
                self.logger.info("rebalance received (RDClient)")
                events.append(self.handleRebalance(event))
//            case .error:
//                #if true
//                let err = rd_kafka_event_error(event)
//                if err == RD_KAFKA_RESP_ERR__PARTITION_EOF {
//                    let topicPartition = rd_kafka_event_topic_partition(event)
//                    if let topicPartition {
//                        events.append(
//                            .consumerMessages(
//                                result: .success(
//                                    .init(topicPartitionPointer: topicPartition)
//                                )
//                            )
//                        )
//                    }
////                    if let forwardEvent = self.handleFetchEvent(event) {
////                        events.append(forwardEvent)
////                    }
////                    events.append(.consumerMessages(result: .failure(KafkaError.partitionEOF())))
//                }
//                #endif
//                break
            case .none:
                // Finished reading events, return early
                return shouldSleep
            default:
                break // Ignored Event
            }
        }

        return shouldSleep
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

    private func handleStatistics(_ event: OpaquePointer?) -> KafkaEvent {
        let jsonStr = String(cString: rd_kafka_event_stats(event))
        return .statistics(KafkaStatistics(jsonString: jsonStr))
    }

    private func handleRebalance(_ event: OpaquePointer?) -> KafkaEvent {
        guard let partitions = rd_kafka_event_topic_partition_list(event) else {
            fatalError("Must never happen") // TODO: remove
        }
        
        
        let code = rd_kafka_event_error(event)
        
        let protoStringDef = String(cString: rd_kafka_rebalance_protocol(kafkaHandle))
        let rebalanceProtocol = KafkaRebalanceProtocol.convert(from: protoStringDef)
        let list = KafkaTopicList(from: .init(from: partitions))
        switch code {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            return .rebalance(.assign(rebalanceProtocol, list))
        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            return .rebalance(.revoke(rebalanceProtocol, list))
        default:
            return .rebalance(.error(rebalanceProtocol, list, KafkaError.rdKafkaError(wrapping: code)))
        }
    }

    /// Handle event of type `RDKafkaEvent.log`.
    ///
    /// - Parameter event: Pointer to underlying `rd_kafka_event_t`.
    private func handleLogEvent(_ event: OpaquePointer?) {
        var faculty: UnsafePointer<CChar>?
        var buffer: UnsafePointer<CChar>?
        var level: Int32 = 0
        if rd_kafka_event_log(event, &faculty, &buffer, &level) == 0 {
//            rd_kafka_event_debug_contexts
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
        
        /*
         let opaquePointer = rd_kafka_event_opaque(event)
         guard let opaquePointer else {
             let count = rd_kafka_event_message_count(event)
             let str = String(cString: rd_kafka_event_name(event))
             fatalError("Could not resolve reference to catpured Swift callback instance for count \(count) in event \(str)")
         }
         */
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
            return try KafkaConsumerMessage(messagePointer: messagePointer)
        }

        let message = try KafkaConsumerMessage(messagePointer: messagePointer)
        return message
    }
    /// Atomic  incremental assignment of partitions to consume.
    /// - Parameter topicPartitionList: Pointer to a list of topics + partition pairs.
    func incrementalAssign(topicPartitionList: RDKafkaTopicPartitionList) async throws {
        let error = await performBlockingCall(queue: self.gcdQueue) {
            topicPartitionList.withListPointer { rd_kafka_incremental_assign(self.kafkaHandle, $0) }
        }

        defer { rd_kafka_error_destroy(error) }
        let code = rd_kafka_error_code(error)
        if code != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: code)
        }
    }
    
    /// Atomic incremental unassignment of partitions to consume.
    /// - Parameter topicPartitionList: Pointer to a list of topics + partition pairs.
    func incrementalUnassign(topicPartitionList: RDKafkaTopicPartitionList) async throws {
        let error = await performBlockingCall(queue: self.gcdQueue) {
            topicPartitionList.withListPointer { rd_kafka_incremental_unassign(self.kafkaHandle, $0) }
        }

        defer { rd_kafka_error_destroy(error) }
        let code = rd_kafka_error_code(error)
        if code != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: code)
        }
    }
    
    /// Seek for partitions to consume.
    /// - Parameter topicPartitionList: Pointer to a list of topics + partition pairs.
    func seek(topicPartitionList: RDKafkaTopicPartitionList, timeout: Duration) async throws {
        assert(timeout >= .zero, "Timeout must be positive")

        let doSeek = {
            topicPartitionList.withListPointer { rd_kafka_seek_partitions(self.kafkaHandle, $0, Int32(max(timeout, .zero).inMilliseconds)) }
        }
        let error =
            timeout == .zero
            ? doSeek() // async when timeout is zero
            : await performBlockingCall(queue: gcdQueue, body: doSeek)
        
        defer { rd_kafka_error_destroy(error) }
        let code = rd_kafka_error_code(error)
        if code != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: code)
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
    
    // TODO: remove?
    func doOrThrow(_ body: () -> rd_kafka_resp_err_t, isFatal: Bool = false, file: String = #fileID, line: UInt = #line) throws {
        let result = body()
        if result != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: result, isFatal: isFatal, file: file, line: line)
        }
    }

    /// Atomic assignment of partitions to consume.
    /// - Parameter topicPartitionList: Pointer to a list of topics + partition pairs.
    func assign(topicPartitionList: RDKafkaTopicPartitionList?) async throws {
        let result = await performBlockingCall(queue: self.gcdQueue) {
            if let topicPartitionList {
                return topicPartitionList.withListPointer { pointer in
                    rd_kafka_assign(self.kafkaHandle, pointer)
                }
            }
            return rd_kafka_assign(self.kafkaHandle, nil)
        }
        if result != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.rdKafkaError(wrapping: result)
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
            offset: message.eof ? message.offset : .init(rawValue: message.offset.rawValue + 1)
        )

        let error = changesList.withListPointer { listPointer in
            rd_kafka_offsets_store(
                self.kafkaHandle,
                listPointer
            )
        }

        if error != RD_KAFKA_RESP_ERR_NO_ERROR {
            // Ignore RD_KAFKA_RESP_ERR__STATE error.
            // RD_KAFKA_RESP_ERR__STATE indicates an attempt to commit to an unassigned partition,
            // which can occur during rebalancing or when the consumer is shutting down.
            // See "Upgrade considerations" for more details: https://github.com/confluentinc/librdkafka/releases/tag/v1.9.0
            // Since Kafka Consumers are designed for at-least-once processing, failing to commit here is acceptable.
            if error == RD_KAFKA_RESP_ERR__STATE {
                return
            }
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
            offset: .init(rawValue: message.offset.rawValue + 1)
        )

        let error = changesList.withListPointer { listPointer in
            return rd_kafka_commit(
                self.kafkaHandle,
                listPointer,
                1 // async = true
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
                offset: .init(rawValue: message.offset.rawValue + 1)
            )

            // Unretained pass because the reference that librdkafka holds to capturedClosure
            // should not be counted in ARC as this can lead to memory leaks.
            let opaquePointer: UnsafeMutableRawPointer? = Unmanaged.passUnretained(capturedClosure).toOpaque()

            changesList.withListPointer { listPointer in
                rd_kafka_commit_queue(
                    self.kafkaHandle,
                    listPointer,
                    self.queue,
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
        let result = rd_kafka_consumer_close_queue(self.kafkaHandle, self.queue)
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

    /// Scoped accessor that enables safe access to the pointer of the client's Kafka handle with async closure.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the Kafka handle pointer.
    @discardableResult
    func withKafkaHandlePointer<T>(_ body: (OpaquePointer) async throws -> T) async rethrows -> T {
        return try await body(self.kafkaHandle)
    }

    func initTransactions(timeout: Duration) async throws {
        let result = await performBlockingCall(queue: gcdQueue) {
            rd_kafka_init_transactions(self.kafkaHandle, timeout.totalMilliseconds)
        }

        if result != nil {
            let code = rd_kafka_error_code(result)
            rd_kafka_error_destroy(result)
            throw KafkaError.rdKafkaError(wrapping: code)
        }
    }

    func beginTransaction() throws {
        let result = rd_kafka_begin_transaction(kafkaHandle)
        if result != nil {
            let code = rd_kafka_error_code(result)
            rd_kafka_error_destroy(result)
            throw KafkaError.rdKafkaError(wrapping: code)
        }
    }

    func send(
        attempts: UInt64,
        offsets: RDKafkaTopicPartitionList,
        forConsumerKafkaHandle consumer: OpaquePointer,
        timeout: Duration
    ) async throws {
        try await offsets.withListPointer { topicPartitionList in

            let consumerMetadata = rd_kafka_consumer_group_metadata(consumer)
            defer { rd_kafka_consumer_group_metadata_destroy(consumerMetadata) }

            // TODO: actually it should be withing some timeout (like transaction timeout or session timeout)
            for idx in 0..<attempts {
                let error = await performBlockingCall(queue: gcdQueue) {
                    rd_kafka_send_offsets_to_transaction(self.kafkaHandle, topicPartitionList,
                                                         consumerMetadata, timeout.totalMillisecondsOrMinusOne)
                }

                /* check if offset commit is completed successfully  */
                if error == nil {
                    return
                }
                defer { rd_kafka_error_destroy(error) }

                /* check if offset commit is retriable */
                if rd_kafka_error_is_retriable(error) == 1 {
                    continue
                }

                /* check if transaction need to be aborted */
                if rd_kafka_error_txn_requires_abort(error) == 1 {
                    do {
                        try await self.abortTransaction(attempts: attempts - idx, timeout: timeout)
                    } catch {
                        throw KafkaError.transactionIncomplete(
                            reason: "Could not complete or abort transaction with error \(error)")
                    }
                    throw KafkaError.transactionAborted(reason: "Transaction aborted and can be started from scratch")
                }
                let isFatal = (rd_kafka_error_is_fatal(error) == 1) // fatal when Producer/Consumer must be restarted
                throw KafkaError.rdKafkaError(wrapping: rd_kafka_error_code(error), isFatal: isFatal)
            }
            throw KafkaError.transactionOutOfAttempts(numOfAttempts: attempts)
        }
    }

    func abortTransaction(attempts: UInt64, timeout: Duration) async throws {
        for _ in 0..<attempts {
            let error = await performBlockingCall(queue: gcdQueue) {
                rd_kafka_abort_transaction(self.kafkaHandle, timeout.totalMillisecondsOrMinusOne)
            }
            /* check if transaction abort is completed successfully  */
            if error == nil {
                return
            }
            defer { rd_kafka_error_destroy(error) }

            if rd_kafka_error_code(error) == RD_KAFKA_RESP_ERR__STATE { // No active transaction
                return
            }

            /* check if transaction abort is retriable */
            if rd_kafka_error_is_retriable(error) == 1 {
                continue
            }
            let isFatal = (rd_kafka_error_is_fatal(error) == 1) // fatal when Producer/Consumer must be restarted
            throw KafkaError.rdKafkaError(wrapping: rd_kafka_error_code(error), isFatal: isFatal)
        }
        throw KafkaError.transactionOutOfAttempts(numOfAttempts: attempts)
    }

    func commitTransaction(attempts: UInt64, timeout: Duration) async throws {
        for idx in 0..<attempts {
            let error = await performBlockingCall(queue: gcdQueue) {
                rd_kafka_commit_transaction(self.kafkaHandle, timeout.totalMillisecondsOrMinusOne)
            }
            /* check if transaction is completed successfully  */
            if error == nil {
                return
            }
            /* check if transaction is retriable */
            if rd_kafka_error_is_retriable(error) == 1 {
                continue
            }
            defer { rd_kafka_error_destroy(error) }

            /* check if transaction need to be aborted */
            if rd_kafka_error_txn_requires_abort(error) == 1 {
                do {
                    try await self.abortTransaction(attempts: attempts - idx, timeout: timeout)
                } catch {
                    throw KafkaError.transactionIncomplete(
                        reason: "Could not complete or abort transaction with error \(error)")
                }
                throw KafkaError.transactionAborted(reason: "Transaction aborted and can be started from scratch")
            }
            /* check if error is fatal */
            let isFatal = (rd_kafka_error_is_fatal(error) == 1) // fatal when Producer/Consumer must be restarted
            throw KafkaError.rdKafkaError(wrapping: rd_kafka_error_code(error), isFatal: isFatal)
        }
        throw KafkaError.transactionOutOfAttempts(numOfAttempts: attempts)
    }

    func inSync() {
        self.withKafkaHandlePointer {

            /**
             * @brief Returns the current partition assignment as set by rd_kafka_assign()
             *        or rd_kafka_incremental_assign().
             *
             * @returns An error code on failure, otherwise \p partitions is updated
             *          to point to a newly allocated partition list (possibly empty).
             *
             * @remark The application is responsible for calling
             *         rd_kafka_topic_partition_list_destroy on the returned list.
             *
             * @remark This assignment represents the partitions assigned through the
             *         assign functions and not the partitions assigned to this consumer
             *         instance by the consumer group leader.
             *         They are usually the same following a rebalance but not necessarily
             *         since an application is free to assign any partitions.
             */
//            RD_EXPORT rd_kafka_resp_err_t
//            rd_kafka_assignment(rd_kafka_t *rk,
//                                rd_kafka_topic_partition_list_t **partitions);
            var partitions: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?
            _ = rd_kafka_assignment($0, &partitions)
//            if err == nil {
//
//            }
            defer {
                rd_kafka_topic_partition_list_destroy(partitions)
            }
            rd_kafka_position($0, partitions)
            
            guard let partitions else {
                fatalError("TODO")
            }
            
            var str = String()
            for idx in 0..<Int(partitions.pointee.cnt) {
                let elem = partitions.pointee.elems[idx]
                str += "topic: \(String(describing: elem.topic)), offset: \(elem.offset), partition: \(elem.partition), \(String(describing: elem.metadata))"
            }
        }
    }
}

extension Duration {
    // Internal usage only: librdkafka accepts Int32 as timeouts
    internal var totalMilliseconds: Int32 {
        return Int32(self.components.seconds * 1000 + self.components.attoseconds / 1_000_000_000_000_000)
    }

    internal var totalMillisecondsOrMinusOne: Int32 {
        return max(self.totalMilliseconds, -1)
    }

    public static var kafkaUntilEndOfTransactionTimeout: Duration = .milliseconds(-1)
    public static var kafkaNoWaitTransaction: Duration = .zero
}
