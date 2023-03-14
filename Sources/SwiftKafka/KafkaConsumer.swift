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
import struct Foundation.UUID
import Logging
import NIOCore

/// `NIOAsyncSequenceProducerDelegate` implementation handling backpressure for ``KafkaConsumer``.
private struct ConsumerMessagesAsyncSequenceDelegate: NIOAsyncSequenceProducerDelegate {
    let produceMoreClosure: @Sendable () -> Void
    let didTerminateClosure: @Sendable () -> Void

    func produceMore() {
        produceMoreClosure()
    }

    func didTerminate() {
        didTerminateClosure()
    }
}

/// `AsyncSequence` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
public struct ConsumerMessagesAsyncSequence: AsyncSequence {
    public typealias Element = Result<KafkaConsumerMessage, KafkaError>
    typealias HighLowWatermark = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark
    fileprivate let wrappedSequence: NIOAsyncSequenceProducer<Element, HighLowWatermark, ConsumerMessagesAsyncSequenceDelegate>

    /// `AsynceIteratorProtocol` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
    public struct ConsumerMessagesAsyncIterator: AsyncIteratorProtocol {
        fileprivate let wrappedIterator: NIOAsyncSequenceProducer<
            Element,
            HighLowWatermark,
            ConsumerMessagesAsyncSequenceDelegate
        >.AsyncIterator

        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    public func makeAsyncIterator() -> ConsumerMessagesAsyncIterator {
        return ConsumerMessagesAsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

/// Receive messages from the Kafka cluster.
public final class KafkaConsumer {
    /// The configuration object of the consumer client.
    private var config: KafkaConfig
    /// A logger.
    private let logger: Logger
    /// Used for handling the connection to the Kafka cluster.
    private let client: KafkaClient
    /// Pointer to a list of topics + partition pairs.
    private let subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>
    /// Variable to ensure that no operations are invoked on closed consumer.
    private var closed = false

    /// Serial queue used to run all blocking operations. Additionally ensures that no data races occur.
    private let serialQueue: DispatchQueue

    // We use implicitly unwrapped optionals here as these properties need to access self upon initialization
    /// Type of the values returned by the ``messages`` sequence.
    private typealias Element = Result<KafkaConsumerMessage, KafkaError>
    private var messagesSource: NIOAsyncSequenceProducer<
        Element,
        ConsumerMessagesAsyncSequence.HighLowWatermark,
        ConsumerMessagesAsyncSequenceDelegate
    >.Source!
    /// `AsyncSequence` that returns all ``KafkaConsumerMessage`` objects that the consumer receives.
    public private(set) var messages: ConsumerMessagesAsyncSequence!

    /// Initialize a new ``KafkaConsumer``.
    /// To listen to incoming messages, please subscribe to a list of topics using ``subscribe(topics:)``
    /// or assign the consumer to a particular topic + partition pair using ``assign(topic:partition:offset:)``.
    /// - Parameter config: The ``KafkaConfig`` for configuring the ``KafkaConsumer``.
    /// - Parameter logger: A logger.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    private init(
        config: KafkaConfig,
        logger: Logger
    ) throws {
        self.config = config
        self.logger = logger
        self.client = try KafkaClient(type: .consumer, config: self.config, logger: self.logger)

        self.subscribedTopicsPointer = rd_kafka_topic_partition_list_new(1)

        // Events that would be triggered by rd_kafka_poll
        // will now be also triggered by rd_kafka_consumer_poll
        let result = self.client.withKafkaHandlePointer { handle in
            rd_kafka_poll_set_consumer(handle)
        }
        guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.rdKafkaError(wrapping: result)
        }

        self.serialQueue = DispatchQueue(label: "swift-kafka-gsoc.consumer.serial")

        let backpressureStrategy = ConsumerMessagesAsyncSequence.HighLowWatermark(
            lowWatermark: 5,
            highWatermark: 10
        )

        let messagesSequenceDelegate = ConsumerMessagesAsyncSequenceDelegate { [weak self] in
            self?.produceMore()
        } didTerminateClosure: { [weak self] in
            self?.close()
        }
        let messagesSourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: Element.self,
            backPressureStrategy: backpressureStrategy,
            delegate: messagesSequenceDelegate
        )
        self.messagesSource = messagesSourceAndSequence.source
        self.messages = ConsumerMessagesAsyncSequence(
            wrappedSequence: messagesSourceAndSequence.sequence
        )
    }

    /// Initialize a new ``KafkaConsumer`` and subscribe to the given list of `topics` as part of
    /// the consumer group specified in `groupID`.
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Parameter groupID: Name of the consumer group that this ``KafkaConsumer`` will create / join.
    /// - Parameter config: The ``KafkaConfig`` for configuring the ``KafkaConsumer``.
    /// - Parameter logger: A logger.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    public convenience init(
        topics: [String],
        groupID: String,
        config: KafkaConfig,
        logger: Logger
    ) throws {
        var config = config
        if let configGroupID = config.value(forKey: "group.id") {
            if configGroupID != groupID {
                throw KafkaError.config(reason: "Group ID does not match with group ID found in the configuration")
            }
        } else {
            try config.set(groupID, forKey: "group.id")
        }

        try self.init(
            config: config,
            logger: logger
        )
        try self.subscribe(topics: topics)
    }

    /// Initialize a new ``KafkaConsumer`` and assign it to a specific `partition` of a `topic`.
    /// - Parameter topic: Name of the topic that this ``KafkaConsumer`` will read from.
    /// - Parameter partition: Partition that this ``KafkaConsumer`` will read from.
    /// - Parameter offset: The topic offset where reading begins. Defaults to the offset of the last read message.
    /// - Parameter config: The ``KafkaConfig`` for configuring the ``KafkaConsumer``.
    /// - Parameter logger: A logger.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    /// - Note: This consumer ignores the `group.id` property of its `config`.
    public convenience init(
        topic: String,
        partition: KafkaPartition,
        offset: Int64 = Int64(RD_KAFKA_OFFSET_END),
        config: KafkaConfig,
        logger: Logger
    ) throws {
        // Although an assignment is not related to a consumer group,
        // librdkafka requires us to set a `group.id`.
        // This is a known issue:
        // https://github.com/edenhill/librdkafka/issues/3261
        var config = config
        try config.set(UUID().uuidString, forKey: "group.id")

        try self.init(
            config: config,
            logger: logger
        )
        try self.assign(
            topic: topic,
            partition: partition,
            offset: offset
        )
    }

    // MARK: - Initialisers with new config

    public convenience init(
        topics: [String],
        config: ConsumerConfig = ConsumerConfig(),
        logger: Logger
    ) throws {
        try self.init(
            topics: topics,
            groupID: config.groupID,
            config: KafkaConfig(consumerConfig: config),
            logger: logger
        )
    }

    public convenience init(
        topic: String,
        partition: KafkaPartition,
        offset: Int64,
        config: ConsumerConfig = ConsumerConfig(),
        logger: Logger
    ) throws {
        try self.init(
            topic: topic,
            partition: partition,
            offset: offset,
            config: KafkaConfig(consumerConfig: config),
            logger: logger
        )
    }

    /// Subscribe to the given list of `topics`.
    /// The partition assignment happens automatically using `KafkaConsumer`'s consumer group.
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Throws: A ``KafkaError`` if subscribing to the topic list failed.
    private func subscribe(topics: [String]) throws {
        assert(!closed)

        for topic in topics {
            rd_kafka_topic_partition_list_add(
                self.subscribedTopicsPointer,
                topic,
                KafkaPartition.unassigned.rawValue
            )
        }

        let result = self.client.withKafkaHandlePointer { handle in
            rd_kafka_subscribe(handle, subscribedTopicsPointer)
        }

        guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.rdKafkaError(wrapping: result)
        }
    }

    /// Assign the``KafkaConsumer`` to a specific `partition` of a `topic`.
    /// - Parameter topic: Name of the topic that this ``KafkaConsumer`` will read from.
    /// - Parameter partition: Partition that this ``KafkaConsumer`` will read from.
    /// - Parameter offset: The topic offset where reading begins. Defaults to the offset of the last read message.
    /// - Throws: A ``KafkaError`` if the consumer could not be assigned to the topic + partition pair.
    private func assign(
        topic: String,
        partition: KafkaPartition,
        offset: Int64
    ) throws {
        assert(!closed)

        guard let partitionPointer = rd_kafka_topic_partition_list_add(
            self.subscribedTopicsPointer,
            topic,
            partition.rawValue
        ) else {
            fatalError("rd_kafka_topic_partition_list_add returned invalid pointer")
        }

        partitionPointer.pointee.offset = offset

        let result = self.client.withKafkaHandlePointer { handle in
            rd_kafka_assign(handle, self.subscribedTopicsPointer)
        }

        guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.rdKafkaError(wrapping: result)
        }
    }

    /// Receive new messages and forward the result to the ``messages`` `AsyncSequence`.
    func produceMore() {
        self.serialQueue.async {
            guard !self.closed else {
                return
            }

            let messageResult: Element
            do {
                guard let message = try self.poll() else {
                    self.produceMore()
                    return
                }
                messageResult = .success(message)
            } catch let kafkaError as KafkaError {
                messageResult = .failure(kafkaError)
            } catch {
                self.logger.error("KafkaConsumer caught error: \(error)")
                return
            }

            let yieldresult = self.messagesSource.yield(messageResult)
            switch yieldresult {
            case .produceMore:
                self.produceMore()
            case .dropped, .stopProducing:
                return
            }
        }
    }

    /// Request a new message from the Kafka cluster.
    /// This method blocks for a maximum of `timeout` milliseconds.
    /// - Parameter timeout: Maximum amount of milliseconds this method waits for a new message.
    /// - Returns: A ``KafkaConsumerMessage`` or `nil` if there are no new messages.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
    private func poll(timeout: Int32 = 100) throws -> KafkaConsumerMessage? {
        dispatchPrecondition(condition: .onQueue(self.serialQueue))
        assert(!closed)

        guard let messagePointer = self.client.withKafkaHandlePointer({ handle in
            rd_kafka_consumer_poll(handle, timeout)
        }) else {
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

        do {
            let message = try KafkaConsumerMessage(messagePointer: messagePointer)
            return message
        } catch {
            throw error
        }
    }

    /// Mark `message` in the topic as read and request the next message from the topic.
    /// This method is only used for manual offset management.
    /// - Parameter message: Last received message that shall be marked as read.
    /// - Throws: A ``KafkaError`` if committing failed.
    /// - Warning: This method fails if the `enable.auto.commit` configuration property is set to `true`.
    public func commitSync(_ message: KafkaConsumerMessage) async throws {
        try await self.serializeWithThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in

            do {
                try self._commitSync(message)
                continuation.resume()
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }

    private func _commitSync(_ message: KafkaConsumerMessage) throws {
        dispatchPrecondition(condition: .onQueue(self.serialQueue))
        guard !self.closed else {
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        }

        guard self.config.value(forKey: "enable.auto.commit") == "false" else {
            throw KafkaError.config(reason: "Committing manually only works if enable.auto.commit is set to false")
        }

        let changesList = rd_kafka_topic_partition_list_new(1)
        defer { rd_kafka_topic_partition_list_destroy(changesList) }
        guard let partitionPointer = rd_kafka_topic_partition_list_add(
            changesList,
            message.topic,
            message.partition.rawValue
        ) else {
            fatalError("rd_kafka_topic_partition_list_add returned invalid pointer")
        }

        // The offset committed is always the offset of the next requested message.
        // Thus, we increase the offset of the current message by one before committing it.
        // See: https://github.com/edenhill/librdkafka/issues/2745#issuecomment-598067945
        partitionPointer.pointee.offset = message.offset + 1
        let result = self.client.withKafkaHandlePointer { handle in
            rd_kafka_commit(
                handle,
                changesList,
                0
            )
        }
        guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.rdKafkaError(wrapping: result)
        }
        return
    }

    /// Stop consuming messages. This step is irreversible.
    func close() {
        self.serialQueue.async {
            guard !self.closed else {
                return
            }

            let result = self.client.withKafkaHandlePointer { handle in
                rd_kafka_consumer_close(handle)
            }

            rd_kafka_topic_partition_list_destroy(self.subscribedTopicsPointer)

            guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
                let error = KafkaError.rdKafkaError(wrapping: result)
                self.logger.error("Closing KafkaConsumer failed: \(error.description)")
                return
            }

            self.closed = true
        }
    }

    /// Helper function that enqueues a task with a checked throwing continuation into the ``KafkaConsumer``'s serial queue.
    private func serializeWithThrowingContinuation<T>(_ body: @escaping (CheckedContinuation<T, Error>) -> Void) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            self.serialQueue.async {
                body(continuation)
                // Note: we do not support cancellation yet
                // https://github.com/swift-server/swift-kafka-gsoc/issues/33
            }
        }
    }
}
