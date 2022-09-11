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
import struct Foundation.UUID // TODO: can we avoid this Foundation import??
import Logging
import NIOCore

/// `NIOAsyncSequenceProducerDelegate` implementation handling backpressure for ``KafkaConsumer``.
actor ConsumerMessagesAsyncSequenceDelegate: NIOAsyncSequenceProducerDelegate {
    weak var consumer: KafkaConsumer?

    // TODO: is it ok to block here -> we can make poll non-async and get rid of the task here
    nonisolated func produceMore() {
        Task {
            await self.consumer?.produceMore()
        }
    }

    nonisolated func didTerminate() {
        return
    }

    func setConsumer(_ consumer: KafkaConsumer) {
        self.consumer = consumer
    }
}

/// `AsyncSequence` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
public struct ConsumerMessagesAsyncSequence: AsyncSequence {
    public typealias Element = Result<KafkaConsumerMessage, Error> // TODO: replace with something like KafkaConsumerError
    typealias HighLowWatermark = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark
    let wrappedSequence: NIOAsyncSequenceProducer<Element, HighLowWatermark, ConsumerMessagesAsyncSequenceDelegate>

    /// `AsynceIteratorProtocol` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
    public struct ConsumerMessagesAsyncIterator: AsyncIteratorProtocol {
        let wrappedIterator: NIOAsyncSequenceProducer<
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
public actor KafkaConsumer {
    /// State of the ``KafkaConsumer``.
    private var state: State

    /// States that the ``KafkaConsumer`` can have.
    private enum State {
        /// The ``KafkaConsumer`` has started and is ready to use.
        case started
        /// The ``KafkaConsumer`` has been closed and does not receive any more messages.
        case closed
    }

    // TODO: do we want to allow users to subscribe / assign to more topics during runtime?
    // TODO: function that returns all partitions for topic -> use rd_kafka_metadata, dedicated MetaData class
    /// The configuration object of the consumer client.
    private var config: KafkaConfig
    /// A logger.
    private let logger: Logger
    /// Used for handling the connection to the Kafka cluster.
    private let client: KafkaClient
    /// Pointer to a list of topics + partition pairs.
    private let subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>

    /// Type of the values returned by the ``messages`` sequence.
    private typealias Element = Result<KafkaConsumerMessage, Error> // TODO: replace with a more specific Error type
    private let messagesSource: NIOAsyncSequenceProducer<
        Element,
        ConsumerMessagesAsyncSequence.HighLowWatermark,
        ConsumerMessagesAsyncSequenceDelegate
    >.Source
    /// `AsyncSequence` that returns all ``KafkaConsumerMessage`` objects that the consumer receives.
    public nonisolated let messages: ConsumerMessagesAsyncSequence

    /// Initialize a new ``KafkaConsumer``.
    /// To listen to incoming messages, please subscribe to a list of topics using ``subscribe(topics:)``
    /// or assign the consumer to a particular topic + partition pair using ``assign(topic:partition:offset:)``.
    /// - Parameter config: The ``KafkaConfig`` for configuring the ``KafkaConsumer``.
    /// - Parameter logger: A logger.
    private init(
        config: KafkaConfig = KafkaConfig(),
        logger: Logger
    ) async throws {
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
            throw KafkaError(rawValue: result.rawValue)
        }

        let backpressureStrategy = ConsumerMessagesAsyncSequence.HighLowWatermark(
            lowWatermark: 5,
            highWatermark: 10
        )

        self.state = .started

        let messagesSequenceDelegate = ConsumerMessagesAsyncSequenceDelegate()
        let messagesSourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            of: Element.self,
            backPressureStrategy: backpressureStrategy,
            delegate: messagesSequenceDelegate
        )
        self.messagesSource = messagesSourceAndSequence.source
        self.messages = ConsumerMessagesAsyncSequence(
            wrappedSequence: messagesSourceAndSequence.sequence
        )

        await messagesSequenceDelegate.setConsumer(self)
    }

    /// Initialize a new ``KafkaConsumer`` and subscribe to the given list of `topics` as part of
    /// the consumer group specified in `groupID`.
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Parameter groupID: Name of the consumer group that this ``KafkaConsumer`` will create / join.
    /// - Parameter config: The ``KafkaConfig`` for configuring the ``KafkaConsumer``.
    /// - Parameter logger: A logger.
    public convenience init(
        topics: [String],
        groupID: String,
        config: KafkaConfig = KafkaConfig(),
        logger: Logger
    ) async throws {
        // TODO: make prettier
        // TODO: use inout parameter here?
        var config = config
        if let configGroupID = config.value(forKey: "group.id") {
            if configGroupID != groupID {
                throw KafkaError(description: "Group ID does not match with group ID found in the configuration")
            }
        } else {
            try config.set(groupID, forKey: "group.id")
        }

        try await self.init(
            config: config,
            logger: logger
        )
        try await self.subscribe(topics: topics)
    }

    /// Initialize a new ``KafkaConsumer`` and assign it to a specific `partition` of a `topic`.
    /// - Parameter topic: Name of the topic that this ``KafkaConsumer`` will read from.
    /// - Parameter partition: Partition that this ``KafkaConsumer`` will read from.
    /// - Parameter config: The ``KafkaConfig`` for configuring the ``KafkaConsumer``.
    /// - Parameter logger: A logger.
    /// - Note: This consumer ignores the `group.id` property of its `config`.
    public convenience init(
        topic: String,
        partition: KafkaPartition,
        config: KafkaConfig = KafkaConfig(),
        logger: Logger
    ) async throws {
        // Althogh an assignment is not related to a consumer group,
        // librdkafka requires us to set a `group.id`.
        // This is a known issue:
        // https://github.com/edenhill/librdkafka/issues/3261
        var config = config
        try config.set(UUID().uuidString, forKey: "group.id")

        try await self.init(
            config: config,
            logger: logger
        )
        try await self.assign(topic: topic, partition: partition)
    }

    deinit {
        switch self.state {
        case .started:
            Task {
                try await self._close()
            }
        case .closed:
            return
        }
    }

    /// Subscribe to the given list of `topics`.
    /// The partition assignment happens automatically using `KafkaConsumer`'s consumer group.
    /// - Parameter topics: An array of topic names to subscribe to.
    private func subscribe(topics: [String]) throws {
        // TODO: is this state needed for a method that is only invoked upon init?
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }

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

        guard result.rawValue == 0 else {
            throw KafkaError(rawValue: result.rawValue)
        }
    }

    /// Assign the``KafkaConsumer`` to a specific `partition` of a `topic`.
    /// - Parameter topic: Name of the topic that this ``KafkaConsumer`` will read from.
    /// - Parameter partition: Partition that this ``KafkaConsumer`` will read from.
    /// - Parameter offset: The topic offset where reading begins. Defaults to the offset of the last read message.
    private func assign(
        topic: String,
        partition: KafkaPartition,
        offset: Int64 = Int64(RD_KAFKA_OFFSET_END)
    ) throws {
        // TODO: is this state needed for a method that is only invoked upon init?
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }

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
            throw KafkaError(rawValue: result.rawValue)
        }
    }

    /// Receive new messages and forward the result to the ``messages`` `AsyncSequence`.
    func produceMore() async {
        let messageresult: Element
        do {
            guard let message = try await self.poll() else {
                return
            }
            messageresult = .success(message)
        } catch {
            messageresult = .failure(error)
        }

        let yieldresult = self.messagesSource.yield(messageresult)
        switch yieldresult {
        case .produceMore:
            await self.produceMore()
        case .dropped, .stopProducing:
            return
        }
    }

    /// Request a new message from the Kafka cluster.
    /// This method blocks for a maximum of `timeout` milliseconds.
    /// - Parameter timeout: Maximum amount of milliseconds this method waits for a new message.
    /// - Returns: A ``KafkaConsumerMessage`` or `nil` if there are no new messages.
    func poll(timeout: Int32 = 100) async throws -> KafkaConsumerMessage? {
        // TODO: clock API
        // TODO: ideally: make private
        // TODO: is this state needed here?
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }

        return try await withCheckedThrowingContinuation { continuation in
            guard let messagePointer = self.client.withKafkaHandlePointer({ handle in
                rd_kafka_consumer_poll(handle, timeout)
            }) else {
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

    /// Mark `message` in the topic as read and request the next message from the topic.
    /// This method is only used for manual offset management.
    /// - Parameter message: Last received message that shall be marked as read.
    /// - Warning: This method fails if the `enable.auto.commit` configuration property is set to `true`.
    public func commitSync(_ message: KafkaConsumerMessage) async throws {
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }
        try await self._commitSync(message)
    }

    // TODO: commit multiple messages at once -> different topics + test
    // TODO: docc: https://github.com/segmentio/kafka-go#explicit-commits note about highest offset
    private func _commitSync(_ message: KafkaConsumerMessage) async throws {
        guard self.config.value(forKey: "enable.auto.commit") == "false" else {
            throw KafkaError(description: "Committing manually only works if enable.auto.commit is set to false")
        }

        return try await withCheckedThrowingContinuation { continuation in
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
                continuation.resume(throwing: KafkaError(rawValue: result.rawValue))
                return
            }
            continuation.resume()
        }
    }

    /// Stop consuming messages. This step is irreversible.
    public func close() async throws {
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }
        try await self._close()
    }

    private func _close() async throws {
        return try await withCheckedThrowingContinuation { continuation in
            let result = self.client.withKafkaHandlePointer { handle in
                rd_kafka_consumer_close(handle)
            }

            rd_kafka_topic_partition_list_destroy(subscribedTopicsPointer)
            self.state = .closed

            guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
                continuation.resume(throwing: KafkaError(rawValue: result.rawValue))
                return
            }
            continuation.resume()
        }
    }
}
