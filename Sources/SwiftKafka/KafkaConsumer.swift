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
import NIOConcurrencyHelpers
import NIOCore

// MARK: - NoBackPressure

/// `NIOAsyncSequenceProducerBackPressureStrategy` that always returns true.
struct NoBackPressure: NIOAsyncSequenceProducerBackPressureStrategy {
    func didYield(bufferDepth: Int) -> Bool { true }
    func didConsume(bufferDepth: Int) -> Bool { true }
}

// MARK: - ShutDownOnTerminate

/// `NIOAsyncSequenceProducerDelegate` that terminates the shuts the consumer down when
/// `didTerminate()` is invoked.
struct ShutdownOnTerminate: @unchecked Sendable { // We can do that because our stored propery is protected by a lock
    let stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>
}

extension ShutdownOnTerminate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        // No back pressure
        return
    }

    func didTerminate() {
        // Duplicate of _shutdownGracefully
        let action = self.stateMachine.withLockedValue { $0.finish() }
        switch action {
        case .shutdownGracefullyAndFinishSource(let client, let source, let subscribedTopicsPointer, let logger):
            source.finish()

            let result = client.withKafkaHandlePointer { handle in
                rd_kafka_consumer_close(handle)
            }

            rd_kafka_topic_partition_list_destroy(subscribedTopicsPointer)

            guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
                let error = KafkaError.rdKafkaError(wrapping: result)
                logger.error("Closing KafkaConsumer failed: \(error.description)")
                return
            }
        case .none:
            return
        }
    }
}

// MARK: - KafkaConsumerMessages

/// `AsyncSequence` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
public struct KafkaConsumerMessages: AsyncSequence {
    public typealias Element = Result<KafkaConsumerMessage, KafkaError>
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, NoBackPressure, ShutdownOnTerminate>
    let wrappedSequence: WrappedSequence

    /// `AsynceIteratorProtocol` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
    public struct ConsumerMessagesAsyncIterator: AsyncIteratorProtocol {
        var wrappedIterator: WrappedSequence.AsyncIterator

        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    public func makeAsyncIterator() -> ConsumerMessagesAsyncIterator {
        return ConsumerMessagesAsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

// MARK: - KafkaConsumer

/// Receive messages from the Kafka cluster.
public final class KafkaConsumer {
    typealias Producer = NIOAsyncSequenceProducer<
        Result<KafkaConsumerMessage, KafkaError>,
        NoBackPressure,
        ShutdownOnTerminate
    >
    /// The configuration object of the consumer client.
    private var config: KafkaConsumerConfiguration
    /// State of the `KafkaConsumer`.
    private let stateMachine: NIOLockedValueBox<StateMachine>

    /// `AsyncSequence` that returns all ``KafkaConsumerMessage`` objects that the consumer receives.
    public let messages: KafkaConsumerMessages

    /// Initialize a new ``KafkaConsumer``.
    /// To listen to incoming messages, please subscribe to a list of topics using ``subscribe(topics:)``
    /// or assign the consumer to a particular topic + partition pair using ``assign(topic:partition:offset:)``.
    /// - Parameter config: The ``KafkaConsumerConfiguration`` for configuring the ``KafkaConsumer``.
    /// - Parameter logger: A logger.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    public init(
        pollInterval: Duration = .milliseconds(100), // TODO(felix): poll intervals through config in separate PR
        config: KafkaConsumerConfiguration,
        logger: Logger
    ) throws {
        self.config = config

        let client = try RDKafka.createClient(type: .consumer, configDictionary: config.dictionary, logger: logger)

        guard let subscribedTopicsPointer = rd_kafka_topic_partition_list_new(1) else {
            throw KafkaError.client(reason: "Failed to allocate Topic+Partition list.")
        }

        self.stateMachine = NIOLockedValueBox(StateMachine())

        let sourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: Result<KafkaConsumerMessage, KafkaError>.self,
            backPressureStrategy: NoBackPressure(),
            delegate: ShutdownOnTerminate(stateMachine: self.stateMachine)
        )

        self.messages = KafkaConsumerMessages(
            wrappedSequence: sourceAndSequence.sequence
        )

        self.stateMachine.withLockedValue {
            $0.initialize(
                pollInterval: pollInterval,
                client: client,
                source: sourceAndSequence.source,
                subscribedTopicsPointer: subscribedTopicsPointer,
                logger: logger
            )
        }

        // Events that would be triggered by rd_kafka_poll
        // will now be also triggered by rd_kafka_consumer_poll
        let result = client.withKafkaHandlePointer { handle in
            rd_kafka_poll_set_consumer(handle)
        }
        guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.rdKafkaError(wrapping: result)
        }

        switch config.consumptionStrategy._internal {
        case .partition(topic: let topic, partition: let partition, offset: let offset):
            try self.assign(topic: topic, partition: partition, offset: offset)
        case .group(groupID: _, topics: let topics):
            try self.subscribe(topics: topics)
        }
    }

    deinit {
        // This occurs e.g. when for await loop is exited through break
        self.shutdownGracefully()
    }

    /// Subscribe to the given list of `topics`.
    /// The partition assignment happens automatically using `KafkaConsumer`'s consumer group.
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Throws: A ``KafkaError`` if subscribing to the topic list failed.
    private func subscribe(topics: [String]) throws {
        let action = self.stateMachine.withLockedValue { $0.setUpConnection() }
        switch action {
        case .setUpConnection(let client, let subscribedTopicsPointer):
            for topic in topics {
                rd_kafka_topic_partition_list_add(
                    subscribedTopicsPointer,
                    topic,
                    KafkaPartition.unassigned.rawValue
                )
            }

            let result = client.withKafkaHandlePointer { handle in
                rd_kafka_subscribe(handle, subscribedTopicsPointer)
            }

            guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: result)
            }
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
        offset: Int
    ) throws {
        let action = self.stateMachine.withLockedValue { $0.setUpConnection() }
        switch action {
        case .setUpConnection(let client, let subscribedTopicsPointer):
            guard let partitionPointer = rd_kafka_topic_partition_list_add(
                subscribedTopicsPointer,
                topic,
                partition.rawValue
            ) else {
                fatalError("rd_kafka_topic_partition_list_add returned invalid pointer")
            }

            partitionPointer.pointee.offset = Int64(offset)

            let result = client.withKafkaHandlePointer { handle in
                rd_kafka_assign(handle, subscribedTopicsPointer)
            }

            guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: result)
            }
        }
    }

    /// Start polling Kafka for messages.
    ///
    /// - Returns: An awaitable task representing the execution of the poll loop.
    public func run() async throws {
        // TODO(felix): make pollInterval part of config -> easier to adapt to Service protocol (service-lifecycle)
        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextPollLoopAction() }
            switch nextAction {
            case .pollForAndYieldMessage(let pollInterval, let client, let source, let logger):
                let messageResult: Result<KafkaConsumerMessage, KafkaError>
                do {
                    guard let message = try client.consumerPoll() else {
                        break
                    }
                    messageResult = .success(message)
                } catch let kafkaError as KafkaError {
                    messageResult = .failure(kafkaError)
                } catch {
                    logger.error("KafkaConsumer caught error: \(error)")
                    break
                }
                // We support no back pressure, we can ignore the yield result
                _ = source.yield(messageResult)
                try await Task.sleep(for: pollInterval)
            case .killPollLoop:
                return
            }
        }
    }

    /// Mark `message` in the topic as read and request the next message from the topic.
    /// This method is only used for manual offset management.
    /// - Parameter message: Last received message that shall be marked as read.
    /// - Throws: A ``KafkaError`` if committing failed.
    /// - Warning: This method fails if the `enable.auto.commit` configuration property is set to `true`.
    public func commitSync(_ message: KafkaConsumerMessage) async throws {
        try await withCheckedThrowingContinuation { continuation in
            do {
                try self._commitSync(message) // Blocks until commiting the offset is done
                continuation.resume()
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }

    private func _commitSync(_ message: KafkaConsumerMessage) throws {
        let action = self.stateMachine.withLockedValue { $0.commitSync() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        case .commitSync(let client):
            guard self.config.enableAutoCommit == false else {
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
            partitionPointer.pointee.offset = Int64(message.offset + 1)
            let result = client.withKafkaHandlePointer { handle in
                rd_kafka_commit(
                    handle,
                    changesList,
                    0
                ) // Blocks until commiting the offset is done
            }
            guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: result)
            }
        }
    }

    /// This function is used to gracefully shut down a Kafka consumer client.
    ///
    /// - Note: Invoking this function is not always needed as the ``KafkaConsumer``
    /// will already shut down when consumption of the ``KafkaConsumerMessages`` has ended.
    public func shutdownGracefully() {
        let action = self.stateMachine.withLockedValue { $0.finish() }
        switch action {
        case .shutdownGracefullyAndFinishSource(let client, let source, let subscribedTopicsPointer, let logger):
            self._shutdownGracefullyAndFinishSource(
                client: client,
                source: source,
                subscribedTopicsPointer: subscribedTopicsPointer,
                logger: logger
            )
        case .none:
            return
        }
    }

    private func _shutdownGracefullyAndFinishSource(
        client: KafkaClient,
        source: Producer.Source,
        subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>,
        logger: Logger
    ) {
        source.finish()

        let result = client.withKafkaHandlePointer { handle in
            rd_kafka_consumer_close(handle)
        }

        rd_kafka_topic_partition_list_destroy(subscribedTopicsPointer)

        guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
            let error = KafkaError.rdKafkaError(wrapping: result)
            logger.error("Closing KafkaConsumer failed: \(error.description)")
            return
        }
    }
}

// MARK: - KafkaConsumer + StateMachine

extension KafkaConsumer {
    /// State machine representing the state of the ``KafkaConsumer``.
    struct StateMachine {
        /// The state of the ``StateMachine``.
        enum State {
            /// The state machine has been initialized with init() but is not yet Initialized
            /// using `func initialize()` (required).
            case uninitialized
            /// We are in the process of initializing the ``KafkaConsumer``,
            /// though ``subscribe()`` / ``assign()`` have not been invoked.
            ///
            /// - Parameter pollInterval: Amount of time between two subsequent polls invocations.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            /// - Parameter subscribedTopicsPointer: Pointer to a list of topics + partition pairs.
            /// - Parameter logger: A logger.
            case initializing(
                pollInterval: Duration,
                client: KafkaClient,
                source: Producer.Source,
                subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>,
                logger: Logger
            )
            /// The ``KafkaConsumer`` is consuming messages.
            ///
            /// - Parameter pollInterval: Amount of time between two subsequent polls invocations.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            /// - Parameter subscribedTopicsPointer: Pointer to a list of topics + partition pairs.
            /// - Parameter logger: A logger.
            case consuming(
                pollInterval: Duration,
                client: KafkaClient,
                source: Producer.Source,
                subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>,
                logger: Logger
            )
            /// The ``KafkaConsumer`` has been closed.
            case finished
        }

        /// The current state of the StateMachine.
        var state: State = .uninitialized

        /// Delayed initialization of `StateMachine` as the `source` and the `pollClosure` are
        /// not yet available when the normal initialization occurs.
        mutating func initialize(
            pollInterval: Duration,
            client: KafkaClient,
            source: Producer.Source,
            subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>,
            logger: Logger
        ) {
            guard case .uninitialized = self.state else {
                fatalError("\(#function) can only be invoked in state .uninitialized, but was invoked in state \(self.state)")
            }
            self.state = .initializing(
                pollInterval: pollInterval,
                client: client,
                source: source,
                subscribedTopicsPointer: subscribedTopicsPointer,
                logger: logger
            )
        }

        /// Action to be taken when wanting to poll for a new message.
        enum PollLoopAction {
            /// Poll for a new ``KafkaConsumerMessage``.
            ///
            /// - Parameter pollInterval: Amount of time between two subsequent polls invocations.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            /// - Parameter logger: A logger.
            case pollForAndYieldMessage(
                pollInterval: Duration,
                client: KafkaClient,
                source: Producer.Source,
                logger: Logger
            )
            /// Kill the poll loop.
            case killPollLoop
        }

        /// Returns the next action to be taken when wanting to poll.
        /// - Returns: The next action to be taken when wanting to poll, or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        func nextPollLoopAction() -> PollLoopAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
            case .consuming(let pollInterval, let client, let source, _, let logger):
                return .pollForAndYieldMessage(pollInterval: pollInterval, client: client, source: source, logger: logger)
            case .finished:
                return .killPollLoop
            }
        }

        /// Action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        enum SetUpConnectionAction {
            /// Set up the connection through ``subscribe()`` or ``assign()``.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter subscribedTopicsPointer: Pointer to a list of topics + partition pairs.
            case setUpConnection(client: KafkaClient, subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>)
        }

        /// Get action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        ///
        /// - Parameter pollInterval: Amount of time between two subsequent polls invocations.
        /// - Parameter client: Client used for handling the connection to the Kafka cluster.
        /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
        /// - Parameter subscribedTopicsPointer: Pointer to a list of topics + partition pairs.
        /// - Parameter logger: A logger.
        ///
        /// - Returns: The action to be taken.
        mutating func setUpConnection() -> SetUpConnectionAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing(let pollInterval, let client, let source, let subscribedTopicsPointer, let logger):
                self.state = .consuming(
                    pollInterval: pollInterval,
                    client: client,
                    source: source,
                    subscribedTopicsPointer: subscribedTopicsPointer,
                    logger: logger
                )
                return .setUpConnection(client: client, subscribedTopicsPointer: subscribedTopicsPointer)
            case .consuming, .finished:
                fatalError("\(#function) should only be invoked upon initialization of KafkaConsumer")
            }
        }

        /// Action to be taken when wanting to do a synchronous commit.
        enum CommitSyncAction {
            /// Do a synchronous commit.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case commitSync(
                client: KafkaClient
            )
            /// Throw an error. The ``KafkaConsumer`` is closed.
            case throwClosedError
        }

        /// Get action to be taken when wanting to do a synchronous commit.
        /// - Returns: The action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        func commitSync() -> CommitSyncAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before committing offsets")
            case .consuming(_, let client, _, _, _):
                return .commitSync(client: client)
            case .finished:
                return .throwClosedError
            }
        }

        /// Action to be taken when wanting to do close the consumer.
        enum FinishAction {
            /// Shut down the ``KafkaConsumer`` and finish the given `source` object.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            /// - Parameter subscribedTopicsPointer: Pointer to a list of topics + partition pairs.
            /// - Parameter logger: A logger.
            case shutdownGracefullyAndFinishSource(
                client: KafkaClient,
                source: Producer.Source,
                subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>,
                logger: Logger
            )
        }

        /// Get action to be taken when wanting to do close the consumer.
        /// - Returns: The action to be taken,  or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func finish() -> FinishAction? {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("subscribe() / assign() should have been invoked before \(#function)")
            case .consuming(_, let client, let source, let subscribedTopicsPointer, let logger):
                self.state = .finished
                return .shutdownGracefullyAndFinishSource(
                    client: client,
                    source: source,
                    subscribedTopicsPointer: subscribedTopicsPointer,
                    logger: logger
                )
            case .finished:
                return nil
            }
        }
    }
}
