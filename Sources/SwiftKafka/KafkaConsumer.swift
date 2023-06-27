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
        case .shutdownGracefullyAndFinishSource(let client, let source):
            source.finish()

            do {
                try client.consumerClose()
            } catch {
                self.stateMachine.withLockedValue {
                    if let error = error as? KafkaError {
                        $0.logger.error("Closing KafkaConsumer failed: \(error.description)")
                    } else {
                        $0.logger.error("Caught unknown error: \(error)")
                    }
                }
            }
        case .none:
            return
        }
    }
}

// MARK: - KafkaConsumerMessages

/// `AsyncSequence` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
public struct KafkaConsumerMessages: AsyncSequence {
    public typealias Element = KafkaConsumerMessage
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
        KafkaConsumerMessage,
        NoBackPressure,
        ShutdownOnTerminate
    >
    /// The configuration object of the consumer client.
    private var config: KafkaConsumerConfiguration
    /// A logger.
    private let logger: Logger
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
        config: KafkaConsumerConfiguration,
        logger: Logger
    ) throws {
        self.config = config
        self.logger = logger

        let client = try RDKafka.createClient(type: .consumer, configDictionary: config.dictionary, logger: logger)

        self.stateMachine = NIOLockedValueBox(StateMachine(logger: self.logger))

        let sourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: KafkaConsumerMessage.self,
            backPressureStrategy: NoBackPressure(),
            delegate: ShutdownOnTerminate(stateMachine: self.stateMachine)
        )

        self.messages = KafkaConsumerMessages(
            wrappedSequence: sourceAndSequence.sequence
        )

        self.stateMachine.withLockedValue {
            $0.initialize(
                client: client,
                source: sourceAndSequence.source
            )
        }

        // Events that would be triggered by ``KafkaClient/poll(timeout:)``
        // are now triggered by ``KafkaClient/consumerPoll``.
        try client.pollSetConsumer()

        switch config.consumptionStrategy._internal {
        case .partition(topic: let topic, partition: let partition, offset: let offset):
            try self.assign(topic: topic, partition: partition, offset: offset)
        case .group(groupID: _, topics: let topics):
            try self.subscribe(topics: topics)
        }
    }

    deinit {
        self.shutdownGracefully()
    }

    /// Subscribe to the given list of `topics`.
    /// The partition assignment happens automatically using `KafkaConsumer`'s consumer group.
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Throws: A ``KafkaError`` if subscribing to the topic list failed.
    private func subscribe(topics: [String]) throws {
        let action = self.stateMachine.withLockedValue { $0.setUpConnection() }
        switch action {
        case .setUpConnection(let client):
            let subscription = RDKafkaTopicPartitionList()
            for topic in topics {
                subscription.add(
                    topic: topic,
                    partition: KafkaPartition.unassigned
                )
            }
            try client.subscribe(topicPartitionList: subscription)
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
        case .setUpConnection(let client):
            let assignment = RDKafkaTopicPartitionList()
            assignment.setOffset(topic: topic, partition: partition, offset: Int64(offset))
            try client.assign(topicPartitionList: assignment)
        }
    }

    /// Start polling Kafka for messages.
    ///
    /// - Returns: An awaitable task representing the execution of the poll loop.
    public func run() async throws {
        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextPollLoopAction() }
            switch nextAction {
            case .pollForAndYieldMessage(let client, let source):
                do {
                    if let message = try client.consumerPoll() {
                        // We do not support back pressure, we can ignore the yield result
                        _ = source.yield(message)
                    }
                } catch {
                    source.finish()
                    throw error
                }
                try await Task.sleep(for: self.config.pollInterval)
            case .pollUntilClosed(let client):
                // Ignore poll result, we are closing down and just polling to commit
                // outstanding consumer state
                _ = try client.consumerPoll()
                try await Task.sleep(for: self.config.pollInterval)
            case .terminatePollLoop:
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
        let action = self.stateMachine.withLockedValue { $0.commitSync() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        case .commitSync(let client):
            guard self.config.enableAutoCommit == false else {
                throw KafkaError.config(reason: "Committing manually only works if enable.auto.commit is set to false")
            }

            try await client.commitSync(message)
        }
    }

    /// This function is used to gracefully shut down a Kafka consumer client.
    ///
    /// - Note: Invoking this function is not always needed as the ``KafkaConsumer``
    /// will already shut down when consumption of the ``KafkaConsumerMessages`` has ended.
    private func shutdownGracefully() {
        let action = self.stateMachine.withLockedValue { $0.finish() }
        switch action {
        case .shutdownGracefullyAndFinishSource(let client, let source):
            self._shutdownGracefullyAndFinishSource(
                client: client,
                source: source,
                logger: self.logger
            )
        case .none:
            return
        }
    }

    private func _shutdownGracefullyAndFinishSource(
        client: KafkaClient,
        source: Producer.Source,
        logger: Logger
    ) {
        source.finish()

        do {
            try client.consumerClose()
        } catch {
            if let error = error as? KafkaError {
                logger.error("Closing KafkaConsumer failed: \(error.description)")
            } else {
                logger.error("Caught unknown error: \(error)")
            }
        }
    }
}

// MARK: - KafkaConsumer + StateMachine

extension KafkaConsumer {
    /// State machine representing the state of the ``KafkaConsumer``.
    struct StateMachine {
        /// A logger.
        let logger: Logger

        /// The state of the ``StateMachine``.
        enum State {
            /// The state machine has been initialized with init() but is not yet Initialized
            /// using `func initialize()` (required).
            case uninitialized
            /// We are in the process of initializing the ``KafkaConsumer``,
            /// though ``subscribe()`` / ``assign()`` have not been invoked.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case initializing(
                client: KafkaClient,
                source: Producer.Source
            )
            /// The ``KafkaConsumer`` is consuming messages.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case consuming(
                client: KafkaClient,
                source: Producer.Source
            )
            /// The ``KafkaConsumer`` has been closed.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case finished(client: KafkaClient)
        }

        /// The current state of the StateMachine.
        var state: State = .uninitialized

        /// Delayed initialization of `StateMachine` as the `source` and the `pollClosure` are
        /// not yet available when the normal initialization occurs.
        mutating func initialize(
            client: KafkaClient,
            source: Producer.Source
        ) {
            guard case .uninitialized = self.state else {
                fatalError("\(#function) can only be invoked in state .uninitialized, but was invoked in state \(self.state)")
            }
            self.state = .initializing(
                client: client,
                source: source
            )
        }

        /// Action to be taken when wanting to poll for a new message.
        enum PollLoopAction {
            /// Poll for a new ``KafkaConsumerMessage``.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case pollForAndYieldMessage(
                client: KafkaClient,
                source: Producer.Source
            )
            /// The ``KafkaConsumer`` is in the process of closing down, but still needs to poll
            /// to commit its state to the broker.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case pollUntilClosed(client: KafkaClient)
            /// Terminate the poll loop.
            case terminatePollLoop
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
            case .consuming(let client, let source):
                return .pollForAndYieldMessage(client: client, source: source)
            case .finished(let client):
                if client.isConsumerClosed {
                    return .terminatePollLoop
                } else {
                    return .pollUntilClosed(client: client)
                }
            }
        }

        /// Action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        enum SetUpConnectionAction {
            /// Set up the connection through ``subscribe()`` or ``assign()``.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case setUpConnection(client: KafkaClient)
        }

        /// Get action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        ///
        /// - Returns: The action to be taken.
        mutating func setUpConnection() -> SetUpConnectionAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing(let client, let source):
                self.state = .consuming(
                    client: client,
                    source: source
                )
                return .setUpConnection(client: client)
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
            case .consuming(let client, _):
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
            case shutdownGracefullyAndFinishSource(
                client: KafkaClient,
                source: Producer.Source
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
            case .consuming(let client, let source):
                self.state = .finished(client: client)
                return .shutdownGracefullyAndFinishSource(
                    client: client,
                    source: source
                )
            case .finished:
                return nil
            }
        }
    }
}
