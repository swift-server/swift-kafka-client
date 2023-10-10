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

import Logging
import NIOConcurrencyHelpers
import NIOCore
import ServiceLifecycle

// MARK: - KafkaConsumerEventsDelegate

/// `NIOAsyncSequenceProducerDelegate` for ``KafkaConsumerEvents``.
internal struct KafkaConsumerEventsDelegate: Sendable {
    let stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>
}

extension KafkaConsumerEventsDelegate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        return // No back pressure
    }

    func didTerminate() {
        return // We have to call poll for events anyway, nothing to do here
    }
}

// MARK: - KafkaConsumerMessagesDelegate

/// `NIOAsyncSequenceProducerDelegate` for ``KafkaConsumerMessages``.
internal struct KafkaConsumerMessagesDelegate: Sendable {
    let stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>
}

extension KafkaConsumerMessagesDelegate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        self.stateMachine.withLockedValue { $0.produceMore() }
    }

    func didTerminate() {
        self.stateMachine.withLockedValue { $0.messageSequenceTerminated() }
    }
}

// MARK: - KafkaConsumerEvents

/// `AsyncSequence` implementation for handling ``KafkaConsumerEvent``s emitted by Kafka.
public struct KafkaConsumerEvents: Sendable, AsyncSequence {
    public typealias Element = KafkaConsumerEvent
    typealias BackPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, BackPressureStrategy, KafkaConsumerEventsDelegate>
    let wrappedSequence: WrappedSequence

    /// `AsynceIteratorProtocol` implementation for handling ``KafkaConsumerEvent``s emitted by Kafka.
    public struct AsyncIterator: AsyncIteratorProtocol {
        var wrappedIterator: WrappedSequence.AsyncIterator

        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return AsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

// MARK: - KafkaConsumerMessages

/// `AsyncSequence` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
public struct KafkaConsumerMessages: Sendable, AsyncSequence {
    let stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>

    public typealias Element = KafkaConsumerMessage
    typealias BackPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark
    typealias WrappedSequence = NIOThrowingAsyncSequenceProducer<
        Result<KafkaConsumerMessage, Error>,
        Error,
        BackPressureStrategy,
        KafkaConsumerMessagesDelegate
    >
    let wrappedSequence: WrappedSequence

    /// `AsynceIteratorProtocol` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
    public struct AsyncIterator: AsyncIteratorProtocol {
        let stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>
        var wrappedIterator: WrappedSequence.AsyncIterator?

        public mutating func next() async throws -> Element? {
            guard let result = try await self.wrappedIterator?.next() else {
                self.deallocateIterator()
                return nil
            }

            switch result {
            case .success(let message):
                let action = self.stateMachine.withLockedValue { $0.storeOffset() }
                switch action {
                case .storeOffset(let client):
                    do {
                        try client.storeMessageOffset(message)
                    } catch {
                        self.deallocateIterator()
                        throw error
                    }
                    return message
                case .terminateConsumerSequence:
                    self.deallocateIterator()
                    return nil
                }
            case .failure(let error):
                self.deallocateIterator()
                throw error
            }
        }

        private mutating func deallocateIterator() {
            self.wrappedIterator = nil
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return AsyncIterator(
            stateMachine: self.stateMachine,
            wrappedIterator: self.wrappedSequence.makeAsyncIterator()
        )
    }
}

// MARK: - KafkaConsumer

/// A ``KafkaConsumer `` can be used to consume messages from a Kafka cluster.
public final class KafkaConsumer: Sendable, Service {
    typealias Producer = NIOThrowingAsyncSequenceProducer<
        Result<KafkaConsumerMessage, Error>,
        Error,
        NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark,
        KafkaConsumerMessagesDelegate
    >
    /// The configuration object of the consumer client.
    private let configuration: KafkaConsumerConfiguration
    /// A logger.
    private let logger: Logger
    /// State of the `KafkaConsumer`.
    private let stateMachine: NIOLockedValueBox<StateMachine>

    /// An asynchronous sequence containing messages from the Kafka cluster.
    public let messages: KafkaConsumerMessages

    // Private initializer, use factory method or convenience init to create KafkaConsumer
    /// Initialize a new ``KafkaConsumer``.
    /// To listen to incoming messages, please subscribe to a list of topics using ``subscribe(topics:)``
    /// or assign the consumer to a particular topic + partition pair using ``assign(topic:partition:offset:)``.
    ///
    /// - Parameters:
    ///     - client: Client used for handling the connection to the Kafka cluster.
    ///     - stateMachine: The state machine containing the state of the ``KafkaConsumer``.
    ///     - configuration: The ``KafkaConsumerConfiguration`` for configuring the ``KafkaConsumer``.
    ///     - logger: A logger.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    private init(
        client: RDKafkaClient,
        stateMachine: NIOLockedValueBox<StateMachine>,
        configuration: KafkaConsumerConfiguration,
        logger: Logger
    ) throws {
        self.configuration = configuration
        self.stateMachine = stateMachine
        self.logger = logger

        let sourceAndSequence = NIOThrowingAsyncSequenceProducer.makeSequence(
            elementType: Result<KafkaConsumerMessage, Error>.self,
            backPressureStrategy: {
                switch configuration.backPressureStrategy._internal {
                case .watermark(let lowWatermark, let highWatermark):
                    return NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark(
                        lowWatermark: lowWatermark,
                        highWatermark: highWatermark
                    )
                }
            }(),
            delegate: KafkaConsumerMessagesDelegate(stateMachine: self.stateMachine)
        )

        self.messages = KafkaConsumerMessages(
            stateMachine: self.stateMachine,
            wrappedSequence: sourceAndSequence.sequence
        )

        self.stateMachine.withLockedValue {
            $0.initialize(
                client: client,
                source: sourceAndSequence.source
            )
        }
    }

    /// Initialize a new ``KafkaConsumer``.
    ///
    /// This creates a consumer without that does not listen to any events other than consumer messages.
    ///
    /// - Parameters:
    ///     - configuration: The ``KafkaConsumerConfiguration`` for configuring the ``KafkaConsumer``.
    ///     - logger: A logger.
    /// - Returns: The newly created ``KafkaConsumer``.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    public convenience init(
        configuration: KafkaConsumerConfiguration,
        logger: Logger
    ) throws {
        let stateMachine = NIOLockedValueBox(StateMachine(logger: logger))

        var subscribedEvents: [RDKafkaEvent] = [.log]
        // Only listen to offset commit events when autoCommit is false
        if configuration.isAutoCommitEnabled == false {
            subscribedEvents.append(.offsetCommit)
        }

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger
        )

        try self.init(
            client: client,
            stateMachine: stateMachine,
            configuration: configuration,
            logger: logger
        )
    }

    /// Initialize a new ``KafkaConsumer`` and a ``KafkaConsumerEvents`` asynchronous sequence.
    ///
    /// Use the asynchronous sequence to consume events.
    ///
    /// - Important: When the asynchronous sequence is deinited the producer will be shut down and disallowed from sending more messages.
    /// Additionally, make sure to consume the asynchronous sequence otherwise the events will be buffered in memory indefinitely.
    ///
    /// - Parameters:
    ///     - configuration: The ``KafkaConsumerConfiguration`` for configuring the ``KafkaConsumer``.
    ///     - logger: A logger.
    /// - Returns: A tuple containing the created ``KafkaConsumer`` and the ``KafkaConsumerEvents``
    /// `AsyncSequence` used for receiving message events.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    public static func makeConsumerWithEvents(
        configuration: KafkaConsumerConfiguration,
        logger: Logger
    ) throws -> (KafkaConsumer, KafkaConsumerEvents) {
        let stateMachine = NIOLockedValueBox(StateMachine(logger: logger))

        var subscribedEvents: [RDKafkaEvent] = [.log]
        // Only listen to offset commit events when autoCommit is false
        if configuration.isAutoCommitEnabled == false {
            subscribedEvents.append(.offsetCommit)
        }

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger
        )

        let consumer = try KafkaConsumer(
            client: client,
            stateMachine: stateMachine,
            configuration: configuration,
            logger: logger
        )

        // Note:
        // It's crucial to initialize the `sourceAndSequence` variable AFTER `client`.
        // This order is important to prevent the accidental triggering of `KafkaConsumerCloseOnTerminate.didTerminate()`.
        // If this order is not met and `RDKafkaClient.makeClient()` fails,
        // it leads to a call to `stateMachine.messageSequenceTerminated()` while it's still in the `.uninitialized` state.
        let sourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: KafkaConsumerEvent.self,
            backPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure(),
            delegate: KafkaConsumerEventsDelegate(stateMachine: stateMachine)
        )

        let eventsSequence = KafkaConsumerEvents(wrappedSequence: sourceAndSequence.sequence)
        return (consumer, eventsSequence)
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
    /// - Parameter offset: The offset to start consuming from.
    /// Defaults to the end of the Kafka partition queue (meaning wait for next produced message).
    /// - Throws: A ``KafkaError`` if the consumer could not be assigned to the topic + partition pair.
    private func assign(
        topic: String,
        partition: KafkaPartition,
        offset: KafkaOffset
    ) throws {
        let action = self.stateMachine.withLockedValue { $0.setUpConnection() }
        switch action {
        case .setUpConnection(let client):
            let assignment = RDKafkaTopicPartitionList()
            assignment.setOffset(topic: topic, partition: partition, offset: Int64(offset.rawValue))
            try client.assign(topicPartitionList: assignment)
        }
    }

    /// Start the ``KafkaConsumer``.
    ///
    /// - Important: This method **must** be called and will run until either the calling task is cancelled or gracefully shut down.
    public func run() async throws {
        try await withGracefulShutdownHandler {
            try await self._run()
        } onGracefulShutdown: {
            self.triggerGracefulShutdown()
        }
    }

    private func _run() async throws {
        switch self.configuration.consumptionStrategy._internal {
        case .partition(topic: let topic, partition: let partition, offset: let offset):
            try self.assign(topic: topic, partition: partition, offset: offset)
        case .group(groupID: _, topics: let topics):
            try self.subscribe(topics: topics)
        }

        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextPollLoopAction() }
            switch nextAction {
            case .pollForEventsAndMessage(let client, let source):
                // Event poll to serve any events queued inside of `librdkafka`.
                _ = client.eventPoll()
                // Poll for new consumer message.
                var result: Result<KafkaConsumerMessage, Error>?
                do {
                    if let message = try client.consumerPoll() {
                        result = .success(message)
                    }
                } catch {
                    result = .failure(error)
                }
                if let result {
                    let yieldResult = source.yield(result)
                    switch yieldResult {
                    case .produceMore:
                        break
                    case .stopProducing:
                        self.stateMachine.withLockedValue { $0.stopProducing() }
                    case .dropped:
                        break
                    }
                }
                try await Task.sleep(for: self.configuration.pollInterval)
            case .pollForEvents(let client):
                // Event poll to serve any events queued inside of `librdkafka`.
                _ = client.eventPoll()
                try await Task.sleep(for: self.configuration.pollInterval)
            case .terminatePollLoop:
                return
            }
        }
    }

    /// Mark all messages up to the passed message in the topic as read.
    /// Schedules a commit and returns immediately.
    /// Any errors encountered after scheduling the commit will be discarded.
    ///
    /// This method is only used for manual offset management.
    ///
    /// - Warning: This method fails if the ``KafkaConsumerConfiguration/isAutoCommitEnabled`` configuration property is set to `true` (default).
    ///
    /// - Parameters:
    ///     - message: Last received message that shall be marked as read.
    /// - Throws: A ``KafkaError`` if committing failed.
    public func scheduleCommit(_ message: KafkaConsumerMessage) throws {
        let action = self.stateMachine.withLockedValue { $0.commit() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        case .commit(let client):
            guard self.configuration.isAutoCommitEnabled == false else {
                throw KafkaError.config(reason: "Committing manually only works if isAutoCommitEnabled set to false")
            }

            try client.scheduleCommit(message)
        }
    }

    @available(*, deprecated, renamed: "commit")
    public func commitSync(_ message: KafkaConsumerMessage) async throws {
        try await self.commit(message)
    }

    /// Mark all messages up to the passed message in the topic as read.
    /// Awaits until the commit succeeds or an error is encountered.
    ///
    /// This method is only used for manual offset management.
    ///
    /// - Warning: This method fails if the ``KafkaConsumerConfiguration/isAutoCommitEnabled`` configuration property is set to `true` (default).
    ///
    /// - Parameters:
    ///     - message: Last received message that shall be marked as read.
    /// - Throws: A ``KafkaError`` if committing failed.
    public func commit(_ message: KafkaConsumerMessage) async throws {
        let action = self.stateMachine.withLockedValue { $0.commit() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        case .commit(let client):
            guard self.configuration.isAutoCommitEnabled == false else {
                throw KafkaError.config(reason: "Committing manually only works if isAutoCommitEnabled set to false")
            }

            try await client.commit(message)
        }
    }

    /// This function is used to gracefully shut down a Kafka consumer client.
    ///
    /// - Note: Invoking this function is not always needed as the ``KafkaConsumer``
    /// will already shut down when consumption of the ``KafkaConsumerMessages`` has ended.
    public func triggerGracefulShutdown() {
        let action = self.stateMachine.withLockedValue { $0.finish() }
        switch action {
        case .triggerGracefulShutdown(let client):
            self._triggerGracefulShutdown(
                client: client,
                logger: self.logger
            )
        case .triggerGracefulShutdownAndFinishSource(let client, let source):
            source.finish()
            self._triggerGracefulShutdown(
                client: client,
                logger: self.logger
            )
        case .none:
            return
        }
    }

    private func _triggerGracefulShutdown(
        client: RDKafkaClient,
        logger: Logger
    ) {
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
    struct StateMachine: Sendable {
        /// A logger.
        let logger: Logger

        /// Represents the state of the ``KafkaConsumerMessages`` asynchronous sequence.
        enum ConsumerMessagesSequenceState {
            /// The sequence can take more messages.
            ///
            /// - Parameter source: The source for yielding new messages.
            case open(source: Producer.Source)
            /// Sequence suspended due to back pressure.
            ///
            /// - Parameter source: The source for yielding new messages.
            case suspended(source: Producer.Source)
            /// The sequence has finished, and no more messages will be produced.
            case finished
        }

        /// The state of the ``StateMachine``.
        enum State: Sendable {
            /// The state machine has been initialized with init() but is not yet Initialized
            /// using `func initialize()` (required).
            case uninitialized
            /// We are in the process of initializing the ``KafkaConsumer``,
            /// though ``subscribe()`` / ``assign()`` have not been invoked.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter messagesSequenceState: State of ``KafkaConsumerMessages`` `AsyncSequence` we use to give ``KafkaConsumerMessage``s back to the user.
            case initializing(
                client: RDKafkaClient,
                messagesSequenceState: ConsumerMessagesSequenceState
            )
            /// The ``KafkaConsumer`` is consuming messages.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter messagesSequenceState: State of ``KafkaConsumerMessages`` `AsyncSequence` we use to give ``KafkaConsumerMessage``s back to the user.
            case consuming(
                client: RDKafkaClient,
                messagesSequenceState: ConsumerMessagesSequenceState
            )
            /// The ``KafkaConsumer/triggerGracefulShutdown()`` has been invoked.
            /// We are now in the process of commiting our last state to the broker.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case finishing(client: RDKafkaClient)
            /// The ``KafkaConsumer`` is closed.
            case finished
        }

        /// The current state of the StateMachine.
        var state: State = .uninitialized

        /// Delayed initialization of `StateMachine` as the `source` and the `pollClosure` are
        /// not yet available when the normal initialization occurs.
        mutating func initialize(
            client: RDKafkaClient,
            source: Producer.Source
        ) {
            guard case .uninitialized = self.state else {
                fatalError("\(#function) can only be invoked in state .uninitialized, but was invoked in state \(self.state)")
            }
            self.state = .initializing(
                client: client,
                messagesSequenceState: .open(source: source)
            )
        }

        /// Action to be taken when wanting to poll for a new message.
        enum PollLoopAction {
            /// Poll for a new ``KafkaConsumerMessage``.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case pollForEventsAndMessage(
                client: RDKafkaClient,
                source: Producer.Source
            )
            /// Serve any queued callbacks on the event queue.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case pollForEvents(
                client: RDKafkaClient
            )
            /// Terminate the poll loop.
            case terminatePollLoop
        }

        /// Returns the next action to be taken when wanting to poll.
        /// - Returns: The next action to be taken when wanting to poll, or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func nextPollLoopAction() -> PollLoopAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
            case .consuming(let client, let messagesSequenceState):
                switch messagesSequenceState {
                case .open(let source):
                    return .pollForEventsAndMessage(client: client, source: source)
                case .suspended, .finished:
                    return .pollForEvents(client: client)
                }
            case .finishing(let client):
                if client.isConsumerClosed {
                    self.state = .finished
                    return .terminatePollLoop
                } else {
                    return .pollForEvents(client: client)
                }
            case .finished:
                return .terminatePollLoop
            }
        }

        /// Action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        enum SetUpConnectionAction {
            /// Set up the connection through ``subscribe()`` or ``assign()``.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case setUpConnection(client: RDKafkaClient)
        }

        /// Get action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        ///
        /// - Returns: The action to be taken.
        mutating func setUpConnection() -> SetUpConnectionAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing(let client, let messagesSequenceState):
                self.state = .consuming(
                    client: client,
                    messagesSequenceState: messagesSequenceState
                )
                return .setUpConnection(client: client)
            case .consuming, .finishing, .finished:
                fatalError("\(#function) should only be invoked upon initialization of KafkaConsumer")
            }
        }

        /// The messages asynchronous sequence was terminated.
        /// Stop polling for new messages.
        mutating func messageSequenceTerminated() {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Call to \(#function) before setUpConnection() was invoked")
            case .consuming(let client, let messagesSequenceState):
                switch messagesSequenceState {
                case .suspended, .open:
                    self.state = .consuming(client: client, messagesSequenceState: .finished)
                case .finished:
                    fatalError("messageSequenceTerminated() must not be invoked more than once")
                }
            case .finishing, .finished:
                break
            }
        }

        /// ``KafkaConsumerMessages``'s back pressure mechanism asked us to produce more messages.
        mutating func produceMore() {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                break // This case can be triggered by `NIOAsyncSequenceProducerDelegate`, ignore
            case .consuming(let client, let messagesSequenceState):
                switch messagesSequenceState {
                case .open, .finished:
                    break
                case .suspended(let source):
                    self.state = .consuming(client: client, messagesSequenceState: .open(source: source))
                }
            case .finishing, .finished:
                break
            }
        }

        /// ``KafkaConsumerMessages``'s back pressure mechanism asked us to temporarily stop producing messages.
        mutating func stopProducing() {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Call to \(#function) before setUpConnection() was invoked")
            case .consuming(let client, let messagesSequenceState):
                switch messagesSequenceState {
                case .suspended, .finished:
                    break
                case .open(let source):
                    self.state = .consuming(client: client, messagesSequenceState: .suspended(source: source))
                }
            case .finishing, .finished:
                break
            }
        }

        /// Action to take when wanting to store a message offset (to be auto-committed by `librdkafka`).
        enum StoreOffsetAction {
            /// Store the message offset with the given `client`.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case storeOffset(client: RDKafkaClient)
            /// The consumer is in the process of `.finishing` or even `.finished`.
            /// Stop yielding new elements and terminate the asynchronous sequence.
            case terminateConsumerSequence
        }

        /// Get action to take when wanting to store a message offset (to be auto-committed by `librdkafka`).
        func storeOffset() -> StoreOffsetAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before committing offsets")
            case .consuming(let client, _):
                return .storeOffset(client: client)
            case .finishing, .finished:
                return .terminateConsumerSequence
            }
        }

        /// Action to be taken when wanting to do a commit.
        enum CommitAction {
            /// Do a commit.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case commit(
                client: RDKafkaClient
            )
            /// Throw an error. The ``KafkaConsumer`` is closed.
            case throwClosedError
        }

        /// Get action to be taken when wanting to do a commit.
        /// - Returns: The action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        func commit() -> CommitAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before committing offsets")
            case .consuming(let client, _):
                return .commit(client: client)
            case .finishing, .finished:
                return .throwClosedError
            }
        }

        /// Action to be taken when wanting to do close the consumer.
        enum FinishAction {
            /// Shut down the ``KafkaConsumer``.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case triggerGracefulShutdown(
                client: RDKafkaClient
            )
            /// Shut down the ``KafkaConsumer`` and finish the given `source` object.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case triggerGracefulShutdownAndFinishSource(
                client: RDKafkaClient,
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
            case .consuming(let client, let messagesSequenceState):
                self.state = .finishing(client: client)

                switch messagesSequenceState {
                case .open(let source):
                    return .triggerGracefulShutdownAndFinishSource(
                        client: client,
                        source: source
                    )
                case .suspended(let source):
                    return .triggerGracefulShutdownAndFinishSource(
                        client: client,
                        source: source
                    )
                case .finished:
                    return .triggerGracefulShutdown(client: client)
                }
            case .finishing, .finished:
                return nil
            }
        }
    }
}
