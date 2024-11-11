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
        return  // No back pressure
    }

    func didTerminate() {
        return  // We have to call poll for events anyway, nothing to do here
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
        AsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

// MARK: - KafkaConsumerMessages

/// `AsyncSequence` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
public struct KafkaConsumerMessages: Sendable, AsyncSequence {
    typealias LockedMachine = NIOLockedValueBox<KafkaConsumer.StateMachine>

    let stateMachine: LockedMachine
    let pollInterval: Duration

    public typealias Element = KafkaConsumerMessage

    /// `AsynceIteratorProtocol` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
    public struct AsyncIterator: AsyncIteratorProtocol {
        private let stateMachineHolder: MachineHolder
        let pollInterval: Duration

        private final class MachineHolder: Sendable {  // only for deinit
            let stateMachine: LockedMachine

            init(stateMachine: LockedMachine) {
                self.stateMachine = stateMachine
            }

            deinit {
                self.stateMachine.withLockedValue { $0.finishMessageConsumption() }
            }
        }

        init(stateMachine: LockedMachine, pollInterval: Duration) {
            self.stateMachineHolder = .init(stateMachine: stateMachine)
            self.pollInterval = pollInterval
        }

        public func next() async throws -> Element? {
            // swift-kafka-client issue: https://github.com/swift-server/swift-kafka-client/issues/165
            // Currently use Task.sleep() if no new messages, should use task executor preference when implemented:
            // https://github.com/apple/swift-evolution/blob/main/proposals/0417-task-executor-preference.md
            while !Task.isCancelled {
                let action = self.stateMachineHolder.stateMachine.withLockedValue { $0.nextConsumerPollLoopAction() }

                switch action {
                case .poll(let client):
                    if let message = try client.consumerPoll() {  // non-blocking call
                        return message
                    }
                    try await Task.sleep(for: self.pollInterval)
                case .suspendPollLoop:
                    try await Task.sleep(for: self.pollInterval)  // not started yet
                case .terminatePollLoop:
                    return nil
                }
            }
            return nil
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            stateMachine: self.stateMachine,
            pollInterval: self.pollInterval
        )
    }
}

// MARK: - KafkaConsumer

/// A ``KafkaConsumer `` can be used to consume messages from a Kafka cluster.
public final class KafkaConsumer: Sendable, Service {
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
    /// To listen to incoming messages, please subscribe to a list of topics using ``subscribe()``
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

        self.messages = KafkaConsumerMessages(
            stateMachine: self.stateMachine,
            pollInterval: configuration.pollInterval
        )

        self.stateMachine.withLockedValue {
            $0.initialize(
                client: client
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
        var subscribedEvents: [RDKafkaEvent] = [.log]
        // Only listen to offset commit events when autoCommit is false
        if configuration.isAutoCommitEnabled == false {
            subscribedEvents.append(.offsetCommit)
        }
        if configuration.metrics.enabled {
            subscribedEvents.append(.statistics)
        }

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger
        )

        let stateMachine = NIOLockedValueBox(StateMachine())

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
        var subscribedEvents: [RDKafkaEvent] = [.log]
        // Only listen to offset commit events when autoCommit is false
        if configuration.isAutoCommitEnabled == false {
            subscribedEvents.append(.offsetCommit)
        }
        if configuration.metrics.enabled {
            subscribedEvents.append(.statistics)
        }

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger
        )

        let stateMachine = NIOLockedValueBox(StateMachine())

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
            finishOnDeinit: true,
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
        case .consumerClosed:
            throw KafkaError.connectionClosed(reason: "Consumer deinitialized before setup")
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
        case .consumerClosed:
            throw KafkaError.connectionClosed(reason: "Consumer deinitialized before setup")
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
        case .partition(groupID: _, let topic, let partition, let offset):
            try self.assign(topic: topic, partition: partition, offset: offset)
        case .group(groupID: _, let topics):
            try self.subscribe(topics: topics)
        }
        try await self.eventRunLoop()
    }

    /// Run loop polling Kafka for new events.
    private func eventRunLoop() async throws {
        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextEventPollLoopAction() }
            switch nextAction {
            case .pollForEvents(let client):
                // Event poll to serve any events queued inside of `librdkafka`.
                let events = client.eventPoll()
                for event in events {
                    switch event {
                    case .statistics(let statistics):
                        self.configuration.metrics.update(with: statistics)
                    default:
                        break
                    }
                }
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
        /// The state of the ``StateMachine``.
        enum State: Sendable {
            /// The state machine has been initialized with init() but is not yet Initialized
            /// using `func initialize()` (required).
            case uninitialized
            /// We are in the process of initializing the ``KafkaConsumer``,
            /// though ``subscribe()`` / ``assign()`` have not been invoked.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: The source for yielding new messages.
            case initializing(
                client: RDKafkaClient
            )
            /// The ``KafkaConsumer`` is consuming messages.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter state: State of the event loop fetching new consumer messages.
            case running(client: RDKafkaClient)
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
            client: RDKafkaClient
        ) {
            guard case .uninitialized = self.state else {
                fatalError(
                    "\(#function) can only be invoked in state .uninitialized, but was invoked in state \(self.state)"
                )
            }
            self.state = .initializing(
                client: client
            )
        }

        /// Action to be taken when wanting to poll for a new message.
        enum EventPollLoopAction {
            /// Serve any queued callbacks on the event queue.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case pollForEvents(client: RDKafkaClient)
            /// Terminate the poll loop.
            case terminatePollLoop
        }

        /// Returns the next action to be taken when wanting to poll.
        /// - Returns: The next action to be taken when wanting to poll, or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func nextEventPollLoopAction() -> EventPollLoopAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
            case .running(let client):
                return .pollForEvents(client: client)
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

        /// Action to be taken when wanting to poll for a new message.
        enum ConsumerPollLoopAction {
            /// Poll for a new ``KafkaConsumerMessage``.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case poll(client: RDKafkaClient)
            /// Sleep for ``KafkaConsumerConfiguration/pollInterval``.
            case suspendPollLoop
            /// Terminate the poll loop.
            case terminatePollLoop
        }

        /// Returns the next action to be taken when wanting to poll.
        /// - Returns: The next action to be taken when wanting to poll, or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func nextConsumerPollLoopAction() -> ConsumerPollLoopAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                return .suspendPollLoop
            case .running(let client):
                return .poll(client: client)
            case .finishing, .finished:
                return .terminatePollLoop
            }
        }

        /// Action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        enum SetUpConnectionAction {
            /// Set up the connection through ``subscribe()`` or ``assign()``.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case setUpConnection(client: RDKafkaClient)
            /// The ``KafkaConsumer`` is closed.
            case consumerClosed
        }

        /// Get action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        ///
        /// - Returns: The action to be taken.
        mutating func setUpConnection() -> SetUpConnectionAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing(let client):
                self.state = .running(client: client)
                return .setUpConnection(client: client)
            case .running:
                fatalError("\(#function) should not be invoked more than once")
            case .finishing:
                fatalError("\(#function) should only be invoked when KafkaConsumer is running")
            case .finished:
                return .consumerClosed
            }
        }

        /// Action to be taken when wanting to do a commit.
        enum CommitAction {
            /// Do a commit.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case commit(client: RDKafkaClient)
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
                fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
            case .running(let client):
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
            case triggerGracefulShutdown(client: RDKafkaClient)
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
                fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
            case .running(let client):
                self.state = .finishing(client: client)
                return .triggerGracefulShutdown(client: client)
            case .finishing, .finished:
                return nil
            }
        }

        /// The ``KafkaConsumerMessages`` asynchronous sequence was terminated.
        mutating func finishMessageConsumption() {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                self.state = .finished
            case .running:
                self.state = .finished
            case .finishing, .finished:
                break
            }
        }
    }
}
