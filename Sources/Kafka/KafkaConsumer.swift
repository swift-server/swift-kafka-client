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

// MARK: - KafkaConsumerCloseOnTerminate

/// `NIOAsyncSequenceProducerDelegate` that terminates the closes the producer when
/// `didTerminate()` is invoked.
internal struct KafkaConsumerCloseOnTerminate: Sendable {
    let isMessageSequence: Bool
    let stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>
}

extension KafkaConsumerCloseOnTerminate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        return // No back pressure
    }

    func didTerminate() {
        let seq = self.stateMachine.withLockedValue { $0.messageSequenceTerminated(isMessageSequence: isMessageSequence) }
        seq?.finish()
    }
}

// MARK: - KafkaConsumerEvents

/// `AsyncSequence` implementation for handling ``KafkaConsumerEvent``s emitted by Kafka.
public struct KafkaConsumerEvents: Sendable, AsyncSequence {
    public typealias Element = KafkaConsumerEvent
    typealias BackPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, BackPressureStrategy, KafkaConsumerCloseOnTerminate>
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
//    typealias BackPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure
    typealias WrappedSequence = NIOThrowingAsyncSequenceProducer<
        Element,
        Error,
        NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark,
//        NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure,
        KafkaConsumerCloseOnTerminate
    >
    let wrappedSequence: WrappedSequence

    /// `AsynceIteratorProtocol` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
    public struct AsyncIterator: AsyncIteratorProtocol {
        let stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>
        var wrappedIterator: WrappedSequence.AsyncIterator?

        public mutating func next() async throws -> Element? {
            repeat {
                guard let element = try await self.wrappedIterator?.next() else {
                    self.deallocateIterator()
                    return nil
                }
                
                let action = self.stateMachine.withLockedValue { $0.storeOffset() }
                switch action {
                case .storeOffset(let client):
                    do {
                        try client.storeMessageOffset(element)
                    } catch {
                        self.deallocateIterator()
                        throw error
                    }
                }
                if element.eof {
                    continue
                }
                return element
            } while true
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
        KafkaConsumerMessage,
        Error,
        NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark,
//        NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure,
        KafkaConsumerCloseOnTerminate
    >
    typealias ProducerEvents = NIOAsyncSequenceProducer<
        KafkaConsumerEvent,
        NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure,
        KafkaConsumerCloseOnTerminate
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
        logger: Logger,
        eventSource: ProducerEvents.Source? = nil
    ) throws {
        self.configuration = configuration
        self.stateMachine = stateMachine
        self.logger = logger
                // TODO:+ [.rebalance]
        let sourceAndSequence = NIOThrowingAsyncSequenceProducer.makeSequence(
            elementType: KafkaConsumerMessage.self,
            backPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark(lowWatermark: 50, highWatermark: 100),
//            backPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure(),
            delegate: KafkaConsumerCloseOnTerminate(isMessageSequence: true, stateMachine: self.stateMachine)
        )

        self.messages = KafkaConsumerMessages(
            stateMachine: self.stateMachine,
            wrappedSequence: sourceAndSequence.sequence
        )

        self.stateMachine.withLockedValue {
            $0.initialize(
                client: client,
                source: sourceAndSequence.source,
                eventSource: eventSource
            )
        }

        // Forward main queue events to the consumer queue.
        try client.pollSetConsumer()

        switch configuration.consumptionStrategy._internal {
        case .partition(topic: let topic, partition: let partition, offset: let offset):
            try self.assign(topic: topic, partition: partition, offset: offset)
        case .group(groupID: _, topics: let topics):
            try self.subscribe(topics: topics)
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

        var subscribedEvents: [RDKafkaEvent] = [.log, .fetch, .error]
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

        var subscribedEvents: [RDKafkaEvent] = [.log, .fetch, .error]
        // Only listen to offset commit events when autoCommit is false
        if configuration.isAutoCommitEnabled == false {
            subscribedEvents.append(.offsetCommit)
        }
        if configuration.statisticsInterval != .disable {
            subscribedEvents.append(.statistics)
        }
        if configuration.listenForRebalance {
            subscribedEvents.append(.rebalance)
        }

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger
        )

        let sourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: KafkaConsumerEvent.self,
            backPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure(),
            delegate: KafkaConsumerCloseOnTerminate(isMessageSequence: false, stateMachine: stateMachine)
        )

        let eventsSequence = KafkaConsumerEvents(wrappedSequence: sourceAndSequence.sequence)

        let consumer = try KafkaConsumer(
            client: client,
            stateMachine: stateMachine,
            configuration: configuration,
            logger: logger,
            eventSource: sourceAndSequence.source
        )
        
        return (consumer, eventsSequence)
    }

    /// Subscribe to the given list of `topics`.
    /// The partition assignment happens automatically using `KafkaConsumer`'s consumer group.
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Throws: A ``KafkaError`` if subscribing to the topic list failed.
    private func subscribe(topics: [String]) throws {
        let action = self.stateMachine.withLockedValue { $0.setUpConnection() }
        if topics.isEmpty {
            return
        }
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
            assignment.setOffset(topic: topic, partition: partition, offset: offset)
            try client.assign(topicPartitionList: assignment)
        }
    }
    
    /// Subscribe to the given list of `topics`.
    /// The partition assignment happens automatically using `KafkaConsumer`'s consumer group.
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Throws: A ``KafkaError`` if subscribing to the topic list failed.
    public func subscribeTopics(topics: [String]) throws {
        if topics.isEmpty {
            return
        }
        let client = try self.stateMachine.withLockedValue { try $0.client() }
        let subscription = RDKafkaTopicPartitionList()
        for topic in topics {
            subscription.add(topic: topic, partition: KafkaPartition.unassigned)
        }
        try client.subscribe(topicPartitionList: subscription)
    }
        
    
    public func assign(_ list: KafkaTopicList?) throws {
        let action = self.stateMachine.withLockedValue { $0.seekOrRebalance() }
        switch action {
        case .allowed(let client):
            try client.assign(topicPartitionList: list?.list)
        case .denied(let err):
            throw KafkaError.client(reason: err)
        }
    }
    
    public func incrementalAssign(_ list: KafkaTopicList) throws {
        let action = self.stateMachine.withLockedValue { $0.seekOrRebalance() }
        switch action {
        case .allowed(let client):
            try client.incrementalAssign(topicPartitionList: list.list)
        case .denied(let err):
            throw KafkaError.client(reason: err)
        }
    }
    
    public func incrementalUnassign(_ list: KafkaTopicList) throws {
        let action = self.stateMachine.withLockedValue { $0.seekOrRebalance() }
        switch action {
        case .allowed(let client):
            try client.incrementalUnassign(topicPartitionList: list.list)
        case .denied(let err):
            throw KafkaError.client(reason: err)
        }
    }
    
    // TODO: add docc: timeout = 0 -> async (no errors reported)
    public func seek(_ list: KafkaTopicList, timeout: Duration) async throws {
        let action = self.stateMachine.withLockedValue { $0.seekOrRebalance() }
        switch action {
        case .allowed(let client):
            try await client.seek(topicPartitionList: list.list, timeout: timeout)
        case .denied(let err):
            throw KafkaError.client(reason: err)
        }
    }

    public func seek(_ list: KafkaTopicList) throws {
        let action = self.stateMachine.withLockedValue { $0.seekOrRebalance() }
        switch action {
        case .allowed(let client):
            try client.seek(topicPartitionList: list.list)
        case .denied(let err):
            throw KafkaError.client(reason: err)
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
        var events = [RDKafkaClient.KafkaEvent]()
        var maxEvents = 100
        var pollInterval = self.configuration.pollInterval
        var producing = true
        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextPollLoopAction() }
            switch nextAction {
            case .pollForAndYieldMessage(let client, let source, let eventSource):
                if !producing {
                    maxEvents = 1
                } else {
                    maxEvents = 100
                }
                let shouldSleep = client.eventPoll(events: &events, maxEvents: &maxEvents)
                for event in events {
                    switch event {
                    case .consumerMessages(let result):
                        switch result {
                        case .success(let message):
                            // We do not support back pressure, we can ignore the yield result
                            let result = source.yield(message)
                            switch result {
                            case .stopProducing:
                                producing = false
                            case .produceMore:
                                producing = true
                            case .dropped:
                                break // ignore, sequence terminated
                            }
                        case .failure(let error):
                            source.finish()
                            eventSource?.finish()
                            throw error
                        }
                    case .statistics(let statistics):
                        _ = eventSource?.yield(.statistics(statistics))
                    case .rebalance(let rebalance):
                        self.logger.info("rebalance received \(rebalance)")
                        _ = eventSource?.yield(.rebalance(rebalance))
                    default:
                        break // Ignore
                    }
                }
                logger.trace("Processed \(events.count) shouldSleep: \(shouldSleep), pollInterval: \(pollInterval), maxEvents: \(maxEvents)")
                /*if !producing {
                    try await Task.sleep(for: configuration.maximumPollInterval - .milliseconds(100))
                }
                else */if shouldSleep || !producing {
                    pollInterval = min(self.configuration.pollInterval, pollInterval * 2)
                    try await Task.sleep(for: pollInterval)
                } else {
                    pollInterval = max(pollInterval / 3, .microseconds(1))
                    await Task.yield()
                }
            case .pollWithoutYield(let client):
                // Ignore poll result.
                // We are just polling to serve any remaining events queued inside of `librdkafka`.
                // All remaining queued consumer messages will get dropped and not be committed (marked as read).
                //let events =
                let shouldSleep = client.eventPoll(events: &events, maxEvents: &maxEvents)
                for event in events {
                    switch event {
                    case .rebalance(let type):
                        switch type {
                        case .assign(let proto, let list),
                             .revoke(let proto, let list),
                             .error(let proto, let list, _):
                            if proto == .cooperative {
                                try self.incrementalUnassign(list)
                            } else {
                                try self.assign(nil)
                            }
                        }
                    default:
                        continue
                    }
                }
//                logger.info("Processed without yield \(events.count) shouldSleep: \(true), pollInterval: \(pollInterval)")
                
                if shouldSleep {
                    pollInterval = min(self.configuration.pollInterval, pollInterval * 2)
                    try await Task.sleep(for: pollInterval)
                } else {
                    pollInterval = max(pollInterval / 2, .milliseconds(1))
                    await Task.yield()
                }
//                try await Task.sleep(for: self.configuration.pollInterval)
            case .terminatePollLoop:
                return
            }
        }
    }

    /// Mark all messages up to the passed message in the topic as read.
    ///
    /// This method is only used for manual offset management.
    ///
    /// - Warning: This method fails if the ``KafkaConsumerConfiguration/isAutoCommitEnabled`` configuration property is set to `true` (default).
    ///
    /// - Parameters:
    ///     - message: Last received message that shall be marked as read.
    /// - Throws: A ``KafkaError`` if committing failed.
    public func commitSync(_ message: KafkaConsumerMessage) async throws {
        let action = self.stateMachine.withLockedValue { $0.commitSync() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        case .commitSync(let client):
            guard self.configuration.isAutoCommitEnabled == false else {
                throw KafkaError.config(reason: "Committing manually only works if isAutoCommitEnabled set to false")
            }

            try await client.commitSync(message)
        }
    }

    /// This function is used to gracefully shut down a Kafka consumer client.
    ///
    /// - Note: Invoking this function is not always needed as the ``KafkaConsumer``
    /// will already shut down when consumption of the ``KafkaConsumerMessages`` has ended.
    private func triggerGracefulShutdown() {
        let action = self.stateMachine.withLockedValue { $0.finish() }
        switch action {
        case .triggerGracefulShutdown(let client):
            self._triggerGracefulShutdown(
                client: client,
                logger: self.logger
            )
        case .triggerGracefulShutdownAndFinishSource(let client, let source, let eventSource):
            source.finish()
            eventSource?.finish()
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

    func client() throws -> RDKafkaClient {
        return try self.stateMachine.withLockedValue { try $0.client() }
    }
}

// MARK: - KafkaConsumer + StateMachine

extension KafkaConsumer {
    /// State machine representing the state of the ``KafkaConsumer``.
    struct StateMachine: Sendable {
        /// A logger.
        let logger: Logger

        /// The state of the ``StateMachine``.
        enum State: Sendable {
            /// The state machine has been initialized with init() but is not yet Initialized
            /// using `func initialize()` (required).
            case uninitialized
            /// We are in the process of initializing the ``KafkaConsumer``,
            /// though ``subscribe()`` / ``assign()`` have not been invoked.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            /// - Parameter eventSource: ``NIOAsyncSequenceProducer/Source`` used for yielding new events.
            case initializing(
                client: RDKafkaClient,
                source: Producer.Source,
                eventSource: ProducerEvents.Source?
            )
            /// The ``KafkaConsumer`` is consuming messages.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter eventSource: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case consuming(
                client: RDKafkaClient,
                source: Producer.Source,
                eventSource: ProducerEvents.Source?
            )
            /// Consumer is still running but the messages asynchronous sequence was terminated.
            /// All incoming messages will be dropped.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case consumptionStopped(client: RDKafkaClient)
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
            source: Producer.Source,
            eventSource: ProducerEvents.Source?
        ) {
            guard case .uninitialized = self.state else {
                fatalError("\(#function) can only be invoked in state .uninitialized, but was invoked in state \(self.state)")
            }
            self.state = .initializing(
                client: client,
                source: source,
                eventSource: eventSource
            )
        }

        /// Action to be taken when wanting to poll for a new message.
        enum PollLoopAction {
            /// Poll for a new ``KafkaConsumerMessage``.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case pollForAndYieldMessage(
                client: RDKafkaClient,
                source: Producer.Source,
                eventSource: ProducerEvents.Source?
            )
            /// The ``KafkaConsumer`` stopped consuming messages or
            /// is in the process of shutting down.
            /// Poll to serve any queued events and commit outstanding state to the broker.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case pollWithoutYield(client: RDKafkaClient)
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
            case .consuming(let client, let source, let eventSource):
                return .pollForAndYieldMessage(client: client, source: source, eventSource: eventSource)
            case .consumptionStopped(let client):
                return .pollWithoutYield(client: client)
            case .finishing(let client):
                if client.isConsumerClosed {
                    self.state = .finished
                    return .terminatePollLoop
                } else {
                    return .pollWithoutYield(client: client)
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
            case .initializing(let client, let source, let eventSource):
                self.state = .consuming(
                    client: client,
                    source: source,
                    eventSource: eventSource
                )
                return .setUpConnection(client: client)
            case .consuming, .consumptionStopped, .finishing, .finished:
                fatalError("\(#function) should only be invoked upon initialization of KafkaConsumer")
            }
        }

        /// The messages asynchronous sequence was terminated.
        /// All incoming messages will be dropped.
        mutating func messageSequenceTerminated(isMessageSequence: Bool) -> ProducerEvents.Source? {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Call to \(#function) before setUpConnection() was invoked")
            case .consumptionStopped:
                if isMessageSequence {
                    fatalError("messageSequenceTerminated() must not be invoked more than once")
                }
            case .consuming(let client, let source, let eventSource):
                // only move to stopping if messages sequence was finished
                if isMessageSequence {
                    self.state = .consumptionStopped(client: client)
                    // If message sequence is being terminated, it means class deinit is called
                    // see `messages` field, it is last change to call finish for `eventSource`
                    // but we cannot do it under lock => return to make it outside
                    return eventSource
                }
                else {
                    // Messages are still consuming, only event source was finished
                    // Ok, probably, noone wants to listen to events,
                    // though it might be very bad for rebalancing
                    self.state = .consuming(client: client, source: source, eventSource: nil)
                }
            case .finishing, .finished:
                break
            }
            return nil
        }

        /// Action to take when wanting to store a message offset (to be auto-committed by `librdkafka`).
        enum StoreOffsetAction {
            /// Store the message offset with the given `client`.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case storeOffset(client: RDKafkaClient)
        }

        /// Get action to take when wanting to store a message offset (to be auto-committed by `librdkafka`).
        func storeOffset() -> StoreOffsetAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before committing offsets")
            case .consumptionStopped:
                fatalError("Cannot store offset when consumption has been stopped")
            case .consuming(let client, _, _):
                return .storeOffset(client: client)
            case .finishing(let client):
                return .storeOffset(client: client)
            case .finished:
                fatalError("\(#function) invoked while still in state \(self.state)")
            }
        }

        /// Action to be taken when wanting to do a synchronous commit.
        enum CommitSyncAction {
            /// Do a synchronous commit.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case commitSync(
                client: RDKafkaClient
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
            case .consumptionStopped:
                fatalError("Cannot commit when consumption has been stopped")
            case .consuming(let client, _, _):
                return .commitSync(client: client)
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
                source: Producer.Source,
                eventSource: ProducerEvents.Source?
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
            case .consuming(let client, let source, let eventSource):
                self.state = .finishing(client: client)
                return .triggerGracefulShutdownAndFinishSource(
                    client: client,
                    source: source,
                    eventSource: eventSource
                )
            case .consumptionStopped(let client):
                self.state = .finishing(client: client)
                return .triggerGracefulShutdown(client: client)
            case .finishing, .finished:
                return nil
            }
        }
        
        enum RebalanceAction {
            /// Rebalance is still possible
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case allowed(
                client: RDKafkaClient
            )
            /// Throw an error. The ``KafkaConsumer`` is closed.
            case denied(error: String)
        }

        
        func seekOrRebalance() -> RebalanceAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before committing offsets")
            case .consumptionStopped(let client),
                 .consuming(let client, _, _),
                 .finishing(let client):
                return .allowed(client: client)
            case .finished:
                return .denied(error: "Cannot perform reblance actions, consumer stopped")
            }
        }


        func client() throws -> RDKafkaClient {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing(let client, _, _):
                return client
            case .consuming(let client, _, _):
                return client
            case .consumptionStopped(let client):
                return client
            case .finishing(let client):
                return client
            case .finished:
                throw KafkaError.client(reason: "Client is stopped")
            }
        }
    }
}
