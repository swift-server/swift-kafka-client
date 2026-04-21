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

import Dispatch
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
        private let queue: DispatchQueueTaskExecutor

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
            self.queue = DispatchQueueTaskExecutor(
                DispatchQueue(label: "com.swift-server.swift-kafka.message-consumer")
            )
        }

        public func next() async throws -> Element? {
            while !Task.isCancelled {
                let action = self.stateMachineHolder.stateMachine.withLockedValue { $0.nextConsumerPollLoopAction() }

                switch action {
                case .poll(let client):
                    // Attempt to fetch a message synchronously. Bail
                    // immediately if no message is waiting for us.
                    if let message = try client.consumerPoll() {
                        return message
                    }

                    if #available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *) {
                        // Wait on a separate thread for the next message.
                        // The call below will block for `pollInterval`.
                        if let message = try await withTaskExecutorPreference(
                            queue,
                            operation: { try client.consumerPoll(for: Int32(self.pollInterval.inMilliseconds)) }
                        ) {
                            return message
                        }
                    } else {
                        // No messages. Sleep a little.
                        try await Task.sleep(for: self.pollInterval)
                    }
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

/// Can be used to consume messages from a Kafka cluster.
public final class KafkaConsumer: Sendable, Service {
    typealias ConsumerEventsProducer = NIOAsyncSequenceProducer<
        KafkaConsumerEvent,
        NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure,
        KafkaConsumerEventsDelegate
    >

    /// The configuration object of the consumer client.
    private let config: KafkaConsumerConfig
    /// A logger.
    private let logger: Logger
    /// State of the `KafkaConsumer`.
    /// - Important: Must be declared BEFORE `rebalanceContext` so that `RDKafkaClient`
    ///   (held inside the state machine) is destroyed first during deinit. This ensures
    ///   `rd_kafka_destroy` stops all librdkafka threads before the `RebalanceContext`
    ///   (which holds the C callback's unretained pointer target) is deallocated.
    private let stateMachine: NIOLockedValueBox<StateMachine>
    /// Source for yielding consumer events (rebalance, etc.). `nil` when created without events.
    private let eventsSource: ConsumerEventsProducer.Source?
    /// Context for the C rebalance callback. Must outlive the `RDKafkaClient` because the
    /// C callback holds an unretained pointer to it.
    /// - Important: Must be declared AFTER `stateMachine` — see ordering note above.
    private let rebalanceContext: RebalanceContext

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
    ///     - config: The ``KafkaConsumerConfig`` for configuring the ``KafkaConsumer``.
    ///     - rebalanceContext: The context for the C rebalance callback.
    ///     - logger: A logger.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    private init(
        client: RDKafkaClient,
        stateMachine: NIOLockedValueBox<StateMachine>,
        config: KafkaConsumerConfig,
        rebalanceContext: RebalanceContext,
        logger: Logger,
        eventsSource: ConsumerEventsProducer.Source? = nil
    ) throws {
        self.config = config
        self.stateMachine = stateMachine
        self.logger = logger
        self.eventsSource = eventsSource
        self.rebalanceContext = rebalanceContext

        self.messages = KafkaConsumerMessages(
            stateMachine: self.stateMachine,
            pollInterval: config.pollInterval
        )

        self.stateMachine.withLockedValue {
            $0.initialize(
                client: client,
                rebalanceContext: rebalanceContext
            )
        }
    }

    /// Initialize a new ``KafkaConsumer``.
    ///
    /// This creates a consumer that does not listen to any events other than consumer messages.
    ///
    /// - Parameters:
    ///     - config: The ``KafkaConsumerConfig`` for configuring the ``KafkaConsumer``.
    ///     - logger: A logger.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    public convenience init(
        config: KafkaConsumerConfig,
        logger: Logger
    ) throws {
        var subscribedEvents: [RDKafkaEvent] = [.log, .rebalance, .error]
        let isAutoCommitEnabled = config.enableAutoCommit ?? true
        if !isAutoCommitEnabled {
            subscribedEvents.append(.offsetCommit)
        }
        if config.metrics.enabled {
            subscribedEvents.append(.statistics)
        }

        let rebalanceContext = RebalanceContext(logger: logger)

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: config.config,
            events: subscribedEvents,
            logger: logger,
            rebalanceContext: rebalanceContext
        )

        let stateMachine = NIOLockedValueBox(StateMachine())

        try self.init(
            client: client,
            stateMachine: stateMachine,
            config: config,
            rebalanceContext: rebalanceContext,
            logger: logger
        )
    }

    /// Initialize a new ``KafkaConsumer`` and a ``KafkaConsumerEvents`` asynchronous sequence.
    ///
    /// Use the asynchronous sequence to consume events.
    ///
    /// - Important: When the asynchronous sequence is deinited the consumer will be shut down and disallowed from sending more messages.
    /// Additionally, make sure to consume the asynchronous sequence otherwise the events will be buffered in memory indefinitely.
    ///
    /// - Parameters:
    ///     - config: The ``KafkaConsumerConfig`` for configuring the ``KafkaConsumer``.
    ///     - logger: A logger.
    /// - Returns: A tuple containing the created ``KafkaConsumer`` and the ``KafkaConsumerEvents``
    /// `AsyncSequence` used for receiving message events.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    public static func makeConsumerWithEvents(
        config: KafkaConsumerConfig,
        logger: Logger
    ) throws -> (KafkaConsumer, KafkaConsumerEvents) {
        var subscribedEvents: [RDKafkaEvent] = [.log, .rebalance, .error]
        let isAutoCommitEnabled = config.enableAutoCommit ?? true
        if !isAutoCommitEnabled {
            subscribedEvents.append(.offsetCommit)
        }
        if config.metrics.enabled {
            subscribedEvents.append(.statistics)
        }

        let rebalanceContext = RebalanceContext(logger: logger)

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: config.config,
            events: subscribedEvents,
            logger: logger,
            rebalanceContext: rebalanceContext
        )

        let stateMachine = NIOLockedValueBox(StateMachine())

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

        let consumer = try KafkaConsumer(
            client: client,
            stateMachine: stateMachine,
            config: config,
            rebalanceContext: rebalanceContext,
            logger: logger,
            eventsSource: sourceAndSequence.source
        )

        let eventsSequence = KafkaConsumerEvents(wrappedSequence: sourceAndSequence.sequence)
        return (consumer, eventsSequence)
    }

    @available(*, deprecated, message: "Use init(config:logger:) instead")
    public convenience init(
        configuration: KafkaConsumerConfiguration,
        logger: Logger
    ) throws {
        try self.init(
            config: configuration.asKafkaConsumerConfig,
            logger: logger
        )
    }

    @available(*, deprecated, message: "Use makeConsumerWithEvents(config:logger:) instead")
    public static func makeConsumerWithEvents(
        configuration: KafkaConsumerConfiguration,
        logger: Logger
    ) throws -> (KafkaConsumer, KafkaConsumerEvents) {
        try Self.makeConsumerWithEvents(
            config: configuration.asKafkaConsumerConfig,
            logger: logger
        )
    }

    // MARK: - Subscription Management

    /// Subscribe to the given list of topics.
    ///
    /// This replaces any previous subscription. The partition assignment happens
    /// automatically using the consumer's consumer group. Topic names prefixed
    /// with `^` are treated as regular expressions. Passing an empty array is
    /// equivalent to calling ``unsubscribe()``.
    ///
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Throws: A ``KafkaError`` if the consumer is closed or subscribing failed.
    public func subscribe(topics: [String]) throws {
        guard !topics.isEmpty else {
            try self.unsubscribe()
            return
        }

        let action = self.stateMachine.withLockedValue { $0.withClientForSubscription() }
        switch action {
        case .client(let client):
            let subscription = RDKafkaTopicPartitionList()
            for topic in topics {
                subscription.add(
                    topic: topic,
                    partition: KafkaPartition.unassigned
                )
            }
            try client.subscribe(topicPartitionList: subscription)
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Consumer is closed")
        }
    }

    /// Unsubscribe from the current subscription.
    ///
    /// Clears all topic subscriptions. The consumer leaves the consumer group
    /// and stops receiving messages. This triggers a rebalance event.
    /// Can re-subscribe later with ``subscribe(topics:)``.
    ///
    /// - Throws: A ``KafkaError`` if the consumer is closed or unsubscribing failed.
    public func unsubscribe() throws {
        let action = self.stateMachine.withLockedValue { $0.withClientForSubscription() }
        switch action {
        case .client(let client):
            try client.unsubscribe()
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Consumer is closed")
        }
    }

    /// Get the current topic subscription.
    ///
    /// - Returns: An array of topic names or patterns the consumer is currently subscribed to.
    /// - Throws: A ``KafkaError`` if the consumer is closed or the query failed.
    public func subscribedTopics() throws -> [String] {
        let action = self.stateMachine.withLockedValue { $0.withClientForSubscription() }
        switch action {
        case .client(let client):
            return try client.subscription()
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Consumer is closed")
        }
    }

    /// Pause consumption for the given partitions.
    ///
    /// Paused partitions remain in the consumer group and continue heartbeating
    /// but will not return messages from ``messages``.
    ///
    /// - Parameter topicPartitions: The partitions to pause.
    /// - Throws: A ``KafkaError`` if the consumer is closed or pausing failed.
    public func pause(topicPartitions: [KafkaTopicPartition]) throws {
        let action = self.stateMachine.withLockedValue { $0.withClient() }
        switch action {
        case .client(let client):
            let tpl = RDKafkaTopicPartitionList()
            for tp in topicPartitions {
                tpl.add(topic: tp.topic, partition: tp.partition)
            }
            try client.pausePartitions(topicPartitionList: tpl)
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Consumer is closed")
        }
    }

    /// Resume consumption for the given partitions.
    ///
    /// - Parameter topicPartitions: The partitions to resume.
    /// - Throws: A ``KafkaError`` if the consumer is closed or resuming failed.
    public func resume(topicPartitions: [KafkaTopicPartition]) throws {
        let action = self.stateMachine.withLockedValue { $0.withClient() }
        switch action {
        case .client(let client):
            let tpl = RDKafkaTopicPartitionList()
            for tp in topicPartitions {
                tpl.add(topic: tp.topic, partition: tp.partition)
            }
            try client.resumePartitions(topicPartitionList: tpl)
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Consumer is closed")
        }
    }

    /// Internal startup subscription that transitions the state machine from `.initializing` to `.running`.
    /// Called once during `_run()` to set up the initial subscription from `consumptionStrategy`.
    private func initialSubscribe(topics: [String]) throws {
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
        if let strategy = self.config.consumptionStrategy?._internal {
            switch strategy {
            case .partition(groupID: _, let topic, let partition, let offset):
                try self.assign(topic: topic, partition: partition, offset: offset)
            case .group(groupID: _, let topics):
                try self.initialSubscribe(topics: topics)
            }
        } else {
            // No consumptionStrategy set — user will call subscribe(topics:) manually.
            // Transition state machine to .running so the event loop can start.
            let action = self.stateMachine.withLockedValue { $0.setUpConnection() }
            switch action {
            case .setUpConnection:
                break
            case .consumerClosed:
                throw KafkaError.connectionClosed(reason: "Consumer closed before run")
            }
        }
        try await self.eventRunLoop()
    }

    /// Run loop polling Kafka for new events.
    private func eventRunLoop() async throws {
        defer {
            // Guarantee the events stream is finished on ALL exit paths:
            // normal .terminatePollLoop, Task cancellation (CancellationError from Task.sleep),
            // or any other thrown error. Without this, `for await event in events` hangs forever.
            self.eventsSource?.finish()
        }

        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextEventPollLoopAction() }
            switch nextAction {
            case .pollForEvents(let client):
                // Event poll to serve any events queued inside of `librdkafka` (statistics, logs, offset commits).
                let events = client.consumerEventPoll()
                for event in events {
                    switch event {
                    case .statistics(let statistics):
                        self.config.metrics.update(with: statistics)
                    case .error(let kafkaError):
                        if let source = self.eventsSource {
                            _ = source.yield(.error(kafkaError))
                        }
                        self.logger.error(
                            "Kafka client error",
                            metadata: ["error": "\(kafkaError)"]
                        )
                    }
                }

                // Drain rebalance events from the callback context.
                // These were buffered by the C rebalance callback during rd_kafka_consumer_poll().
                let rebalanceEvents = self.rebalanceContext.drainEvents()
                for event in rebalanceEvents {
                    let kind: KafkaConsumerRebalance.Kind
                    switch event.kind {
                    case .assign:
                        kind = .assign
                    case .revoke:
                        kind = .revoke
                    case .error(let description):
                        kind = .error(description)
                    }

                    let rebalance = KafkaConsumerRebalance(
                        kind: kind,
                        partitions: event.partitions.map {
                            KafkaTopicPartition(topic: $0.topic, partition: KafkaPartition(rawValue: $0.partition))
                        }
                    )
                    if let source = self.eventsSource {
                        _ = source.yield(.rebalance(rebalance))
                    }
                    self.logger.info(
                        "Consumer rebalance",
                        metadata: [
                            "kind": "\(rebalance.kind)",
                            "partitions": "\(rebalance.partitions.map { "\($0.topic):\($0.partition)" })",
                        ]
                    )
                }

                try await Task.sleep(for: self.config.pollInterval)
            case .terminatePollLoop(let client):
                // Final drain: the close process may have completed (isConsumerClosed = true)
                // before we polled the shutdown revoke from mainQueue. Do one last poll+drain
                // so no rebalance events are lost.
                if let client {
                    self.finalDrain(client: client)
                }
                return
            }
        }
    }

    /// Final drain of the event queue and rebalance buffer before shutdown completes.
    ///
    /// Called after `isConsumerClosed` returns true. The close process
    /// (`rd_kafka_consumer_close_queue`) may have enqueued a final revoke on
    /// mainQueue and completed (`rkcg_terminated = true`) before the event loop
    /// had a chance to poll it. This one last poll+drain ensures that event
    /// is not lost.
    private func finalDrain(client: RDKafkaClient) {
        let events = client.consumerEventPoll()
        for event in events {
            switch event {
            case .statistics(let statistics):
                self.config.metrics.update(with: statistics)
            case .error(let kafkaError):
                if let source = self.eventsSource {
                    _ = source.yield(.error(kafkaError))
                }
                self.logger.error(
                    "Kafka client error",
                    metadata: ["error": "\(kafkaError)"]
                )
            }
        }

        let rebalanceEvents = self.rebalanceContext.drainEvents()
        for event in rebalanceEvents {
            let kind: KafkaConsumerRebalance.Kind
            switch event.kind {
            case .assign:
                kind = .assign
            case .revoke:
                kind = .revoke
            case .error(let description):
                kind = .error(description)
            }

            let rebalance = KafkaConsumerRebalance(
                kind: kind,
                partitions: event.partitions.map {
                    KafkaTopicPartition(topic: $0.topic, partition: KafkaPartition(rawValue: $0.partition))
                }
            )
            if let source = self.eventsSource {
                _ = source.yield(.rebalance(rebalance))
            }
            self.logger.info(
                "Consumer rebalance",
                metadata: [
                    "kind": "\(rebalance.kind)",
                    "partitions": "\(rebalance.partitions.map { "\($0.topic):\($0.partition)" })",
                ]
            )
        }
    }

    /// Store the offset of a consumed message in the local offset store.
    ///
    /// This is used for **at-least-once** delivery semantics. The typical pattern is:
    /// 1. Set `enableAutoOffsetStore` to `false` in the consumer configuration.
    /// 2. Keep `enableAutoCommit` as `true` (the default).
    /// 3. After successfully processing a message, call `storeOffset(_:)`.
    /// 4. The auto-commit timer will periodically commit stored offsets to the broker.
    ///
    /// This ensures that only offsets for **successfully processed** messages are committed.
    /// If the application crashes before calling `storeOffset`, the message will be
    /// re-delivered on the next consumer start (at-least-once).
    ///
    /// - Warning: This method fails if `enableAutoOffsetStore` is not set to `false`.
    ///
    /// - Parameter message: The message whose offset should be stored.
    /// - Throws: A ``KafkaError`` if storing the offset failed or the consumer is closed.
    public func storeOffset(_ message: KafkaConsumerMessage) throws {
        let action = self.stateMachine.withLockedValue { $0.withClient() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to store message offset on a closed consumer")
        case .client(let client):
            // enableAutoOffsetStore is Bool? — nil means the user did not set it,
            // so librdkafka's default (true) applies. We require an explicit `false`.
            guard self.config.enableAutoOffsetStore == false else {
                throw KafkaError.config(
                    reason: "storeOffset requires enableAutoOffsetStore to be set to false"
                )
            }

            try client.storeOffset(message)
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
        let action = self.stateMachine.withLockedValue { $0.withClient() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        case .client(let client):
            guard (self.config.enableAutoCommit ?? true) == false else {
                throw KafkaError.config(reason: "Committing manually only works if enableAutoCommit is set to false")
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
        let action = self.stateMachine.withLockedValue { $0.withClient() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        case .client(let client):
            guard (self.config.enableAutoCommit ?? true) == false else {
                throw KafkaError.config(reason: "Committing manually only works if enableAutoCommit is set to false")
            }

            try await client.commit(message)
        }
    }

    /// Retrieve the last-committed offsets for the given topic+partition pairs from the broker.
    ///
    /// This is useful for monitoring consumer lag and verifying that offsets have been
    /// successfully committed.
    ///
    /// - Parameters:
    ///   - topicPartitions: An array of ``KafkaTopicPartition`` to query.
    ///   - timeout: Maximum time to wait for broker response. Default: 5 seconds.
    /// - Returns: An array of ``KafkaTopicPartitionOffset``. The ``KafkaTopicPartitionOffset/offset``
    ///   is `nil` if no committed offset exists for that partition.
    /// - Throws: A ``KafkaError`` if the query failed or the consumer is closed.
    public func committed(
        topicPartitions: [KafkaTopicPartition],
        timeout: Duration = .milliseconds(5000)
    ) async throws -> [KafkaTopicPartitionOffset] {
        let action = self.stateMachine.withLockedValue { $0.withClient() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to query committed offsets on a closed consumer")
        case .client(let client):
            return try await client.committed(
                topicPartitions: topicPartitions,
                timeoutMilliseconds: Int32(timeout.inMilliseconds)
            )
        }
    }

    /// Retrieve the current positions (next offset to be fetched) for the given topic+partition pairs.
    ///
    /// The position reflects the consumer's in-memory position, which is the last consumed
    /// message's offset + 1. This is useful for computing consumer lag when compared with
    /// the committed offsets.
    ///
    /// - Parameter topicPartitions: An array of ``KafkaTopicPartition`` to query.
    /// - Returns: An array of ``KafkaTopicPartitionOffset``. The ``KafkaTopicPartitionOffset/offset``
    ///   is `nil` if no position is available for that partition.
    /// - Throws: A ``KafkaError`` if the query failed or the consumer is closed.
    public func position(
        topicPartitions: [KafkaTopicPartition]
    ) throws -> [KafkaTopicPartitionOffset] {
        let action = self.stateMachine.withLockedValue { $0.withClient() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to query consumer position on a closed consumer")
        case .client(let client):
            return try client.position(topicPartitions: topicPartitions)
        }
    }

    /// Check if the current partition assignment has been lost involuntarily.
    ///
    /// This is primarily useful when reacting to a rebalance event.
    /// When partitions are lost (e.g., because `max.poll.interval.ms` was exceeded),
    /// committing offsets for those partitions will fail because they may already
    /// be owned by another consumer in the group.
    ///
    /// - Returns: `true` if the current assignment is considered lost, `false` otherwise.
    /// - Throws: A ``KafkaError`` if the consumer is closed.
    public var isAssignmentLost: Bool {
        get throws {
            let action = self.stateMachine.withLockedValue { $0.withClient() }
            switch action {
            case .throwClosedError:
                throw KafkaError.connectionClosed(reason: "Tried to check assignment on a closed consumer")
            case .client(let client):
                return client.isAssignmentLost
            }
        }
    }

    /// Seek to specific offsets for the given partitions.
    ///
    /// This is useful for replaying messages or skipping ahead. The partitions
    /// must already be assigned to this consumer (via subscription or manual assignment).
    ///
    /// This call purges all pre-fetched messages for the given partitions.
    ///
    /// - Parameters:
    ///   - topicPartitionOffsets: An array of ``KafkaTopicPartitionOffset`` specifying where to seek.
    ///     The ``KafkaTopicPartitionOffset/offset`` must be non-`nil`.
    ///   - timeout: Maximum time to wait. Pass `.zero` for async (fire-and-forget).
    ///              Default: `.milliseconds(5000)`.
    /// - Throws: A ``KafkaError`` if the seek failed or the consumer is closed.
    public func seek(
        topicPartitionOffsets: [KafkaTopicPartitionOffset],
        timeout: Duration = .milliseconds(5000)
    ) async throws {
        let action = self.stateMachine.withLockedValue { $0.withClient() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to seek on a closed consumer")
        case .client(let client):
            try await client.seekPartitions(
                topicPartitionOffsets: topicPartitionOffsets,
                timeoutMilliseconds: Int32(timeout.inMilliseconds)
            )
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
            /// - Parameter rebalanceContext: The context for the C rebalance callback.
            case initializing(
                client: RDKafkaClient,
                rebalanceContext: RebalanceContext
            )
            /// The ``KafkaConsumer`` is consuming messages.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter rebalanceContext: The context for the C rebalance callback.
            case running(client: RDKafkaClient, rebalanceContext: RebalanceContext)
            /// The ``KafkaConsumer/triggerGracefulShutdown()`` has been invoked.
            /// We are now in the process of commiting our last state to the broker.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter rebalanceContext: The context for the C rebalance callback.
            case finishing(client: RDKafkaClient, rebalanceContext: RebalanceContext)
            /// The ``KafkaConsumer`` is closed.
            case finished
        }

        /// The current state of the StateMachine.
        var state: State = .uninitialized

        /// Delayed initialization of `StateMachine` as the `source` and the `pollClosure` are
        /// not yet available when the normal initialization occurs.
        mutating func initialize(
            client: RDKafkaClient,
            rebalanceContext: RebalanceContext
        ) {
            guard case .uninitialized = self.state else {
                fatalError(
                    "\(#function) can only be invoked in state .uninitialized, but was invoked in state \(self.state)"
                )
            }
            self.state = .initializing(
                client: client,
                rebalanceContext: rebalanceContext
            )
        }

        /// Action to be taken when wanting to poll for a new message.
        enum EventPollLoopAction {
            /// Serve any queued callbacks on the event queue.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case pollForEvents(client: RDKafkaClient)
            /// Terminate the poll loop.
            ///
            /// - Parameter client: Client for final drain (may be nil if state was already `.finished`).
            case terminatePollLoop(client: RDKafkaClient?)
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
            case .running(let client, _):
                return .pollForEvents(client: client)
            case .finishing(let client, _):
                if client.isConsumerClosed {
                    self.state = .finished
                    return .terminatePollLoop(client: client)
                } else {
                    return .pollForEvents(client: client)
                }
            case .finished:
                return .terminatePollLoop(client: nil)
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
            case .running(let client, _):
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
            case .initializing(let client, let rebalanceContext):
                self.state = .running(client: client, rebalanceContext: rebalanceContext)
                return .setUpConnection(client: client)
            case .running:
                fatalError("\(#function) should not be invoked more than once")
            case .finishing:
                fatalError("\(#function) should only be invoked when KafkaConsumer is running")
            case .finished:
                return .consumerClosed
            }
        }

        /// Action to be taken when needing access to the running client.
        enum ClientAction {
            /// The client is available.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case client(RDKafkaClient)
            /// Throw an error. The ``KafkaConsumer`` is closed.
            case throwClosedError
        }

        /// Get the running client for performing an operation that requires an active consumer.
        ///
        /// - Returns: The action to be taken.
        func withClient() -> ClientAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                return .throwClosedError
            case .running(let client, _):
                return .client(client)
            case .finishing, .finished:
                return .throwClosedError
            }
        }

        /// Get the client for subscription management operations.
        ///
        /// Unlike ``withClient()``, this accessor works in both `.initializing` and `.running`
        /// states, allowing subscribe/unsubscribe to be called at any time after consumer creation.
        ///
        /// - Returns: The action to be taken.
        func withClientForSubscription() -> ClientAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing(let client, _):
                return .client(client)
            case .running(let client, _):
                return .client(client)
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

        /// Get action to be taken when wanting to close the consumer.
        /// - Returns: The action to be taken, or `nil` if there is no action to be taken.
        mutating func finish() -> FinishAction? {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .initializing:
                // No poll loop is active in .initializing state. Since consumerClose()
                // requires an active poll loop to process broker responses, we skip it
                // and transition straight to .finished.
                self.state = .finished
                return nil
            case .running(let client, let rebalanceContext):
                self.state = .finishing(client: client, rebalanceContext: rebalanceContext)
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

        /// Returns the client if available, for cleanup purposes (e.g., final event drain).
        /// Unlike ``withClient()``, this does not fatalError in transitional states —
        /// it returns `nil` when no client is available.
        func clientForCleanup() -> RDKafkaClient? {
            switch self.state {
            case .uninitialized, .finished:
                return nil
            case .initializing(let client, _),
                .running(let client, _),
                .finishing(let client, _):
                return client
            }
        }
    }
}
