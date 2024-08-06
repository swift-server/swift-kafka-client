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
import Logging
import NIOConcurrencyHelpers
import NIOCore
import ServiceLifecycle

// MARK: - KafkaProducerCloseOnTerminate

/// `NIOAsyncSequenceProducerDelegate` that terminates the closes the producer when
/// `didTerminate()` is invoked.
internal struct KafkaProducerCloseOnTerminate: Sendable {
    let stateMachine: NIOLockedValueBox<KafkaProducer.StateMachine>
}

extension KafkaProducerCloseOnTerminate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        return // No back pressure
    }

    func didTerminate() {
        let action = self.stateMachine.withLockedValue { $0.stopConsuming() }
        switch action {
        case .finishSource(let source):
            source?.finish()
        case .none:
            break
        }
    }
}

// MARK: - KafkaProducerSharedSettings

internal protocol KafkaProducerSharedProperties: Sendable {
    /// If the ``isAutoCreateTopicsEnabled`` option is set to `true`,
    /// the broker will automatically generate topics when producing data to non-existent topics.
    /// The configuration specified in this ``KafkaTopicConfiguration`` will be applied to the newly created topic.
    /// Default: See default values of ``KafkaTopicConfiguration``
    var topicConfiguration: KafkaTopicConfiguration { get }

    /// The time between two consecutive polls.
    /// Effectively controls the rate at which incoming events are consumed.
    /// Default: `.milliseconds(100)`
    var pollInterval: Duration { get }

    /// Maximum timeout for flushing outstanding produce requests when the ``KafkaProducer`` is shutting down.
    /// Default: `10000`
    var flushTimeoutMilliseconds: Int { get }

    /// Interval for librdkafka statistics reports
    var metrics: KafkaConfiguration.ProducerMetrics { get }

    var dictionary: [String: String] { get }
}

extension KafkaProducerConfiguration: KafkaProducerSharedProperties {}
extension KafkaTransactionalProducerConfiguration: KafkaProducerSharedProperties {}

// MARK: - KafkaProducerEvents

/// `AsyncSequence` implementation for handling ``KafkaProducerEvent``s emitted by Kafka.
public struct KafkaProducerEvents: Sendable, AsyncSequence {
    public typealias Element = KafkaProducerEvent
    typealias BackPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, BackPressureStrategy, KafkaProducerCloseOnTerminate>
    let wrappedSequence: WrappedSequence

    /// `AsynceIteratorProtocol` implementation for handling ``KafkaProducerEvent``s emitted by Kafka.
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

// MARK: - KafkaProducer

/// Send messages to the Kafka cluster.
/// - Note: When messages get published to a non-existent topic, a new topic is created using the ``KafkaTopicConfiguration``
/// configuration object (only works if server has ``KafkaProducerConfiguration/isAutoCreateTopicsEnabled`` property set to `true`).
public final class KafkaProducer: Service, Sendable {
    typealias Producer = NIOAsyncSequenceProducer<
        KafkaProducerEvent,
        NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure,
        KafkaProducerCloseOnTerminate
    >

    /// State of the ``KafkaProducer``.
    private let stateMachine: NIOLockedValueBox<StateMachine>

    /// The configuration object of the producer client.
    private let configuration: KafkaProducerSharedProperties
    /// Topic configuration that is used when a new topic has to be created by the producer.
    private let topicConfiguration: KafkaTopicConfiguration

    // Private initializer, use factory method or convenience init to create KafkaProducer
    /// Initialize a new ``KafkaProducer``.
    ///
    /// - Parameter stateMachine: The ``KafkaProducer/StateMachine`` instance associated with the ``KafkaProducer``.///
    /// - Parameter configuration: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfiguration: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    private init(
        stateMachine: NIOLockedValueBox<KafkaProducer.StateMachine>,
        configuration: KafkaProducerSharedProperties,
        topicConfiguration: KafkaTopicConfiguration
    ) {
        self.stateMachine = stateMachine
        self.configuration = configuration
        self.topicConfiguration = topicConfiguration
    }

    /// Initialize a new ``KafkaProducer``.
    ///
    /// This creates a producer without listening for events.
    ///
    /// - Parameters:
    ///     - configuration: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    ///     - logger: A logger.
    /// - Returns: The newly created ``KafkaProducer``.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public convenience init(
        configuration: KafkaProducerConfiguration,
        logger: Logger
    ) throws {
        try self.init(configuration: configuration as KafkaProducerSharedProperties, logger: logger)
    }

    internal convenience init(
        configuration: KafkaProducerSharedProperties,
        logger: Logger
    ) throws {
        let stateMachine = NIOLockedValueBox(StateMachine(logger: logger))

        var subscribedEvents: [RDKafkaEvent] = [.log] // No .deliveryReport here!

        if configuration.metrics.enabled {
            subscribedEvents.append(.statistics)
        }

        let client = try RDKafkaClient.makeClient(
            type: .producer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger
        )

        stateMachine.withLockedValue {
            $0.initialize(
                client: client,
                source: nil
            )
        }

        self.init(
            stateMachine: stateMachine,
            configuration: configuration,
            topicConfiguration: configuration.topicConfiguration
        )
    }

    /// Initialize a new ``KafkaProducer`` and a ``KafkaProducerEvents`` asynchronous sequence.
    ///
    /// Use the asynchronous sequence to consume events.
    ///
    /// - Important: When the asynchronous sequence is deinited the producer will be shut down and disallowed from sending more messages.
    /// Additionally, make sure to consume the asynchronous sequence otherwise the events will be buffered in memory indefinitely.
    ///
    /// - Parameters:
    ///     - configuration: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    ///     - logger: A logger.
    /// - Returns: A tuple containing the created ``KafkaProducer`` and the ``KafkaProducerEvents``
    /// `AsyncSequence` used for receiving message events.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public static func makeProducerWithEvents(
        configuration: KafkaProducerConfiguration,
        logger: Logger
    ) throws -> (KafkaProducer, KafkaProducerEvents) {
        return try self.makeProducerWithEvents(configuration: configuration as (any KafkaProducerSharedProperties), logger: logger)
    }

    internal static func makeProducerWithEvents(
        configuration: KafkaProducerSharedProperties,
        topicConfig: KafkaTopicConfiguration = KafkaTopicConfiguration(),
        logger: Logger
    ) throws -> (KafkaProducer, KafkaProducerEvents) {
        let stateMachine = NIOLockedValueBox(StateMachine(logger: logger))

        var subscribedEvents: [RDKafkaEvent] = [.log, .deliveryReport]
        // Listen to statistics events when statistics enabled
        if configuration.metrics.enabled {
            subscribedEvents.append(.statistics)
        }

        let client = try RDKafkaClient.makeClient(
            type: .producer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger
        )

        let producer = KafkaProducer(
            stateMachine: stateMachine,
            configuration: configuration,
            topicConfiguration: configuration.topicConfiguration
        )

        // Note:
        // It's crucial to initialize the `sourceAndSequence` variable AFTER `client`.
        // This order is important to prevent the accidental triggering of `KafkaProducerCloseOnTerminate.didTerminate()`.
        // If this order is not met and `RDKafkaClient.makeClient()` fails,
        // it leads to a call to `stateMachine.stopConsuming()` while it's still in the `.uninitialized` state.
        let sourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: KafkaProducerEvent.self,
            backPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure(),
            finishOnDeinit: true,
            delegate: KafkaProducerCloseOnTerminate(stateMachine: stateMachine)
        )

        stateMachine.withLockedValue {
            $0.initialize(
                client: client,
                source: sourceAndSequence.source
            )
        }

        let eventsSequence = KafkaProducerEvents(wrappedSequence: sourceAndSequence.sequence)
        return (producer, eventsSequence)
    }

    /// Start the ``KafkaProducer``.
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
        var pollInterval = self.configuration.pollInterval
        var events = [RDKafkaClient.KafkaEvent]()
        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextPollLoopAction() }
            switch nextAction {
            case .pollWithoutYield(let client):
                // Drop any incoming events
                _ = client.eventPoll(events: &events)
            case .pollAndYield(let client, let source):
                let shouldSleep = client.eventPoll(events: &events)
                for event in events {
                    switch event {
                    case .deliveryReport(let reports):
                        // Ignore YieldResult as we don't support back pressure in KafkaProducer
                        _ = source?.yield(.deliveryReports(reports))
                    case .statistics(let statistics):
                        self.configuration.metrics.update(with: statistics)
                    default:
                        fatalError("Cannot cast \(event) to KafkaProducerEvent")
                    }
                }
                if shouldSleep {
                    pollInterval = min(self.configuration.pollInterval, pollInterval * 2)
                    try await Task.sleep(for: pollInterval)
                } else {
                    pollInterval = max(pollInterval / 3, .microseconds(1))
                    await Task.yield()
                }
            case .flushFinishSourceAndTerminatePollLoop(let client, let source):
                precondition(
                    0...Int(Int32.max) ~= self.configuration.flushTimeoutMilliseconds,
                    "Flush timeout outside of valid range \(0...Int32.max)"
                )
                defer { // we should finish source indefinetely of exception in client.flush()
                    source?.finish()
                }
                try await client.flush(timeoutMilliseconds: Int32(self.configuration.flushTimeoutMilliseconds))
                return
            case .terminatePollLoop:
                return
            }
        }
    }

    /// Method to shutdown the ``KafkaProducer``.
    ///
    /// This method flushes any buffered messages and waits until a callback is received for all of them.
    /// Afterwards, it shuts down the connection to Kafka and cleans any remaining state up.
    public func triggerGracefulShutdown() {
        self.stateMachine.withLockedValue { $0.finish() }
    }

    /// Send a ``KafkaProducerMessage`` to the Kafka cluster.
    ///
    /// This method does not wait until the message is sent and acknowledged by the cluster.
    /// Instead, it buffers the message and returns immediately.
    /// The message will be sent out with the next batch of messages.
    ///
    /// - Parameter message: The ``KafkaProducerMessage`` to send.
    /// - Returns: Unique ``KafkaProducerMessageID``matching the ``KafkaDeliveryReport/id`` property
    /// of the corresponding ``KafkaDeliveryReport``.
    /// - Throws: A ``KafkaError`` if sending the message failed.
    @discardableResult
    public func send<Key, Value>(_ message: KafkaProducerMessage<Key, Value>) throws -> KafkaProducerMessageID {
        let action = try self.stateMachine.withLockedValue { try $0.send() }
        switch action {
        case .send(let client, let newMessageID, let topicHandles):
            try client.produce(
                message: message,
                newMessageID: newMessageID,
                topicConfiguration: self.topicConfiguration,
                topicHandles: topicHandles
            )
            return KafkaProducerMessageID(rawValue: newMessageID)
        }
    }
    
    @discardableResult
    public func flush(timeout: Duration) async -> Bool {
        do {
            let client = try client()
            try await client.flush(timeoutMilliseconds: Int32(timeout.inMilliseconds))
        } catch {
            return false
        }
        return true
    }

    public func partitionForKey(_ key: some KafkaContiguousBytes, in topic: String, partitionCount: Int) -> KafkaPartition? {
        self.stateMachine.withLockedValue { (stateMachine) -> KafkaPartition? in
            guard let topicHandles = stateMachine.topicHandles else {
                return nil
            }

            let partition: Int32? = try? topicHandles.withTopicHandlePointer(topic: topic, topicConfiguration: topicConfiguration) { topicHandle in
                key.withUnsafeBytes { buffer in
                    switch topicConfiguration.partitioner {
                    case .random: nil
                    case .consistent: rd_kafka_msg_partitioner_consistent_random(topicHandle, buffer.baseAddress, buffer.count, Int32(partitionCount), nil, nil)
                    case .consistentRandom: buffer.count == 0 ? nil : rd_kafka_msg_partitioner_consistent_random(topicHandle, buffer.baseAddress, buffer.count, Int32(partitionCount), nil, nil)
                    case .murmur2: rd_kafka_msg_partitioner_murmur2(topicHandle, buffer.baseAddress, buffer.count, Int32(partitionCount), nil, nil)
                    case .murmur2Random: buffer.count == 0 ? nil : rd_kafka_msg_partitioner_murmur2_random(topicHandle, buffer.baseAddress, buffer.count, Int32(partitionCount), nil, nil)
                    case .fnv1a: rd_kafka_msg_partitioner_fnv1a(topicHandle, buffer.baseAddress, buffer.count, Int32(partitionCount), nil, nil)
                    case .fnv1aRandom: buffer.count == 0 ? nil : rd_kafka_msg_partitioner_fnv1a_random(topicHandle, buffer.baseAddress, buffer.count, Int32(partitionCount), nil, nil)
                    default: nil
                    }
                }
            }

            return partition.map { KafkaPartition(rawValue: Int($0)) }
        }
    }

    func client() throws -> RDKafkaClient {
        try self.stateMachine.withLockedValue { try $0.client() }
    }
}

// MARK: - KafkaProducer + StateMachine

extension KafkaProducer {
    /// State machine representing the state of the ``KafkaProducer``.
    struct StateMachine: Sendable {
        /// A logger.
        let logger: Logger

        /// The state of the ``StateMachine``.
        enum State: Sendable {
            /// The state machine has been initialized with init() but is not yet Initialized
            /// using `func initialize()` (required).
            case uninitialized
            /// The ``KafkaProducer`` has started and is ready to use.
            ///
            /// - Parameter messageIDCounter:Used to incrementally assign unique IDs to messages.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            /// - Parameter topicHandles: Class containing all topic names with their respective `rd_kafka_topic_t` pointer.
            case started(
                client: RDKafkaClient,
                messageIDCounter: UInt,
                source: Producer.Source?,
                topicHandles: RDKafkaTopicHandles
            )
            /// Producer is still running but the event asynchronous sequence was terminated.
            /// All incoming events will be dropped.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case eventConsumptionFinished(client: RDKafkaClient)
            /// ``KafkaProducer/triggerGracefulShutdown()`` was invoked so we are flushing
            /// any messages that wait to be sent and serve any remaining queued callbacks.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case finishing(
                client: RDKafkaClient,
                source: Producer.Source?
            )
            /// The ``KafkaProducer`` has been shut down and cannot be used anymore.
            case finished
        }

        /// The current state of the StateMachine.
        var state: State = .uninitialized

        /// Delayed initialization of `StateMachine` as the `source` is not yet available
        /// when the normal initialization occurs.
        mutating func initialize(
            client: RDKafkaClient,
            source: Producer.Source?
        ) {
            guard case .uninitialized = self.state else {
                fatalError("\(#function) can only be invoked in state .uninitialized, but was invoked in state \(self.state)")
            }
            self.state = .started(
                client: client,
                messageIDCounter: 0,
                source: source,
                topicHandles: RDKafkaTopicHandles(client: client)
            )
        }

        /// Action to be taken when wanting to poll.
        enum PollLoopAction {
            /// Poll client.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case pollWithoutYield(client: RDKafkaClient)
            /// Poll client and yield events if any received.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case pollAndYield(client: RDKafkaClient, source: Producer.Source?)
            /// Flush any outstanding producer messages.
            /// Then terminate the poll loop and finish the given `NIOAsyncSequenceProducerSource`.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case flushFinishSourceAndTerminatePollLoop(client: RDKafkaClient, source: Producer.Source?)
            /// Terminate the poll loop.
            case terminatePollLoop
        }

        /// Returns the next action to be taken when wanting to poll.
        /// - Returns: The next action to be taken, either polling or terminating the poll loop.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func nextPollLoopAction() -> PollLoopAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .started(let client, _, let source, _):
                return .pollAndYield(client: client, source: source)
            case .eventConsumptionFinished(let client):
                return .pollWithoutYield(client: client)
            case .finishing(let client, let source):
                return .flushFinishSourceAndTerminatePollLoop(client: client, source: source)
            case .finished:
                return .terminatePollLoop
            }
        }

        /// Action to be taken when wanting to send a message.
        enum SendAction {
            /// Send the message.
            ///
            /// - Important: `newMessageID` is the new message ID assigned to the message to be sent.
            case send(
                client: RDKafkaClient,
                newMessageID: UInt,
                topicHandles: RDKafkaTopicHandles
            )
        }

        /// Get action to be taken when wanting to send a message.
        ///
        /// - Returns: The action to be taken.
        mutating func send() throws -> SendAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .started(let client, let messageIDCounter, let source, let topicHandles):
                let newMessageID = messageIDCounter + 1
                self.state = .started(
                    client: client,
                    messageIDCounter: newMessageID,
                    source: source,
                    topicHandles: topicHandles
                )
                return .send(
                    client: client,
                    newMessageID: newMessageID,
                    topicHandles: topicHandles
                )
            case .eventConsumptionFinished:
                throw KafkaError.connectionClosed(reason: "Sequence consuming events was abruptly terminated, producer closed")
            case .finishing:
                throw KafkaError.connectionClosed(reason: "Producer in the process of finishing")
            case .finished:
                throw KafkaError.connectionClosed(reason: "Tried to produce a message with a closed producer")
            }
        }

        /// Action to take after invoking ``KafkaProducer/StateMachine/stopConsuming()``.
        enum StopConsumingAction {
            /// Finish the given `NIOAsyncSequenceProducerSource`.
            ///
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case finishSource(source: Producer.Source?)
        }

        /// The events asynchronous sequence was terminated.
        /// All incoming events will be dropped.
        mutating func stopConsuming() -> StopConsumingAction? {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .eventConsumptionFinished:
                fatalError("messageSequenceTerminated() must not be invoked more than once")
            case .started(let client, _, let source, _):
                self.state = .eventConsumptionFinished(client: client)
                return .finishSource(source: source)
            case .finishing(let client, let source):
                // Setting source to nil to prevent incoming events from buffering in `source`
                self.state = .finishing(client: client, source: nil)
                return .finishSource(source: source)
            case .finished:
                break
            }
            return nil
        }

        /// Get action to be taken when wanting to do close the producer.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func finish() {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .started(let client, _, let source, _):
                self.state = .finishing(client: client, source: source)
            case .eventConsumptionFinished(let client):
                self.state = .finishing(client: client, source: nil)
            case .finishing, .finished:
                break
            }
        }

        func client() throws -> RDKafkaClient {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .started(let client, _, _, _):
                return client
            case .eventConsumptionFinished(let client):
                return client
            case .finishing(let client, _):
                return client
            case .finished:
                throw KafkaError.connectionClosed(reason: "Client stopped")
            }
        }

        var topicHandles: RDKafkaTopicHandles? {
            if case .started(_, _, _, let topicHandles) = self.state {
                return topicHandles
            }
            return nil
        }
    }
}
