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
/// Please make sure to explicitly call ``triggerGracefulShutdown()`` when the ``KafkaProducer`` is not used anymore.
/// - Note: When messages get published to a non-existent topic, a new topic is created using the ``KafkaTopicConfiguration``
/// configuration object (only works if server has `auto.create.topics.enable` property set).
public final class KafkaProducer: Service, Sendable {
    typealias Producer = NIOAsyncSequenceProducer<
        KafkaProducerEvent,
        NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure,
        KafkaProducerCloseOnTerminate
    >

    /// State of the ``KafkaProducer``.
    private let stateMachine: NIOLockedValueBox<StateMachine>

    /// The configuration object of the producer client.
    private let config: KafkaProducerConfiguration
    /// Topic configuration that is used when a new topic has to be created by the producer.
    private let topicConfig: KafkaTopicConfiguration

    // Private initializer, use factory method or convenience init to create KafkaProducer
    /// Initialize a new ``KafkaProducer``.
    ///
    /// - Parameter stateMachine: The ``KafkaProducer/StateMachine`` instance associated with the ``KafkaProducer``.///
    /// - Parameter config: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    private init(
        stateMachine: NIOLockedValueBox<KafkaProducer.StateMachine>,
        config: KafkaProducerConfiguration,
        topicConfig: KafkaTopicConfiguration
    ) throws {
        self.stateMachine = stateMachine
        self.config = config
        self.topicConfig = topicConfig
    }

    /// Initialize a new ``KafkaProducer``.
    ///
    /// This creates a producer without listening for events.
    ///
    /// - Parameter config: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Returns: The newly created ``KafkaProducer``.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public convenience init(
        config: KafkaProducerConfiguration = KafkaProducerConfiguration(),
        topicConfig: KafkaTopicConfiguration = KafkaTopicConfiguration(),
        logger: Logger
    ) throws {
        let stateMachine = NIOLockedValueBox(StateMachine(logger: logger))

        var subscribedEvents: [RDKafkaEvent] = [.log] // No .deliveryReport here!
        // Listen to statistics events when statistics enabled
        if config.statisticsInterval != .zero {
            subscribedEvents.append(.statistics)
        }

        let client = try RDKafkaClient.makeClient(
            type: .producer,
            configDictionary: config.dictionary,
            events: subscribedEvents,
            logger: logger
        )

        stateMachine.withLockedValue {
            $0.initialize(
                client: client,
                source: nil
            )
        }

        try self.init(
            stateMachine: stateMachine,
            config: config,
            topicConfig: topicConfig
        )
    }

    /// Initialize a new ``KafkaProducer`` and a ``KafkaProducerEvents`` asynchronous sequence.
    ///
    /// Use the asynchronous sequence to consume events.
    ///
    /// - Important: When the asynchronous sequence is deinited the producer will be shutdown and disallow sending more messages.
    /// Additionally, make sure to consume the asynchronous sequence otherwise the events will be buffered in memory indefinitely.
    ///
    /// - Parameter config: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Returns: A tuple containing the created ``KafkaProducer`` and the ``KafkaProducerEvents``
    /// `AsyncSequence` used for receiving message events.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public static func makeProducerWithEvents(
        config: KafkaProducerConfiguration = KafkaProducerConfiguration(),
        topicConfig: KafkaTopicConfiguration = KafkaTopicConfiguration(),
        logger: Logger
    ) throws -> (KafkaProducer, KafkaProducerEvents) {
        let stateMachine = NIOLockedValueBox(StateMachine(logger: logger))

        let sourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: KafkaProducerEvent.self,
            backPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure(),
            delegate: KafkaProducerCloseOnTerminate(stateMachine: stateMachine)
        )
        let source = sourceAndSequence.source
        
        var subscribedEvents: [RDKafkaEvent] = [.log, .deliveryReport]
        // Listen to statistics events when statistics enabled
        if config.statisticsInterval != .zero {
            subscribedEvents.append(.statistics)
        }

        let client = try RDKafkaClient.makeClient(
            type: .producer,
            configDictionary: config.dictionary,
            events: subscribedEvents,
            logger: logger
        )

        let producer = try KafkaProducer(
            stateMachine: stateMachine,
            config: config,
            topicConfig: topicConfig
        )

        stateMachine.withLockedValue {
            $0.initialize(
                client: client,
                source: source
            )
        }

        let eventsSequence = KafkaProducerEvents(wrappedSequence: sourceAndSequence.sequence)
        return (producer, eventsSequence)
    }

    /// Start polling Kafka for events.
    ///
    /// - Returns: An awaitable task representing the execution of the poll loop.
    public func run() async throws {
        try await withGracefulShutdownHandler {
            try await self._run()
        } onGracefulShutdown: {
            self.triggerGracefulShutdown()
        }
    }

    private func _run() async throws {
        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextPollLoopAction() }
            switch nextAction {
            case .pollWithoutYield(let client):
                // Drop any incoming events
                let _ = client.eventPoll()
            case .pollAndYield(let client, let source):
                let events = client.eventPoll()
                for event in events {
                    let producerEvent = KafkaProducerEvent(event)
                    // Ignore YieldResult as we don't support back pressure in KafkaProducer
                    _ = source?.yield(producerEvent)
                }
                try await Task.sleep(for: self.config.pollInterval)
            case .flushFinishSourceAndTerminatePollLoop(let client, let source):
                precondition(
                    0...Int(Int32.max) ~= self.config.flushTimeoutMilliseconds,
                    "Flush timeout outside of valid range \(0...Int32.max)"
                )
                try await client.flush(timeoutMilliseconds: Int32(self.config.flushTimeoutMilliseconds))
                source?.finish()
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
    private func triggerGracefulShutdown() {
        self.stateMachine.withLockedValue { $0.finish() }
    }

    /// Send messages to the Kafka cluster asynchronously. This method is non-blocking.
    /// Message send results shall be handled through the ``KafkaMessageAcknowledgements`` `AsyncSequence`.
    ///
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    /// - Returns: Unique ``KafkaProducerMessageID``matching the ``KafkaAcknowledgedMessage/id`` property
    /// of the corresponding ``KafkaAcknowledgedMessage``.
    /// - Throws: A ``KafkaError`` if sending the message failed.
    @discardableResult
    public func send<Key, Value>(_ message: KafkaProducerMessage<Key, Value>) throws -> KafkaProducerMessageID {
        let action = try self.stateMachine.withLockedValue { try $0.send() }
        switch action {
        case .send(let client, let newMessageID, let topicHandles):
            try client.produce(
                message: message,
                newMessageID: newMessageID,
                topicConfig: self.topicConfig,
                topicHandles: topicHandles
            )
            return KafkaProducerMessageID(rawValue: newMessageID)
        }
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
            case consumptionStopped(client: RDKafkaClient)
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
            case .consumptionStopped(let client):
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
            case .consumptionStopped:
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
            case .consumptionStopped:
                fatalError("messageSequenceTerminated() must not be invoked more than once")
            case .started(let client, _, let source, _):
                self.state = .consumptionStopped(client: client)
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
            case .consumptionStopped(let client):
                self.state = .finishing(client: client, source: nil)
            case .finishing, .finished:
                break
            }
        }
    }
}
