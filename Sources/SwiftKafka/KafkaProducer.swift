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

// MARK: - KafkaProducerShutdownOnTerminate

/// `NIOAsyncSequenceProducerDelegate` that terminates the shuts the producer down when
/// `didTerminate()` is invoked.
internal struct KafkaProducerShutdownOnTerminate: @unchecked Sendable { // We can do that because our stored propery is protected by a lock
    let stateMachine: NIOLockedValueBox<KafkaProducer.StateMachine>
}

extension KafkaProducerShutdownOnTerminate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        // No back pressure
        return
    }

    func didTerminate() {
        let action = self.stateMachine.withLockedValue { $0.finish() }
        switch action {
        case .shutdownGracefullyAndFinishSource(let client, let source):
            Task {
                await KafkaProducer._shutDownGracefully(
                    client: client,
                    source: source,
                    timeout: 10000
                )
            }
        case .none:
            return
        }
    }
}

// MARK: - KafkaMessageAcknowledgements

/// `AsyncSequence` implementation for handling messages acknowledged by the Kafka cluster (``KafkaAcknowledgedMessage``).
public struct KafkaMessageAcknowledgements: AsyncSequence {
    public typealias Element = Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>
    typealias BackPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, BackPressureStrategy, KafkaProducerShutdownOnTerminate>
    let wrappedSequence: WrappedSequence

    /// `AsynceIteratorProtocol` implementation for handling messages acknowledged by the Kafka cluster (``KafkaAcknowledgedMessage``).
    public struct AcknowledgedMessagesAsyncIterator: AsyncIteratorProtocol {
        var wrappedIterator: WrappedSequence.AsyncIterator

        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    public func makeAsyncIterator() -> AcknowledgedMessagesAsyncIterator {
        return AcknowledgedMessagesAsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

/// Send messages to the Kafka cluster.
/// Please make sure to explicitly call ``shutdownGracefully(timeout:)`` when the ``KafkaProducer`` is not used anymore.
/// - Note: When messages get published to a non-existent topic, a new topic is created using the ``KafkaTopicConfiguration``
/// configuration object (only works if server has `auto.create.topics.enable` property set).
public final class KafkaProducer {
    typealias Producer = NIOAsyncSequenceProducer<
        Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>,
        NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure,
        KafkaProducerShutdownOnTerminate
    >

    /// State of the ``KafkaProducer``.
    private let stateMachine: NIOLockedValueBox<StateMachine>

    /// The configuration object of the producer client.
    private let config: KafkaProducerConfiguration
    /// Topic configuration that is used when a new topic has to be created by the producer.
    private let topicConfig: KafkaTopicConfiguration

    // Private initializer, use factory methods to create KafkaProducer
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
    /// This factory method creates a producer without message acknowledgements.
    ///
    /// - Parameter config: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Returns: The newly created ``KafkaProducer``.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public static func makeProducer(
        config: KafkaProducerConfiguration = KafkaProducerConfiguration(),
        topicConfig: KafkaTopicConfiguration = KafkaTopicConfiguration(),
        logger: Logger
    ) throws -> KafkaProducer {
        let stateMachine = NIOLockedValueBox(StateMachine(logger: logger))

        let client = try RDKafka.createClient(
            type: .producer,
            configDictionary: config.dictionary,
            // Having no callback will discard any incoming acknowledgement messages
            // Ref: rdkafka_broker.c:rd_kafka_dr_msgq
            deliveryReportCallback: nil,
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
                source: nil
            )
        }

        return producer
    }

    /// Initialize a new ``KafkaProducer`` and a ``KafkaMessageAcknowledgements`` asynchronous sequence.
    ///
    /// Use the asynchronous sequence to consume message acknowledgements.
    ///
    /// - Important: When the asynchronous sequence is deinited the producer will be shutdown.
    ///
    /// - Parameter config: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Returns: A tuple containing the created ``KafkaProducer`` and the ``KafkaMessageAcknowledgements``
    /// `AsyncSequence` used for receiving message acknowledgements.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public static func makeProducerWithAcknowledgements(
        config: KafkaProducerConfiguration = KafkaProducerConfiguration(),
        topicConfig: KafkaTopicConfiguration = KafkaTopicConfiguration(),
        logger: Logger
    ) throws -> (KafkaProducer, KafkaMessageAcknowledgements) {
        let stateMachine = NIOLockedValueBox(StateMachine(logger: logger))

        let sourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>.self,
            backPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure(),
            delegate: KafkaProducerShutdownOnTerminate(stateMachine: stateMachine)
        )
        let source = sourceAndSequence.source

        let client = try RDKafka.createClient(
            type: .producer,
            configDictionary: config.dictionary,
            deliveryReportCallback: { [logger, source] messageResult in
                guard let messageResult else {
                    logger.error("Could not resolve acknowledged message")
                    return
                }

                // Ignore YieldResult as we don't support back pressure in KafkaProducer
                _ = source.yield(messageResult)
            },
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

        let acknowlegementsSequence = KafkaMessageAcknowledgements(wrappedSequence: sourceAndSequence.sequence)
        return (producer, acknowlegementsSequence)
    }

    /// Method to shutdown the ``KafkaProducer``.
    ///
    /// This method flushes any buffered messages and waits until a callback is received for all of them.
    /// Afterwards, it shuts down the connection to Kafka and cleans any remaining state up.
    /// - Parameter timeout: Maximum amount of milliseconds this method waits for any outstanding messages to be sent.
    public func shutdownGracefully(timeout: Int32 = 10000) async {
        let action = self.stateMachine.withLockedValue { $0.finish() }
        switch action {
        case .shutdownGracefullyAndFinishSource(let client, let source):
            await KafkaProducer._shutDownGracefully(
                client: client,
                source: source,
                timeout: timeout
            )
        case .none:
            return
        }
    }

    // Static so we perform this without needing a reference to `KafkaProducer`
    static func _shutDownGracefully(
        client: KafkaClient,
        source: Producer.Source?,
        timeout: Int32
    ) async {
        source?.finish()

        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            // Wait `timeout` seconds for outstanding messages to be sent and callbacks to be called
            client.withKafkaHandlePointer { handle in
                rd_kafka_flush(handle, timeout)
                continuation.resume()
            }
        }
    }

    /// Start polling Kafka for acknowledged messages.
    ///
    /// - Returns: An awaitable task representing the execution of the poll loop.
    public func run() async throws {
        // TODO(felix): make pollInterval part of config -> easier to adapt to Service protocol (service-lifecycle)
        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextPollLoopAction() }
            switch nextAction {
            case .poll(let client):
                client.poll(timeout: 0)
                try await Task.sleep(for: self.config.pollInterval)
            case .terminatePollLoop:
                return
            }
        }
    }

    /// Send messages to the Kafka cluster asynchronously. This method is non-blocking.
    /// Message send results shall be handled through the ``KafkaMessageAcknowledgements`` `AsyncSequence`.
    ///
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    /// - Returns: Unique ``KafkaProducerMessageID``matching the ``KafkaAcknowledgedMessage/id`` property
    /// of the corresponding ``KafkaAcknowledgedMessage``.
    /// - Throws: A ``KafkaError`` if sending the message failed.
    @discardableResult
    public func send(_ message: KafkaProducerMessage) throws -> KafkaProducerMessageID {
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
    struct StateMachine {
        /// A logger.
        let logger: Logger

        /// The state of the ``StateMachine``.
        enum State {
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
                client: KafkaClient,
                messageIDCounter: UInt,
                source: Producer.Source?,
                topicHandles: RDKafkaTopicHandles
            )
            /// The ``KafkaProducer`` has been shut down and cannot be used anymore.
            case finished
        }

        /// The current state of the StateMachine.
        var state: State = .uninitialized

        /// Delayed initialization of `StateMachine` as the `source` is not yet available
        /// when the normal initialization occurs.
        mutating func initialize(
            client: KafkaClient,
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
            /// Poll client for new consumer messages.
            case poll(client: KafkaClient)
            /// Terminate the poll loop.
            case terminatePollLoop
        }

        /// Returns the next action to be taken when wanting to poll.
        /// - Returns: The next action to be taken, either polling or terminating the poll loop.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        func nextPollLoopAction() -> PollLoopAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .started(let client, _, _, _):
                return .poll(client: client)
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
                client: KafkaClient,
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
            case .finished:
                throw KafkaError.connectionClosed(reason: "Tried to produce a message with a closed producer")
            }
        }

        /// Action to be taken when wanting to do close the producer.
        enum FinishAction {
            /// Shut down the ``KafkaProducer`` and finish the given `source` object.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            case shutdownGracefullyAndFinishSource(
                client: KafkaClient,
                source: Producer.Source?
            )
        }

        /// Get action to be taken when wanting to do close the producer.
        /// - Returns: The action to be taken,  or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func finish() -> FinishAction? {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .started(let client, _, let source, _):
                self.state = .finished
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
