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

/// `NIOAsyncSequenceProducerBackPressureStrategy` that always returns true.
struct NoBackPressure: NIOAsyncSequenceProducerBackPressureStrategy {
    func didYield(bufferDepth: Int) -> Bool { true }
    func didConsume(bufferDepth: Int) -> Bool { true }
}

// MARK: - ShutDownOnTerminate

/// `NIOAsyncSequenceProducerDelegate` that terminates the shuts the producer down when
/// `didTerminate()` is invoked.
struct ShutdownOnTerminate: @unchecked Sendable { // We can do that because our stored propery is protected by a lock
    let stateMachine: NIOLockedValueBox<KafkaProducer.StateMachine>
}

extension ShutdownOnTerminate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        // No back pressure
        return
    }

    func didTerminate() {
        let action = self.stateMachine.withLockedValue { $0.finish() }
        switch action {
        case .shutdownGracefullyAndFinishSource(let client, let source, let topicHandles):
            Task {
                await KafkaProducer._shutDownGracefully(
                    client: client,
                    topicHandles: topicHandles,
                    source: source,
                    timeout: 10000
                )
            }
        case .none:
            return
        }
    }
}

/// `AsyncSequence` implementation for handling messages acknowledged by the Kafka cluster (``KafkaAcknowledgedMessage``).
public struct KafkaMessageAcknowledgements: AsyncSequence {
    public typealias Element = Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, NoBackPressure, ShutdownOnTerminate>
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
        NoBackPressure,
        ShutdownOnTerminate
    >

    /// State of the ``KafkaProducer``.
    private let stateMachine: NIOLockedValueBox<StateMachine>

    /// Topic configuration that is used when a new topic has to be created by the producer.
    private let topicConfig: KafkaTopicConfiguration

    // Private initializer, use factory methods to create KafkaProducer
    /// Initialize a new ``KafkaProducer``.
    /// - Parameter config: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    private init(
        stateMachine: NIOLockedValueBox<KafkaProducer.StateMachine>,
        topicConfig: KafkaTopicConfiguration
    ) throws {
        self.stateMachine = stateMachine
        self.topicConfig = topicConfig
    }

    /// Initialize a new ``KafkaProducer``.
    ///
    /// This factory method creates a producer without message acknowledgements.
    ///
    /// - Parameter configuration: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
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
            callback: nil,
            logger: logger
        )

        let producer = try KafkaProducer(
            stateMachine: stateMachine,
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
            backPressureStrategy: NoBackPressure(),
            delegate: ShutdownOnTerminate(stateMachine: stateMachine)
        )
        let source = sourceAndSequence.source

        let client = try RDKafka.createClient(
            type: .producer,
            configDictionary: config.dictionary,
            callback: { [logger, source] messageResult in
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
        case .shutdownGracefullyAndFinishSource(let client, let source, let topicHandles):
            await KafkaProducer._shutDownGracefully(
                client: client,
                topicHandles: topicHandles,
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
        topicHandles: [String: OpaquePointer],
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

        for (_, topicHandle) in topicHandles {
            rd_kafka_topic_destroy(topicHandle)
        }
    }

    /// Start polling Kafka for acknowledged messages.
    ///
    /// - Parameter pollInterval: The desired time interval between two consecutive polls.
    /// - Returns: An awaitable task representing the execution of the poll loop.
    public func run(pollInterval: Duration = .milliseconds(100)) async throws {
        // TODO(felix): make pollInterval part of config -> easier to adapt to Service protocol (service-lifecycle)
        while !Task.isCancelled {
            let nextAction = self.stateMachine.withLockedValue { $0.nextPollLoopAction() }
            switch nextAction {
            case .poll(let client):
                client.poll(timeout: 0)
                try await Task.sleep(for: pollInterval)
            case .killPollLoop:
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
        case .send(let client, let newMessageID):
            try self._send(
                client: client,
                message: message,
                newMessageID: newMessageID
            )
            return KafkaProducerMessageID(rawValue: newMessageID)
        }
    }

    private func _send(
        client: KafkaClient,
        message: KafkaProducerMessage,
        newMessageID: UInt
    ) throws {
        let topicHandle = try self._createTopicHandleIfNeeded(topic: message.topic)

        let keyBytes: [UInt8]?
        if var key = message.key {
            keyBytes = key.readBytes(length: key.readableBytes)
        } else {
            keyBytes = nil
        }

        let responseCode = message.value.withUnsafeReadableBytes { valueBuffer in

            // Pass message over to librdkafka where it will be queued and sent to the Kafka Cluster.
            // Returns 0 on success, error code otherwise.
            return rd_kafka_produce(
                topicHandle,
                message.partition.rawValue,
                RD_KAFKA_MSG_F_COPY,
                UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress),
                valueBuffer.count,
                keyBytes,
                keyBytes?.count ?? 0,
                UnsafeMutableRawPointer(bitPattern: newMessageID)
            )
        }

        guard responseCode == 0 else {
            throw KafkaError.rdKafkaError(wrapping: rd_kafka_last_error())
        }
    }

    /// Check `topicHandles` for a handle matching the topic name and create a new handle if needed.
    /// - Parameter topic: The name of the topic that is addressed.
    private func _createTopicHandleIfNeeded(topic: String) throws -> OpaquePointer? {
        try self.stateMachine.withLockedValue { state in
            let action = try state.createTopicHandleIfNeeded(topic: topic)
            switch action {
            case .handleExists(let handle):
                return handle
            case .createTopicHandle(let client, let topic):
                let newHandle = try client.withKafkaHandlePointer { handle in
                    let rdTopicConf = try RDKafkaTopicConfig.createFrom(topicConfig: self.topicConfig)
                    return rd_kafka_topic_new(
                        handle,
                        topic,
                        rdTopicConf
                    )
                    // rd_kafka_topic_new deallocates topic config object
                }
                if let newHandle {
                    try state.addTopicHandle(topic: topic, handle: newHandle)
                }
                return newHandle
            }
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
            /// - Parameter topicHandles: Dictionary containing all topic names with their respective `rd_kafka_topic_t` pointer.
            case started(
                client: KafkaClient,
                messageIDCounter: UInt,
                source: Producer.Source?,
                topicHandles: [String: OpaquePointer]
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
                topicHandles: [:]
            )
        }

        /// Action to be taken when wanting to poll.
        enum PollLoopAction {
            /// Poll client for new consumer messages.
            case poll(client: KafkaClient)
            /// Kill the poll loop.
            case killPollLoop
        }

        /// Returns the next action to be taken when wanting to poll.
        /// - Returns: The next action to be taken, either polling or killing the poll loop.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        func nextPollLoopAction() -> PollLoopAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .started(let client, _, _, _):
                return .poll(client: client)
            case .finished:
                return .killPollLoop
            }
        }

        /// Action to take when  wanting to create a topic handle.
        enum CreateTopicHandleAction {
            /// Do create a new topic handle.
            case createTopicHandle(
                client: KafkaClient,
                topic: String
            )
            /// No need to create a new handle. It exists already: `handle`.
            case handleExists(handle: OpaquePointer)
        }

        /// Returns action to be taken when wanting to create a new topic handle.
        func createTopicHandleIfNeeded(topic: String) throws -> CreateTopicHandleAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .started(let client, _, _, let topicHandles):
                if let handle = topicHandles[topic] {
                    return .handleExists(handle: handle)
                } else {
                    return .createTopicHandle(client: client, topic: topic)
                }
            case .finished:
                throw KafkaError.connectionClosed(reason: "Tried to create topic handle on closed connection")
            }
        }

        /// Add a newly created topic handle to the list of topic handles contained in the state machine.
        mutating func addTopicHandle(
            topic: String,
            handle: OpaquePointer
        ) throws {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state \(self.state)")
            case .started(let client, let messageIDCounter, let source, let topicHandles):
                var topicHandles = topicHandles
                topicHandles[topic] = handle
                self.state = .started(
                    client: client,
                    messageIDCounter: messageIDCounter,
                    source: source,
                    topicHandles: topicHandles
                )
            case .finished:
                throw KafkaError.connectionClosed(reason: "Tried to create topic handle on closed connection")
            }
        }

        /// Action to be taken when wanting to send a message.
        enum SendAction {
            /// Send the message.
            ///
            /// - Important: `newMessageID` is the new message ID assigned to the message to be sent.
            case send(
                client: KafkaClient,
                newMessageID: UInt
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
                    newMessageID: newMessageID
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
            /// - Parameter topicHandles: Dictionary containing all topic names with their respective `rd_kafka_topic_t` pointer.
            case shutdownGracefullyAndFinishSource(
                client: KafkaClient,
                source: Producer.Source?,
                topicHandles: [String: OpaquePointer]
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
            case .started(let client, _, let source, let topicHandles):
                self.state = .finished
                return .shutdownGracefullyAndFinishSource(
                    client: client,
                    source: source,
                    topicHandles: topicHandles
                )
            case .finished:
                return nil
            }
        }
    }
}
