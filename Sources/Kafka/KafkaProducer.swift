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

// MARK: - KafkaProducerCloseOnTerminate

/// `NIOAsyncSequenceProducerDelegate` that terminates and closes the producer when
/// `didTerminate()` is invoked.
internal struct KafkaProducerCloseOnTerminate: Sendable {
    let stateMachine: NIOLockedValueBox<KafkaProducer.StateMachine>
}

extension KafkaProducerCloseOnTerminate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        return  // No back pressure
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

/// An asynchronous sequence of producer events emitted by Kafka.
///
/// The sequence yields ``KafkaProducerEvent`` values such as delivery reports and errors.
public struct KafkaProducerEvents: Sendable, AsyncSequence {
    /// The type of event the sequence yields.
    public typealias Element = KafkaProducerEvent
    typealias BackPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, BackPressureStrategy, KafkaProducerCloseOnTerminate>
    let wrappedSequence: WrappedSequence

    /// An asynchronous iterator over producer events emitted by Kafka.
    ///
    /// The iterator yields ``KafkaProducerEvent`` values such as delivery reports and errors.
    public struct AsyncIterator: AsyncIteratorProtocol {
        var wrappedIterator: WrappedSequence.AsyncIterator

        /// Returns the next producer event, or `nil` if the sequence has finished.
        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    /// Returns an asynchronous iterator over the producer event sequence.
    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

// MARK: - KafkaProducer

/// Sends messages to the Kafka cluster.
///
/// - Note: When messages get published to a nonexistent topic, a new topic is created using the default topic configuration
///   (based on topic-level configuration properties set on `KafkaProducerConfig`).
public final class KafkaProducer: Service, Sendable {
    typealias Producer = NIOAsyncSequenceProducer<
        KafkaProducerEvent,
        NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure,
        KafkaProducerCloseOnTerminate
    >

    /// State of the ``KafkaProducer``.
    private let stateMachine: NIOLockedValueBox<StateMachine>

    /// The configuration object of the producer client.
    private let config: KafkaProducerConfig

    /// A logger.
    private let logger: Logger

    // Private initializer, use factory method or convenience init to create KafkaProducer
    /// Creates a new ``KafkaProducer``.
    ///
    /// - Parameter stateMachine: The ``KafkaProducer/StateMachine`` instance associated with the ``KafkaProducer``.
    /// - Parameter config: The ``KafkaProducerConfig`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfiguration: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    private init(
        stateMachine: NIOLockedValueBox<KafkaProducer.StateMachine>,
        config: KafkaProducerConfig,
        logger: Logger
    ) {
        self.stateMachine = stateMachine
        self.config = config
        var enrichedLogger = logger
        if let clientId = config.clientId {
            enrichedLogger[metadataKey: KafkaLoggingKeys.clientId] = "\(clientId)"
        }
        enrichedLogger[metadataKey: KafkaLoggingKeys.clientType] = "producer"
        self.logger = enrichedLogger

        self.logger.debug(
            "Kafka producer initialized",
            metadata: [
                KafkaLoggingKeys.bootstrapServers: "\(config.bootstrapServers?.joined(separator: ",") ?? "default")"
            ]
        )
    }

    /// Creates a producer and its paired events sequence.
    ///
    /// Every producer receives an events sequence for delivery reports from
    /// ``send(_:)`` and for error observation. Callers who only use
    /// ``sendAndAwait(_:)`` may discard the returned `events` value.
    ///
    /// - Important: When the events sequence is deinitialized, the producer shuts down
    ///   and stops accepting new messages. Iterate the sequence; otherwise events buffer
    ///   in memory indefinitely.
    ///
    /// - Parameters:
    ///     - config: The ``KafkaProducerConfig`` for configuring the ``KafkaProducer``.
    ///     - logger: A logger.
    /// - Returns: A named tuple containing the created ``KafkaProducer`` and its
    ///   ``KafkaProducerEvents`` `AsyncSequence`.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public static func makeProducer(
        config: KafkaProducerConfig,
        logger: Logger
    ) throws -> (producer: KafkaProducer, events: KafkaProducerEvents) {
        let stateMachine = NIOLockedValueBox(StateMachine(logger: logger))

        var subscribedEvents: [RDKafkaEvent] = [.log, .deliveryReport, .error]
        // Listen to statistics events when statistics enabled
        if config.metrics.enabled {
            subscribedEvents.append(.statistics)
        }

        let client = try RDKafkaClient.makeClient(
            type: .producer,
            configDictionary: config.config,
            events: subscribedEvents,
            logger: logger
        )

        let producer = KafkaProducer(
            stateMachine: stateMachine,
            config: config,
            logger: logger
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
        return (producer: producer, events: eventsSequence)
    }

    /// Starts the producer.
    ///
    /// - Important: Call this method to drive the producer. It runs until either the calling task is canceled or gracefully shut down.
    ///
    /// Stop the producer with ``triggerGracefulShutdown()``.
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
                let events = client.producerEventPoll()
                for event in events {
                    switch event {
                    case .statistics(let statistics):
                        self.config.metrics.update(with: statistics)
                    case .deliveryReport(let reports):
                        _ = self.dispatchDeliveryReports(reports)
                    case .error(let kafkaError):
                        self.logger.info(
                            "Kafka client error",
                            error: kafkaError
                        )
                    }
                }
                try await Task.sleep(for: self.config.pollInterval)
            case .pollAndYield(let client, let source):
                let events = client.producerEventPoll()
                for event in events {
                    switch event {
                    case .statistics(let statistics):
                        self.config.metrics.update(with: statistics)
                    case .deliveryReport(let reports):
                        let reportsForEvents = self.dispatchDeliveryReports(reports)
                        if !reportsForEvents.isEmpty {
                            _ = source?.yield(.deliveryReports(reportsForEvents))
                        }
                    case .error(let kafkaError):
                        _ = source?.yield(.error(kafkaError))
                        self.logger.info(
                            "Kafka client error",
                            error: kafkaError
                        )
                    }
                }
                try await Task.sleep(for: self.config.pollInterval)
            case .flushFinishSourceAndTerminatePollLoop(let client, let source):
                precondition(
                    0...Int(Int32.max) ~= self.config.shutdownFlushTimeoutMs,
                    "Flush timeout outside of valid range \(0...Int32.max)"
                )
                defer {  // we should finish source indefinetely of exception in client.flush()
                    source?.finish()
                    self.stateMachine.withLockedValue { $0.failPendingContinuations() }
                }
                self.logger.trace("Flushing outstanding messages before shutdown")
                do {
                    try await client.flush(timeoutMilliseconds: Int32(self.config.shutdownFlushTimeoutMs))
                } catch {
                    self.logger.info(
                        "Flush timed out during shutdown, some messages may not have been delivered",
                        error: error,
                        metadata: [
                            KafkaLoggingKeys.timeoutMs: "\(self.config.shutdownFlushTimeoutMs)"
                        ]
                    )
                    throw error
                }
                self.logger.debug("Kafka producer shut down")
                return
            case .terminatePollLoop:
                self.stateMachine.withLockedValue { $0.failPendingContinuations() }
                self.logger.debug("Kafka producer shut down")
                return
            }
        }
    }

    /// Route each delivery report to exactly one channel.
    ///
    /// - Reports whose message ID has a pending `sendAndAwait(_:)` continuation are resolved
    ///   on that continuation only. The report is not re-emitted on the events sequence.
    /// - Reports whose message ID has no pending continuation came from `send(_:)`;
    ///   these are returned so the caller can emit them on the events sequence.
    ///
    /// Each delivery lands on exactly one destination. Callers using both `sendAndAwait(_:)`
    /// and iterating `KafkaProducerEvents` never see the same report twice.
    private func dispatchDeliveryReports(_ reports: [KafkaDeliveryReport]) -> [KafkaDeliveryReport] {
        var reportsForEvents: [KafkaDeliveryReport] = []
        reportsForEvents.reserveCapacity(reports.count)
        for report in reports {
            switch report.status {
            case .acknowledged:
                break
            case .failure(let error):
                self.logger.debug(
                    "Message delivery failed",
                    error: error,
                    metadata: [
                        KafkaLoggingKeys.messageId: "\(report.id.rawValue)"
                    ]
                )
            }

            let continuation: CheckedContinuation<KafkaDeliveryReport, Error>? =
                self.stateMachine.withLockedValue {
                    $0.removeContinuation(for: report.id.rawValue)
                }
            if let continuation {
                switch report.status {
                case .acknowledged:
                    continuation.resume(returning: report)
                case .failure(let error):
                    continuation.resume(throwing: error)
                }
            } else {
                reportsForEvents.append(report)
            }
        }
        return reportsForEvents
    }

    /// Shuts the producer down gracefully.
    ///
    /// Flushes any buffered messages and waits until the producer receives a callback for each one. After flushing, this method shuts down the connection to Kafka and cleans up any remaining state.
    ///
    /// Pairs with ``run()``.
    public func triggerGracefulShutdown() {
        self.logger.debug("Kafka producer shutting down")
        self.stateMachine.withLockedValue { $0.finish() }
    }

    /// Sends a message to the Kafka cluster.
    ///
    /// This method does not wait until the message is sent and acknowledged by the cluster.
    /// Instead, it buffers the message and returns immediately.
    /// The producer sends the message with the next batch of messages.
    ///
    /// For acknowledged delivery, use ``sendAndAwait(_:)``.
    ///
    /// - Parameter message: The ``KafkaProducerMessage`` to send.
    /// - Returns: Unique ``KafkaProducerMessageID`` matching the ``KafkaDeliveryReport/id`` property
    /// of the corresponding ``KafkaDeliveryReport``.
    /// - Throws: A ``KafkaError`` if sending the message failed.
    @discardableResult
    public func send<Key, Value>(_ message: KafkaProducerMessage<Key, Value>) throws -> KafkaProducerMessageID {
        let action = try self.stateMachine.withLockedValue { try $0.send() }
        switch action {
        case .send(let client, let newMessageID, let topicHandles):
            do {
                try client.produce(
                    message: message,
                    newMessageID: newMessageID,
                    topicHandles: topicHandles
                )
            } catch {
                self.logger.info(
                    "Failed to produce message",
                    error: error,
                    metadata: [KafkaLoggingKeys.topic: "\(message.topic)"]
                )
                throw error
            }
            return KafkaProducerMessageID(rawValue: newMessageID)
        }
    }

    /// Sends a message to the Kafka cluster and awaits the delivery report.
    ///
    /// Unlike ``send(_:)``, this method suspends until the broker acknowledges (or rejects)
    /// the message. The returned ``KafkaDeliveryReport`` contains the acknowledgment status,
    /// partition, offset, and other metadata.
    ///
    /// - Parameter message: The ``KafkaProducerMessage`` to send.
    /// - Returns: A ``KafkaDeliveryReport`` with the delivery status.
    /// - Throws: A ``KafkaError`` if the message could not be enqueued or was rejected by the broker.
    public func sendAndAwait<Key, Value>(
        _ message: KafkaProducerMessage<Key, Value>
    ) async throws -> KafkaDeliveryReport {
        // Get the message ID and produce BEFORE entering the continuation,
        // so we have the ID available for the cancellation handler.
        let action = try self.stateMachine.withLockedValue { try $0.send() }
        let newMessageID: UInt
        let client: RDKafkaClient
        let topicHandles: RDKafkaTopicHandles

        switch action {
        case .send(let c, let id, let th):
            newMessageID = id
            client = c
            topicHandles = th
        }

        // Initialize the continuation slot before entering withTaskCancellationHandler.
        // This ensures onCancel can distinguish a live request from a stale ID.
        self.stateMachine.withLockedValue {
            $0.initializeContinuation(for: newMessageID)
        }

        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation {
                (continuation: CheckedContinuation<KafkaDeliveryReport, Error>) in
                // Transition from .initialized to .pending BEFORE producing.
                // The delivery report could arrive before produce() returns.
                let result = self.stateMachine.withLockedValue {
                    $0.registerContinuation(continuation, for: newMessageID)
                }

                switch result {
                case .registered:
                    break
                case .alreadyCancelled:
                    continuation.resume(throwing: CancellationError())
                    return
                }

                do {
                    try client.produce(
                        message: message,
                        newMessageID: newMessageID,
                        topicHandles: topicHandles
                    )
                } catch {
                    let removed = self.stateMachine.withLockedValue {
                        $0.removeContinuation(for: newMessageID)
                    }
                    removed?.resume(throwing: error)
                }
            }
        } onCancel: {
            // Task cancelled. If the continuation is pending, remove and return it
            // for resumption. If still .initialized, leave a .cancelled tombstone
            // so registerContinuation knows to reject it.
            let cancelled = self.stateMachine.withLockedValue {
                $0.cancelContinuation(for: newMessageID)
            }
            cancelled?.resume(throwing: CancellationError())
        }
    }
}

// MARK: - KafkaProducer + StateMachine

extension KafkaProducer {
    /// State machine representing the state of the ``KafkaProducer``.
    struct StateMachine: Sendable {
        /// A logger.
        let logger: Logger

        /// Tri-state for a pending `sendAndAwait` call.
        ///
        /// Handles the `withTaskCancellationHandler` race where `onCancel` can fire
        /// before the operation body registers the continuation:
        /// - `.initialized`: Slot allocated, continuation not yet registered
        /// - `.pending`: Continuation registered, waiting for delivery report
        /// - `.cancelled`: `onCancel` fired before registration — reject on register
        enum ContinuationState {
            case initialized
            case pending(CheckedContinuation<KafkaDeliveryReport, Error>)
            case cancelled
        }

        /// Pending continuations for ``sendAndAwait(_:)`` calls, keyed by message ID.
        /// When a delivery report arrives, the event loop checks this dictionary.
        /// If a match is found, the continuation is resumed and removed.
        /// Protected by the enclosing `NIOLockedValueBox`.
        private var pendingContinuations: [UInt: ContinuationState] = [:]

        init(logger: Logger) {
            self.logger = logger
        }

        /// The state of the ``StateMachine``.
        enum State: Sendable {
            /// The state machine has been initialized with init() but is not yet Initialized
            /// using `func initialize()` (required).
            case uninitialized
            /// The ``KafkaProducer`` has started and is ready to use.
            ///
            /// - Parameter messageIDCounter: Used to incrementally assign unique IDs to messages.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter source: ``NIOAsyncSequenceProducer/Source`` used for yielding new elements.
            /// - Parameter topicHandles: Class containing all topic names with their respective `rd_kafka_topic_t` pointer.
            case started(
                client: RDKafkaClient,
                messageIDCounter: UInt,
                source: Producer.Source?,
                topicHandles: RDKafkaTopicHandles
            )
            /// The producer is still running but the events asynchronous sequence terminated.
            /// The producer drops all incoming events.
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
                fatalError(
                    "\(#function) can only be invoked in state .uninitialized, but was invoked in state \(self.state)"
                )
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
        /// - Important: This function traps with a `fatalError` if called while in the `.uninitialized` state.
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
                throw KafkaError.connectionClosed(
                    reason: "Sequence consuming events was abruptly terminated, producer closed"
                )
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

        /// The events asynchronous sequence terminated.
        /// The producer drops all incoming events.
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
        /// - Important: This function traps with a `fatalError` if called while in the `.uninitialized` state.
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

        // MARK: - sendAndAwait Continuation Management

        enum RegisterContinuationResult {
            case registered
            case alreadyCancelled
        }

        /// Initialize a continuation slot.
        ///
        /// Pre-allocates the slot so `cancelContinuation` can safely detect
        /// if it's canceling an active request versus a stale ID.
        mutating func initializeContinuation(for messageID: UInt) {
            self.pendingContinuations[messageID] = .initialized
        }

        /// Register a continuation for a pending `sendAndAwait` call.
        ///
        /// Transitions the slot from `.initialized` to `.pending(continuation)`.
        /// Returns `.alreadyCancelled` if `onCancel` already set the slot to `.cancelled`.
        mutating func registerContinuation(
            _ continuation: CheckedContinuation<KafkaDeliveryReport, Error>,
            for messageID: UInt
        ) -> RegisterContinuationResult {
            guard let existing = self.pendingContinuations[messageID] else {
                fatalError(
                    "registerContinuation called without prior initializeContinuation for messageID \(messageID)"
                )
            }
            switch existing {
            case .cancelled:
                self.pendingContinuations.removeValue(forKey: messageID)
                return .alreadyCancelled
            case .initialized:
                self.pendingContinuations[messageID] = .pending(continuation)
                return .registered
            case .pending:
                fatalError(
                    "registerContinuation called with already-pending continuation for messageID \(messageID)"
                )
            }
        }

        /// Cancel a pending continuation for the given message ID.
        ///
        /// Called by the `withTaskCancellationHandler`'s `onCancel` closure.
        /// If the continuation hasn't been registered yet, this marks the slot
        /// as `.cancelled` so `registerContinuation` can reject it later.
        mutating func cancelContinuation(
            for messageID: UInt
        ) -> CheckedContinuation<KafkaDeliveryReport, Error>? {
            guard let existing = self.pendingContinuations.removeValue(forKey: messageID) else {
                // Missing: the event loop already removed it via removeContinuation.
                return nil
            }

            switch existing {
            case .initialized:
                // Continuation hasn't been registered yet — leave a tombstone
                // so registerContinuation can detect the cancellation.
                self.pendingContinuations[messageID] = .cancelled
                return nil
            case .pending(let continuation):
                return continuation
            case .cancelled:
                // Already cancelled — should not happen, but handle gracefully.
                return nil
            }
        }

        /// Remove and return a pending continuation for the given message ID.
        ///
        /// Called from the event loop when a delivery report arrives, or from
        /// the produce error path. Returns `nil` if no continuation is pending
        /// (message sent via `send()`, or already cancelled/resumed).
        @discardableResult
        mutating func removeContinuation(
            for messageID: UInt
        ) -> CheckedContinuation<KafkaDeliveryReport, Error>? {
            guard let existing = self.pendingContinuations.removeValue(forKey: messageID) else {
                return nil
            }
            switch existing {
            case .pending(let continuation):
                return continuation
            case .initialized, .cancelled:
                return nil
            }
        }

        /// Fail all pending continuations.
        ///
        /// Called when the producer shuts down or the event loop terminates.
        /// Prevents `sendAndAwait` from waiting indefinitely if the delivery report
        /// never arrives or isn't processed.
        mutating func failPendingContinuations() {
            let error = KafkaError.connectionClosed(reason: "Producer shut down before delivery report was received")
            for (_, state) in self.pendingContinuations {
                switch state {
                case .pending(let continuation):
                    continuation.resume(throwing: error)
                case .initialized, .cancelled:
                    break
                }
            }
            self.pendingContinuations.removeAll()
        }
    }
}
