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

/// Our `AsyncSequence` implementation wrapping an `NIOAsyncSequenceProducer`.
public struct KafkaAsyncSequence<Element>: AsyncSequence {
    typealias HighLowWatermark = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark
    typealias PollingSystem = KafkaPollingSystem<Element>
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, HighLowWatermark, PollingSystem>
    let wrappedSequence: WrappedSequence

    /// Our `AsynceIteratorProtocol` implementation wrapping `NIOAsyncSequenceProducer.AsyncIterator`.
    public struct KafkaAsyncIterator: AsyncIteratorProtocol {
        let wrappedIterator: NIOAsyncSequenceProducer<Element, HighLowWatermark, PollingSystem>.AsyncIterator

        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    public func makeAsyncIterator() -> KafkaAsyncIterator {
        return KafkaAsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

// MARK: - KafkaPollingSystem

/// A back-pressure aware polling system for managing the poll loop that polls `librdkafka` for new acknowledgements.
final class KafkaPollingSystem<Element>: Sendable {
    typealias HighLowWatermark = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark
    /// The producer type used in the system.
    typealias Producer = NIOAsyncSequenceProducer<Element, HighLowWatermark, KafkaPollingSystem>

    /// The state machine that manages the system's state transitions.
    private let stateMachine: NIOLockedValueBox<StateMachine>

    /// Initializes the ``KafkaPollingSystem``.
    ///
    /// - Note: ``initialize(backPressureStrategy:pollClosure:)`` still has to be invoked for proper initialization.
    init() {
        self.stateMachine = NIOLockedValueBox(StateMachine())
    }

    /// Initialize the ``KafkaPollingSystem`` and create the ``KafkaAsyncSequence`` that publishes
    /// message acknowledgements.
    ///
    /// We use this second `initialize()` method to support delayed initialization,
    /// which is needed because the initialization ``NIOAsyncSequenceProducer`` requires a reference
    /// to an existing ``KafkaPollingSystem`` object but our ``StateMachine`` in turn needs a reference to
    /// the ``NIOAsyncSequenceProducer.Source`` object.
    ///
    /// - Returns: The newly created ``KafkaAsyncSequence`` object.
    func initialize(
        backPressureStrategy: NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark,
        pollClosure: @escaping () -> Void
    ) -> KafkaAsyncSequence<Element> {
        // (NIOAsyncSequenceProducer.makeSequence Documentation Excerpt)
        // This method returns a struct containing a NIOAsyncSequenceProducer.Source and a NIOAsyncSequenceProducer.
        // The source MUST be held by the caller and used to signal new elements or finish.
        // The sequence MUST be passed to the actual consumer and MUST NOT be held by the caller.
        // This is due to the fact that deiniting the sequence is used as part of a trigger to
        // terminate the underlying source.
        let sourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: Element.self,
            backPressureStrategy: backPressureStrategy,
            delegate: self
        )
        let sequence = KafkaAsyncSequence(wrappedSequence: sourceAndSequence.sequence)

        self.stateMachine.withLockedValue {
            $0.initialize(
                source: sourceAndSequence.source,
                pollClosure: pollClosure
            )
        }

        return sequence
    }

    /// Runs the poll loop with the specified poll interval.
    ///
    /// - Parameter pollInterval: The desired time interval between two consecutive polls.
    /// - Returns: An awaitable task representing the execution of the poll loop.
    func run(pollInterval: Duration) async throws {
        switch self.stateMachine.withLockedValue({ $0.run() }) {
        case .alreadyClosed:
            throw KafkaError.pollLoop(reason: "Invocation of \(#function) failed, poll loop already closed.")
        case .alreadyRunning:
            fatalError("Poll loop must not be started more than once")
        case .startLoop:
            break
        }

        while true {
            let action = self.nextPollLoopAction()

            switch action {
            case .pollAndSleep(let pollClosure):
                // Poll Kafka for new acknowledgements and sleep for the given
                // pollInterval to avoid hot looping.
                pollClosure?()
                do {
                    try await Task.sleep(for: pollInterval)
                } catch {
                    self.terminate()
                    throw error
                }
            case .suspendPollLoop:
                // The downstream consumer asked us to stop sending new messages.
                // We therefore await until we are unsuspended again.
                try await withTaskCancellationHandler {
                    // Necessary.
                    // We don't want to create a leaking continuation
                    // when we are already cancelled.
                    // The cancellation of a Task does not mean
                    // its code will not still be executed.
                    guard Task.isCancelled == false else {
                        self.terminate(CancellationError())
                        throw CancellationError()
                    }
                    try await withCheckedThrowingContinuation { continuation in
                        self.stateMachine.withLockedValue { $0.suspendLoop(continuation: continuation) }
                    }
                } onCancel: {
                    self.terminate(CancellationError())
                    return
                }
            case .shutdownPollLoop:
                // We have been asked to close down the poll loop.
                self.terminate()
                return
            }
        }
    }

    /// Yield new elements to the underlying `NIOAsyncSequenceProducer`.
    func yield(_ element: Element) {
        let action = self.stateMachine.withLockedValue { $0.yield(element) }
        switch action {
        case .produceMore:
            self.produceMore()
        case .stopProducing:
            self.stopProducing()
        case .terminate:
            self.terminate()
        case .none:
            break
        }
    }

    /// Determines the next action to be taken in the poll loop based on the current state.
    ///
    /// - Returns: The next action for the poll loop.
    func nextPollLoopAction() -> KafkaPollingSystem.StateMachine.PollLoopAction {
        return self.stateMachine.withLockedValue { $0.nextPollLoopAction() }
    }

    /// Stop producing new elements to the
    /// `source` ``NIOAsyncSequenceProducer``.
    private func stopProducing() {
        self.stateMachine.withLockedValue { $0.stopProducing() }
    }

    /// Kill the ``KafkaPollingSystem``'s poll loop and free its resources.
    func terminate(_ error: Error? = nil) {
        let action = self.stateMachine.withLockedValue { $0.terminate(error) }
        self.handleTerminateAction(action)
    }

    /// Invokes the desired action after ``KafkaPollingSystem/StateMachine/terminate()``
    /// has been invoked.
    private func handleTerminateAction(_ action: StateMachine.TerminateAction?) {
        switch action {
        case .finishSequenceSource(let source):
            source?.finish()
        case .finishSequenceSourceAndResume(let source, let continuation):
            source?.finish()
            continuation?.resume()
        case .finishSequenceSourceAndResumeWithError(let source, let continuation, let error):
            source?.finish()
            continuation?.resume(throwing: error)
        case .none:
            break
        }
    }
}

extension KafkaPollingSystem: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        let action = self.stateMachine.withLockedValue { $0.produceMore() }
        switch action {
        case .resume(let continuation):
            continuation?.resume()
        case .none:
            break
        }
    }

    func didTerminate() {
        self.terminate()
    }
}

extension KafkaPollingSystem {
    /// The state machine used by the ``KafkaPollingSystem``.
    struct StateMachine {
        /// The possible states of the state machine.
        enum State {
            /// The state machine has been initialized with init() but is not yet Initialized
            /// using `func initialize()` (required).
            case uninitialized
            /// Initialized state (idle).
            case idle(
                source: Producer.Source?,
                pollClosure: (() -> Void)?,
                running: Bool
            )
            /// The system is up and producing acknowledgement messages.
            case producing(
                source: Producer.Source?,
                pollClosure: (() -> Void)?,
                running: Bool
            )
            /// The poll loop is currently suspended and we are waiting for an invocation
            /// of `produceMore()` to continue producing messages.
            case stopProducing(
                source: Producer.Source?,
                continuation: CheckedContinuation<Void, Error>?,
                pollClosure: (() -> Void)?,
                running: Bool
            )
            /// The system is shut down.
            case finished
        }

        /// The current state of the state machine.
        private var state: State = .uninitialized

        /// Delayed initialization of `StateMachine` as the `source` and the `pollClosure` are
        /// not yet available when the normal initialization occurs.
        mutating func initialize(
            source: Producer.Source,
            pollClosure: @escaping () -> Void
        ) {
            guard case .uninitialized = self.state else {
                fatalError("\(#function) can only be invoked in state .uninitialized, but was invoked in state \(self.state)")
            }
            self.state = .idle(source: source, pollClosure: pollClosure, running: false)
        }

        /// Actions to take after ``run()`` has been invoked on the ``KafkaPollingSystem/StateMachine``.
        enum RunAction {
            /// Do nothing, the poll loop is already terminated.
            case alreadyClosed
            /// The poll loop is already running. ``run()`` should not be invoked more than once!
            case alreadyRunning
            /// The poll loop can now be started.
            case startLoop
        }

        mutating func run() -> RunAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state .uninitialized")
            case .idle(let source, let pollClosure, let running):
                guard running == false else {
                    return .alreadyRunning
                }
                self.state = .idle(source: source, pollClosure: pollClosure, running: true)
            case .producing(let source, let pollClosure, let running):
                guard running == false else {
                    return .alreadyRunning
                }
                self.state = .producing(source: source, pollClosure: pollClosure, running: true)
            case .stopProducing(let source, let continuation, let pollClosure, let running):
                guard running == false else {
                    return .alreadyRunning
                }
                self.state = .stopProducing(source: source, continuation: continuation, pollClosure: pollClosure, running: true)
            case .finished:
                return .alreadyClosed
            }

            return .startLoop
        }

        /// The possible actions for the poll loop.
        enum PollLoopAction {
            /// Ask `librdkakfa` to receive new message acknowledgements at a given poll interval.
            case pollAndSleep(pollClosure: (() -> Void)?)
            /// Suspend the poll loop.
            case suspendPollLoop
            /// Shutdown the poll loop.
            case shutdownPollLoop
        }

        /// Determines the next action to be taken in the poll loop based on the current state.
        ///
        /// - Returns: The next action for the poll loop.
        func nextPollLoopAction() -> PollLoopAction {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state .uninitialized")
            case .idle(_, let pollClosure, _), .producing(_, let pollClosure, _):
                return .pollAndSleep(pollClosure: pollClosure)
            case .stopProducing:
                // We were asked to stop producing,
                // but the poll loop is still running.
                // Trigger the poll loop to suspend.
                return .suspendPollLoop
            case .finished:
                return .shutdownPollLoop
            }
        }

        /// Actions to take after an element has been yielded to the underlying ``NIOAsyncSequenceProducer.Source``.
        enum YieldAction {
            /// Produce more elements.
            case produceMore
            /// Stop producing new elements.
            case stopProducing
            /// Shut down the ``KafkaPollingSystem``.
            case terminate
        }

        /// Yield new elements to the underlying `NIOAsyncSequenceProducer`.
        mutating func yield(_ element: Element) -> YieldAction? {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state .uninitialized")
            case .idle(let source, _, _), .producing(let source, _, _), .stopProducing(let source, _, _, _):
                // We can also yield when in .stopProducing,
                // the AsyncSequenceProducer will buffer for us
                let yieldResult = source?.yield(element)
                switch yieldResult {
                case .produceMore:
                    return .produceMore
                case .stopProducing:
                    return .stopProducing
                case .dropped:
                    return .terminate
                case .none:
                    return nil
                }
            case .finished:
                return nil
            }
        }

        /// Actions to take after ``produceMore()`` has been invoked on the ``KafkaPollingSystem/StateMachine``.
        enum ProduceMoreAction {
            /// Resume the given `continuation`.
            case resume(CheckedContinuation<Void, Error>?)
        }

        /// Our downstream consumer allowed us to produce more elements.
        mutating func produceMore() -> ProduceMoreAction? {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state .uninitialized")
            case .finished, .producing:
                break
            case .stopProducing(let source, let continuation, let pollClosure, let running):
                self.state = .producing(source: source, pollClosure: pollClosure, running: running)
                return .resume(continuation)
            case .idle(let source, let pollClosure, let running):
                self.state = .producing(source: source, pollClosure: pollClosure, running: running)
            }
            return nil
        }

        /// Our downstream consumer asked us to stop producing new elements.
        mutating func stopProducing() {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state .uninitialized")
            case .idle, .finished, .stopProducing:
                break
            case .producing(let source, let pollClosure, let running):
                self.state = .stopProducing(source: source, continuation: nil, pollClosure: pollClosure, running: running)
            }
        }

        /// Suspend the poll loop.
        ///
        /// - Parameter continuation: The continuation that will be resumed once we are allowed to produce again.
        /// After resuming the continuation, our poll loop will start running again.
        mutating func suspendLoop(continuation: CheckedContinuation<Void, Error>) {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state .uninitialized")
            case .finished:
                return
            case .stopProducing(_, .some, _, _):
                fatalError("Internal state inconsistency. Run loop is running more than once")
            case .idle(let source, let pollClosure, let running), .producing(let source, let pollClosure, let running), .stopProducing(let source, _, let pollClosure, let running):
                self.state = .stopProducing(source: source, continuation: continuation, pollClosure: pollClosure, running: running)
            }
        }

        /// Actions to take after ``terminate()`` has been invoked on the ``KafkaPollingSystem/StateMachine``.
        enum TerminateAction {
            /// Invoke `finish()` on the given `NIOAsyncSequenceProducer.Source`.
            case finishSequenceSource(source: Producer.Source?)
            /// Invoke `finish()` on the given `NIOAsyncSequenceProducer.Source`
            /// and resume the given `continuation`.
            case finishSequenceSourceAndResume(
                source: Producer.Source?,
                continuation: CheckedContinuation<Void, Error>?
            )
            /// Invoke `finish()` on the given `NIOAsyncSequenceProducer.Source`
            /// and resume the given `continuation` with an error.
            case finishSequenceSourceAndResumeWithError(
                source: Producer.Source?,
                continuation: CheckedContinuation<Void, Error>?,
                error: Error
            )
        }

        /// Terminate the state machine and finish producing elements.
        mutating func terminate(_ error: Error? = nil) -> TerminateAction? {
            switch self.state {
            case .uninitialized:
                fatalError("\(#function) invoked while still in state .uninitialized")
            case .finished:
                return nil
            case .idle(let source, _, _), .producing(let source, _, _):
                self.state = .finished
                return .finishSequenceSource(source: source)
            case .stopProducing(let source, let continuation, _, _):
                self.state = .finished
                if let error = error {
                    return .finishSequenceSourceAndResumeWithError(
                        source: source,
                        continuation: continuation,
                        error: error
                    )
                } else {
                    return .finishSequenceSourceAndResume(source: source, continuation: continuation)
                }
            }
        }
    }
}
