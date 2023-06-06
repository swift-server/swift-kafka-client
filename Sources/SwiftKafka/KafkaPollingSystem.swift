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

/// A back-pressure aware polling system for managing the poll loop that polls `librdkafka` for new acknowledgements.
final class KafkaPollingSystem<Element>: Sendable {
    typealias HighLowWatermark = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark
    /// The producer type used in the system.
    typealias Producer = NIOAsyncSequenceProducer<Element, HighLowWatermark, KafkaPollingSystem>

    /// The state machine that manages the system's state transitions.
    let stateMachineLock: NIOLockedValueBox<StateMachine>

    /// Initializes the ``KafkaBackPressurePollingSystem``.
    /// Private initializer. The ``KafkaBackPressurePollingSystem`` is not supposed to be initialized directly.
    /// It must rather be initialized using the ``KafkaBackPressurePollingSystem.createSystemAndSequence`` function.
    init() {
        self.stateMachineLock = NIOLockedValueBox(StateMachine())
    }

    /// Runs the poll loop with the specified poll interval.
    ///
    /// - Parameter pollInterval: The desired time interval between two consecutive polls.
    /// - Returns: An awaitable task representing the execution of the poll loop.
    func run(pollInterval: Duration, pollClosure: @escaping () -> Void, source: Producer.Source) async {
        switch self.stateMachineLock.withLockedValue({ $0.run(source, pollClosure) }) {
        case .alreadyClosed:
            return
        case .alreadyRunning:
            fatalError("Poll loop must not be started more than once")
        case .startLoop:
            break
        }

        while true {
            let action = self.stateMachineLock.withLockedValue { $0.nextPollLoopAction() }

            switch action {
            case .pollAndSleep(let pollClosure):
                // Poll Kafka for new acknowledgements and sleep for the given
                // pollInterval to avoid hot looping.
                pollClosure()
                do {
                    try await Task.sleep(for: pollInterval)
                } catch {
                    let action = self.stateMachineLock.withLockedValue { $0.terminate() }
                    self.handleTerminateAction(action)
                }
            case .suspendPollLoop:
                // The downstream consumer asked us to stop sending new messages.
                // We therefore await until we are unsuspended again.
                await withTaskCancellationHandler {
                    await withCheckedContinuation { continuation in
                        self.stateMachineLock.withLockedValue { $0.suspendLoop(continuation: continuation) }
                    }
                } onCancel: {
                    let action = self.stateMachineLock.withLockedValue { $0.terminate() }
                    self.handleTerminateAction(action)
                }
            case .shutdownPollLoop:
                // We have been asked to close down the poll loop.
                let action = self.stateMachineLock.withLockedValue { $0.terminate() }
                self.handleTerminateAction(action)
            }
        }
    }

    /// Yield new elements to the underlying `NIOAsyncSequenceProducer`.
    func yield(_ element: Element) {
        self.stateMachineLock.withLockedValue { stateMachine in
            switch stateMachine.state {
            case .started(let source, _, _), .producing(let source, _, _), .stopProducing(let source, _, _, _):
                // We can also yield when in .stopProducing,
                // the AsyncSequenceProducer will buffer for us
                let yieldResult = source.yield(element)
                switch yieldResult {
                case .produceMore:
                    let action = stateMachine.produceMore()
                    switch action {
                    case .resume(let continuation):
                        continuation?.resume()
                    case .none:
                        break
                    }
                case .stopProducing:
                    stateMachine.stopProducing()
                case .dropped:
                    let action = stateMachine.terminate()
                    self.handleTerminateAction(action)
                }
            case .idle, .finished:
                return
            }
        }
    }

    /// Invokes the desired action after ``KafkaPollingSystem/StateMachine/terminate()``
    /// has been invoked.
    func handleTerminateAction(_ action: StateMachine.TerminateAction?) {
        switch action {
        case .finishSequenceSource(let source):
            source.finish()
        case .finishSequenceSourceAndResume(let source, let continuation):
            source.finish()
            continuation?.resume()
        case .none:
            break
        }
    }
}

extension KafkaPollingSystem: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        let action = self.stateMachineLock.withLockedValue { $0.produceMore() }
        switch action {
        case .resume(let continuation):
            continuation?.resume()
        case .none:
            break
        }
    }

    func didTerminate() {
        let action = self.stateMachineLock.withLockedValue { $0.terminate() }
        self.handleTerminateAction(action)
    }
}

extension KafkaPollingSystem {
    /// The state machine used by the ``KafkaBackPressurePollingSystem``.
    struct StateMachine {
        /// The possible states of the state machine.
        enum State {
            /// Initial state.
            case idle
            /// The ``run()`` method has been invoked and the ``KafkaPollingSystem`` is ready.
            case started(
                source: Producer.Source,
                pollClosure: () -> Void,
                running: Bool
            )
            /// The system up and producing acknowledgement messages.
            case producing(
                source: Producer.Source,
                pollClosure: () -> Void,
                running: Bool
            )
            /// The pool loop is currently suspended and we are waiting for an invocation
            /// of `produceMore()` to continue producing messages.
            case stopProducing(
                source: Producer.Source,
                continuation: CheckedContinuation<Void, Never>?,
                pollClosure: () -> Void,
                running: Bool
            )
            /// The system is shut down.
            case finished
        }

        /// The current state of the state machine.
        var state: State = .idle

        /// Actions to take after ``run()`` has been invoked on the ``KafkaPollingSystem/StateMachine``.
        enum RunAction {
            /// Do nothing, the poll loop is already terminated.
            case alreadyClosed
            /// The poll loop is already running. ``run()`` should not be invoked more than once!
            case alreadyRunning
            /// The poll loop can now be started.
            case startLoop
        }

        mutating func run(_ source: Producer.Source, _ pollClosure: @escaping () -> Void) -> RunAction {
            switch self.state {
            case .idle:
                self.state = .started(source: source, pollClosure: pollClosure, running: true)
            case .started(let source, let pollClosure, let running):
                guard running == false else {
                    return .alreadyRunning
                }
                self.state = .started(source: source, pollClosure: pollClosure, running: true)
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
            case pollAndSleep(pollClosure: () -> Void)
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
            case .idle:
                fatalError("State machine must be initialized with prepare()")
            case .started(_, let pollClosure, _), .producing(_, let pollClosure, _):
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

        /// Actions to take after ``produceMore()`` has been invoked on the ``KafkaPollingSystem/StateMachine``.
        enum ProduceMoreAction {
            /// Resume the given `continuation`.
            case resume(CheckedContinuation<Void, Never>?)
        }

        /// Our downstream consumer allowed us to produce more elements.
        mutating func produceMore() -> ProduceMoreAction? {
            switch self.state {
            case .idle, .finished, .producing:
                break
            case .stopProducing(let source, let continuation, let pollClosure, let running):
                self.state = .producing(source: source, pollClosure: pollClosure, running: running)
                return .resume(continuation)
            case .started(let source, let pollClosure, let running):
                self.state = .producing(source: source, pollClosure: pollClosure, running: running)
            }
            return nil
        }

        /// Our downstream consumer asked us to stop producing new elements.
        mutating func stopProducing() {
            switch self.state {
            case .idle, .finished, .stopProducing:
                break
            case .started:
                fatalError("\(#function) is not supported in state \(self.state)")
            case .producing(let source, let pollClosure, let running):
                self.state = .stopProducing(source: source, continuation: nil, pollClosure: pollClosure, running: running)
            }
        }

        /// Suspend the poll loop.
        ///
        /// - Parameter continuation: The continuation that will be resumed once we are allowed to produce again.
        /// After resuming the continuation, our poll loop will start running again.
        fileprivate mutating func suspendLoop(continuation: CheckedContinuation<Void, Never>) {
            switch self.state {
            case .idle, .finished:
                return
            case .stopProducing(_, .some, _, _):
                fatalError("Internal state inconsistency. Run loop is running more than once")
            case .started(let source, let pollClosure, let running), .producing(let source, let pollClosure, let running), .stopProducing(let source, _, let pollClosure, let running):
                self.state = .stopProducing(source: source, continuation: continuation, pollClosure: pollClosure, running: running)
            }
        }

        /// Actions to take after ``terminate()`` has been invoked on the ``KafkaPollingSystem/StateMachine``.
        enum TerminateAction {
            /// Invoke `finish()` on the given `NIOAsyncSequenceProducer.Source`.
            case finishSequenceSource(source: Producer.Source)
            /// Invoke `finish()` on the given `NIOAsyncSequenceProducer.Source`
            /// and resume the given `continuation`.
            case finishSequenceSourceAndResume(
                source: Producer.Source,
                continuation: CheckedContinuation<Void, Never>?
            )
        }

        /// Terminate the state machine and finish producing elements.
        mutating func terminate() -> TerminateAction? {
            switch self.state {
            case .idle, .finished:
                return nil
            case .started(let source, _, _), .producing(let source, _, _):
                self.state = .finished
                return .finishSequenceSource(source: source)
            case .stopProducing(let source, let continuation, _, _):
                self.state = .finished
                return .finishSequenceSourceAndResume(source: source, continuation: continuation)
            }
        }
    }
}
