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
public struct KafkaAsyncSequence<Element, BackPressure: NIOAsyncSequenceProducerBackPressureStrategy>: AsyncSequence {
    typealias PollingSystem = KafkaPollingSystem<Element, BackPressure>
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, BackPressure, PollingSystem>
    let wrappedSequence: WrappedSequence

    /// Our `AsynceIteratorProtocol` implementation wrapping `NIOAsyncSequenceProducer.AsyncIterator`.
    public struct KafkaAsyncIterator: AsyncIteratorProtocol {
        let wrappedIterator: NIOAsyncSequenceProducer<Element, BackPressure, PollingSystem>.AsyncIterator

        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    public func makeAsyncIterator() -> KafkaAsyncIterator {
        return KafkaAsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

/// A back-pressure aware polling system for managing the poll loop that polls `librdkafka` for new acknowledgements.
final class KafkaPollingSystem<
    Element,
    BackPressure: NIOAsyncSequenceProducerBackPressureStrategy
>: Sendable {
    /// The producer type used in the system.
    typealias Producer = NIOAsyncSequenceProducer<Element, BackPressure, KafkaPollingSystem>

    /// The state machine that manages the system's state transitions.
    let stateMachineLock: NIOLockedValueBox<StateMachine>

    /// Closure that takes care of polling `librdkafka` for new messages.
    var pollClosure: (() -> Void)? {
        get {
            self.stateMachineLock.withLockedValue { stateMachine in
                return stateMachine.pollClosure
            }
        }
        set {
            self.stateMachineLock.withLockedValue { stateMachine in
                stateMachine.pollClosure = newValue
            }
        }
    }

    /// Initializes the ``KafkaBackPressurePollingSystem``.
    /// Private initializer. The ``KafkaBackPressurePollingSystem`` is not supposed to be initialized directly.
    /// It must rather be initialized using the ``KafkaBackPressurePollingSystem.createSystemAndSequence`` function.
    /* USE createSystemAndSequence, TESTING only */ init() {
        self.stateMachineLock = NIOLockedValueBox(StateMachine())
    }

    /// Factory method creating a ``KafkaBackPressurePollingSystem`` and the ``AsyncSequence`` that receives its messages .
    /// The caller of this function must retain the sequence in order to receive messages.
    ///
    /// - Returns: A tuple containing the ``KafkaBackPressurePollingSystem`` and a reference to the ``KafkaAsyncSequence``.
    static func createSystemAndSequence(
        backPressureStrategy: BackPressure
    ) -> (KafkaPollingSystem, KafkaAsyncSequence<Element, BackPressure>) {
        let pollingSystem = KafkaPollingSystem()

        // (NIOAsyncSequenceProducer.makeSequence Documentation Excerpt)
        // This method returns a struct containing a NIOAsyncSequenceProducer.Source and a NIOAsyncSequenceProducer.
        // The source MUST be held by the caller and used to signal new elements or finish.
        // The sequence MUST be passed to the actual consumer and MUST NOT be held by the caller.
        // This is due to the fact that deiniting the sequence is used as part of a trigger to
        // terminate the underlying source.
        let acknowledgementsSourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: Element.self,
            backPressureStrategy: backPressureStrategy,
            delegate: pollingSystem
        )

        pollingSystem.stateMachineLock.withLockedValue { stateMachine in
            stateMachine.sequenceSource = acknowledgementsSourceAndSequence.source
        }

        let sequence = KafkaAsyncSequence(
            wrappedSequence: acknowledgementsSourceAndSequence.sequence
        )

        return (pollingSystem, sequence)
    }

    /// Runs the poll loop with the specified poll interval.
    ///
    /// - Parameter pollInterval: The desired time interval between two consecutive polls.
    /// - Returns: An awaitable task representing the execution of the poll loop.
    func run(pollInterval: Duration) async {
        self.stateMachineLock.withLockedValue { stateMachine in
            guard stateMachine.running == false else {
                fatalError("Poll loop must not be started more than once")
            }
            stateMachine.running = true
        }

        while true {
            let action = self.stateMachineLock.withLockedValue { $0.nextPollLoopAction() }

            switch action {
            case .pollAndSleep:
                // Poll Kafka for new acknowledgements and sleep for the given
                // pollInterval to avoid hot looping.
                self.pollClosure?()
                do {
                    try await Task.sleep(for: pollInterval)
                } catch {
                    let command = self.stateMachineLock.withLockedValue { $0.shutDown() }
                    self.handleStateMachineCommand(command)
                }
            case .suspendPollLoop:
                // The downstream consumer asked us to stop sending new messages.
                // We therefore await until we are unsuspended again.
                await withTaskCancellationHandler {
                    await withCheckedContinuation { continuation in
                        self.stateMachineLock.withLockedValue { $0.suspendLoop(continuation: continuation) }
                    }
                } onCancel: {
                    let command = self.stateMachineLock.withLockedValue { $0.shutDown() }
                    self.handleStateMachineCommand(command)
                }
            case .shutdownPollLoop:
                // We have been asked to close down the poll loop.
                let command = self.stateMachineLock.withLockedValue { $0.shutDown() }
                self.handleStateMachineCommand(command)
                return
            }
        }
    }

    /// Handles an optional command that the ``KafkaBackPressurePollingSystem/StateMachine`` has wants us to run.
    ///
    /// - Parameter command: The command the ``KafkaBackPressurePollingSystem/StateMachine`` wants us to run.
    func handleStateMachineCommand(_ command: StateMachine.Command?) {
        switch command {
        case .resume(let continuation):
            continuation?.resume()
        case .finishSequenceSource:
            self.stateMachineLock.withLockedValue { $0.sequenceSource?.finish() }
        case .finishSequenceSourceAndResume(let continuation):
            self.stateMachineLock.withLockedValue { $0.sequenceSource?.finish() }
            continuation?.resume()
        case .none:
            break
        }
    }
}

extension KafkaPollingSystem: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        let command = self.stateMachineLock.withLockedValue { $0.produceMore() }
        self.handleStateMachineCommand(command)
    }

    func didTerminate() {
        let command = self.stateMachineLock.withLockedValue { $0.shutDown() }
        self.handleStateMachineCommand(command)
    }
}

extension KafkaPollingSystem {
    /// The state machine used by the ``KafkaBackPressurePollingSystem``.
    struct StateMachine {
        /// A flag that determines if the ``run()`` method has already been invoked.
        var running = false
        /// Closure that takes care of polling `librdkafka` for new messages.
        var pollClosure: (() -> Void)?
        /// The ``NIOAsyncSequenceProducer.Source`` used for yielding the messages to the ``NIOAsyncSequenceProducer``.
        var sequenceSource: Producer.Source?

        /// The possible states of the state machine.
        enum State {
            /// Initial state.
            case initial
            /// The system up and producing acknowledgement messages.
            case producing
            /// The pool loop is currently suspended and we are waiting for an invocation
            /// of `produceMore()` to continue producing messages.
            case stopProducing(CheckedContinuation<Void, Never>?)
            /// The system is shut down.
            case finished
        }

        /// The current state of the state machine.
        var state = State.initial

        /// The possible actions for the poll loop.
        enum PollLoopAction {
            /// Ask `librdkakfa` to receive new message acknowledgements at a given poll interval.
            case pollAndSleep
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
            case .initial, .producing:
                return .pollAndSleep
            case .stopProducing:
                // We were asked to stop producing,
                // but the poll loop is still running.
                // Trigger the poll loop to suspend.
                return .suspendPollLoop
            case .finished:
                return .shutdownPollLoop
            }
        }

        /// Represents the commands that can be returned by a state machine
        /// and shall be executed by the ``KafkaBackPressurePollingSystem``.
        enum Command {
            /// Resume the given continuation.
            case resume(CheckedContinuation<Void, Never>?)
            /// Invoke `.finish()` on the ``NIOAsyncSequence.Source``.
            case finishSequenceSource
            /// Resume the given continuation and invoke `.finish()` on the ``NIOAsyncSequence.Source``.
            case finishSequenceSourceAndResume(CheckedContinuation<Void, Never>?)
        }

        /// Our downstream consumer allowed us to produce more elements.
        mutating func produceMore() -> Command? {
            switch self.state {
            case .finished, .producing:
                break
            case .stopProducing(let continuation):
                self.state = .producing
                return .resume(continuation)
            case .initial:
                self.state = .producing
            }
            return nil
        }

        /// Our downstream consumer asked us to stop producing new elements.
        mutating func stopProducing() {
            switch self.state {
            case .finished, .stopProducing:
                break
            case .initial:
                fatalError("\(#function) is not supported in state \(self.state)")
            case .producing:
                self.state = .stopProducing(nil)
            }
        }

        /// Suspend the poll loop.
        ///
        /// - Parameter continuation: The continuation that will be resumed once we are allowed to produce again.
        /// After resuming the continuation, our poll loop will start running again.
        fileprivate mutating func suspendLoop(continuation: CheckedContinuation<Void, Never>) {
            switch self.state {
            case .finished:
                return
            case .stopProducing(.some):
                fatalError("Internal state inconsistency. Run loop is running more than once")
            case .initial, .producing, .stopProducing:
                self.state = .stopProducing(continuation)
            }
        }

        /// Shut down the state machine and finish producing elements.
        mutating func shutDown() -> Command? {
            switch self.state {
            case .finished:
                return nil
            case .initial, .producing:
                self.state = .finished
                return .finishSequenceSource
            case .stopProducing(let continuation):
                self.state = .finished
                return .finishSequenceSourceAndResume(continuation)
            }
        }
    }
}
