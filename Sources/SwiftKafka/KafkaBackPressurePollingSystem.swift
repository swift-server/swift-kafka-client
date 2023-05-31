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
final class KafkaBackPressurePollingSystem {
    /// The element type for the system, representing either a successful ``KafkaAcknowledgedMessage`` or a ``KafkaAcknowledgedMessageError``.
    typealias Element = Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>
    /// The producer type used in the system.
    typealias Producer = NIOAsyncSequenceProducer<Element, NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark, KafkaBackPressurePollingSystem>

    /// The state machine that manages the system's state transitions.
    let stateMachineLock: NIOLockedValueBox<StateMachine>

    /// Closure that takes care of polling `librdkafka` for new messages.
    var pollClosure: (() -> Void)?
    /// The ``NIOAsyncSequenceProducer.Source`` used for yielding the messages to the ``NIOAsyncSequenceProducer``.
    var sequenceSource: Producer.Source? {
        get {
            self.stateMachineLock.withLockedValue { stateMachine in
                return stateMachine.sequenceSource
            }
        }
        set {
            self.stateMachineLock.withLockedValue { stateMachine in
                stateMachine.sequenceSource = newValue
            }
        }
    }

    /// A logger.
    private let logger: Logger

    /// Initializes the ``KafkaBackPressurePollingSystem``.
    ///
    /// - Parameter logger: The logger to be used for logging.
    init(logger: Logger) {
        self.stateMachineLock = NIOLockedValueBox(StateMachine())
        self.logger = logger
    }

    /// Runs the poll loop with the specified poll interval.
    ///
    /// - Parameter pollInterval: The desired time interval between two consecutive polls.
    /// - Returns: An awaitable task representing the execution of the poll loop.
    func run(pollInterval: Duration) async {
        actionLoop: while true {
            let action = self.stateMachineLock.withLockedValue { $0.nextPollLoopAction() }

            switch action {
            case .pollAndSleep:
                self.pollClosure?()
                do {
                    try await Task.sleep(for: pollInterval)
                } catch {
                    self.stateMachineLock.withLockedValue { $0.shutDown() }
                    break actionLoop
                }
            case .suspendPollLoop:
                await withTaskCancellationHandler {
                    await withCheckedContinuation { continuation in
                        self.stateMachineLock.withLockedValue { $0.suspendLoop(continuation: continuation) }
                    }
                } onCancel: {
                    self.stateMachineLock.withLockedValue { $0.shutDown() }
                }
            case .shutdownPollLoop:
                self.stateMachineLock.withLockedValue { $0.shutDown() }
                break actionLoop
            }
        }
    }

    /// The delivery report callback function that handles acknowledged messages.
    private(set) lazy var deliveryReportCallback: (UnsafePointer<rd_kafka_message_t>?) -> Void = { messagePointer in
        guard let messagePointer = messagePointer else {
            self.logger.error("Could not resolve acknowledged message")
            return
        }

        let messageID = UInt(bitPattern: messagePointer.pointee._private)

        let messageResult: Element
        do {
            let message = try KafkaAcknowledgedMessage(messagePointer: messagePointer, id: messageID)
            messageResult = .success(message)
        } catch {
            guard let error = error as? KafkaAcknowledgedMessageError else {
                fatalError("Caught error that is not of type \(KafkaAcknowledgedMessageError.self)")
            }
            messageResult = .failure(error)
        }

        self.stateMachineLock.withLockedValue { stateMachine in
            let yieldResult = stateMachine.yield(messageResult)
            switch yieldResult {
            case .produceMore:
                stateMachine.produceMore()
            case .stopProducing:
                stateMachine.stopProducing()
            case .dropped:
                stateMachine.shutDown()
            }
        }
        // The messagePointer is automatically destroyed by librdkafka
        // For safety reasons, we only use it inside of this closure
    }
}

extension KafkaBackPressurePollingSystem: NIOAsyncSequenceProducerDelegate {
    func produceMore() {
        self.stateMachineLock.withLockedValue { $0.produceMore() }
    }

    func didTerminate() {
        self.stateMachineLock.withLockedValue { $0.shutDown() }
    }
}

extension KafkaBackPressurePollingSystem {
    /// The state machine used by the ``KafkaBackPressurePollingSystem``.
    struct StateMachine: Sendable {
        /// The ``NIOAsyncSequenceProducer.Source`` used for yielding the messages to the ``NIOAsyncSequenceProducer``.
        var sequenceSource: Producer.Source? // TODO: make sendable?

        /// The events that can be triggered by the ``KafkaBackPressurePollingSystem``.
        enum Event {
            /// Produce more elements.
            case produceMore
            /// Suspend the poll loop until the continuation is resumed.
            case suspendLoop(CheckedContinuation<Void, Never>)
            /// Our `Task` was cancelled why we were waiting for the loop to be unsuspended.
            case loopSuspensionCancelled
            /// Request to stop producing elements. When this reaches the poll loop it will escalate
            /// to a full suspension.
            case stopProducing
            /// Close the entire ``KafkaBackPressurePollingSystem`` with its poll loop.
            case shutdown
        }

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

        /// Our downstream consumer allowed us to produce more elements.
        mutating func produceMore() {
            switch self.state {
            case .finished, .producing:
                return
            case .stopProducing(let continuation):
                continuation?.resume()
                fallthrough
            case .initial:
                self.state = .producing
            }
        }

        /// Our downstream consumer asked us to stop producing new elements.
        mutating func stopProducing() {
            switch self.state {
            case .finished, .stopProducing:
                return
            case .producing:
                self.state = .stopProducing(nil)
            default:
                fatalError("\(#function) is not supported in state \(self.state)")
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
            case .stopProducing(let existingContinuation) where existingContinuation != nil:
                fatalError("Created leaking continuation")
            case .initial, .producing, .stopProducing:
                self.state = .stopProducing(continuation)
            }
        }

        /// Shut down the state machine and finish producing elements.
        mutating func shutDown() {
            self.sequenceSource?.finish()

            switch self.state {
            case .finished:
                return
            case .stopProducing(let continuation):
                continuation?.resume()
                fallthrough
            default:
                self.sequenceSource?.finish()
                self.state = .finished
            }
        }

        /// Yields a new elements to the ``NIOAsyncSequenceProducer``.
        fileprivate func yield(_ element: Element) -> Producer.Source.YieldResult {
            guard let source = self.sequenceSource else {
                fatalError("\(#function) requires the sequenceSource variable to be non-nil")
            }
            return source.yield(element)
        }
    }
}
