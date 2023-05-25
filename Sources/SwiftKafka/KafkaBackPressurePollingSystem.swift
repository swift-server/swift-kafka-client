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
import NIOCore
import NIOConcurrencyHelpers

// TODO: write test for evaluating right state transitions
// TODO: test abrupt run task shutdown -> cancellation handler

/// A back-pressure aware polling system for managing the poll loop that polls `librdkafka` for new acknowledgements.
final class KafkaBackPressurePollingSystem {
    /// The element type for the system, representing either a successful ``KafkaAcknowledgedMessage`` or a ``KafkaAcknowledgedMessageError``.
    typealias Element = Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>
    /// The producer type used in the system.
    typealias Producer = NIOAsyncSequenceProducer<Element, NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark, KafkaBackPressurePollingSystem>

    /// The state machine that manages the system's state transitions.
    let stateMachine: StateMachine

    // TODO: wrapped values are non-sendable
    private let _client: NIOLockedValueBox<KafkaClient?>
    private let _sequenceSource: NIOLockedValueBox<Producer.Source?>

    /// The ``KafkaClient`` used for doing the actual polling.
    var client: KafkaClient? {
        get {
            self._client.withLockedValue {
                return $0
            }
        }
        set {
            self._client.withLockedValue {
                $0 = newValue
            }
        }
    }

    /// The ``NIOAsyncSequenceProducer.Source`` used for yielding the messages to the ``NIOAsyncSequenceProducer``.
    var sequenceSource: Producer.Source? {
        get {
            self._sequenceSource.withLockedValue {
                return $0
            }
        }
        set {
            self._sequenceSource.withLockedValue {
                $0 = newValue
            }
        }
    }

    private let logger: Logger

    /// Initializes the ``KafkaBackPressurePollingSystem``.
    ///
    /// - Parameter logger: The logger to be used for logging.
    init(logger: Logger) {
        self.stateMachine = StateMachine()
        self.logger = logger

        self._client = NIOLockedValueBox(nil)
        self._sequenceSource = NIOLockedValueBox(nil)
    }

    /// Runs the poll loop with the specified poll interval.
    ///
    /// - Parameter pollIntervalNanos: The poll interval in nanoseconds.
    /// - Returns: An awaitable task representing the execution of the poll loop.
    func run(pollIntervalNanos: UInt64) async {
        actionLoop: while true {
            let action = self.stateMachine.nextPollLoopAction()

            switch action {
            case .poll:
                self.client?.withKafkaHandlePointer { handle in
                    rd_kafka_poll(handle, 0)
                }
                try? await Task.sleep(nanoseconds: pollIntervalNanos)
            case .suspendPollLoop:
                await withTaskCancellationHandler {
                    await withCheckedContinuation { continuation in
                        self.stateMachine.eventTriggered(.suspendLoop(continuation))
                    }
                } onCancel: {
                    self.stateMachine.eventTriggered(.loopSuspensionCancelled)
                }
            case .shutdownPollLoop:
                self.stateMachine.eventTriggered(.shutdown)
                break actionLoop
            }
        }
    }

    // TODO: Race between poll loop and delivery report callback?
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

        guard let yieldResult = self.sequenceSource?.yield(messageResult) else {
            fatalError("\(#function) requires the sequenceSource variable to be non-nil")
        }

        switch yieldResult {
        case .produceMore:
            self.stateMachine.eventTriggered(.produceMore)
        case .stopProducing:
            self.stateMachine.eventTriggered(.stopProducing)
        case .dropped:
            self.stateMachine.eventTriggered(.shutdown)
        }

        // The messagePointer is automatically destroyed by librdkafka
        // For safety reasons, we only use it inside of this closure
    }
}

extension KafkaBackPressurePollingSystem: NIOAsyncSequenceProducerDelegate {

    func produceMore() {
        self.stateMachine.eventTriggered(.produceMore)
    }

    func didTerminate() {
        self.stateMachine.eventTriggered(.shutdown)
    }
}

extension KafkaBackPressurePollingSystem {
    // TODO: test state machine

    /// The state machine used by the ``KafkaBackPressurePollingSystem``.
    struct StateMachine {
        /// The possible actions for the poll loop.
        enum PollLoopAction {
            /// Ask `librdkakfa` to receive new message acknowledgements through.
            case poll
            /// Suspend the poll loop.
            case suspendPollLoop
            /// Shutdown the poll loop.
            case shutdownPollLoop
        }

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
            /// We were asked to stop producing messages. However we still wait for the
            /// poll loop to be suspended.
            case suspended
            /// The pool loop is currently suspended and we are waiting for an invocation
            /// of `produceMore()` to continue producing messages.
            case loopSuspended(CheckedContinuation<Void, Never>)
            /// The system is shut down.
            case shutDown
        }

        /// The current state of the state machine.
        let state = NIOLockedValueBox(State.initial)

        /// Determines the next action to be taken in the poll loop based on the current state.
        ///
        /// - Returns: The next action for the poll loop.
        func nextPollLoopAction() -> PollLoopAction {
            self.state.withLockedValue {
                switch $0 {
                case .initial, .producing:
                    return .poll
                case .suspended:
                    // We were asked to stop producing,
                    // but the poll loop is still running.
                    // Trigger the poll loop to suspend.
                    return .suspendPollLoop
                case .loopSuspended:
                    fatalError("Illegal state: cannot invoke \(#function) when poll loop is suspended")
                case .shutDown:
                    return .shutdownPollLoop
                }
            }
        }

        /// Triggers an event in the state machine, causing a state transition.
        ///
        /// - Parameter event: The event to be triggered.
        func eventTriggered(_ event: Event) {
            self.state.withLockedValue { state in
                switch (event, state) {

                case (_, .shutDown):
                    return

                case (.produceMore, .initial):
                    state = .producing
                case (.produceMore, .producing):
                    break // Do nothing.
                case (.produceMore, .loopSuspended(let continuation)):
                    continuation.resume()
                    state = .producing

                case (.stopProducing, .producing):
                    state = .suspended

                case (.suspendLoop(let continuation), .initial):
                    state = .loopSuspended(continuation)
                case (.suspendLoop(let continuation), .producing):
                    state = .loopSuspended(continuation)
                case (.suspendLoop(let continuation), .suspended):
                    // .stopProducing has triggered the .suspended state sometime before.
                    // This has in turn triggered the loop to suspend itself.
                    state = .loopSuspended(continuation)

                case (.loopSuspensionCancelled, .loopSuspended(let continuation)):
                    continuation.resume() // Clean up continuation
                    state = .suspended

                case (_, .suspended):
                    break // Do nothing. We are waiting for .loopSuspened to be triggered.

                case (.shutdown, .loopSuspended(let continuation)):
                    continuation.resume() // Clean up continuation
                    state = .shutDown
                case (.shutdown, _):
                    state = .shutDown

                default:
                    fatalError("\(event) is not supported in state \(state)")
                }
            }
        }
    }
}
