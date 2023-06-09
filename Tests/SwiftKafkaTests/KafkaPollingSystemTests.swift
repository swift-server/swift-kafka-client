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

import NIOConcurrencyHelpers
import NIOCore
@testable import SwiftKafka
import XCTest

final class KafkaPollingSystemTests: XCTestCase {
    typealias Message = Int // Could be any type, this is just for testing
    typealias TestStateMachine = KafkaPollingSystem<Message>.StateMachine

    let pollInterval = Duration.milliseconds(100)

    func testBackPressure() async throws {
        let backPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark(
            lowWatermark: 2,
            highWatermark: 5
        )

        let sut = KafkaPollingSystem<Message>()
        let stream = sut.initialize(
            backPressureStrategy: backPressureStrategy,
            pollClosure: {}
        )

        await withThrowingTaskGroup(of: Void.self) { group in

            // Run Task
            group.addTask {
                try await sut.run(pollInterval: self.pollInterval)
            }

            // Test Task
            group.addTask {
                var iterator: KafkaAsyncSequence<Message>.AsyncIterator? = stream.makeAsyncIterator()

                // Produce elements until we reach the high watermark
                // should work fine.
                for num in 1...4 {
                    sut.yield(num)
                    XCTAssertTrue(Self.isPollAndSleep(sut.nextPollLoopAction()))
                }

                // Producing this element makes us reach the high watermark.
                // The poll loop should suspend now.
                sut.yield(5)
                XCTAssertTrue(Self.isSuspendPollLoop(sut.nextPollLoopAction()))

                // There are now 5 elements that were yielded.
                // Consume first three elements to get to the low watermark (2).
                for num in 1...3 {
                    let consumed = await iterator?.next()
                    XCTAssertEqual(num, consumed)
                    XCTAssertTrue(Self.isSuspendPollLoop(sut.nextPollLoopAction()))
                }

                // We should now have reached our low watermark so we can produce
                // new elements, meaning that the poll loop should resume running.
                let consumed4 = await iterator?.next()
                XCTAssertEqual(4, consumed4)
                XCTAssertTrue(Self.isPollAndSleep(sut.nextPollLoopAction()))

                let consumed5 = await iterator?.next()
                XCTAssertEqual(5, consumed5)
                XCTAssertTrue(Self.isPollAndSleep(sut.nextPollLoopAction()))

                // Terminating our downstream consumer should result
                // in the poll loop shutting down.
                iterator = nil
                XCTAssertTrue(Self.isShutdownPollLoop(sut.nextPollLoopAction()))
            }
        }
    }

    // Reference type struct to wrap the closure
    class ClosureWrapper {
        var closure: () -> Void

        init(closure: @escaping (() -> Void) = {}) {
            self.closure = closure
        }

        func invokeClosure() {
            self.closure()
        }
    }

    func testNoPollsAfterPollLoopSuspension() async throws {
        let backPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark(
            lowWatermark: 2,
            highWatermark: 5
        )

        let closureWrapper = ClosureWrapper()
        let sut = KafkaPollingSystem<Message>()
        // We need to keep a reference to the stream,
        // otherwise the KafkaPollingSystem shuts down immediately
        let stream = sut.initialize(
            backPressureStrategy: backPressureStrategy,
            pollClosure: closureWrapper.invokeClosure
        )
        _ = stream // supress "stream was never used" warning

        await withThrowingTaskGroup(of: Void.self) { group in

            // Run Task
            group.addTask {
                try await sut.run(pollInterval: self.pollInterval)
            }

            // Test Task
            group.addTask {
                // Produce elements until we reach the high watermark
                // should work fine.
                for num in 1...4 {
                    sut.yield(num)
                    XCTAssertTrue(Self.isPollAndSleep(sut.nextPollLoopAction()))
                }

                // Producing this element makes us reach the high watermark.
                // The poll loop should suspend now.
                sut.yield(5)
                XCTAssertTrue(Self.isSuspendPollLoop(sut.nextPollLoopAction()))

                // Ok, we're definitely up and running now.

                // We change the poll closure so that our test fails when the poll closure is invoked.
                closureWrapper.closure = {
                    XCTFail("Poll loop still running after stopProducing() has been invoked")
                }

                // Sleep for 5 seconds to let the poll loop run for
                // a couple of iterations
                try await Task.sleep(for: .seconds(5))

                // Kill sut to unblock Run Task
                sut.didTerminate()
            }
        }
    }

    func testRunTaskCancellationThrowsRunError() async throws {
        let backPressureStrategy = NIOAsyncSequenceProducerBackPressureStrategies.HighLowWatermark(
            lowWatermark: 2,
            highWatermark: 5
        )

        let sut = KafkaPollingSystem<Message>()
        // We need to keep a reference to the stream,
        // otherwise the KafkaPollingSystem shuts down immediately
        let stream = sut.initialize(
            backPressureStrategy: backPressureStrategy,
            pollClosure: {}
        )
        _ = stream // supress "stream was never used" warning

        let runTask = Task {
            do {
                try await sut.run(pollInterval: self.pollInterval)
                XCTFail("Should have thrown error")
            } catch {
                XCTAssertTrue(error is CancellationError)
            }
        }

        // Produce elements until we reach the high watermark
        // should work fine.
        for num in 1...4 {
            sut.yield(num)
            XCTAssertTrue(Self.isPollAndSleep(sut.nextPollLoopAction()))
        }

        // Producing this element makes us reach the high watermark.
        // The poll loop should suspend now.
        sut.yield(5)
        XCTAssertTrue(Self.isSuspendPollLoop(sut.nextPollLoopAction()))

        // Cancel the Task that runs the poll loop.
        // This should result in the poll loop shutting down.
        runTask.cancel()
        // Sleep for a second to make sure the poll loop's canncellationHandler gets invoked.
        try await Task.sleep(for: .seconds(1))
        XCTAssertTrue(Self.isShutdownPollLoop(sut.nextPollLoopAction()))
    }

    // MARK: - Helpers

    static func isSuspendPollLoop(_ pollLoopAction: KafkaPollingSystem<Message>.StateMachine.PollLoopAction) -> Bool {
        if case .suspendPollLoop = pollLoopAction {
            return true
        } else {
            return false
        }
    }

    static func isPollAndSleep(_ pollLoopAction: KafkaPollingSystem<Message>.StateMachine.PollLoopAction) -> Bool {
        if case .pollAndSleep = pollLoopAction {
            return true
        } else {
            return false
        }
    }

    static func isShutdownPollLoop(_ pollLoopAction: KafkaPollingSystem<Message>.StateMachine.PollLoopAction) -> Bool {
        if case .shutdownPollLoop = pollLoopAction {
            return true
        } else {
            return false
        }
    }
}
