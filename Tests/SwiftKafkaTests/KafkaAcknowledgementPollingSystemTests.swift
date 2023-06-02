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

import NIOCore
@testable import SwiftKafka
import XCTest

final class KafkaAcknowledgementPollingSystemTests: XCTestCase {
    typealias TestStateMachine = KafkaAcknowledgementPollingSystem.StateMachine

    func testBackPressure() async throws {
        let pollInterval = Duration.milliseconds(100)

        var expectation: XCTestExpectation?
        let sut = KafkaAcknowledgementPollingSystem(logger: .kafkaTest)
        sut.pollClosure = {
            expectation?.fulfill()
        }

        let runTask = Task {
            await sut.run(pollInterval: pollInterval)
        }

        expectation = XCTestExpectation(description: "Poll closure invoked after initial produceMore()")
        sut.produceMore()
        XCTAssertEqual(XCTWaiter().wait(for: [expectation!], timeout: 1), .completed)
        XCTAssertEqual(TestStateMachine.PollLoopAction.pollAndSleep, sut.nextPollLoopAction())

        sut.stopProducing()
        XCTAssertEqual(TestStateMachine.PollLoopAction.suspendPollLoop, sut.nextPollLoopAction())

        expectation = XCTestExpectation(description: "Poll closure invoked after second produceMore()")
        sut.produceMore()
        XCTAssertEqual(XCTWaiter().wait(for: [expectation!], timeout: 1), .completed)
        XCTAssertEqual(TestStateMachine.PollLoopAction.pollAndSleep, sut.nextPollLoopAction())

        sut.didTerminate()
        XCTAssertEqual(TestStateMachine.PollLoopAction.shutdownPollLoop, sut.nextPollLoopAction())

        runTask.cancel()
    }

    func testNoPollsAfterPollLoopSuspension() async throws {
        let pollInterval = Duration.milliseconds(100)

        var expectation: XCTestExpectation?
        let sut = KafkaAcknowledgementPollingSystem(logger: .kafkaTest)
        sut.pollClosure = {
            expectation?.fulfill()
        }

        let runTask = Task {
            await sut.run(pollInterval: pollInterval)
        }

        expectation = XCTestExpectation(description: "Poll closure invoked after initial produceMore()")
        sut.produceMore()
        XCTAssertEqual(XCTWaiter().wait(for: [expectation!], timeout: 1), .completed)
        XCTAssertEqual(TestStateMachine.PollLoopAction.pollAndSleep, sut.nextPollLoopAction())

        // We're definitely running now. Now suspend the poll loop.
        sut.stopProducing()
        XCTAssertEqual(TestStateMachine.PollLoopAction.suspendPollLoop, sut.nextPollLoopAction())
        // We change the poll closure so that our test fails when the poll closure is invoked.
        sut.pollClosure = {
            XCTFail("Poll loop still running after stopProducing() has been invoked")
        }

        try await Task.sleep(for: .seconds(5))

        runTask.cancel()
    }

    func testRunTaskCancellationShutsDownStateMachine() async throws {
        let pollInterval = Duration.milliseconds(100)

        var expectation: XCTestExpectation?
        let sut = KafkaAcknowledgementPollingSystem(logger: .kafkaTest)
        sut.pollClosure = {
            expectation?.fulfill()
        }

        let runTask = Task {
            await sut.run(pollInterval: pollInterval)
        }

        expectation = XCTestExpectation(description: "Poll closure invoked after initial produceMore()")
        sut.produceMore()
        XCTAssertEqual(XCTWaiter().wait(for: [expectation!], timeout: 1), .completed)
        XCTAssertEqual(TestStateMachine.PollLoopAction.pollAndSleep, sut.nextPollLoopAction())

        // We're definitely running now. Now suspend the poll loop.
        sut.stopProducing()
        XCTAssertEqual(TestStateMachine.PollLoopAction.suspendPollLoop, sut.nextPollLoopAction())

        // Cancel the Task that runs the poll loop.
        // This should result in the state machine shutting down.
        runTask.cancel()
        // Sleep for a second to make sure the poll loop's canncellationHandler gets invoked.
        try await Task.sleep(for: .seconds(1))
        XCTAssertEqual(TestStateMachine.PollLoopAction.shutdownPollLoop, sut.nextPollLoopAction())
    }
}

// MARK: - KafkaBackPressurePollingSystem + Extensions

/// These testing-only methods provide more convenient access to the polling system's locked `stateMachine` methods.
extension KafkaAcknowledgementPollingSystem {
    func nextPollLoopAction() -> KafkaAcknowledgementPollingSystem.StateMachine.PollLoopAction {
        return self.stateMachineLock.withLockedValue { $0.nextPollLoopAction() }
    }

    func stopProducing() {
        stateMachineLock.withLockedValue { $0.stopProducing() }
    }
}
