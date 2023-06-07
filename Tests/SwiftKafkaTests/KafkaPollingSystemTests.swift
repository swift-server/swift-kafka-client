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
import NIOConcurrencyHelpers
@testable import SwiftKafka
import XCTest

// MARK: - Helper Classes

/// A class that wraps a closure with a reference to that closure, allowing to change the underlying functionality
/// of `funcTofunc` after it has been passed.
class ClosureWrapper {
    /// The wrapped closure.
    var wrappedClosure: (() -> Void)?

    /// Function that should be passed on.
    /// By changing the `wrappedClosure`, the behaviour of `funcTofunc` can be changed.
    func funcTofunc() {
        self.wrappedClosure?()
    }
}

// MARK: - Tests

final class KafkaPollingSystemTests: XCTestCase {
    typealias Message = String // Could be any type, this is just for testing
    typealias TestStateMachine = KafkaPollingSystem<Message>.StateMachine

    func testBackPressure() async throws {
        let pollInterval = Duration.milliseconds(100)

        let closureWrapper = ClosureWrapper()
        let sut = KafkaPollingSystem<Message>(pollClosure: {
            closureWrapper.funcTofunc()
        })
        let runTask = Task {
            await sut.run(pollInterval: pollInterval)
        }

        let expectation = XCTestExpectation(description: "Poll closure invoked after initial produceMore()")
        closureWrapper.wrappedClosure = { expectation.fulfill() }
        sut.produceMore()
        let result = await XCTWaiter.fulfillment(of: [expectation], timeout: 1)
        XCTAssertEqual(result, .completed)
        if case .pollAndSleep = sut.nextPollLoopAction() {
            // Test passed
        } else {
            XCTFail()
        }

        sut.stopProducing()
        if case .suspendPollLoop = sut.nextPollLoopAction() {
            // Test passed
        } else {
            XCTFail()
        }

        let secondExpectation = XCTestExpectation(description: "Poll closure invoked after second produceMore()")
        closureWrapper.wrappedClosure = {
            secondExpectation.fulfill()
        }
        sut.produceMore()
        let secondResult = await XCTWaiter.fulfillment(of: [secondExpectation], timeout: 1)
        XCTAssertEqual(secondResult, .completed)
        if case .pollAndSleep = sut.nextPollLoopAction() {
            // Test passed
        } else {
            XCTFail()
        }

        sut.didTerminate()
        if case .shutdownPollLoop = sut.nextPollLoopAction() {
            // Test passed
        } else {
            XCTFail()
        }

        runTask.cancel()
    }

    func testNoPollsAfterPollLoopSuspension() async throws {
        let pollInterval = Duration.milliseconds(100)

        let closureWrapper = ClosureWrapper()
        let sut = KafkaPollingSystem<Message>(pollClosure: {
            closureWrapper.funcTofunc()
        })
        let runTask = Task {
            await sut.run(pollInterval: pollInterval)
        }

        let expectation = XCTestExpectation(description: "Poll closure invoked after initial produceMore()")
        closureWrapper.wrappedClosure = { expectation.fulfill() }
        sut.produceMore()
        let result = await XCTWaiter.fulfillment(of: [expectation], timeout: 1)
        XCTAssertEqual(result, .completed)
        if case .pollAndSleep = sut.nextPollLoopAction() {
            // Test passed
        } else {
            XCTFail()
        }

        // We're definitely running now. Now suspend the poll loop.
        sut.stopProducing()
        if case .suspendPollLoop = sut.nextPollLoopAction() {
            // Test passed
        } else {
            XCTFail()
        }

        // We change the poll closure so that our test fails when the poll closure is invoked.
        closureWrapper.wrappedClosure = {
            XCTFail("Poll loop still running after stopProducing() has been invoked")
        }

        try await Task.sleep(for: .seconds(5))

        runTask.cancel()
    }

    func testRunTaskCancellationShutsDownStateMachine() async throws {
        let pollInterval = Duration.milliseconds(100)

        let closureWrapper = ClosureWrapper()
        let sut = KafkaPollingSystem<Message>(pollClosure: {
            closureWrapper.funcTofunc()
        })
        let runTask = Task {
            await sut.run(pollInterval: pollInterval)
        }

        let expectation = XCTestExpectation(description: "Poll closure invoked after initial produceMore()")
        closureWrapper.wrappedClosure = { expectation.fulfill() }
        sut.produceMore()
        let result = await XCTWaiter.fulfillment(of: [expectation], timeout: 1)
        XCTAssertEqual(result, .completed)
        if case .pollAndSleep = sut.nextPollLoopAction() {
            // Test passed
        } else {
            XCTFail()
        }

        // We're definitely running now. Now suspend the poll loop.
        sut.stopProducing()
        if case .suspendPollLoop = sut.nextPollLoopAction() {
            // Test passed
        } else {
            XCTFail()
        }

        // Cancel the Task that runs the poll loop.
        // This should result in the state machine shutting down.
        runTask.cancel()
        // Sleep for a second to make sure the poll loop's canncellationHandler gets invoked.
        try await Task.sleep(for: .seconds(1))
        if case .shutdownPollLoop = sut.nextPollLoopAction() {
            // Test passed
        } else {
            XCTFail()
        }
    }
}

// MARK: - KafkaBackPressurePollingSystem + Extensions

/// These testing-only methods provide more convenient access to the polling system's locked `stateMachine` methods.
extension KafkaPollingSystem {
    func nextPollLoopAction() -> KafkaPollingSystem.StateMachine.PollLoopAction {
        return self.stateMachineLock.withLockedValue { $0.nextPollLoopAction() }
    }

    func stopProducing() {
        stateMachineLock.withLockedValue { $0.stopProducing() }
    }
}
