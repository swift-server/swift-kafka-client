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

// TODO: stream logic to setup?
final class KafkaPollingSystemTests: XCTestCase {
    typealias Message = String // Could be any type, this is just for testing
    typealias TestStateMachine = KafkaPollingSystem<Message>.StateMachine

    func testBackPressure() async throws {
        let pollInterval = Duration.milliseconds(100)

        let sut = KafkaPollingSystem<Message>()
        let expectationStream = AsyncStream { continuation in
            sut.pollClosure = {
                continuation.yield()
            }
        }
        var pollIterator = expectationStream.makeAsyncIterator()

        let _ = Task {
            try await sut.run(pollInterval: pollInterval)
        }

        sut.produceMore()
        await pollIterator.next()
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

        sut.produceMore()
        await pollIterator.next()
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
    }

    func testNoPollsAfterPollLoopSuspension() async throws {
        let pollInterval = Duration.milliseconds(100)

        let sut = KafkaPollingSystem<Message>()

        let expectationStream = AsyncStream { continuation in
            sut.pollClosure = {
                continuation.yield()
            }
        }
        var pollIterator = expectationStream.makeAsyncIterator()

        let _ = Task {
            try await sut.run(pollInterval: pollInterval)
        }

        sut.produceMore()
        await pollIterator.next()
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
        sut.pollClosure = {
            XCTFail("Poll loop still running after stopProducing() has been invoked")
        }

        try await Task.sleep(for: .seconds(5))
    }

    func testRunTaskCancellationShutsDownStateMachine() async throws {
        let pollInterval = Duration.milliseconds(100)

        let sut = KafkaPollingSystem<Message>()

        let expectationStream = AsyncStream { continuation in
            sut.pollClosure = {
                continuation.yield()
            }
        }
        var pollIterator = expectationStream.makeAsyncIterator()

        let runTask = Task {
            try await sut.run(pollInterval: pollInterval)
        }

        sut.produceMore()
        await pollIterator.next()
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
