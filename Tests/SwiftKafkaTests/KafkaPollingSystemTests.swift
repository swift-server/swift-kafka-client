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

// TODO: only test publicly available interface -> don't rely on implementation details like produceMore

// final class KafkaPollingSystemTests: XCTestCase {
//    typealias Message = String // Could be any type, this is just for testing
//    typealias TestStateMachine = KafkaPollingSystem<Message>.StateMachine
//
//    let pollInterval = Duration.milliseconds(100)
//    var sut: KafkaPollingSystem<Message>!
//    var expectationStream: AsyncStream<Void>!
//    var pollIterator: AsyncStream<Void>.Iterator!
//    var runTask: Task<Void, Error>!
//
//    override func setUp() {
//        self.sut = KafkaPollingSystem<Message>()
//
//        // Enables us to await the next call to pollClosure
//        self.expectationStream = AsyncStream { continuation in
//            self.sut.pollClosure = {
//                continuation.yield()
//            }
//        }
//        self.pollIterator = self.expectationStream.makeAsyncIterator()
//
//        self.runTask = Task {
//            try await self.sut.run(pollInterval: self.pollInterval)
//        }
//
//        super.setUp()
//    }
//
//    override func tearDown() {
//        self.sut = nil
//        self.expectationStream = nil
//        self.pollIterator = nil
//        self.runTask = nil
//
//        super.tearDown()
//    }
//
//    func testBackPressure() async throws {
//        self.sut.produceMore()
//        await self.pollIterator.next()
//        if case .pollAndSleep = self.sut.nextPollLoopAction() {
//            // Test passed
//        } else {
//            XCTFail()
//        }
//
//        self.sut.stopProducing()
//        if case .suspendPollLoop = self.sut.nextPollLoopAction() {
//            // Test passed
//        } else {
//            XCTFail()
//        }
//
//        self.sut.produceMore()
//        await self.pollIterator.next()
//        if case .pollAndSleep = self.sut.nextPollLoopAction() {
//            // Test passed
//        } else {
//            XCTFail()
//        }
//
//        self.sut.didTerminate()
//        if case .shutdownPollLoop = self.sut.nextPollLoopAction() {
//            // Test passed
//        } else {
//            XCTFail()
//        }
//    }
//
//    func testNoPollsAfterPollLoopSuspension() async throws {
//        self.sut.produceMore()
//        await self.pollIterator.next()
//        if case .pollAndSleep = self.sut.nextPollLoopAction() {
//            // Test passed
//        } else {
//            XCTFail()
//        }
//
//        // We're definitely running now. Now suspend the poll loop.
//        self.sut.stopProducing()
//        if case .suspendPollLoop = self.sut.nextPollLoopAction() {
//            // Test passed
//        } else {
//            XCTFail()
//        }
//
//        // We change the poll closure so that our test fails when the poll closure is invoked.
//        self.sut.pollClosure = {
//            XCTFail("Poll loop still running after stopProducing() has been invoked")
//        }
//
//        try await Task.sleep(for: .seconds(5))
//    }
//
//    func testRunTaskCancellationShutsDownStateMachine() async throws {
//        self.sut.produceMore()
//        await self.pollIterator.next()
//        if case .pollAndSleep = self.sut.nextPollLoopAction() {
//            // Test passed
//        } else {
//            XCTFail()
//        }
//
//        // We're definitely running now. Now suspend the poll loop.
//        self.sut.stopProducing()
//        if case .suspendPollLoop = self.sut.nextPollLoopAction() {
//            // Test passed
//        } else {
//            XCTFail()
//        }
//
//        // Cancel the Task that runs the poll loop.
//        // This should result in the state machine shutting down.
//        self.runTask.cancel()
//        // Sleep for a second to make sure the poll loop's canncellationHandler gets invoked.
//        try await Task.sleep(for: .seconds(1))
//        if case .shutdownPollLoop = self.sut.nextPollLoopAction() {
//            // Test passed
//        } else {
//            XCTFail()
//        }
//    }
// }
