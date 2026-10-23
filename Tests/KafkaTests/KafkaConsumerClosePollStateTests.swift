//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2026 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@_spi(Internal) import Kafka
import Logging
import Testing

@testable import Kafka

/// The consumer must not start `rd_kafka_consumer_close_queue` while a `rd_kafka_consumer_poll` is running.
@Suite struct KafkaConsumerClosePollStateTests {
    private func makeRunningStateMachine() throws -> KafkaConsumer.StateMachine {
        var config = KafkaConsumerConfig()
        config.bootstrapServers = ["localhost:9092"]
        config.groupId = "close-poll-state"
        let rebalanceContext = RebalanceContext()
        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: config.config,
            events: [.log, .rebalance, .error],
            logger: .kafkaTest,
            rebalanceContext: rebalanceContext
        )
        var stateMachine = KafkaConsumer.StateMachine()
        stateMachine.initialize(client: client, rebalanceContext: rebalanceContext)
        _ = stateMachine.transitionToRunning()
        return stateMachine
    }

    @Test func finishWithoutRunningPollClosesImmediately() throws {
        var stateMachine = try self.makeRunningStateMachine()

        guard case .triggerGracefulShutdown = stateMachine.finish() else {
            Issue.record("Expected finish() to start close when no poll is running")
            return
        }
    }

    @Test func finishDuringPollDefersCloseUntilPollReturns() throws {
        var stateMachine = try self.makeRunningStateMachine()
        #expect(stateMachine.startConsumerPoll() != nil)

        #expect(stateMachine.finish() == nil)
        guard case .pollForEvents = stateMachine.nextEventPollLoopAction() else {
            Issue.record("Close must not start while a poll is running")
            return
        }

        stateMachine.finishConsumerPoll()
        guard case .initiateCloseAndPoll = stateMachine.nextEventPollLoopAction() else {
            Issue.record("Expected close to start once the poll returned")
            return
        }
    }

    @Test func messageSequenceEndDuringPollDefersCloseUntilPollReturns() throws {
        var stateMachine = try self.makeRunningStateMachine()
        #expect(stateMachine.startConsumerPoll() != nil)

        stateMachine.finishMessageConsumption()
        guard case .pollForEvents = stateMachine.nextEventPollLoopAction() else {
            Issue.record("Close must not start while a poll is running")
            return
        }

        stateMachine.finishConsumerPoll()
        guard case .initiateCloseAndPoll = stateMachine.nextEventPollLoopAction() else {
            Issue.record("Expected close to start once the poll returned")
            return
        }
    }

    @Test func noPollStartsAfterFinish() throws {
        var stateMachine = try self.makeRunningStateMachine()
        _ = stateMachine.finish()

        #expect(stateMachine.startConsumerPoll() == nil)
    }
}
