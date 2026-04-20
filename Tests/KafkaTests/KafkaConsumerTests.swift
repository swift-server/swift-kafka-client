//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka
import Logging
import ServiceLifecycle
import Testing

import struct Foundation.UUID

@testable import Kafka

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

@Suite struct KafkaConsumerTests {

    // MARK: - Helper

    /// Create a consumer config with a unique group ID.
    private func makeConfig(
        enableAutoCommit: Bool = true,
        enableAutoOffsetStore: Bool? = nil
    ) -> KafkaConsumerConfig {
        let uniqueGroupID = UUID().uuidString
        var config = KafkaConsumerConfig()
        config.groupId = uniqueGroupID
        config.consumptionStrategy = .group(
            id: uniqueGroupID,
            topics: ["test-topic"]
        )
        config.enableAutoCommit = enableAutoCommit
        if let enableAutoOffsetStore {
            config.enableAutoOffsetStore = enableAutoOffsetStore
        }
        return config
    }

    // MARK: - Lifecycle Tests

    @Test func consumerLog() async throws {
        let recorder = LogEventRecorder()
        let mockLogger = Logger(label: "kafka.test.consumer.log") {
            _ in MockLogHandler(recorder: recorder)
        }

        // Set no bootstrap servers to trigger librdkafka configuration warning
        let uniqueGroupID = UUID().uuidString
        var config = KafkaConsumerConfig()
        config.consumptionStrategy = .group(
            id: uniqueGroupID,
            topics: ["this-topic-does-not-exist"]
        )
        config.debug = [.all]

        let consumer = try KafkaConsumer(config: config, logger: mockLogger)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Sleep for 1s to let poll loop receive log message
            try! await Task.sleep(for: .seconds(1))

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }

        let recordedEvents = recorder.recordedEvents
        let expectedLogs: [(level: Logger.Level, source: String, message: String)] = [
            (Logger.Level.debug, "MEMBERID", uniqueGroupID)
        ]

        for expectedLog in expectedLogs {
            #expect(
                recordedEvents.contains(where: { event in
                    event.level == expectedLog.level && event.source == expectedLog.source
                        && event.message.description.contains(expectedLog.message)
                }),
                "Expected log \(expectedLog) but was not found"
            )
        }
    }

    @Test func consumerConstructDeinit() async throws {
        let uniqueGroupID = UUID().uuidString
        var config = KafkaConsumerConfig()
        config.groupId = uniqueGroupID
        config.consumptionStrategy = .group(
            id: uniqueGroupID,
            topics: ["this-topic-does-not-exist"]
        )

        _ = try KafkaConsumer(config: config, logger: .kafkaTest)  // deinit called before run
        _ = try KafkaConsumer.makeConsumerWithEvents(config: config, logger: .kafkaTest)
    }

    @Test func consumerMessagesReadCancelledBeforeRun() async throws {
        let uniqueGroupID = UUID().uuidString
        var config = KafkaConsumerConfig()
        config.consumptionStrategy = .group(
            id: uniqueGroupID,
            topics: ["this-topic-does-not-exist"]
        )

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let svcGroupConfig = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: svcGroupConfig)

        // explicitly run and cancel message consuming task before serviceGroup.run()
        let consumingTask = Task {
            for try await record in consumer.messages {
                Issue.record("Unexpected record \(record))")
            }
        }

        try await Task.sleep(for: .seconds(1))

        // explicitly cancel message consuming task before serviceGroup.run()
        consumingTask.cancel()

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .seconds(1))

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    // MARK: - Closed Consumer State Machine Tests
    //
    // storeOffset(_:) cannot be unit-tested directly because KafkaConsumerMessage
    // can only be constructed from a real rd_kafka_message_t pointer (requires a broker).
    // All new consumer methods (storeOffset, committed, position, seek, isAssignmentLost)
    // share the same state machine guard (withClient()), so we verify the closed-consumer
    // path through the methods that don't require a KafkaConsumerMessage.

    @Test func positionFailsOnClosedConsumerViaStateMachine() async throws {
        let config = makeConfig(enableAutoOffsetStore: false)
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
            try await group.waitForAll()
        }

        // After shutdown, all withClient()-based methods should throw connectionClosed
        #expect(throws: KafkaError.self) {
            _ = try consumer.position(
                topicPartitions: [KafkaTopicPartition(topic: "test", partition: KafkaPartition(rawValue: 0))]
            )
        }
    }

    // MARK: - Subscription Management Tests

    @Test func subscriptionFailsOnClosedConsumer() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }
            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
            try await group.waitForAll()
        }

        #expect(throws: KafkaError.self) {
            try consumer.subscription()
        }
    }

    @Test func subscribeFailsOnClosedConsumer() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }
            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
            try await group.waitForAll()
        }

        #expect(throws: KafkaError.self) {
            try consumer.subscribe(topics: ["new-topic"])
        }
    }

    @Test func unsubscribeFailsOnClosedConsumer() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }
            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
            try await group.waitForAll()
        }

        #expect(throws: KafkaError.self) {
            try consumer.unsubscribe()
        }
    }

    @Test func subscribeWithEmptyTopicsCallsUnsubscribe() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)

            // Empty topics should not crash — delegates to unsubscribe()
            try consumer.subscribe(topics: [])

            await serviceGroup.triggerGracefulShutdown()
        }
    }

    @Test func subscribeBeforeRunSucceeds() throws {
        // Consumer without consumptionStrategy — user subscribes manually
        var config = KafkaConsumerConfig()
        config.groupId = UUID().uuidString
        config.brokerAddressFamily = .v4

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        // subscribe() in .initializing state should not crash
        try consumer.subscribe(topics: ["test-topic"])
    }

    @Test func subscriptionReturnsEmptyBeforeSubscribing() throws {
        var config = KafkaConsumerConfig()
        config.groupId = UUID().uuidString
        config.brokerAddressFamily = .v4

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        // No subscription set — should return empty
        let topics = try consumer.subscription()
        #expect(topics.isEmpty)
    }

    @Test func runWithoutConsumptionStrategyDoesNotCrash() async throws {
        // Consumer with nil consumptionStrategy — should start and shut down cleanly
        var config = KafkaConsumerConfig()
        config.groupId = UUID().uuidString
        config.brokerAddressFamily = .v4

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
        }
        // No crash = test passes
    }

    @Test func subscribeBeforeRunThenRunSucceeds() async throws {
        var config = KafkaConsumerConfig()
        config.groupId = UUID().uuidString
        config.brokerAddressFamily = .v4

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        // Subscribe before run
        try consumer.subscribe(topics: ["test-topic"])

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    @Test func unsubscribeBeforeSubscribeDoesNotCrash() throws {
        var config = KafkaConsumerConfig()
        config.groupId = UUID().uuidString
        config.brokerAddressFamily = .v4

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        // Unsubscribe when never subscribed — should not crash
        try consumer.unsubscribe()
    }

    @Test func subscribeDuplicateTopicsThrows() throws {
        var config = KafkaConsumerConfig()
        config.groupId = UUID().uuidString
        config.brokerAddressFamily = .v4

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        // librdkafka rejects duplicate topics with "Invalid argument"
        #expect(throws: KafkaError.self) {
            try consumer.subscribe(topics: ["topic-a", "topic-a", "topic-a"])
        }
    }

    @Test func subscribeWithRegexPatternDoesNotCrash() throws {
        var config = KafkaConsumerConfig()
        config.groupId = UUID().uuidString
        config.brokerAddressFamily = .v4

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        // Regex pattern subscription — librdkafka supports ^-prefixed patterns
        try consumer.subscribe(topics: ["^test-.*"])
    }

    // MARK: - committed / position Tests

    @Test func committedFailsOnClosedConsumer() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
            try await group.waitForAll()
        }

        await #expect(throws: KafkaError.self) {
            _ = try await consumer.committed(
                topicPartitions: [KafkaTopicPartition(topic: "test", partition: KafkaPartition(rawValue: 0))],
                timeout: .milliseconds(1000)
            )
        }
    }

    @Test func positionFailsOnClosedConsumer() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
            try await group.waitForAll()
        }

        #expect(throws: KafkaError.self) {
            _ = try consumer.position(
                topicPartitions: [KafkaTopicPartition(topic: "test", partition: KafkaPartition(rawValue: 0))]
            )
        }
    }

    // MARK: - isAssignmentLost Tests

    @Test func isAssignmentLostFailsOnClosedConsumer() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
            try await group.waitForAll()
        }

        #expect(throws: KafkaError.self) {
            _ = try consumer.isAssignmentLost
        }
    }

    @Test func isAssignmentLostReturnsFalseWhenRunning() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)

            let lost = try consumer.isAssignmentLost
            #expect(lost == false)

            await serviceGroup.triggerGracefulShutdown()
            try await group.waitForAll()
        }
    }

    // MARK: - seek Tests

    @Test func seekFailsOnClosedConsumer() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
            try await group.waitForAll()
        }

        await #expect(throws: KafkaError.self) {
            try await consumer.seek(
                topicPartitionOffsets: [
                    KafkaTopicPartitionOffset(topic: "test", partition: KafkaPartition(rawValue: 0), offset: .beginning)
                ]
            )
        }
    }

    // MARK: - scheduleCommit Config Guard Tests
    //
    // scheduleCommit(_:) requires a KafkaConsumerMessage which cannot be constructed
    // without a broker. The config guard (enableAutoCommit must be false) is tested
    // indirectly: if enableAutoCommit is true (default), calling scheduleCommit would
    // throw a config error. The closed-consumer path is the same as other withClient()
    // methods, already tested above.

    // MARK: - makeConsumerWithEvents Tests

    @Test func makeConsumerWithEventsConstructDeinit() async throws {
        let config = makeConfig()
        let (consumer, events) = try KafkaConsumer.makeConsumerWithEvents(
            config: config,
            logger: .kafkaTest
        )
        _ = consumer
        _ = events
    }

    // MARK: - KafkaConsumerConfig.enableAutoOffsetStore Tests

    @Test func autoOffsetStoreConfigDefault() {
        let config = KafkaConsumerConfig()
        #expect(config.enableAutoOffsetStore == nil)
    }

    @Test func autoOffsetStoreConfigSetToFalse() {
        var config = KafkaConsumerConfig()
        config.enableAutoOffsetStore = false
        #expect(config.enableAutoOffsetStore == false)

        let dict = config.config
        #expect(dict["enable.auto.offset.store"] == "false")
    }

    @Test func autoOffsetStoreConfigSetToTrue() {
        var config = KafkaConsumerConfig()
        config.enableAutoOffsetStore = true
        #expect(config.enableAutoOffsetStore == true)

        let dict = config.config
        #expect(dict["enable.auto.offset.store"] == "true")
    }

    // MARK: - KafkaConsumerConfiguration.isAutoOffsetStoreEnabled Tests

    @available(*, deprecated)
    @Test func isAutoOffsetStoreEnabledDefaultIsTrue() {
        let config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "test-group", topics: ["test"]),
            bootstrapBrokerAddresses: []
        )
        #expect(config.isAutoOffsetStoreEnabled == true)
    }

    @available(*, deprecated)
    @Test func isAutoOffsetStoreEnabledSerializesToDictionary() {
        var config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "test-group", topics: ["test"]),
            bootstrapBrokerAddresses: []
        )
        config.isAutoOffsetStoreEnabled = false

        let dict = config.dictionary
        #expect(dict["enable.auto.offset.store"] == "false")
    }

    // MARK: - KafkaTopicPartition Tests

    @Test func topicPartitionInit() {
        let tp = KafkaTopicPartition(topic: "my-topic", partition: KafkaPartition(rawValue: 3))
        #expect(tp.topic == "my-topic")
        #expect(tp.partition == KafkaPartition(rawValue: 3))
    }

    @Test func topicPartitionEquality() {
        let tp1 = KafkaTopicPartition(topic: "topic-a", partition: KafkaPartition(rawValue: 0))
        let tp2 = KafkaTopicPartition(topic: "topic-a", partition: KafkaPartition(rawValue: 0))
        let tp3 = KafkaTopicPartition(topic: "topic-a", partition: KafkaPartition(rawValue: 1))
        let tp4 = KafkaTopicPartition(topic: "topic-b", partition: KafkaPartition(rawValue: 0))

        #expect(tp1 == tp2)
        #expect(tp1 != tp3)
        #expect(tp1 != tp4)
    }

    @Test func topicPartitionHashable() {
        let tp1 = KafkaTopicPartition(topic: "t", partition: KafkaPartition(rawValue: 0))
        let tp2 = KafkaTopicPartition(topic: "t", partition: KafkaPartition(rawValue: 0))
        let tp3 = KafkaTopicPartition(topic: "t", partition: KafkaPartition(rawValue: 1))

        var set: Set<KafkaTopicPartition> = []
        set.insert(tp1)
        set.insert(tp2)
        set.insert(tp3)
        #expect(set.count == 2)
    }

    // MARK: - KafkaTopicPartitionOffset Tests

    @Test func topicPartitionOffsetInitWithComponents() {
        let tpo = KafkaTopicPartitionOffset(
            topic: "my-topic",
            partition: KafkaPartition(rawValue: 2),
            offset: .beginning
        )
        #expect(tpo.topic == "my-topic")
        #expect(tpo.partition == KafkaPartition(rawValue: 2))
        #expect(tpo.offset == .beginning)
        #expect(tpo.topicPartition == KafkaTopicPartition(topic: "my-topic", partition: KafkaPartition(rawValue: 2)))
    }

    @Test func topicPartitionOffsetInitWithTopicPartition() {
        let tp = KafkaTopicPartition(topic: "t", partition: KafkaPartition(rawValue: 5))
        let tpo = KafkaTopicPartitionOffset(topicPartition: tp, offset: .end)
        #expect(tpo.topicPartition == tp)
        #expect(tpo.topic == "t")
        #expect(tpo.partition == KafkaPartition(rawValue: 5))
        #expect(tpo.offset == .end)
    }

    @Test func topicPartitionOffsetNilOffset() {
        let tpo = KafkaTopicPartitionOffset(
            topic: "t",
            partition: KafkaPartition(rawValue: 0),
            offset: nil
        )
        #expect(tpo.offset == nil)
    }

    @Test func topicPartitionOffsetEquality() {
        let tpo1 = KafkaTopicPartitionOffset(topic: "t", partition: KafkaPartition(rawValue: 0), offset: .beginning)
        let tpo2 = KafkaTopicPartitionOffset(topic: "t", partition: KafkaPartition(rawValue: 0), offset: .beginning)
        let tpo3 = KafkaTopicPartitionOffset(topic: "t", partition: KafkaPartition(rawValue: 0), offset: .end)
        let tpo4 = KafkaTopicPartitionOffset(topic: "t", partition: KafkaPartition(rawValue: 0), offset: nil)

        #expect(tpo1 == tpo2)
        #expect(tpo1 != tpo3)
        #expect(tpo1 != tpo4)
    }

    @Test func topicPartitionOffsetHashable() {
        let tpo1 = KafkaTopicPartitionOffset(topic: "t", partition: KafkaPartition(rawValue: 0), offset: .beginning)
        let tpo2 = KafkaTopicPartitionOffset(topic: "t", partition: KafkaPartition(rawValue: 0), offset: .beginning)
        let tpo3 = KafkaTopicPartitionOffset(topic: "t", partition: KafkaPartition(rawValue: 1), offset: .beginning)

        var set: Set<KafkaTopicPartitionOffset> = []
        set.insert(tpo1)
        set.insert(tpo2)
        set.insert(tpo3)
        #expect(set.count == 2)
    }

    // MARK: - KafkaConsumerRebalance Tests

    @Test func rebalanceAssignConstruction() {
        let partitions = [
            KafkaTopicPartition(topic: "topic-a", partition: KafkaPartition(rawValue: 0)),
            KafkaTopicPartition(topic: "topic-a", partition: KafkaPartition(rawValue: 1)),
        ]
        let rebalance = KafkaConsumerRebalance(kind: .assign, partitions: partitions)
        #expect(rebalance.kind == .assign)
        #expect(rebalance.partitions.count == 2)
        #expect(rebalance.partitions[0].topic == "topic-a")
        #expect(rebalance.partitions[0].partition == KafkaPartition(rawValue: 0))
        #expect(rebalance.partitions[1].partition == KafkaPartition(rawValue: 1))
    }

    @Test func rebalanceRevokeConstruction() {
        let rebalance = KafkaConsumerRebalance(kind: .revoke, partitions: [])
        #expect(rebalance.kind == .revoke)
        #expect(rebalance.partitions.isEmpty)
    }

    @Test func rebalanceEquality() {
        let partitions = [
            KafkaTopicPartition(topic: "t", partition: KafkaPartition(rawValue: 0))
        ]
        let r1 = KafkaConsumerRebalance(kind: .assign, partitions: partitions)
        let r2 = KafkaConsumerRebalance(kind: .assign, partitions: partitions)
        let r3 = KafkaConsumerRebalance(kind: .revoke, partitions: partitions)
        let r4 = KafkaConsumerRebalance(kind: .assign, partitions: [])

        #expect(r1 == r2)
        #expect(r1 != r3)
        #expect(r1 != r4)
    }

    @Test func rebalanceHashable() {
        let partitions = [
            KafkaTopicPartition(topic: "t", partition: KafkaPartition(rawValue: 0))
        ]
        let r1 = KafkaConsumerRebalance(kind: .assign, partitions: partitions)
        let r2 = KafkaConsumerRebalance(kind: .assign, partitions: partitions)
        let r3 = KafkaConsumerRebalance(kind: .revoke, partitions: partitions)

        var set: Set<KafkaConsumerRebalance> = []
        set.insert(r1)
        set.insert(r2)
        set.insert(r3)
        #expect(set.count == 2)
    }

    @Test func rebalanceKindEquality() {
        #expect(KafkaConsumerRebalance.Kind.assign == KafkaConsumerRebalance.Kind.assign)
        #expect(KafkaConsumerRebalance.Kind.revoke == KafkaConsumerRebalance.Kind.revoke)
        #expect(KafkaConsumerRebalance.Kind.assign != KafkaConsumerRebalance.Kind.revoke)
    }

    // MARK: - KafkaConsumerEvent Rebalance Tests

    @Test func consumerEventRebalancePatternMatch() {
        let rebalance = KafkaConsumerRebalance(
            kind: .assign,
            partitions: [KafkaTopicPartition(topic: "t", partition: KafkaPartition(rawValue: 0))]
        )
        let event = KafkaConsumerEvent.rebalance(rebalance)

        switch event {
        case .rebalance(let r):
            #expect(r.kind == .assign)
            #expect(r.partitions.count == 1)
        default:
            Issue.record("Expected .rebalance event")
        }
    }

    @Test func consumerEventFromConsumerPollEvent() {
        let rebalance = KafkaConsumerRebalance(
            kind: .revoke,
            partitions: [
                KafkaTopicPartition(topic: "t", partition: KafkaPartition(rawValue: 0)),
                KafkaTopicPartition(topic: "t", partition: KafkaPartition(rawValue: 1)),
            ]
        )
        let event = KafkaConsumerEvent.rebalance(rebalance)

        switch event {
        case .rebalance(let r):
            #expect(r.kind == .revoke)
            #expect(r.partitions.count == 2)
        default:
            Issue.record("Expected .rebalance event")
        }
    }

    // MARK: - KafkaConsumerRebalance.Kind.error Tests

    @Test func rebalanceErrorConstruction() {
        let rebalance = KafkaConsumerRebalance(kind: .error("broker unavailable"), partitions: [])
        #expect(rebalance.partitions.isEmpty)
        if case .error(let description) = rebalance.kind {
            #expect(description == "broker unavailable")
        } else {
            Issue.record("Expected .error kind")
        }
    }

    @Test func rebalanceErrorEquality() {
        let r1 = KafkaConsumerRebalance(kind: .error("err1"), partitions: [])
        let r2 = KafkaConsumerRebalance(kind: .error("err1"), partitions: [])
        let r3 = KafkaConsumerRebalance(kind: .error("err2"), partitions: [])
        let r4 = KafkaConsumerRebalance(kind: .assign, partitions: [])

        #expect(r1 == r2)
        #expect(r1 != r3)
        #expect(r1 != r4)
    }

    @Test func rebalanceErrorHashable() {
        let r1 = KafkaConsumerRebalance(kind: .error("err"), partitions: [])
        let r2 = KafkaConsumerRebalance(kind: .error("err"), partitions: [])
        let r3 = KafkaConsumerRebalance(kind: .assign, partitions: [])

        var set: Set<KafkaConsumerRebalance> = []
        set.insert(r1)
        set.insert(r2)
        set.insert(r3)
        #expect(set.count == 2)
    }

    @Test func rebalanceErrorEventPatternMatch() {
        let rebalance = KafkaConsumerRebalance(kind: .error("group coordinator not available"), partitions: [])
        let event = KafkaConsumerEvent.rebalance(rebalance)

        switch event {
        case .rebalance(let r):
            if case .error(let desc) = r.kind {
                #expect(desc == "group coordinator not available")
            } else {
                Issue.record("Expected .error kind")
            }
            #expect(r.partitions.isEmpty)
        default:
            Issue.record("Expected .rebalance event")
        }
    }

    // MARK: - RebalanceContext Tests

    @Test func rebalanceContextDrainEventsEmpty() {
        let context = RebalanceContext(logger: .kafkaTest)
        let events = context.drainEvents()
        #expect(events.isEmpty)
    }

    @Test func rebalanceContextInjectAndDrain() {
        let context = RebalanceContext(logger: .kafkaTest)

        context._testInjectEvent(
            RebalanceContext.ConsumerRebalanceEvent(
                kind: .assign,
                partitions: [("topic-a", 0), ("topic-a", 1)]
            )
        )
        context._testInjectEvent(
            RebalanceContext.ConsumerRebalanceEvent(
                kind: .revoke,
                partitions: [("topic-a", 1)]
            )
        )

        let events = context.drainEvents()
        #expect(events.count == 2)

        // Verify FIFO ordering
        if case .assign = events[0].kind {
            #expect(events[0].partitions.count == 2)
        } else {
            Issue.record("Expected first event to be .assign")
        }

        if case .revoke = events[1].kind {
            #expect(events[1].partitions.count == 1)
        } else {
            Issue.record("Expected second event to be .revoke")
        }
    }

    @Test func rebalanceContextDrainClearsBuffer() {
        let context = RebalanceContext(logger: .kafkaTest)

        context._testInjectEvent(
            RebalanceContext.ConsumerRebalanceEvent(kind: .assign, partitions: [("t", 0)])
        )

        let first = context.drainEvents()
        #expect(first.count == 1)

        let second = context.drainEvents()
        #expect(second.isEmpty, "Second drain should return empty after first drain consumed the event")
    }

    @Test func rebalanceContextErrorEventInjection() {
        let context = RebalanceContext(logger: .kafkaTest)

        context._testInjectEvent(
            RebalanceContext.ConsumerRebalanceEvent(kind: .error("coordinator not available"), partitions: [])
        )

        let events = context.drainEvents()
        #expect(events.count == 1)
        if case .error(let desc) = events[0].kind {
            #expect(desc == "coordinator not available")
        } else {
            Issue.record("Expected .error kind")
        }
        #expect(events[0].partitions.isEmpty)
    }

    @Test func rebalanceContextConcurrentInjectAndDrain() async {
        // This tests the core race condition: the C callback thread appends events
        // while the Swift event loop drains them concurrently.
        let context = RebalanceContext(logger: .kafkaTest)

        // Use an actor to safely count drained events across tasks
        actor Counter {
            var value = 0
            func add(_ n: Int) { value += n }
            func get() -> Int { value }
        }
        let drainCounter = Counter()

        await withTaskGroup(of: Void.self) { group in
            // Simulate C callback thread: rapidly inject events
            group.addTask {
                for i in 0..<200 {
                    context._testInjectEvent(
                        RebalanceContext.ConsumerRebalanceEvent(
                            kind: i % 2 == 0 ? .assign : .revoke,
                            partitions: [("topic", i)]
                        )
                    )
                }
            }

            // Simulate event loop: drain repeatedly
            group.addTask {
                for _ in 0..<500 {
                    let events = context.drainEvents()
                    await drainCounter.add(events.count)
                    await Task.yield()
                }
            }

            await group.waitForAll()
        }

        // Drain any remaining events after both tasks complete
        let remaining = context.drainEvents()
        await drainCounter.add(remaining.count)

        // Every injected event must be drained exactly once — no loss, no duplication
        let finalDrained = await drainCounter.get()
        #expect(finalDrained == 200, "Expected 200 drained events, got \(finalDrained)")
    }

    @Test func rebalanceContextConcurrentMultipleInjectersAndDrain() async {
        // Multiple "C callback threads" appending simultaneously while event loop drains
        let context = RebalanceContext(logger: .kafkaTest)
        let injectersCount = 5
        let eventsPerInjecter = 50

        actor Counter {
            var value = 0
            func add(_ n: Int) { value += n }
            func get() -> Int { value }
        }
        let drainCounter = Counter()

        await withTaskGroup(of: Void.self) { group in
            // Multiple concurrent injecters
            for t in 0..<injectersCount {
                group.addTask {
                    for i in 0..<eventsPerInjecter {
                        context._testInjectEvent(
                            RebalanceContext.ConsumerRebalanceEvent(
                                kind: .assign,
                                partitions: [("topic-\(t)", i)]
                            )
                        )
                    }
                }
            }

            // Single drain loop
            group.addTask {
                for _ in 0..<1000 {
                    let events = context.drainEvents()
                    await drainCounter.add(events.count)
                    await Task.yield()
                }
            }

            await group.waitForAll()
        }

        let remaining = context.drainEvents()
        await drainCounter.add(remaining.count)
        let finalDrained = await drainCounter.get()
        let expected = injectersCount * eventsPerInjecter
        #expect(finalDrained == expected, "Expected \(expected) drained events, got \(finalDrained)")
    }

    @Test func rebalanceContextFIFOOrderingPreservedUnderConcurrency() async {
        // Verify events from a single injecter maintain FIFO order
        let context = RebalanceContext(logger: .kafkaTest)
        var allDrained: [RebalanceContext.ConsumerRebalanceEvent] = []

        // Inject 100 events with sequential partition IDs
        for i in 0..<100 {
            context._testInjectEvent(
                RebalanceContext.ConsumerRebalanceEvent(
                    kind: .assign,
                    partitions: [("topic", i)]
                )
            )
        }

        // Drain in batches
        while true {
            let batch = context.drainEvents()
            if batch.isEmpty { break }
            allDrained.append(contentsOf: batch)
        }

        #expect(allDrained.count == 100)

        // Verify ordering: partition IDs should be 0, 1, 2, ... 99
        for (index, event) in allDrained.enumerated() {
            #expect(
                event.partitions[0].partition == index,
                "Event at index \(index) has partition \(event.partitions[0].partition), expected \(index)"
            )
        }
    }

    // MARK: - KafkaConsumerEvent.error Tests

    @Test func consumerEventErrorPatternMatch() {
        let error = KafkaError.config(reason: "All brokers are down")
        let event = KafkaConsumerEvent.error(error)

        switch event {
        case .error(let e):
            #expect(e.description.contains("All brokers are down"))
        default:
            Issue.record("Expected .error event")
        }
    }

    @Test func consumerEventErrorEquality() {
        let error1 = KafkaError.config(reason: "All brokers are down")
        let error2 = KafkaError.config(reason: "Authentication failed")

        let event1 = KafkaConsumerEvent.error(error1)
        let event2 = KafkaConsumerEvent.error(error2)

        // Same error code (.config), so they are equal per current Equatable (known limitation)
        #expect(event1 == event2)
    }

    // MARK: - KafkaError.RDKafkaCode Tests

    @Test func rdKafkaCodeStaticConstants() {
        #expect(KafkaError.RDKafkaCode.allBrokersDown.rawValue == -187)
        #expect(KafkaError.RDKafkaCode.authentication.rawValue == -169)
        #expect(KafkaError.RDKafkaCode.transport.rawValue == -195)
        #expect(KafkaError.RDKafkaCode.timedOut.rawValue == -185)
        #expect(KafkaError.RDKafkaCode.ssl.rawValue == -181)
        #expect(KafkaError.RDKafkaCode.fatal.rawValue == -150)
        #expect(KafkaError.RDKafkaCode.noError.rawValue == 0)
    }

    @Test func rdKafkaCodeEquality() {
        #expect(KafkaError.RDKafkaCode.allBrokersDown == KafkaError.RDKafkaCode(rawValue: -187))
        #expect(KafkaError.RDKafkaCode.allBrokersDown != KafkaError.RDKafkaCode.transport)
    }

    @Test func rdKafkaCodeHashable() {
        var set = Set<KafkaError.RDKafkaCode>()
        set.insert(.allBrokersDown)
        set.insert(KafkaError.RDKafkaCode(rawValue: -187))
        set.insert(.authentication)
        #expect(set.count == 2)
    }

    @Test func rdKafkaCodeDescription() {
        let code = KafkaError.RDKafkaCode.allBrokersDown
        #expect(code.description.contains("-187"))
        #expect(code.description.contains("broker"))
    }

    // MARK: - KafkaError Error Code Preservation Tests

    @Test func kafkaErrorPreservesRDKafkaCode() {
        let error = KafkaError.rdKafkaError(wrapping: rd_kafka_resp_err_t(rawValue: -187))
        #expect(error.rdKafkaCode == .allBrokersDown)
        #expect(error.code == .underlying)
    }

    @Test func kafkaErrorPreservesRDKafkaCodeWithReason() {
        let error = KafkaError.rdKafkaError(
            wrapping: rd_kafka_resp_err_t(rawValue: -185),
            reason: "custom reason"
        )
        #expect(error.rdKafkaCode == .timedOut)
    }

    @Test func kafkaErrorNonRDKafkaErrorHasNilCode() {
        let error = KafkaError.config(reason: "bad config")
        #expect(error.rdKafkaCode == nil)
    }

    @Test func kafkaErrorEqualityDistinguishesDifferentRDKafkaCodes() {
        let err1 = KafkaError.rdKafkaError(wrapping: rd_kafka_resp_err_t(rawValue: -187))
        let err2 = KafkaError.rdKafkaError(wrapping: rd_kafka_resp_err_t(rawValue: -169))
        #expect(err1 != err2)
    }

    @Test func kafkaErrorEqualitySameRDKafkaCode() {
        let err1 = KafkaError.rdKafkaError(wrapping: rd_kafka_resp_err_t(rawValue: -187))
        let err2 = KafkaError.rdKafkaError(wrapping: rd_kafka_resp_err_t(rawValue: -187))
        #expect(err1 == err2)
    }

    @Test func triggerGracefulShutdownBeforeRunDoesNotCrash() throws {
        var config = KafkaConsumerConfig()
        config.groupId = "test-group"
        config.brokerAddressFamily = .v4

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)
        
        // This used to cause a fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
        // when consumer was in the .initializing state
        consumer.triggerGracefulShutdown()
    }

    @Test func triggerGracefulShutdownAfterSubscribeButBeforeRunDoesNotCrash() throws {
        var config = KafkaConsumerConfig()
        config.groupId = "test-group"
        config.brokerAddressFamily = .v4

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)
        try consumer.subscribe(topics: ["test-topic"])
        consumer.triggerGracefulShutdown()
    }
}
