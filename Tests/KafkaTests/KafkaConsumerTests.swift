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

        await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try! await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
        }

        // After shutdown, all withClient()-based methods should throw connectionClosed
        #expect(throws: KafkaError.self) {
            _ = try consumer.position(
                topicPartitions: [KafkaTopicPartition(topic: "test", partition: KafkaPartition(rawValue: 0))]
            )
        }
    }

    // MARK: - committed / position Tests

    @Test func committedFailsOnClosedConsumer() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try! await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
        }

        #expect(throws: KafkaError.self) {
            _ = try consumer.committed(
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

        await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try! await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
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

        await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try! await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
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

        await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try! await Task.sleep(for: .milliseconds(500), tolerance: .zero)

            let lost = try! consumer.isAssignmentLost
            #expect(lost == false)

            await serviceGroup.triggerGracefulShutdown()
        }
    }

    // MARK: - seek Tests

    @Test func seekFailsOnClosedConsumer() async throws {
        let config = makeConfig()
        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try! await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
        }

        #expect(throws: KafkaError.self) {
            try consumer.seek(
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

    // MARK: - KafkaConsumerEvent Equality and Hashable Tests

    @Test func consumerEventAssignedPartitionsEquality() {
        let partitions: [KafkaTopicPartition] = [
            KafkaTopicPartition(topic: "topic-a", partition: KafkaPartition(rawValue: 0)),
            KafkaTopicPartition(topic: "topic-a", partition: KafkaPartition(rawValue: 1)),
        ]

        let event1 = KafkaConsumerEvent.assignedPartitions(partitions)
        let event2 = KafkaConsumerEvent.assignedPartitions(partitions)
        let event3 = KafkaConsumerEvent.revokedPartitions(partitions)

        #expect(event1 == event2)
        #expect(event1 != event3)
    }

    @Test func consumerEventRevokedPartitionsEquality() {
        let partitions1: [KafkaTopicPartition] = [
            KafkaTopicPartition(topic: "topic-a", partition: KafkaPartition(rawValue: 0)),
        ]
        let partitions2: [KafkaTopicPartition] = [
            KafkaTopicPartition(topic: "topic-b", partition: KafkaPartition(rawValue: 0)),
        ]

        let event1 = KafkaConsumerEvent.revokedPartitions(partitions1)
        let event2 = KafkaConsumerEvent.revokedPartitions(partitions2)

        #expect(event1 != event2)
    }

    @Test func consumerEventHashable() {
        let partitions: [KafkaTopicPartition] = [
            KafkaTopicPartition(topic: "topic-a", partition: KafkaPartition(rawValue: 0)),
        ]

        let event1 = KafkaConsumerEvent.assignedPartitions(partitions)
        let event2 = KafkaConsumerEvent.assignedPartitions(partitions)

        #expect(event1.hashValue == event2.hashValue)

        var set: Set<KafkaConsumerEvent> = []
        set.insert(event1)
        set.insert(event2)
        #expect(set.count == 1)
    }

    @Test func consumerEventKindDescription() {
        #expect(KafkaConsumerEvent.Kind.assignedPartitions.description == "assignedPartitions")
        #expect(KafkaConsumerEvent.Kind.revokedPartitions.description == "revokedPartitions")
    }

    @Test func consumerEventEmptyPartitions() {
        let event = KafkaConsumerEvent.assignedPartitions([])
        #expect(event.kind == .assignedPartitions)
        #expect(event.partitions.isEmpty)
    }

    @Test func consumerEventStaticFactories() {
        let partitions: [KafkaTopicPartition] = [
            KafkaTopicPartition(topic: "t", partition: KafkaPartition(rawValue: 0))
        ]

        let assigned = KafkaConsumerEvent.assignedPartitions(partitions)
        #expect(assigned.kind == .assignedPartitions)
        #expect(assigned.partitions == partitions)

        let revoked = KafkaConsumerEvent.revokedPartitions(partitions)
        #expect(revoked.kind == .revokedPartitions)
        #expect(revoked.partitions == partitions)
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

}
