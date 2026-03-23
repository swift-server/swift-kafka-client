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

import Atomics
@_spi(Internal) import Kafka
import NIOCore
import ServiceLifecycle
import Testing
import UnixSignals

import struct Foundation.UUID

@testable import Kafka

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

private let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
private let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!

func withTestTopic(partitions: Int32 = 1, _ body: (_ testTopic: String) async throws -> Void) async throws {
    var basicConfig = KafkaConsumerConfig()
    basicConfig.groupId = UUID().uuidString
    basicConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
    basicConfig.brokerAddressFamily = .v4

    let client = try RDKafkaClient.makeClientForTopics(
        config: basicConfig,
        logger: .kafkaTest
    )
    let testTopic = try await client._createUniqueTopic(partitions: partitions)

    try await body(testTopic)

    try await client._deleteTopic(testTopic)
}

@Suite struct KafkaIntegrationTests {
    var producerConfig: KafkaProducerConfig

    init() throws {

        self.producerConfig = KafkaProducerConfig()
        self.producerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
        self.producerConfig.brokerAddressFamily = .v4
    }

    @Test func produceAndConsumeWithConsumerGroup() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 10)

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(id: UUID().uuidString, topics: [testTopic])
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.autoOffsetReset = .beginning  // Always read topics from beginning
            consumerConfig.brokerAddressFamily = .v4

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                // Run Task
                group.addTask {
                    try await serviceGroup.run()
                }

                // Consumer Task
                group.addTask {
                    var consumedMessages = [KafkaConsumerMessage]()
                    for try await message in consumer.messages {
                        consumedMessages.append(message)

                        if consumedMessages.count >= testMessages.count {
                            break
                        }
                    }

                    #expect(testMessages.count == consumedMessages.count)

                    for (index, consumedMessage) in consumedMessages.enumerated() {
                        #expect(testMessages[index].topic == consumedMessage.topic)
                        #expect(ByteBuffer(string: testMessages[index].key!) == consumedMessage.key)
                        #expect(ByteBuffer(string: testMessages[index].value) == consumedMessage.value)
                    }
                }

                // Wait for Consumer Task to complete
                try await group.next()
                // Shutdown the serviceGroup
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    @Test func produceAndConsumeWithAssignedTopicPartition() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 10)

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .partition(
                KafkaPartition(rawValue: 0),
                topic: testTopic,
                offset: KafkaOffset(rawValue: 0)
            )
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.autoOffsetReset = .beginning  // Always read topics from beginning
            consumerConfig.brokerAddressFamily = .v4

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                // Run Task
                group.addTask {
                    try await serviceGroup.run()
                }

                // Consumer Task
                group.addTask {
                    var consumedMessages = [KafkaConsumerMessage]()
                    for try await message in consumer.messages {
                        consumedMessages.append(message)

                        if consumedMessages.count >= testMessages.count {
                            break
                        }
                    }

                    #expect(testMessages.count == consumedMessages.count)

                    for (index, consumedMessage) in consumedMessages.enumerated() {
                        #expect(testMessages[index].topic == consumedMessage.topic)
                        #expect(ByteBuffer(string: testMessages[index].key!) == consumedMessage.key)
                        #expect(ByteBuffer(string: testMessages[index].value) == consumedMessage.value)
                    }
                }

                // Wait for Consumer Task to complete
                try await group.next()
                // Shutdown the serviceGroup
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    @Test func produceAndConsumeWithScheduleCommit() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 10)

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(id: UUID().uuidString, topics: [testTopic])
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.enableAutoCommit = false
            consumerConfig.autoOffsetReset = .beginning  // Always read topics from beginning
            consumerConfig.brokerAddressFamily = .v4

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                // Consumer Run Task
                group.addTask {
                    try await serviceGroup.run()
                }

                // Consumer Task
                group.addTask {
                    var consumedMessages = [KafkaConsumerMessage]()
                    for try await message in consumer.messages {
                        consumedMessages.append(message)
                        try consumer.scheduleCommit(message)

                        if consumedMessages.count >= testMessages.count {
                            break
                        }
                    }

                    #expect(testMessages.count == consumedMessages.count)
                }

                // Wait for Consumer Task to complete
                try await group.next()
                // Shutdown the serviceGroup
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    @Test func produceAndConsumeWithCommit() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 10)

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(id: UUID().uuidString, topics: [testTopic])
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.enableAutoCommit = false
            consumerConfig.autoOffsetReset = .beginning  // Always read topics from beginning
            consumerConfig.brokerAddressFamily = .v4

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                // Consumer Run Task
                group.addTask {
                    try await serviceGroup.run()
                }

                // Consumer Task
                group.addTask {
                    var consumedMessages = [KafkaConsumerMessage]()
                    for try await message in consumer.messages {
                        consumedMessages.append(message)
                        try await consumer.commit(message)

                        if consumedMessages.count >= testMessages.count {
                            break
                        }
                    }

                    #expect(testMessages.count == consumedMessages.count)
                }

                // Wait for Consumer Task to complete
                try await group.next()
                // Shutdown the serviceGroup
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    @Test func produceAndConsumeWithMessageHeaders() async throws {
        try await withTestTopic { testTopic in
            let testMessages = Self.createTestMessages(
                topic: testTopic,
                headers: [
                    KafkaHeader(key: "some.header", value: ByteBuffer(string: "some-header-value")),
                    KafkaHeader(key: "some.null.header", value: nil),
                ],
                count: 10
            )
            try await self.produceMessages(messages: testMessages)

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(
                id: "produce-and-consume-with-message-headers-group-id",
                topics: [testTopic]
            )
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.enableAutoCommit = false
            consumerConfig.autoOffsetReset = .beginning  // Always read topics from beginning
            consumerConfig.brokerAddressFamily = .v4

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                // Run Task
                group.addTask {
                    try await serviceGroup.run()
                }

                // Consumer Task
                group.addTask {
                    var consumedMessages = [KafkaConsumerMessage]()
                    for try await messageResult in consumer.messages {
                        guard case let message = messageResult else {
                            continue
                        }
                        consumedMessages.append(message)
                        try await consumer.commit(message)

                        if consumedMessages.count >= testMessages.count {
                            break
                        }
                    }

                    #expect(testMessages.count == consumedMessages.count)

                    for (index, consumedMessage) in consumedMessages.enumerated() {
                        #expect(testMessages[index].headers == consumedMessage.headers)
                    }
                }

                // Wait for Consumer Task to complete
                try await group.next()
                // Shutdown the serviceGroup
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    @Test func noNewConsumerMessagesAfterGracefulShutdown() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 2)

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(
                id: UUID().uuidString,
                topics: [testTopic]
            )
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.autoOffsetReset = .beginning  // Read topic from beginning

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                // Run Task
                group.addTask {
                    try await serviceGroup.run()
                }

                // Verify that we receive the first message
                let consumerIterator = consumer.messages.makeAsyncIterator()

                let consumedMessage = try await consumerIterator.next()
                #expect(testMessages.first!.topic == consumedMessage!.topic)
                #expect(ByteBuffer(string: testMessages.first!.key!) == consumedMessage!.key)
                #expect(ByteBuffer(string: testMessages.first!.value) == consumedMessage!.value)

                // Trigger a graceful shutdown
                await serviceGroup.triggerGracefulShutdown()

                // Wait to ensure the KafkaConsumer's shutdown handler has
                // been invoked.
                try await Task.sleep(for: .seconds(2))

                // We should not be able to read any new messages after the KafkaConsumer's
                // shutdown handler was invoked
                let stoppedConsumingMessage = try await consumerIterator.next()
                #expect(stoppedConsumingMessage == nil)
            }
        }
    }

    @Test func committedOffsetsAreCorrect() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 10)
            let firstConsumerOffset = testMessages.count / 2

            // Important: both consumer must have the same group.id
            let uniqueGroupID = UUID().uuidString

            // MARK: First Consumer

            var consumer1Config = KafkaConsumerConfig()
            consumer1Config.consumptionStrategy = .group(
                id: uniqueGroupID,
                topics: [testTopic]
            )
            consumer1Config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumer1Config.autoOffsetReset = .beginning  // Read topic from beginning
            consumer1Config.brokerAddressFamily = .v4

            let consumer1 = try KafkaConsumer(
                config: consumer1Config,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration1 = ServiceGroupConfiguration(
                services: [consumer1],
                logger: .kafkaTest
            )
            let serviceGroup1 = ServiceGroup(configuration: serviceGroupConfiguration1)

            try await withThrowingTaskGroup(of: Void.self) { group in
                // Run Task
                group.addTask {
                    try await serviceGroup1.run()
                }

                // First Consumer Task
                group.addTask {
                    var consumedMessages = [KafkaConsumerMessage]()
                    for try await message in consumer1.messages {
                        consumedMessages.append(message)

                        // Only read first half of messages
                        if consumedMessages.count >= firstConsumerOffset {
                            break
                        }
                    }

                    #expect(firstConsumerOffset == consumedMessages.count)

                    for (index, consumedMessage) in consumedMessages.enumerated() {
                        #expect(testMessages[index].topic == consumedMessage.topic)
                        #expect(ByteBuffer(string: testMessages[index].key!) == consumedMessage.key)
                        #expect(ByteBuffer(string: testMessages[index].value) == consumedMessage.value)
                    }
                }

                // Wait for Consumer Task to complete
                try await group.next()
                // Wait for a couple of more run loop iterations.
                // We do this to process the remaining 5 messages.
                // These messages shall be discarded and their offsets should not be committed.
                try await Task.sleep(for: .seconds(2))
                // Shutdown the serviceGroup
                await serviceGroup1.triggerGracefulShutdown()
            }

            // MARK: Second Consumer

            // The first consumer has now read the first half of the messages in the test topic.
            // This means our second consumer should be able to read the second
            // half of messages without any problems.

            var consumer2Config = KafkaConsumerConfig()
            consumer2Config.consumptionStrategy = .group(
                id: uniqueGroupID,
                topics: [testTopic]
            )
            consumer2Config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumer2Config.autoOffsetReset = .latest
            consumer2Config.brokerAddressFamily = .v4

            let consumer2 = try KafkaConsumer(
                config: consumer2Config,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration2 = ServiceGroupConfiguration(services: [consumer2], logger: .kafkaTest)
            let serviceGroup2 = ServiceGroup(configuration: serviceGroupConfiguration2)

            try await withThrowingTaskGroup(of: Void.self) { group in
                // Run Task
                group.addTask {
                    try await serviceGroup2.run()
                }

                // Second Consumer Task
                group.addTask {
                    var consumedMessages = [KafkaConsumerMessage]()
                    for try await message in consumer2.messages {
                        consumedMessages.append(message)

                        // Read second half of messages
                        if consumedMessages.count >= (testMessages.count - firstConsumerOffset) {
                            break
                        }
                    }

                    #expect(testMessages.count - firstConsumerOffset == consumedMessages.count)

                    for (index, consumedMessage) in consumedMessages.enumerated() {
                        #expect(testMessages[firstConsumerOffset + index].topic == consumedMessage.topic)
                        #expect(
                            ByteBuffer(string: testMessages[firstConsumerOffset + index].key!)
                                == consumedMessage.key
                        )
                        #expect(
                            ByteBuffer(string: testMessages[firstConsumerOffset + index].value)
                                == consumedMessage.value
                        )
                    }
                }

                // Wait for second Consumer Task to complete
                try await group.next()
                // Shutdown the serviceGroup
                await serviceGroup2.triggerGracefulShutdown()
            }
        }
    }

    @Test func noDuplicatedMessagesOnRebalance() async throws {
        try await withTestTopic(partitions: 4) { testTopic in
            let numOfMessages: UInt = 100
            let _ = try await self.produceMessages(topic: testTopic, count: numOfMessages)

            let uniqueGroupID = UUID().uuidString

            var consumer1Config = KafkaConsumerConfig()
            consumer1Config.consumptionStrategy = .group(
                id: uniqueGroupID,
                topics: [testTopic]
            )
            consumer1Config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumer1Config.autoOffsetReset = .beginning
            consumer1Config.brokerAddressFamily = .v4
            consumer1Config.pollInterval = .milliseconds(1)
            consumer1Config.enableAutoCommit = false

            let consumer1 = try KafkaConsumer(
                config: consumer1Config,
                logger: .kafkaTest
            )

            var consumer2Config = KafkaConsumerConfig()
            consumer2Config.consumptionStrategy = .group(
                id: uniqueGroupID,
                topics: [testTopic]
            )
            consumer2Config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumer2Config.autoOffsetReset = .beginning
            consumer2Config.brokerAddressFamily = .v4
            consumer2Config.pollInterval = .milliseconds(1)
            consumer2Config.enableAutoCommit = false

            let consumer2 = try KafkaConsumer(
                config: consumer2Config,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration1 = ServiceGroupConfiguration(
                services: [consumer1],
                gracefulShutdownSignals: [.sigterm, .sigint],
                logger: .kafkaTest
            )
            let serviceGroup1 = ServiceGroup(configuration: serviceGroupConfiguration1)

            let serviceGroupConfiguration2 = ServiceGroupConfiguration(
                services: [consumer2],
                gracefulShutdownSignals: [.sigterm, .sigint],
                logger: .kafkaTest
            )
            let serviceGroup2 = ServiceGroup(configuration: serviceGroupConfiguration2)

            let c1messages = ManagedAtomic(0)
            let c2messages = ManagedAtomic(0)

            try await withThrowingTaskGroup(of: Void.self) { group in
                // Run Task for 1st consumer
                group.addTask {
                    try await serviceGroup1.run()
                }
                // Run Task for 2nd consumer
                group.addTask {
                    while c1messages.load(ordering: .relaxed) < 2 {
                        // Wait for consumer 1 to process some messages
                        try await Task.sleep(for: .milliseconds(50))
                    }
                    try await serviceGroup2.run()
                }

                // First Consumer Task
                group.addTask {
                    // 6 partitions
                    for try await record in consumer1.messages {
                        c1messages.wrappingIncrement(ordering: .relaxed)

                        try consumer1.scheduleCommit(record)  // commit time to time
                        if c2messages.load(ordering: .relaxed) == 0 {
                            // Don't read all messages before 2nd consumer
                            try await Task.sleep(for: .milliseconds(100))
                        }
                    }
                }

                // Second Consumer Task
                group.addTask {
                    // 6 partitions
                    for try await record in consumer2.messages {
                        c2messages.wrappingIncrement(ordering: .relaxed)

                        try consumer2.scheduleCommit(record)  // commit time to time
                    }
                }

                // Monitoring task
                group.addTask {
                    while true {
                        let currentCtr = c1messages.load(ordering: .relaxed) + c2messages.load(ordering: .relaxed)
                        guard currentCtr >= numOfMessages else {
                            try await Task.sleep(for: .milliseconds(100))  // wait if new messages come here
                            continue
                        }
                        try await Task.sleep(for: .milliseconds(100))  // wait for extra messages
                        await serviceGroup1.triggerGracefulShutdown()
                        await serviceGroup2.triggerGracefulShutdown()
                        break
                    }
                }

                try await group.waitForAll()

                let c1total = c1messages.load(ordering: .relaxed)
                let c2total = c2messages.load(ordering: .relaxed)
                #expect(c1total > 0)
                #expect(c2total > 0)
                #expect(c1total + c2total == Int(numOfMessages))
            }
        }
    }

    // MARK: - storeOffset Tests

    @Test func produceAndConsumeWithStoreOffset() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 10)
            let firstConsumerCount = testMessages.count / 2

            let uniqueGroupID = UUID().uuidString

            // MARK: First Consumer — uses storeOffset for at-least-once delivery

            var consumer1Config = KafkaConsumerConfig()
            consumer1Config.consumptionStrategy = .group(
                id: uniqueGroupID,
                topics: [testTopic]
            )
            consumer1Config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumer1Config.autoOffsetReset = .beginning
            consumer1Config.brokerAddressFamily = .v4
            // At-least-once config: auto-commit ON, auto-offset-store OFF
            consumer1Config.enableAutoOffsetStore = false

            let consumer1 = try KafkaConsumer(
                config: consumer1Config,
                logger: .kafkaTest
            )

            let serviceGroupConfig1 = ServiceGroupConfiguration(
                services: [consumer1],
                logger: .kafkaTest
            )
            let serviceGroup1 = ServiceGroup(configuration: serviceGroupConfig1)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await serviceGroup1.run()
                }

                group.addTask {
                    var consumedMessages = [KafkaConsumerMessage]()
                    for try await message in consumer1.messages {
                        // Store offset only after "processing" — at-least-once pattern
                        try consumer1.storeOffset(message)
                        consumedMessages.append(message)

                        if consumedMessages.count >= firstConsumerCount {
                            break
                        }
                    }

                    #expect(firstConsumerCount == consumedMessages.count)
                }

                try await group.next()
                // Give auto-commit time to flush stored offsets to broker
                try await Task.sleep(for: .seconds(2))
                await serviceGroup1.triggerGracefulShutdown()
            }

            // MARK: Second Consumer — should resume where first left off

            var consumer2Config = KafkaConsumerConfig()
            consumer2Config.consumptionStrategy = .group(
                id: uniqueGroupID,
                topics: [testTopic]
            )
            consumer2Config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumer2Config.autoOffsetReset = .latest
            consumer2Config.brokerAddressFamily = .v4

            let consumer2 = try KafkaConsumer(
                config: consumer2Config,
                logger: .kafkaTest
            )

            let serviceGroupConfig2 = ServiceGroupConfiguration(
                services: [consumer2],
                logger: .kafkaTest
            )
            let serviceGroup2 = ServiceGroup(configuration: serviceGroupConfig2)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await serviceGroup2.run()
                }

                group.addTask {
                    var consumedMessages = [KafkaConsumerMessage]()
                    for try await message in consumer2.messages {
                        consumedMessages.append(message)

                        if consumedMessages.count >= (testMessages.count - firstConsumerCount) {
                            break
                        }
                    }

                    #expect(testMessages.count - firstConsumerCount == consumedMessages.count)

                    // Verify second consumer got the second half of messages
                    for (index, consumedMessage) in consumedMessages.enumerated() {
                        #expect(testMessages[firstConsumerCount + index].topic == consumedMessage.topic)
                        #expect(
                            ByteBuffer(string: testMessages[firstConsumerCount + index].key!)
                                == consumedMessage.key
                        )
                        #expect(
                            ByteBuffer(string: testMessages[firstConsumerCount + index].value)
                                == consumedMessage.value
                        )
                    }
                }

                try await group.next()
                await serviceGroup2.triggerGracefulShutdown()
            }
        }
    }

    @Test func storeOffsetFailsWhenAutoOffsetStoreNotDisabled() async throws {
        try await withTestTopic { testTopic in
            _ = try await self.produceMessages(topic: testTopic, count: 1)

            // Default config — enableAutoOffsetStore is NOT set to false
            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(
                id: UUID().uuidString,
                topics: [testTopic]
            )
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.autoOffsetReset = .beginning
            consumerConfig.brokerAddressFamily = .v4

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await serviceGroup.run()
                }

                group.addTask {
                    // Consume one real message, then try storeOffset — should throw config error
                    for try await message in consumer.messages {
                        #expect(throws: KafkaError.self) {
                            try consumer.storeOffset(message)
                        }
                        break
                    }
                }

                try await group.next()
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    // MARK: - committed / position Tests

    @Test func committedOffsetsMatchAfterCommit() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 5)

            let uniqueGroupID = UUID().uuidString

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(
                id: uniqueGroupID,
                topics: [testTopic]
            )
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.enableAutoCommit = false
            consumerConfig.autoOffsetReset = .beginning
            consumerConfig.brokerAddressFamily = .v4

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await serviceGroup.run()
                }

                group.addTask {
                    var consumedCount = 0
                    for try await message in consumer.messages {
                        consumedCount += 1
                        if consumedCount >= testMessages.count {
                            // Commit and verify BEFORE breaking — breaking drops the
                            // iterator which triggers consumer shutdown.
                            try await consumer.commit(message)

                            let tp = KafkaTopicPartition(
                                topic: message.topic,
                                partition: message.partition
                            )
                            let committedOffsets = try consumer.committed(
                                topicPartitions: [tp],
                                timeout: .milliseconds(5000)
                            )

                            // committed offset should be message.offset + 1 (next offset to consume)
                            let committedOffset = try #require(committedOffsets.first)
                            #expect(committedOffset.topic == message.topic)
                            #expect(committedOffset.partition == message.partition)
                            let expectedOffset = KafkaOffset(rawValue: message.offset.rawValue + 1)
                            #expect(committedOffset.offset == expectedOffset)
                            break
                        }
                    }

                    #expect(consumedCount == testMessages.count)
                }

                try await group.next()
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    @Test func positionAdvancesWithConsumption() async throws {
        try await withTestTopic { testTopic in
            _ = try await self.produceMessages(topic: testTopic, count: 5)

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(
                id: UUID().uuidString,
                topics: [testTopic]
            )
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.autoOffsetReset = .beginning
            consumerConfig.brokerAddressFamily = .v4

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await serviceGroup.run()
                }

                group.addTask {
                    var consumedCount = 0
                    for try await message in consumer.messages {
                        consumedCount += 1

                        // After consuming, position should be message.offset + 1
                        let tp = KafkaTopicPartition(
                            topic: message.topic,
                            partition: message.partition
                        )
                        let positions = try consumer.position(topicPartitions: [tp])
                        let position = try #require(positions.first)
                        let expectedOffset = KafkaOffset(rawValue: message.offset.rawValue + 1)
                        #expect(position.offset == expectedOffset)

                        if consumedCount >= 3 {
                            break
                        }
                    }

                    #expect(consumedCount == 3)
                }

                try await group.next()
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    // MARK: - seek Tests

    @Test func seekReplaysPreviouslyConsumedMessages() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 5)

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(
                id: UUID().uuidString,
                topics: [testTopic]
            )
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.enableAutoCommit = false
            consumerConfig.autoOffsetReset = .beginning
            consumerConfig.brokerAddressFamily = .v4

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await serviceGroup.run()
                }

                group.addTask {
                    let iterator = consumer.messages.makeAsyncIterator()

                    // Consume all 5 messages
                    var firstPassMessages = [KafkaConsumerMessage]()
                    for _ in 0..<testMessages.count {
                        let optionalMessage = try await iterator.next()
                        let message = try #require(optionalMessage)
                        firstPassMessages.append(message)
                    }
                    #expect(firstPassMessages.count == testMessages.count)

                    // Seek back to beginning of partition 0
                    let seekTarget = KafkaTopicPartitionOffset(
                        topic: testTopic,
                        partition: firstPassMessages[0].partition,
                        offset: .beginning
                    )
                    try consumer.seek(topicPartitionOffsets: [seekTarget])

                    // Consume again — should replay the same messages
                    var replayedMessages = [KafkaConsumerMessage]()
                    for _ in 0..<testMessages.count {
                        let optionalMessage = try await iterator.next()
                        let message = try #require(optionalMessage)
                        replayedMessages.append(message)
                    }
                    #expect(replayedMessages.count == testMessages.count)

                    // Verify replayed messages match the originals
                    for (index, replayed) in replayedMessages.enumerated() {
                        #expect(firstPassMessages[index].topic == replayed.topic)
                        #expect(firstPassMessages[index].key == replayed.key)
                        #expect(firstPassMessages[index].value == replayed.value)
                        #expect(firstPassMessages[index].offset == replayed.offset)
                    }
                }

                try await group.next()
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    // MARK: - storeOffset + committed integration

    @Test func committedOffsetsMatchAfterStoreOffset() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 5)

            let uniqueGroupID = UUID().uuidString

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(
                id: uniqueGroupID,
                topics: [testTopic]
            )
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.autoOffsetReset = .beginning
            consumerConfig.brokerAddressFamily = .v4
            // At-least-once: auto-commit ON (default), auto-offset-store OFF
            consumerConfig.enableAutoOffsetStore = false
            // Lower auto-commit interval so stored offsets flush quickly in this test
            consumerConfig.autoCommitIntervalMs = 500

            let consumer = try KafkaConsumer(
                config: consumerConfig,
                logger: .kafkaTest
            )

            let serviceGroupConfiguration = ServiceGroupConfiguration(
                services: [consumer],
                logger: .kafkaTest
            )
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await serviceGroup.run()
                }

                group.addTask {
                    var consumedCount = 0
                    for try await message in consumer.messages {
                        try consumer.storeOffset(message)
                        consumedCount += 1
                        if consumedCount >= testMessages.count {
                            // Wait for auto-commit to flush stored offsets to broker.
                            // Must happen BEFORE breaking — breaking drops the iterator
                            // which triggers consumer shutdown.
                            try await Task.sleep(for: .seconds(2))

                            let tp = KafkaTopicPartition(
                                topic: message.topic,
                                partition: message.partition
                            )
                            let committedOffsets = try consumer.committed(
                                topicPartitions: [tp],
                                timeout: .milliseconds(5000)
                            )

                            let committedOffset = try #require(committedOffsets.first)
                            #expect(committedOffset.topic == message.topic)
                            #expect(committedOffset.partition == message.partition)
                            // storeOffset stores message.offset + 1 internally
                            let expectedOffset = KafkaOffset(rawValue: message.offset.rawValue + 1)
                            #expect(committedOffset.offset == expectedOffset)
                            break
                        }
                    }

                    #expect(consumedCount == testMessages.count)
                }

                try await group.next()
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    // MARK: - Helpers

    func produceMessages(topic: String, count: UInt) async throws -> [KafkaProducerMessage<String, String>] {
        let testMessages = Self.createTestMessages(topic: topic, count: count)
        try await self.produceMessages(messages: testMessages)
        return testMessages
    }

    func produceMessages(
        messages: [KafkaProducerMessage<String, String>]
    ) async throws {
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(
            config: self.producerConfig,
            logger: .kafkaTest
        )
        let serviceGroupConfiguration = ServiceGroupConfiguration(
            services: [producer],
            gracefulShutdownSignals: [.sigterm, .sigint],
            logger: .kafkaTest
        )
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    events: events,
                    messages: messages
                )
            }
            // Wait for Producer Task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    private static func createTestMessages(
        topic: String,
        headers: [KafkaHeader] = [],
        count: UInt
    ) -> [KafkaProducerMessage<String, String>] {
        _createTestMessages(topic: topic, headers: headers, count: count)
    }

    private static func sendAndAcknowledgeMessages(
        producer: KafkaProducer,
        events: KafkaProducerEvents,
        messages: [KafkaProducerMessage<String, String>],
        skipConsistencyCheck: Bool = false
    ) async throws {
        try await _sendAndAcknowledgeMessages(
            producer: producer,
            events: events,
            messages: messages,
            skipConsistencyCheck: skipConsistencyCheck
        )
    }
}
