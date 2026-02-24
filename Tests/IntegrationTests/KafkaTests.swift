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

// For testing locally on Mac, do the following:
//
// 1. Install Kafka and Zookeeper using homebrew
//
// https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273
//
// 2. Start Zookeeper & Kafka Server
//
// (Homebrew - Apple Silicon)
// zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties & kafka-server-start /opt/homebrew/etc/kafka/server.properties
//
// (Homebrew - Intel Mac)
// zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties

private let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
private let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!

func withTestTopic(partitions: Int32 = 1, _ body: (_ testTopic: String) async throws -> Void) async throws {
    let bootstrapBrokerAddress = KafkaConfiguration.BrokerAddress(
        host: kafkaHost,
        port: kafkaPort
    )

    var basicConfig = KafkaConsumerConfiguration(
        consumptionStrategy: .group(id: UUID().uuidString, topics: []),
        bootstrapBrokerAddresses: [bootstrapBrokerAddress]
    )
    basicConfig.broker.addressFamily = .v4

    let client = try RDKafkaClient.makeClient(
        type: .consumer,
        configDictionary: basicConfig.dictionary,
        events: [],
        logger: .kafkaTest
    )
    let testTopic = try await client._createUniqueTopic(partitions: partitions)

    try await body(testTopic)

    try await client._deleteTopic(testTopic)
}

@Suite struct KafkaIntegrationTests {
    var bootstrapBrokerAddress: KafkaConfiguration.BrokerAddress
    var producerConfig: KafkaProducerConfiguration

    init() throws {
        self.bootstrapBrokerAddress = KafkaConfiguration.BrokerAddress(
            host: kafkaHost,
            port: kafkaPort
        )

        self.producerConfig = KafkaProducerConfiguration(bootstrapBrokerAddresses: [self.bootstrapBrokerAddress])
        self.producerConfig.broker.addressFamily = .v4
    }

    @Test func produceAndConsumeWithConsumerGroup() async throws {
        try await withTestTopic { testTopic in
            let testMessages = try await self.produceMessages(topic: testTopic, count: 10)

            var consumerConfig = KafkaConsumerConfiguration(
                consumptionStrategy: .group(id: UUID().uuidString, topics: [testTopic]),
                bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
            )
            consumerConfig.autoOffsetReset = .beginning  // Always read topics from beginning
            consumerConfig.broker.addressFamily = .v4

            let consumer = try KafkaConsumer(
                configuration: consumerConfig,
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

            var consumerConfig = KafkaConsumerConfiguration(
                consumptionStrategy: .partition(
                    KafkaPartition(rawValue: 0),
                    topic: testTopic,
                    offset: KafkaOffset(rawValue: 0)
                ),
                bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
            )
            consumerConfig.autoOffsetReset = .beginning  // Always read topics from beginning
            consumerConfig.broker.addressFamily = .v4

            let consumer = try KafkaConsumer(
                configuration: consumerConfig,
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

            var consumerConfig = KafkaConsumerConfiguration(
                consumptionStrategy: .group(id: UUID().uuidString, topics: [testTopic]),
                bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
            )
            consumerConfig.isAutoCommitEnabled = false
            consumerConfig.autoOffsetReset = .beginning  // Always read topics from beginning
            consumerConfig.broker.addressFamily = .v4

            let consumer = try KafkaConsumer(
                configuration: consumerConfig,
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

            var consumerConfig = KafkaConsumerConfiguration(
                consumptionStrategy: .group(id: UUID().uuidString, topics: [testTopic]),
                bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
            )
            consumerConfig.isAutoCommitEnabled = false
            consumerConfig.autoOffsetReset = .beginning  // Always read topics from beginning
            consumerConfig.broker.addressFamily = .v4

            let consumer = try KafkaConsumer(
                configuration: consumerConfig,
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

            var consumerConfig = KafkaConsumerConfiguration(
                consumptionStrategy: .group(
                    id: "produce-and-consume-with-message-headers-group-id",
                    topics: [testTopic]
                ),
                bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
            )
            consumerConfig.isAutoCommitEnabled = false
            consumerConfig.autoOffsetReset = .beginning  // Always read topics from beginning
            consumerConfig.broker.addressFamily = .v4

            let consumer = try KafkaConsumer(
                configuration: consumerConfig,
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

            var consumerConfig = KafkaConsumerConfiguration(
                consumptionStrategy: .group(
                    id: UUID().uuidString,
                    topics: [testTopic]
                ),
                bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
            )
            consumerConfig.autoOffsetReset = .beginning  // Read topic from beginning

            let consumer = try KafkaConsumer(
                configuration: consumerConfig,
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

            var consumer1Config = KafkaConsumerConfiguration(
                consumptionStrategy: .group(
                    id: uniqueGroupID,
                    topics: [testTopic]
                ),
                bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
            )
            consumer1Config.autoOffsetReset = .beginning  // Read topic from beginning
            consumer1Config.broker.addressFamily = .v4

            let consumer1 = try KafkaConsumer(
                configuration: consumer1Config,
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

            var consumer2Config = KafkaConsumerConfiguration(
                consumptionStrategy: .group(
                    id: uniqueGroupID,
                    topics: [testTopic]
                ),
                bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
            )
            consumer2Config.autoOffsetReset = .largest
            consumer2Config.broker.addressFamily = .v4

            let consumer2 = try KafkaConsumer(
                configuration: consumer2Config,
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

            var consumer1Config = KafkaConsumerConfiguration(
                consumptionStrategy: .group(
                    id: uniqueGroupID,
                    topics: [testTopic]
                ),
                bootstrapBrokerAddresses: [bootstrapBrokerAddress]
            )
            consumer1Config.autoOffsetReset = .beginning
            consumer1Config.broker.addressFamily = .v4
            consumer1Config.pollInterval = .milliseconds(1)
            consumer1Config.isAutoCommitEnabled = false

            let consumer1 = try KafkaConsumer(
                configuration: consumer1Config,
                logger: .kafkaTest
            )

            var consumer2Config = KafkaConsumerConfiguration(
                consumptionStrategy: .group(
                    id: uniqueGroupID,
                    topics: [testTopic]
                ),
                bootstrapBrokerAddresses: [bootstrapBrokerAddress]
            )
            consumer2Config.autoOffsetReset = .beginning
            consumer2Config.broker.addressFamily = .v4
            consumer2Config.pollInterval = .milliseconds(1)
            consumer2Config.isAutoCommitEnabled = false

            let consumer2 = try KafkaConsumer(
                configuration: consumer2Config,
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
            configuration: self.producerConfig,
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
