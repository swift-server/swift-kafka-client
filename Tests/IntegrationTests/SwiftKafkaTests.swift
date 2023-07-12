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

import struct Foundation.UUID
import ServiceLifecycle
@testable import SwiftKafka
import XCTest

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

final class SwiftKafkaTests: XCTestCase {
    // Read environment variables to get information about the test Kafka server
    let kafkaHost = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort = ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092"
    var bootstrapServer: String!
    var producerConfig: KafkaProducerConfiguration!
    var uniqueTestTopic: String!

    override func setUpWithError() throws {
        self.bootstrapServer = "\(self.kafkaHost):\(self.kafkaPort)"

        self.producerConfig = KafkaProducerConfiguration()
        self.producerConfig.bootstrapServers = [self.bootstrapServer]
        self.producerConfig.broker.addressFamily = .v4

        var basicConfig = KafkaConsumerConfiguration()
        basicConfig.consumptionStrategy = .group(groupID: "no-group", topics: [])
        basicConfig.bootstrapServers = [self.bootstrapServer]
        basicConfig.broker.addressFamily = .v4

        // TODO: ok to block here? How to make setup async?
        let client = try RDKafka.createClient(
            type: .consumer,
            configDictionary: basicConfig.dictionary,
            events: [],
            logger: .kafkaTest
        )
        self.uniqueTestTopic = try client._createUniqueTopic(timeout: 10 * 1000)
    }

    override func tearDownWithError() throws {
        var basicConfig = KafkaConsumerConfiguration()
        basicConfig.consumptionStrategy = .group(groupID: "no-group", topics: [])
        basicConfig.bootstrapServers = [self.bootstrapServer]
        basicConfig.broker.addressFamily = .v4

        // TODO: ok to block here? Problem: Tests may finish before topic is deleted
        let client = try RDKafka.createClient(
            type: .consumer,
            configDictionary: basicConfig.dictionary,
            events: [],
            logger: .kafkaTest
        )
        try client._deleteTopic(self.uniqueTestTopic, timeout: 10 * 1000)

        self.bootstrapServer = nil
        self.producerConfig = nil
        self.uniqueTestTopic = nil
    }

    func testProduceAndConsumeWithConsumerGroup() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let (producer, acknowledgments) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration()
        consumerConfig.consumptionStrategy = .group(groupID: "subscription-test-group-id", topics: [self.uniqueTestTopic])
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.bootstrapServers = [self.bootstrapServer]
        consumerConfig.broker.addressFamily = .v4

        let consumer = try KafkaConsumer(
            config: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroup = ServiceGroup(
            services: [producer, consumer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    acknowledgements: acknowledgments,
                    messages: testMessages
                )
            }

            // Consumer Task
            group.addTask {
                var consumedMessages = [KafkaConsumerMessage]()
                for try await messageResult in consumer.messages {
                    guard case let message = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)

                    if consumedMessages.count >= testMessages.count {
                        break
                    }
                }

                XCTAssertEqual(testMessages.count, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
                    XCTAssertEqual(testMessages[index].key, consumedMessage.key)
                    XCTAssertEqual(testMessages[index].value, consumedMessage.value)
                }
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testProduceAndConsumeWithAssignedTopicPartition() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let (producer, acknowledgments) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration()
        consumerConfig.consumptionStrategy = .partition(
            topic: self.uniqueTestTopic,
            partition: KafkaPartition(rawValue: 0),
            offset: 0
        )
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.bootstrapServers = [self.bootstrapServer]
        consumerConfig.broker.addressFamily = .v4

        let consumer = try KafkaConsumer(
            config: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroup = ServiceGroup(
            services: [producer, consumer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    acknowledgements: acknowledgments,
                    messages: testMessages
                )
            }

            // Consumer Task
            group.addTask {
                var consumedMessages = [KafkaConsumerMessage]()
                for try await messageResult in consumer.messages {
                    guard case let message = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)

                    if consumedMessages.count >= testMessages.count {
                        break
                    }
                }

                XCTAssertEqual(testMessages.count, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
                    XCTAssertEqual(testMessages[index].key, consumedMessage.key)
                    XCTAssertEqual(testMessages[index].value, consumedMessage.value)
                }
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testProduceAndConsumeWithCommitSync() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let (producer, acknowledgments) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration()
        consumerConfig.consumptionStrategy = .group(groupID: "commit-sync-test-group-id", topics: [self.uniqueTestTopic])
        consumerConfig.enableAutoCommit = false
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.bootstrapServers = [self.bootstrapServer]
        consumerConfig.broker.addressFamily = .v4

        let consumer = try KafkaConsumer(
            config: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroup = ServiceGroup(
            services: [producer, consumer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Consumer Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    acknowledgements: acknowledgments,
                    messages: testMessages
                )
            }

            // Consumer Task
            group.addTask {
                var consumedMessages = [KafkaConsumerMessage]()
                for try await messageResult in consumer.messages {
                    guard case let message = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)
                    try await consumer.commitSync(message)

                    if consumedMessages.count >= testMessages.count {
                        break
                    }
                }

                XCTAssertEqual(testMessages.count, consumedMessages.count)
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testCommittedOffsetsAreCorrect() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let firstConsumerOffset = testMessages.count / 2
        let (producer, acks) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.producerConfig, logger: .kafkaTest)

        // Important: both consumer must have the same group.id
        let uniqueGroupID = UUID().uuidString

        // MARK: First Consumer

        let consumer1Config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                groupID: uniqueGroupID,
                topics: [self.uniqueTestTopic]
            ),
            autoOffsetReset: .beginning, // Read topic from beginning
            bootstrapServers: [self.bootstrapServer],
            brokerAddressFamily: .v4
        )

        let consumer1 = try KafkaConsumer(
            config: consumer1Config,
            logger: .kafkaTest
        )

        let serviceGroup1 = ServiceGroup(
            services: [producer, consumer1],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup1.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    acknowledgements: acks,
                    messages: testMessages
                )
            }

            // First Consumer Task
            group.addTask {
                var consumedMessages = [KafkaConsumerMessage]()
                for try await messageResult in consumer1.messages {
                    guard case let message = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)

                    // Only read first half of messages
                    if consumedMessages.count >= firstConsumerOffset {
                        break
                    }
                }

                XCTAssertEqual(firstConsumerOffset, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
                    XCTAssertEqual(testMessages[index].key, consumedMessage.key)
                    XCTAssertEqual(testMessages[index].value, consumedMessage.value)
                }
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
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

        let consumer2Config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                groupID: uniqueGroupID,
                topics: [self.uniqueTestTopic]
            ),
            autoOffsetReset: .largest,
            bootstrapServers: [self.bootstrapServer],
            brokerAddressFamily: .v4
        )

        let consumer2 = try KafkaConsumer(
            config: consumer2Config,
            logger: .kafkaTest
        )

        let serviceGroup2 = ServiceGroup(
            services: [consumer2],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup2.run()
            }

            // Second Consumer Task
            group.addTask {
                var consumedMessages = [KafkaConsumerMessage]()
                for try await messageResult in consumer2.messages {
                    guard case let message = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)

                    // Read second half of messages
                    if consumedMessages.count >= (testMessages.count - firstConsumerOffset) {
                        break
                    }
                }

                XCTAssertEqual(testMessages.count - firstConsumerOffset, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[firstConsumerOffset + index].topic, consumedMessage.topic)
                    XCTAssertEqual(testMessages[firstConsumerOffset + index].key, consumedMessage.key)
                    XCTAssertEqual(testMessages[firstConsumerOffset + index].value, consumedMessage.value)
                }
            }

            // Wait for second Consumer Task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup2.triggerGracefulShutdown()
        }
    }

    // MARK: - Helpers

    private static func createTestMessages(topic: String, count: UInt) -> [KafkaProducerMessage] {
        return Array(0..<count).map {
            KafkaProducerMessage(
                topic: topic,
                key: "key",
                value: "Hello, World! \($0) - \(Date().description)"
            )
        }
    }

    private static func sendAndAcknowledgeMessages(
        producer: KafkaProducer,
        acknowledgements: KafkaMessageAcknowledgements,
        messages: [KafkaProducerMessage]
    ) async throws {
        var messageIDs = Set<KafkaProducerMessageID>()

        for message in messages {
            messageIDs.insert(try producer.send(message))
        }

        var acknowledgedMessages = Set<KafkaAcknowledgedMessage>()

        for await messageResult in acknowledgements {
            guard case .success(let acknowledgedMessage) = messageResult else {
                XCTFail()
                return
            }

            acknowledgedMessages.insert(acknowledgedMessage)

            if acknowledgedMessages.count >= messages.count {
                break
            }
        }

        XCTAssertEqual(messages.count, acknowledgedMessages.count)
        XCTAssertEqual(Set(acknowledgedMessages.map(\.id)), messageIDs)

        for message in messages {
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.topic == message.topic }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.key == message.key }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.value == message.value }))
        }
    }
}
