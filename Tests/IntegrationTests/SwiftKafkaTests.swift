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
    var producerConfig: KafkaProducerConfig!
    var consumerConfig: KafkaConsumerConfig!
    var uniqueTestTopic: String!

    override func setUpWithError() throws {
        self.bootstrapServer = "\(self.kafkaHost):\(self.kafkaPort)"

        self.producerConfig = KafkaProducerConfig(
            bootstrapServers: [self.bootstrapServer],
            brokerAddressFamily: .v4
        )

        self.consumerConfig = KafkaConsumerConfig(
            autoOffsetReset: .beginning, // Always read topics from beginning
            bootstrapServers: [self.bootstrapServer],
            brokerAddressFamily: .v4
        )

        // TODO: ok to block here? How to make setup async?
        let client = try RDKafka.createClient(type: .consumer, configDictionary: self.consumerConfig.dictionary, logger: .kafkaTest)
        self.uniqueTestTopic = try client._createUniqueTopic(timeout: 10 * 1000)
    }

    override func tearDownWithError() throws {
        // TODO: ok to block here? Problem: Tests may finish before topic is deleted
        let client = try RDKafka.createClient(type: .consumer, configDictionary: self.consumerConfig.dictionary, logger: .kafkaTest)
        try client._deleteTopic(self.uniqueTestTopic, timeout: 10 * 1000)

        self.bootstrapServer = nil
        self.producerConfig = nil
        self.consumerConfig = nil
        self.uniqueTestTopic = nil
    }

    func testProduceAndConsumeWithConsumerGroup() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let producer = try await KafkaProducer(config: producerConfig, logger: .kafkaTest)

        await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await producer.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(producer: producer, messages: testMessages)
                await producer.shutdownGracefully()
            }

            // Consumer Task
            group.addTask {
                self.consumerConfig.groupID = "subscription-test-group-id"
                let consumer = try KafkaConsumer(
                    topics: [self.uniqueTestTopic],
                    config: self.consumerConfig,
                    logger: .kafkaTest
                )

                var consumedMessages = [KafkaConsumerMessage]()
                for await messageResult in consumer.messages {
                    guard case .success(let message) = messageResult else {
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
        }
    }

    func testProduceAndConsumeWithAssignedTopicPartition() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let producer = try await KafkaProducer(config: producerConfig, logger: .kafkaTest)

        await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await producer.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(producer: producer, messages: testMessages)
                await producer.shutdownGracefully()
            }

            // Consumer Task
            group.addTask {
                let consumer = try KafkaConsumer(
                    topic: self.uniqueTestTopic,
                    partition: KafkaPartition(rawValue: 0),
                    offset: 0,
                    config: self.consumerConfig,
                    logger: .kafkaTest
                )

                var consumedMessages = [KafkaConsumerMessage]()
                for await messageResult in consumer.messages {
                    guard case .success(let message) = messageResult else {
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
        }
    }

    func testProduceAndConsumeWithCommitSync() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let producer = try await KafkaProducer(config: producerConfig, logger: .kafkaTest)

        await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await producer.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(producer: producer, messages: testMessages)
                await producer.shutdownGracefully()
            }

            // Consumer Task
            group.addTask {
                self.consumerConfig.groupID = "commit-sync-test-group-id"
                self.consumerConfig.enableAutoCommit = false
                let consumer = try KafkaConsumer(
                    topics: [self.uniqueTestTopic],
                    config: self.consumerConfig,
                    logger: .kafkaTest
                )

                var consumedMessages = [KafkaConsumerMessage]()
                for await messageResult in consumer.messages {
                    guard case .success(let message) = messageResult else {
                        continue
                    }
                    consumedMessages.append(message)
                    try await consumer.commitSync(message)

                    if consumedMessages.count >= testMessages.count {
                        break
                    }
                }

                XCTAssertEqual(testMessages.count, consumedMessages.count)

                // Additionally test that commit does not work on closed consumer
                do {
                    guard let consumedMessage = consumedMessages.first else {
                        XCTFail("No messages consumed")
                        return
                    }
                    try await consumer.commitSync(consumedMessage)
                    XCTFail("Invoking commitSync on closed consumer should have failed")
                } catch {}
            }
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
        messages: [KafkaProducerMessage]
    ) async throws {
        var messageIDs = Set<UInt>()

        for message in messages {
            messageIDs.insert(try await producer.sendAsync(message))
        }

        var acknowledgedMessages = Set<KafkaAcknowledgedMessage>()

        for await messageResult in producer.acknowledgements {
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
        XCTAssertEqual(acknowledgedMessages.map(\.id).sorted(), messageIDs.sorted())

        for message in messages {
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.topic == message.topic }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.key == message.key }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.value == message.value }))
        }
    }
}
