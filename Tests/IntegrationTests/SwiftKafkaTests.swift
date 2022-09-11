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
// 1. Install Kafka and Zookeeper using homebrew
// https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273
// 2. Run the following command
// zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
// TODO: alternative run command for hombrew M1 installations
// zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties & kafka-server-start /opt/homebrew/etc/kafka/server.properties
// TODO: add note that test topics must exist and how to create them

// TODO: test for multiple messages
final class SwiftKafkaTests: XCTestCase {
    // Read environment variables to get information about the test Kafka server
    let kafkaHost = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort = ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092"
    var bootstrapServer: String!
    var config: KafkaConfig!

    override func setUpWithError() throws {
        self.bootstrapServer = "\(self.kafkaHost):\(self.kafkaPort)"

        self.config = KafkaConfig()
        try self.config.set(self.bootstrapServer, forKey: "bootstrap.servers")
        try self.config.set("v4", forKey: "broker.address.family")
    }

    override func tearDownWithError() throws {
        self.bootstrapServer = nil
        self.config = nil
    }

    func testProduceAndConsumeWithConsumerGroup() async throws {
        let topic = "subscription-test-topic"

        let producer = try await KafkaProducer(config: config, logger: .kafkaTest)
        let consumer = try await KafkaConsumer(
            topics: [topic],
            groupID: "subscription-test-group-id",
            config: config,
            logger: .kafkaTest
        )

        let testMessage = KafkaProducerMessage(
            topic: topic,
            key: "key",
            value: "Hello, World! \(Date().description)"
        )

        try await Self.resetConsumerOffset(consumer)
        try await Self.sendAndAcknowledgeMessages(producer: producer, messages: [testMessage])

        var consumedMessage: KafkaConsumerMessage?
        for await messageResult in consumer.messages {
            guard case .success(let message) = messageResult else {
                continue
            }
            consumedMessage = message
            break
        }

        XCTAssertEqual(testMessage.topic, consumedMessage?.topic)
        XCTAssertEqual(testMessage.key, consumedMessage?.key)
        XCTAssertEqual(testMessage.value, consumedMessage?.value)

        try await consumer.close()
        try await producer.shutdownGracefully()
    }

    func testProduceAndConsumeWithAssignedTopicPartition() async throws {
        let topic = "assignment-test-topic"

        let producer = try await KafkaProducer(config: config, logger: .kafkaTest)
        let consumer = try await KafkaConsumer(
            topic: topic,
            partition: KafkaPartition(rawValue: 0),
            config: config,
            logger: .kafkaTest
        )

        let testMessage = KafkaProducerMessage(
            topic: topic,
            key: "key",
            value: "Hello, World! \(Date().description)"
        )

        try await Self.resetConsumerOffset(consumer)
        try await Self.sendAndAcknowledgeMessages(producer: producer, messages: [testMessage])

        var consumedMessage: KafkaConsumerMessage?
        for await messageResult in consumer.messages {
            guard case .success(let message) = messageResult else {
                continue
            }
            consumedMessage = message
            break
        }

        XCTAssertEqual(testMessage.topic, consumedMessage?.topic)
        XCTAssertEqual(testMessage.key, consumedMessage?.key)
        XCTAssertEqual(testMessage.value, consumedMessage?.value)

        try await consumer.close()
        try await producer.shutdownGracefully()
    }

    func testProduceAndConsumeWithCommitSync() async throws {
        let topic = "commit-sync-test-topic"

        try config.set("false", forKey: "enable.auto.commit")

        let producer = try await KafkaProducer(config: config, logger: .kafkaTest)
        let consumer = try await KafkaConsumer(
            topics: [topic], // TODO: multiple topics
            groupID: "commit-sync-test-group-id",
            config: config,
            logger: .kafkaTest
        )

        let testMessages = Array(0...9).map {
            KafkaProducerMessage(
                topic: topic,
                key: "key",
                value: "Hello, World! \($0) - \(Date().description)"
            )
        }

        try await Self.resetConsumerOffset(consumer)
        try await Self.sendAndAcknowledgeMessages(producer: producer, messages: testMessages)

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

        try await consumer.close()
        try await producer.shutdownGracefully()
    }

    // TODO: also test concurrently?

    // MARK: - Helpers

    private static func resetConsumerOffset(_ consumer: KafkaConsumer) async throws {
        let start = Date()
        let timeout = 10.0
        while Date() < start + timeout {
            _ = try await consumer.poll(timeout: 1000) // TODO: poll needs to be private, do something else here
        }
    }

    private static func sendAndAcknowledgeMessages(
        producer: KafkaProducer,
        messages: [KafkaProducerMessage]
    ) async throws {
        var messageIDs = Set<UInt>()

        for message in messages {
            messageIDs.insert(try await producer.sendAsync(message: message))
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
