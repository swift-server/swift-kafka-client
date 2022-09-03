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

// TODO: add note that topic "integration-test-topic" must exist
final class SwiftKafkaTests: XCTestCase {
    // Read environment variables to get information about the test Kafka server
    let kafkaHost = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort = ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092"
    var bootstrapServer: String!
    var producerConfig: KafkaConfig!
    var consumerConfig: KafkaConfig!

    override func setUpWithError() throws {
        self.bootstrapServer = "\(self.kafkaHost):\(self.kafkaPort)"

        self.producerConfig = KafkaConfig()
        try self.producerConfig.set(self.bootstrapServer, forKey: "bootstrap.servers")
        try self.producerConfig.set("v4", forKey: "broker.address.family")

        self.consumerConfig = KafkaConfig()
        try self.consumerConfig.set(self.bootstrapServer, forKey: "bootstrap.servers")
        try self.consumerConfig.set("integration-test-group-id", forKey: "group.id")
        try self.consumerConfig.set("v4", forKey: "broker.address.family")
    }

    override func tearDownWithError() throws {
        self.bootstrapServer = nil
        self.producerConfig = nil
        self.consumerConfig = nil
    }

    func testProduceAndConsume() async throws {
        let producer = try await KafkaProducer(config: producerConfig, logger: .kafkaTest)
        let consumer = try KafkaConsumer(config: consumerConfig, logger: .kafkaTest)

        let topic = "integration-test-topic"

        let testMessage = KafkaProducerMessage(
            topic: topic,
            key: "key",
            value: "Hello, World! \(Date().description)"
        )

        try consumer.subscribe(topics: [topic])

        // Reset offset of consumer
        let start = Date()
        let timeout = 10.0
        while(Date() < start + timeout) {
            _ = try await consumer.poll(timeout: 1000)
        }

        let messageID = try await producer.sendAsync(message: testMessage)

        for await messageResult in producer.acknowledgements {
            guard case .success(let acknowledgedMessage) = messageResult else {
                XCTFail()
                return
            }
            XCTAssertEqual(messageID, acknowledgedMessage.id)
            break
        }

        // Poll Kafka in a loop until either an error is thrown or the timeout is reached.
        var consumedMessages = [KafkaConsumerMessage]()
        for _ in 0..<10 {
            if let consumedMessage = try await consumer.poll() {
                consumedMessages.append(consumedMessage)
            }
            if consumedMessages.count >= 1 {
                break
            }
        }

        XCTAssertEqual(consumedMessages.count, 1)
        XCTAssertEqual(testMessage.topic, consumedMessages.first?.topic)
        XCTAssertEqual(testMessage.key, consumedMessages.first?.key)
        XCTAssertEqual(testMessage.value, consumedMessages.first?.value)

        try consumer.close()
        try await producer.shutdownGracefully()
    }

    func testProduceAndSequenceConsume() async throws {
        let producer = try await KafkaProducer(config: producerConfig, logger: .kafkaTest)
        let injectedConsumer = try KafkaConsumer(config: consumerConfig, logger: .kafkaTest)
        let consumer = KafkaSequenceConsumer(consumer: injectedConsumer)

        let topic = "integration-test-topic"

        let testMessage = KafkaProducerMessage(
            topic: topic,
            key: "key",
            value: "Hello, World! \(Date().description)"
        )

        try consumer.subscribe(topics: [topic])
        // Reset offset of injected consumer
        let start = Date()
        let timeout = 10.0
        while(Date() < start + timeout) {
            _ = try await injectedConsumer.poll(timeout: 1000)
        }

        let messageID = try await producer.sendAsync(message: testMessage)

        for await messageResult in producer.acknowledgements {
            guard case .success(let acknowledgedMessage) = messageResult else {
                XCTFail()
                return
            }
            XCTAssertEqual(messageID, acknowledgedMessage.id)
            break
        }

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

        try consumer.close()
        try await producer.shutdownGracefully()
    }
    // TODO: also test concurrently?
}
