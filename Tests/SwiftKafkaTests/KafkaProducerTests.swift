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

final class KafkaProducerTests: XCTestCase {
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

    // For testing locally on Mac, do the following:
    // 1. Install Kafka and Zookeeper using homebrew
    // https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273
    // 2. Run the following command
    // zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
    func testSendAsync() async throws {
        let producer = try await KafkaProducer(config: config, logger: .kafkaTest)

        let expectedTopic = "test-topic"
        let message = KafkaProducerMessage(
            topic: expectedTopic,
            value: "Hello, World!"
        )

        let messageID = try await producer.sendAsync(message: message)

        for await acknowledgedMessage in producer.acknowledgements {
            XCTAssertEqual(messageID, acknowledgedMessage.id)
            XCTAssertEqual(expectedTopic, acknowledgedMessage.topic)
            XCTAssertEqual(message.value, acknowledgedMessage.value)
            break
        }

        await producer.shutdownGracefully()
    }

    func testSendAsyncTwoTopics() async throws {
        let producer = try await KafkaProducer(config: config, logger: .kafkaTest)

        let message1 = KafkaProducerMessage(
            topic: "test-topic1",
            value: "Hello, Munich!"
        )
        let message2 = KafkaProducerMessage(
            topic: "test-topic2",
            value: "Hello, London!"
        )

        var messageIDs = Set<UInt>()

        messageIDs.insert(try await producer.sendAsync(message: message1))
        messageIDs.insert(try await producer.sendAsync(message: message2))

        var acknowledgedMessages = Set<KafkaAckedMessage>()

        for await acknowledgedMessage in producer.acknowledgements {
            acknowledgedMessages.insert(acknowledgedMessage)

            if acknowledgedMessages.count >= 2 {
                break
            }
        }

        XCTAssertEqual(2, acknowledgedMessages.count)
        XCTAssertEqual(acknowledgedMessages.compactMap(\.id).sorted(), messageIDs.sorted())
        XCTAssertTrue(acknowledgedMessages.contains(where: { $0.topic == message1.topic }))
        XCTAssertTrue(acknowledgedMessages.contains(where: { $0.topic == message2.topic }))
        XCTAssertTrue(acknowledgedMessages.contains(where: { $0.value == message1.value }))
        XCTAssertTrue(acknowledgedMessages.contains(where: { $0.value == message2.value }))

        await producer.shutdownGracefully()
    }

    // TODO: test concurrent send async
}
