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
    func testSendAsync() throws {
        let producer = try KafkaProducer(config: config, logger: .kafkaTest)

        let expectedTopic = "test-topic"
        let expectedValue = "Hello, World!"
        let message = KafkaProducerMessage(
            topic: expectedTopic,
            value: expectedValue
        )

        let expectation = expectation(description: "Send complete")

        producer.sendAsync(message: message) { result in
            expectation.fulfill()

            switch result {
            case .success(let receivedMessage):
                XCTAssertEqual(expectedTopic, receivedMessage.topic)
                XCTAssertEqual(expectedValue, receivedMessage.valueString)
            case .failure:
                XCTFail("Sending message was unsuccessful")
            }
        }

        waitForExpectations(timeout: 2)
    }

    // TODO: test concurrent send async
}
