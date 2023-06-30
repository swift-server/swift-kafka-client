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

import NIOCore
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

final class KafkaProducerTests: XCTestCase {
    // Read environment variables to get information about the test Kafka server
    let kafkaHost = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort = ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092"
    var bootstrapServer: String!
    var config: KafkaProducerConfiguration!

    override func setUpWithError() throws {
        self.bootstrapServer = "\(self.kafkaHost):\(self.kafkaPort)"

        self.config = KafkaProducerConfiguration(
            bootstrapServers: [self.bootstrapServer],
            brokerAddressFamily: .v4
        )
    }

    override func tearDownWithError() throws {
        self.bootstrapServer = nil
        self.config = nil
    }

    func testSend() async throws {
        let (producerService, acks) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.config, logger: .kafkaTest)

        let serviceGroup = ServiceGroup(
            services: [producerService],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Test Task
            group.addTask {
                let expectedTopic = "test-topic"
                let message = KafkaProducerMessage(
                    topic: expectedTopic,
                    key: "key",
                    value: "Hello, World!"
                )

                let messageID = try producerService.send(message)

                for await messageResult in acks {
                    guard case .success(let acknowledgedMessage) = messageResult else {
                        XCTFail()
                        return
                    }

                    XCTAssertEqual(messageID, acknowledgedMessage.id)
                    XCTAssertEqual(expectedTopic, acknowledgedMessage.topic)
                    XCTAssertEqual(message.key, acknowledgedMessage.key)
                    XCTAssertEqual(message.value, acknowledgedMessage.value)
                    break
                }
            }

            // Wait for test task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testSendEmptyMessage() async throws {
        let (producerService, acks) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.config, logger: .kafkaTest)

        let serviceGroup = ServiceGroup(
            services: [producerService],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Test Task
            group.addTask {
                let expectedTopic = "test-topic"
                let message = KafkaProducerMessage(
                    topic: expectedTopic,
                    value: ByteBuffer()
                )

                let messageID = try producerService.send(message)

                for await messageResult in acks {
                    guard case .success(let acknowledgedMessage) = messageResult else {
                        XCTFail()
                        return
                    }

                    XCTAssertEqual(messageID, acknowledgedMessage.id)
                    XCTAssertEqual(expectedTopic, acknowledgedMessage.topic)
                    XCTAssertEqual(message.key, acknowledgedMessage.key)
                    XCTAssertEqual(message.value, acknowledgedMessage.value)
                    break
                }
            }

            // Wait for test task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testSendTwoTopics() async throws {
        let (producerService, acks) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.config, logger: .kafkaTest)

        let serviceGroup = ServiceGroup(
            services: [producerService],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Test Task
            group.addTask {
                let message1 = KafkaProducerMessage(
                    topic: "test-topic1",
                    key: "key1",
                    value: "Hello, Munich!"
                )
                let message2 = KafkaProducerMessage(
                    topic: "test-topic2",
                    key: "key2",
                    value: "Hello, London!"
                )

                var messageIDs = Set<KafkaProducerMessageID>()

                messageIDs.insert(try producerService.send(message1))
                messageIDs.insert(try producerService.send(message2))

                var acknowledgedMessages = Set<KafkaAcknowledgedMessage>()

                for await messageResult in acks {
                    guard case .success(let acknowledgedMessage) = messageResult else {
                        XCTFail()
                        return
                    }

                    acknowledgedMessages.insert(acknowledgedMessage)

                    if acknowledgedMessages.count >= 2 {
                        break
                    }
                }

                XCTAssertEqual(2, acknowledgedMessages.count)
                XCTAssertEqual(Set(acknowledgedMessages.map(\.id)), messageIDs)
                XCTAssertTrue(acknowledgedMessages.contains(where: { $0.topic == message1.topic }))
                XCTAssertTrue(acknowledgedMessages.contains(where: { $0.topic == message2.topic }))
                XCTAssertTrue(acknowledgedMessages.contains(where: { $0.key == message1.key }))
                XCTAssertTrue(acknowledgedMessages.contains(where: { $0.key == message2.key }))
                XCTAssertTrue(acknowledgedMessages.contains(where: { $0.value == message1.value }))
                XCTAssertTrue(acknowledgedMessages.contains(where: { $0.value == message2.value }))
            }

            // Wait for test task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testNoMemoryLeakAfterShutdown() async throws {
        var producerService: KafkaProducer?
        var acks: KafkaMessageAcknowledgements?
        (producerService, acks) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.config, logger: .kafkaTest)
        _ = acks

        weak var producerServiceCopy = producerService

        await withThrowingTaskGroup(of: Void.self) { group in
            // Initialize serviceGroup here so it gets dereferenced when this closure is complete
            let serviceGroup = ServiceGroup(
                services: [producerService!],
                configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
                logger: .kafkaTest
            )

            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            await serviceGroup.triggerGracefulShutdown()
        }

        producerService = nil
        // Make sure to terminate the AsyncSequence
        acks = nil

        XCTAssertNil(producerServiceCopy)
    }
}
