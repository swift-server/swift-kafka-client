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

import struct Foundation.UUID
@testable import Kafka
import NIOCore
import ServiceLifecycle
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

final class KafkaTests: XCTestCase {
    // Read environment variables to get information about the test Kafka server
    let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!
    var bootstrapBrokerAddress: KafkaConfiguration.BrokerAddress!
    var producerConfig: KafkaProducerConfiguration!
    var uniqueTestTopic: String!

    override func setUpWithError() throws {
        self.bootstrapBrokerAddress = KafkaConfiguration.BrokerAddress(
            host: self.kafkaHost,
            port: self.kafkaPort
        )

        self.producerConfig = KafkaProducerConfiguration(bootstrapBrokerAddresses: [self.bootstrapBrokerAddress])
        self.producerConfig.broker.addressFamily = .v4

        var basicConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "no-group", topics: []),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        basicConfig.broker.addressFamily = .v4

        // TODO: ok to block here? How to make setup async?
        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: basicConfig.dictionary,
            events: [],
            logger: .kafkaTest
        )
        self.uniqueTestTopic = try client._createUniqueTopic(timeout: 10 * 1000)
    }

    override func tearDownWithError() throws {
        var basicConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "no-group", topics: []),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        basicConfig.broker.addressFamily = .v4

        // TODO: ok to block here? Problem: Tests may finish before topic is deleted
        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: basicConfig.dictionary,
            events: [],
            logger: .kafkaTest
        )
        try client._deleteTopic(self.uniqueTestTopic, timeout: 10 * 1000)

        self.bootstrapBrokerAddress = nil
        self.producerConfig = nil
        self.uniqueTestTopic = nil
    }

    func testProduceAndConsumeWithConsumerGroup() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "subscription-test-group-id", topics: [self.uniqueTestTopic]),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.broker.addressFamily = .v4

        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer, consumer], logger: .kafkaTest)
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
                    messages: testMessages
                )
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

                XCTAssertEqual(testMessages.count, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].key!), consumedMessage.key)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].value), consumedMessage.value)
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
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .partition(
                KafkaPartition(rawValue: 0),
                topic: self.uniqueTestTopic,
                offset: KafkaOffset(rawValue: 0)
            ),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.broker.addressFamily = .v4

        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer, consumer], logger: .kafkaTest)
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
                    messages: testMessages
                )
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

                XCTAssertEqual(testMessages.count, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].key!), consumedMessage.key)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].value), consumedMessage.value)
                }
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testProduceAndConsumeWithScheduleCommit() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "commit-sync-test-group-id", topics: [self.uniqueTestTopic]),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumerConfig.isAutoCommitEnabled = false
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.broker.addressFamily = .v4

        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer, consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Consumer Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    events: events,
                    messages: testMessages
                )
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

                XCTAssertEqual(testMessages.count, consumedMessages.count)
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testProduceAndConsumeWithCommit() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "commit-sync-test-group-id", topics: [self.uniqueTestTopic]),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumerConfig.isAutoCommitEnabled = false
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.broker.addressFamily = .v4

        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer, consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Consumer Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    events: events,
                    messages: testMessages
                )
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

                XCTAssertEqual(testMessages.count, consumedMessages.count)
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testProduceAndConsumeWithMessageHeaders() async throws {
        let testMessages = Self.createTestMessages(
            topic: self.uniqueTestTopic,
            headers: [
                KafkaHeader(key: "some.header", value: ByteBuffer(string: "some-header-value")),
                KafkaHeader(key: "some.null.header", value: nil),
            ],
            count: 10
        )

        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "commit-sync-test-group-id", topics: [self.uniqueTestTopic]),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumerConfig.isAutoCommitEnabled = false
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.broker.addressFamily = .v4

        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer, consumer], logger: .kafkaTest)
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
                    try await consumer.commit(message)

                    if consumedMessages.count >= testMessages.count {
                        break
                    }
                }

                XCTAssertEqual(testMessages.count, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].headers, consumedMessage.headers)
                }
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testNoNewConsumerMessagesAfterGracefulShutdown() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 2)
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        let uniqueGroupID = UUID().uuidString

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: uniqueGroupID,
                topics: [self.uniqueTestTopic]
            ),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumerConfig.autoOffsetReset = .beginning // Read topic from beginning

        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: .kafkaTest
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer, consumer], logger: .kafkaTest)
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
                    messages: testMessages
                )
            }

            // Wait for Producer Task to complete
            try await group.next()

            // Verify that we receive the first message
            var consumerIterator = consumer.messages.makeAsyncIterator()

            let consumedMessage = try await consumerIterator.next()
            XCTAssertEqual(testMessages.first!.topic, consumedMessage!.topic)
            XCTAssertEqual(ByteBuffer(string: testMessages.first!.key!), consumedMessage!.key)
            XCTAssertEqual(ByteBuffer(string: testMessages.first!.value), consumedMessage!.value)

            // Trigger a graceful shutdown
            await serviceGroup.triggerGracefulShutdown()

            // Wait to ensure the KafkaConsumer's shutdown handler has
            // been invoked.
            try await Task.sleep(for: .seconds(2))

            // We should not be able to read any new messages after the KafkaConsumer's
            // shutdown handler was invoked
            let stoppedConsumingMessage = try await consumerIterator.next()
            XCTAssertNil(stoppedConsumingMessage)
        }
    }

    func testCommittedOffsetsAreCorrect() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let firstConsumerOffset = testMessages.count / 2
        let (producer, acks) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        // Important: both consumer must have the same group.id
        let uniqueGroupID = UUID().uuidString

        // MARK: First Consumer

        var consumer1Config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: uniqueGroupID,
                topics: [self.uniqueTestTopic]
            ),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumer1Config.autoOffsetReset = .beginning // Read topic from beginning
        consumer1Config.broker.addressFamily = .v4

        let consumer1 = try KafkaConsumer(
            configuration: consumer1Config,
            logger: .kafkaTest
        )

        let serviceGroupConfiguration1 = ServiceGroupConfiguration(services: [producer, consumer1], logger: .kafkaTest)
        let serviceGroup1 = ServiceGroup(configuration: serviceGroupConfiguration1)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup1.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    events: acks,
                    messages: testMessages
                )
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

                XCTAssertEqual(firstConsumerOffset, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].key!), consumedMessage.key)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].value), consumedMessage.value)
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

        var consumer2Config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: uniqueGroupID,
                topics: [self.uniqueTestTopic]
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

                XCTAssertEqual(testMessages.count - firstConsumerOffset, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[firstConsumerOffset + index].topic, consumedMessage.topic)
                    XCTAssertEqual(ByteBuffer(string: testMessages[firstConsumerOffset + index].key!), consumedMessage.key)
                    XCTAssertEqual(ByteBuffer(string: testMessages[firstConsumerOffset + index].value), consumedMessage.value)
                }
            }

            // Wait for second Consumer Task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup2.triggerGracefulShutdown()
        }
    }

    // MARK: - Helpers

    private static func createTestMessages(
        topic: String,
        headers: [KafkaHeader] = [],
        count: UInt
    ) -> [KafkaProducerMessage<String, String>] {
        return Array(0..<count).map {
            KafkaProducerMessage(
                topic: topic,
                headers: headers,
                key: "key",
                value: "Hello, World! \($0) - \(Date().description)"
            )
        }
    }

    private static func sendAndAcknowledgeMessages(
        producer: KafkaProducer,
        events: KafkaProducerEvents,
        messages: [KafkaProducerMessage<String, String>]
    ) async throws {
        var messageIDs = Set<KafkaProducerMessageID>()

        for message in messages {
            messageIDs.insert(try producer.send(message))
        }

        var receivedDeliveryReports = Set<KafkaDeliveryReport>()

        for await event in events {
            switch event {
            case .deliveryReports(let deliveryReports):
                for deliveryReport in deliveryReports {
                    receivedDeliveryReports.insert(deliveryReport)
                }
            default:
                break // Ignore any other events
            }

            if receivedDeliveryReports.count >= messages.count {
                break
            }
        }

        XCTAssertEqual(Set(receivedDeliveryReports.map(\.id)), messageIDs)

        let acknowledgedMessages: [KafkaAcknowledgedMessage] = receivedDeliveryReports.compactMap {
            guard case .acknowledged(let receivedMessage) = $0.status else {
                return nil
            }
            return receivedMessage
        }

        XCTAssertEqual(messages.count, acknowledgedMessages.count)
        for message in messages {
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.topic == message.topic }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.key == ByteBuffer(string: message.key!) }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.value == ByteBuffer(string: message.value) }))
        }
    }
}
