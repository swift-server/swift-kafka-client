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
import Logging

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
    let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "linux-dev"
    let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!
    var bootstrapBrokerAddress: KafkaConfiguration.BrokerAddress!
    var producerConfig: KafkaProducerConfiguration!
    var uniqueTestTopic: String!
    var uniqueTestTopic2: String!

    override func setUpWithError() throws {
        self.bootstrapBrokerAddress = KafkaConfiguration.BrokerAddress(
            host: self.kafkaHost,
            port: self.kafkaPort
        )

        self.producerConfig = KafkaProducerConfiguration(bootstrapBrokerAddresses: [self.bootstrapBrokerAddress])
        self.producerConfig.broker.addressFamily = .v4

        self.uniqueTestTopic = try createUniqueTopic(partitions: 1)
        self.uniqueTestTopic2 = try createUniqueTopic(partitions: 1)
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
        if let uniqueTestTopic {
            try client._deleteTopic(uniqueTestTopic, timeout: 10 * 1000)
        }
        if let uniqueTestTopic2 {
            try client._deleteTopic(uniqueTestTopic2, timeout: 10 * 1000)
        }

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
    #if false
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
        
        var cont: AsyncStream<KafkaConsumerMessage>.Continuation!
        let sequenceForAcks = AsyncStream<KafkaConsumerMessage>(
            bufferingPolicy: .bufferingOldest(1_000)) {
            continuation in
            cont = continuation
        }
        let continuation = cont


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
//            group.addTask {
//                logger1.info("Task for consumer 1 started")
//                var consumedMessages = [KafkaConsumerMessage]()
//                for try await messageResult in consumer.messages {
//                    if !consumerConfig.enableAutoCommit {
//                        try await consumer.commitSync(messageResult)
//                    }
//                    guard case let message = messageResult else {
//                        continue
//                    }
//                    consumedMessages.append(message)
//                    if consumedMessages.count % max(testMessages.count / 10, 1) == 0 {
//                        logger1.info("Got \(consumedMessages.count) out of \(testMessages.count)")
//                    }
//
//                    if consumedMessages.count >= testMessages.count {
//                        break
//                    }
//                }
//
//                logger1.info("Task for consumer 1 finished, fetched \(consumedMessages.count)")
////                XCTAssertEqual(testMessages.count, consumedMessages.count)
////
////                for (index, consumedMessage) in consumedMessages.enumerated() {
////                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
////                    XCTAssertEqual(testMessages[index].key, consumedMessage.key)
////                    XCTAssertEqual(testMessages[index].value, consumedMessage.value)
////                }
//            }
//
            group.addTask {
                var consumedMessages = [KafkaConsumerMessage]()
                for try await message in consumer.messages {
                    consumedMessages.append(message)

                    if consumedMessages.count >= testMessages.count {
                        break
                    }
                    continuation?.yield(messageResult)
                    aa += 1
                    if aa % max(testMessages.count / 10, 1) == 0 {
                        logger2.info("Got \(aa) out of \(testMessages.count)")
                    }
                }

                XCTAssertEqual(testMessages.count, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].key!), consumedMessage.key)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].value), consumedMessage.value)
                }
            }

//            try? await Task.sleep(for: .seconds(5))

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }
    #endif

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

    func testPartitionForKey() async throws {
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        let numberOfPartitions = 6
        let expectedTopic = try createUniqueTopic(partitions: Int32(numberOfPartitions))
        let key = "key"

        let expectedPartition = producer.partitionForKey(key, in: expectedTopic, partitionCount: numberOfPartitions)
        XCTAssertNotNil(expectedPartition)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            let message = KafkaProducerMessage(
                topic: expectedTopic,
                key: key,
                value: "Hello, World!"
            )

            let messageID = try producer.send(message)

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

                if receivedDeliveryReports.count >= 1 {
                    break
                }
            }

            let receivedDeliveryReport = receivedDeliveryReports.first!
            XCTAssertEqual(messageID, receivedDeliveryReport.id)

            guard case .acknowledged(let receivedMessage) = receivedDeliveryReport.status else {
                XCTFail()
                return
            }

            XCTAssertEqual(expectedTopic, receivedMessage.topic)
            XCTAssertEqual(expectedPartition, receivedMessage.partition)
            XCTAssertEqual(ByteBuffer(string: message.key!), receivedMessage.key)
            XCTAssertEqual(ByteBuffer(string: message.value), receivedMessage.value)

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }
    
    func testPartitionEof() async throws {
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)
        
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
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
            
            try await group.next()

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
        
        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: "test",
                topics: [self.uniqueTestTopic]
            ),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumerConfig.autoOffsetReset = .beginning // Read topic from beginning
        consumerConfig.broker.addressFamily = .v4
        consumerConfig.enablePartitionEof = true

        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: .kafkaTest
        )
        
        let consumerServiceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let consumerServiceGroup = ServiceGroup(configuration: consumerServiceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await consumerServiceGroup.run()
            }
            
            group.addTask {
                var messages = [KafkaConsumerMessage]()
                for try await record in consumer.messages {
                    guard !record.eof else {
                        break
                    }
                    messages.append(record)
                }
                XCTAssertEqual(messages.count, testMessages.count)
            }
            
            try await group.next()
            
            await consumerServiceGroup.triggerGracefulShutdown()
        }
    }
    
    func testMetadata() async throws {
        let uniqueTopic = try createUniqueTopic(partitions: 7)
        defer {
            // delete topic
            var basicConfig = KafkaConsumerConfiguration(
                consumptionStrategy: .group(id: "no-group", topics: []),
                bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
            )
            basicConfig.broker.addressFamily = .v4

            let client = try? RDKafkaClient.makeClient(
                type: .consumer,
                configDictionary: basicConfig.dictionary,
                events: [],
                logger: .kafkaTest
            )
            try? client?._deleteTopic(uniqueTopic, timeout: 10 * 1000)
        }
        
        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: "test",
                topics: []
            ),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumerConfig.autoOffsetReset = .beginning // Read topic from beginning
        consumerConfig.broker.addressFamily = .v4
        consumerConfig.enablePartitionEof = true

        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: .kafkaTest
        )
        let metadata = try await consumer.metadata()
        let topic = metadata.topics.first { topic in
            topic.name == uniqueTopic
        }
        guard let topic else {
            XCTFail("Topic was not found")
            return
        }
        let partitions = topic.partitions
        XCTAssertEqual(partitions.count, 7)
    }
    // MARK: - Helpers

    func createUniqueTopic(partitions: Int32 = -1 /* default num for cluster */) throws -> String {
        // TODO: ok to block here? How to make setup async?

        var basicConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "no-group", topics: []),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        basicConfig.broker.addressFamily = .v4

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: basicConfig.dictionary,
            events: [],
            logger: .kafkaTest
        )
        return try client._createUniqueTopic(partitions: partitions, timeout: 10 * 1000)
    }
    
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
                continue
//                break // Ignore any other events
            }
            
            print("Sent \(receivedDeliveryReports.count) out of \(messages.count)")

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
/*
    func testProduceAndConsumeWithTransaction() async throws {
        let testMessages = Self.createTestMessages(topic: uniqueTestTopic, count: 10)

        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        let transactionConfigProducer = KafkaTransactionalProducerConfiguration(
            transactionalId: "1234",
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress])

        let transactionalProducer = try await KafkaTransactionalProducer(config: transactionConfigProducer, logger: .kafkaTest)

        let makeConsumerConfig = { (topic: String) -> KafkaConsumerConfiguration in
            var consumerConfig = KafkaConsumerConfiguration(
                consumptionStrategy: .group(id: "subscription-test-group-id", topics: [topic]),
                bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
            )
            consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
            consumerConfig.broker.addressFamily = .v4
            consumerConfig.isAutoCommitEnabled = false
            return consumerConfig
        }

        let consumer = try KafkaConsumer(
            configuration: makeConsumerConfig(uniqueTestTopic),
            logger: .kafkaTest
        )

        let consumerAfterTransaction = try KafkaConsumer(
            configuration: makeConsumerConfig(uniqueTestTopic2),
            logger: .kafkaTest
        )

        let serviceGroup = ServiceGroup(
            services: [
                producer,
                consumer,
                transactionalProducer,
                consumerAfterTransaction,
            ],
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
                    events: events,
                    messages: testMessages
                )
            }

            // Consumer Task
            group.addTask {
                var count = 0
                for try await messageResult in consumer.messages {
                    guard case let message = messageResult else {
                        continue
                    }
                    count += 1
                    try await transactionalProducer.withTransaction { transaction in
                        let newMessage = KafkaProducerMessage(
                            topic: self.uniqueTestTopic2,
                            value: message.value.description + "_updated"
                        )
                        try transaction.send(newMessage)
                        let partitionlist = RDKafkaTopicPartitionList()
                        partitionlist.setOffset(topic: self.uniqueTestTopic, partition: message.partition, offset: message.offset)
                        try await transaction.send(offsets: partitionlist, forConsumer: consumer)
                    }

                    if count >= testMessages.count {
                        break
                    }
                }
                print("Changed all messages \(count)")
            }

            group.addTask {
                var count = 0
                for try await messageAfterTransaction in consumerAfterTransaction.messages {
                    let value = messageAfterTransaction.value.getString(at: 0, length: messageAfterTransaction.value.readableBytes)
                    XCTAssert(value?.contains("_updated") ?? false)
                    count += 1
                    if count >= testMessages.count || Task.isCancelled {
                        break
                    }
                }
                XCTAssertEqual(count, testMessages.count)
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            try await group.next()

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }
    
    
    */
   #if false 
    func testOrdo() async throws {
//        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
//        let firstConsumerOffset = testMessages.count / 2
//        let (producer, acks) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)
//
//        // Important: both consumer must have the same group.id
        let uniqueGroupID = UUID().uuidString

        // MARK: First Consumer
        var logger_ = Logger(label: "ordo")
        logger_.logLevel = .debug
        let logger = logger_
        
        logger.info("unique group id \(uniqueGroupID)")

//        let groupID = uniqueGroupID
        let groupID = "test_group_id_1"
        
        var consumer1Config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: groupID,
                topics: ["transactions-pending-dc-1"] //["transactions-snapshots-dc-1"]
            ),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumer1Config.isAutoCommitEnabled = false
        consumer1Config.autoOffsetReset = .beginning // Read topic from beginning
        consumer1Config.broker.addressFamily = .v4
//        consumer1Config.debugOptions = [.all]
        consumer1Config.groupInstanceId = groupID + "_instance" //"transactions-pending-dc-1-test-instance-id"
        consumer1Config.statisticsInterval = .value(.milliseconds(250))
        

        let (consumer1, events) = try KafkaConsumer.makeConsumerWithEvents(
            configuration: consumer1Config,
            logger: logger
        )

        let serviceGroup1 = ServiceGroup(
            services: [consumer1],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup1.run()
            }

            // First Consumer Task
            group.addTask {
//                try consumer1.subscribeTopics(topics: ["transactions-pending-dc-1"])
                var count = 0
                var totalCount = 0
                var start = Date.now
                var partitions = [Bool]()
                for try await message in consumer1.messages {
                    if message.eof {
                        logger.info("Reached EOF for partition \(message.partition) and topic \(message.topic)! Read \(totalCount)")

                        while partitions.count <= message.partition.rawValue {
                            partitions.append(false)
                        }
                        partitions[message.partition.rawValue] = true
                        let res = partitions.first { $0 == false }
                        if partitions.count == 6 && res == nil {
                            break
                        }

                        continue
                    }
//                    logger.info("Got msg: \(message)")
                    try await consumer1.commitSync(message)
                    count += 1
                    totalCount += 1
                    let now = Date.now
                    if count > 100 && now > start {
                        let diff = -start.timeIntervalSinceNow
                        let rate = Double(count) / diff
                        logger.info("Rate is \(rate) for last \(diff)")
                        count = 0
                        start = now
                        try await consumer1.commitSync(message)
                    }
                }
                logger.info("Finally read \(totalCount)")
            }
            
            group.addTask {
                for try await event in events {
                    switch event {
                    case .statistics(let stat):
//                        logger.info("stats: \(stat)")
                        if let lag = stat.lag, lag == 0 {
                            logger.info("In sync with lag = 0 with stat \(stat)")
                            
//                            await serviceGroup1.triggerGracefulShutdown()
                            return
                        }
                        
                    default:
                        break
                    }
                }
            }
            
            try await group.next()
            
            await serviceGroup1.triggerGracefulShutdown()

        }
    }
#endif
}

//fileprivate let staticLogger = Logger(label: "")

extension KafkaStatistics {
    public var lag: Int? {
        guard let json = try? self.json, // = try? str?.json,
              let topics = json.topics
        else {
            return nil
        }
        
        var maxLag: Int?
        for (_, topic) in topics {
            guard let partitions = topic.partitions else {
                return nil
            }
            for (name, partition) in partitions {
                if name == "-1" {
                    continue
                }
//                guard let eofOffset = partition.eofOffset, eofOffset >= 0 else {
//
//                    return nil
//                }
                var lag: Int?
                // There is no commits to the partition
                if let lsOffset = partition.lsOffset, lsOffset == 0 {
                    lag = 0
                    // sometimes there is no stored offset, and we should check that we read everything before our start
//                } else if let committedOffset = partition.committedOffset,
//                          let eofOffset = partition.eofOffset,
//                          committedOffset >= 0,
//                          eofOffset >= 0,
//                          eofOffset - committedOffset == 1 { // commited one before eof
//                    lag = 0
                } else if let consumerLag = partition.consumerLagStored,
                          consumerLag >= 0 {
                    lag = consumerLag
                }
                guard let lag else {
                    return nil
                }
                maxLag = max(maxLag ?? Int.min, lag)
            }
        }
        return maxLag
    }
}
