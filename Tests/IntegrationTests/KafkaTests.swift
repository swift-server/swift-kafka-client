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
import struct Foundation.UUID
@testable import Kafka
@_spi(Internal) import Kafka
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
    /// Number of partitions in `uniqueTestTopic2` (the shared multi-partition fixture).
    static let uniqueTestTopic2Partitions = 6

    override func setUpWithError() throws {
        self.bootstrapBrokerAddress = KafkaConfiguration.BrokerAddress(
            host: self.kafkaHost,
            port: self.kafkaPort
        )

        self.producerConfig = KafkaProducerConfiguration(bootstrapBrokerAddresses: [self.bootstrapBrokerAddress])
        self.producerConfig.broker.addressFamily = .v4

        self.uniqueTestTopic = try createUniqueTopic(partitions: 1)
        // `uniqueTestTopic2` is the shared multi-partition fixture (used by the rebalance
        // tests). Kept fresh & unique per test, and deleted in `tearDownWithError`.
        self.uniqueTestTopic2 = try createUniqueTopic(partitions: Int32(Self.uniqueTestTopic2Partitions))
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
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].value!), consumedMessage.value)
                }
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testConsumeWithEventsSequence() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "events-sequence-test-group-id", topics: [self.uniqueTestTopic]),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.broker.addressFamily = .v4

        // The initializer only creates the client; it does not subscribe on its own.
        let consumerEvents = try KafkaConsumerStream(
            configuration: consumerConfig,
            logger: .kafkaTest
        )
        try consumerEvents.subscribe([self.uniqueTestTopic])

        // The events sequence is not a `Service`, so only the producer is run in the group.
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

            // Consumer Task: drain the single events sequence, extracting records from `.fetch` events.
            group.addTask {
                // The messages returned by `withMessages` point into the fetch event's memory,
                // which is freed when the closure returns, so copy the fields out immediately.
                var consumedMessages = [(topic: String, key: ByteBuffer?, value: ByteBuffer?)]()

                consumeLoop: for await event in consumerEvents {
                    switch event {
                    case var .fetch(fetch):
                        fetch.withMessages { message in
                            consumedMessages.append((message.topic, message.key, message.value))
                        }
                        if consumedMessages.count >= testMessages.count {
                            break consumeLoop
                        }
                    case .rebalance(let action):
                        // With `.rebalance` events enabled, librdkafka no longer assigns
                        // partitions automatically, so the caller must do it here (the default
                        // `range`/`roundrobin` assignors are eager).
                        switch action {
                        case let .assign(_, topics):
                            try await consumerEvents.assign(topics)
                        case .revoke, .error:
                            try await consumerEvents.unassignAll()
                        }
                    case .error(let error):
                        XCTFail("Unexpected error while consuming: \(error)")
                        break consumeLoop
                    default:
                        break // ignore .partitionEOF / health status / other events
                    }
                }

                XCTAssertEqual(testMessages.count, consumedMessages.count)

                for (index, consumedMessage) in consumedMessages.enumerated() {
                    XCTAssertEqual(testMessages[index].topic, consumedMessage.topic)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].key!), consumedMessage.key)
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].value!), consumedMessage.value)
                }
            }

            // Wait for Producer Task and Consumer Task to complete
            try await group.next()
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testRebalanceWithEventsSequence() async throws {
        // `uniqueTestTopic2` is already created in `setUpWithError`.
        let expectedTotal = Self.uniqueTestTopic2Partitions
        let topic = self.uniqueTestTopic2!
        let brokerAddress = self.bootstrapBrokerAddress!

        // Both consumers share a group so the partitions get split between them, and use
        // the cooperative-sticky assignor so rebalances are incremental.
        let groupID = UUID().uuidString
        func makeConfig() -> KafkaConsumerConfiguration {
            var config = KafkaConsumerConfiguration(
                consumptionStrategy: .group(id: groupID, topics: [topic]),
                bootstrapBrokerAddresses: [brokerAddress]
            )
            config.autoOffsetReset = .beginning
            config.broker.addressFamily = .v4
            config.pollInterval = .milliseconds(10)
            config.rebalanceStrategy = "cooperative-sticky"
            // Emit librdkafka debug logs (delivered via `.log` events and forwarded to the
            // `.kafkaTest` logger, which is at `.debug` level). Focused on the group/rebalance
            // machinery; use `[.all]` for everything.
            config.debugOptions = [.cgrp, .consumer, .broker]
            return config
        }

        // `ready`: consumer 1 owns all partitions (so consumer 2 only joins afterwards).
        // `consumer2HasPartitions`: consumer 2 received its share (so we can stop both).
        let (ready, readyContinuation) = AsyncStream<Void>.makeStream()
        let (consumer2HasPartitions, consumer2Continuation) = AsyncStream<Void>.makeStream()

        // Both consumers are retained in this outer scope for the whole test, so a task
        // returning does not deinit its client and trigger a spurious re-rebalance.
        // Creating the sequence does not join the group — only `subscribe` does.
        let consumer1Events = try KafkaConsumerStream(configuration: makeConfig(), logger: .kafkaTest)
        let consumer2Events = try KafkaConsumerStream(configuration: makeConfig(), logger: .kafkaTest)
        try consumer1Events.subscribe([topic])

        // Each task returns `(consumerId, finalPartitionCount)`.
        let counts = try await withThrowingTaskGroup(of: (Int, Int).self) { group in
            // Consumer 1: joins alone → assigned ALL partitions. Once consumer 2 joins,
            // cooperative-sticky incrementally revokes some of them. Consumer 1 must keep
            // polling to drive the rebalance to completion, so it does not stop on its own —
            // it runs until the group is cancelled (once consumer 2 has its partitions) and
            // reports whatever it holds at that point.
            group.addTask {
                var assigned = 0
                for await event in consumer1Events {
                    switch event {
                    case .rebalance(let action):
                        // The caller owns partition (un)assignment (cooperative assignor).
                        switch action {
                        case let .assign(_, topics):
                            try await consumer1Events.incrementalAssign(topics)
                            assigned += topics.reduce(into: 0) { partial, _ in partial += 1 }
                            if assigned == expectedTotal {
                                // Now owns everything — let consumer 2 join.
                                readyContinuation.yield()
                                readyContinuation.finish()
                            }
                        case let .revoke(_, topics):
                            try await consumer1Events.incrementalUnassign(topics)
                            assigned -= topics.reduce(into: 0) { partial, _ in partial += 1 }
                        case .error:
                            try await consumer1Events.unassignAll()
                        }
                    case .error(let error):
                        XCTFail("consumer1 unexpected error: \(error)")
                        return (1, assigned)
                    default:
                        break // ignore .partitionEOF (empty topic) and other events
                    }
                }
                return (1, assigned) // reached when the group is cancelled
            }

            // Consumer 2: starts only after consumer 1 owns all partitions, then joins and
            // is incrementally assigned a share. Signals once it holds some partitions, then
            // keeps polling (so consumer 1 stays balanced) until the group is cancelled.
            group.addTask {
                var iterator = ready.makeAsyncIterator()
                _ = await iterator.next()

                try consumer2Events.subscribe([topic])

                var assigned = 0
                var signaled = false
                for await event in consumer2Events {
                    switch event {
                    case .rebalance(let action):
                        switch action {
                        case let .assign(_, topics):
                            try await consumer2Events.incrementalAssign(topics)
                            assigned += topics.reduce(into: 0) { partial, _ in partial += 1 }
                            // Cooperative rebalance can deliver an empty initial assign before
                            // the partitions are freed; only signal once we actually own some.
                            if assigned > 0 && !signaled {
                                signaled = true
                                consumer2Continuation.yield()
                                consumer2Continuation.finish()
                            }
                        case let .revoke(_, topics):
                            try await consumer2Events.incrementalUnassign(topics)
                            assigned -= topics.reduce(into: 0) { partial, _ in partial += 1 }
                        case .error:
                            try await consumer2Events.unassignAll()
                        }
                    case .error(let error):
                        XCTFail("consumer2 unexpected error: \(error)")
                        return (2, assigned)
                    default:
                        break // ignore .partitionEOF (empty topic) and other events
                    }
                }
                return (2, assigned) // reached when the group is cancelled
            }

            // Once consumer 2 has its partitions the rebalance has settled; stop both
            // consumers (their sequences end on cancellation) and collect the final counts.
            var doneIterator = consumer2HasPartitions.makeAsyncIterator()
            _ = await doneIterator.next()
            group.cancelAll()

            var result = [Int: Int]()
            for try await (id, count) in group {
                result[id] = count
            }
            return result
        }

        let c1 = counts[1] ?? -1
        let c2 = counts[2] ?? -1
        // No even-split assumption: just verify every partition is still assigned exactly
        // once and that both consumers ended up with a share.
        XCTAssertGreaterThan(c1, 0, "consumer1 should keep some partitions after the rebalance")
        XCTAssertGreaterThan(c2, 0, "consumer2 should be assigned some partitions")
        XCTAssertEqual(c1 + c2, expectedTotal, "all partitions should remain assigned across the two consumers")
    }

    #if false
    func testTempNoConsumerGroupLeft() async throws {
        // resources-snapshots-dc-1

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .partitions(partitions: [
                .init(partition: .init(rawValue: 0), topic: "xxx-snapshots-dc-1", offset: .init(rawValue: 0))
            ]),
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        consumerConfig.autoOffsetReset = .beginning // Always read topics from beginning
        consumerConfig.broker.addressFamily = .v4
        consumerConfig.isAutoCommitEnabled = false
        consumerConfig.debugOptions = [.all]

        let logger = {
            var logger = Logger(label: "test")
            logger.logLevel = .info
            return logger
        } ()
        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: logger
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], gracefulShutdownSignals: [.sigint, .sigterm], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                logger.info("Service task started")
                defer {
                    logger.info("Service task exited")
                }
                try await serviceGroup.run()
            }

            // Consumer Task
            group.addTask {
                logger.info("Consumer task started")
                defer {
                    logger.info("Consumer task exited")
                }

                for try await message in consumer.messages {
                    logger.info("\(message)")
//                    consumer.triggerGracefulShutdown()
//                    return
                }
            }

            try await group.waitForAll()

        }

    }

    func testProduceAndConsumeWithAssignedTopicPartition() async throws {
        let testMessages = Self.createTestMessages(topic: self.uniqueTestTopic, count: 10)
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.producerConfig, logger: .kafkaTest)

        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .partitions(partitions: [
                .init(partition: .init(rawValue: 0), topic: self.uniqueTestTopic, offset: .init(rawValue: 0))
            ]),
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
            XCTAssertEqual(ByteBuffer(string: testMessages.first!.value!), consumedMessage!.value)

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
                    XCTAssertEqual(ByteBuffer(string: testMessages[index].value!), consumedMessage.value)
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
                    XCTAssertEqual(ByteBuffer(string: testMessages[firstConsumerOffset + index].value!), consumedMessage.value)
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
            XCTAssertEqual(ByteBuffer(string: message.value!), receivedMessage.value)

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

    func testDuplicatedMessagesOnRebalance() async throws {
        // `uniqueTestTopic2` is already created with 12 partitions in `setUpWithError`.
        let topic = self.uniqueTestTopic2!

        let numOfMessages: UInt = 1000
        let testMessages = Self.createTestMessages(topic: topic, count: numOfMessages)
        let (producer, acks) = try KafkaProducer.makeProducerWithEvents(configuration: producerConfig, logger: .kafkaTest)

        let producerServiceGroupConfiguration = ServiceGroupConfiguration(services: [producer], gracefulShutdownSignals: [.sigterm, .sigint], logger: .kafkaTest)
        let producerServiceGroup = ServiceGroup(configuration: producerServiceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await producerServiceGroup.run()
            }

            // Producer Task
            group.addTask {
                try await Self.sendAndAcknowledgeMessages(
                    producer: producer,
                    events: acks,
                    messages: testMessages,
                    skipConsistencyCheck: true
                )
            }

            // Wait for Producer Task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await producerServiceGroup.triggerGracefulShutdown()
        }

        // MARK: Consumer

        let uniqueGroupID = UUID().uuidString

        var consumer1Config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: uniqueGroupID,
                topics: [topic]
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
                topics: [topic]
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

        let serviceGroupConfiguration1 = ServiceGroupConfiguration(services: [consumer1], gracefulShutdownSignals: [.sigterm, .sigint], logger: .kafkaTest)
        let serviceGroup1 = ServiceGroup(configuration: serviceGroupConfiguration1)

        let serviceGroupConfiguration2 = ServiceGroupConfiguration(services: [consumer2], gracefulShutdownSignals: [.sigterm, .sigint], logger: .kafkaTest)
        let serviceGroup2 = ServiceGroup(configuration: serviceGroupConfiguration2)

        let sharedCtr = ManagedAtomic(0)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task for 1st consumer
            group.addTask {
                try await serviceGroup1.run()
            }
            // Run Task for 2nd consumer
            group.addTask {
                try await Task.sleep(for: .seconds(20)) // wait a bit that first consumer would form a queue
                try await serviceGroup2.run()
            }

            // First Consumer Task
            group.addTask {
                // 6 partitions
                for try await record in consumer1.messages {
                    sharedCtr.wrappingIncrement(ordering: .relaxed)

                    try consumer1.scheduleCommit(record) // commit time to time
                    try await Task.sleep(for: .milliseconds(100)) // don't read all messages before 2nd consumer
                }
            }

            // Second Consumer Task
            group.addTask {
                // 6 partitions
                for try await record in consumer2.messages {
                    sharedCtr.wrappingIncrement(ordering: .relaxed)

                    try consumer2.scheduleCommit(record) // commit time to time
                }
            }

            // Monitoring task
            group.addTask {
                while true {
                    let currentCtr = sharedCtr.load(ordering: .relaxed)
                    guard currentCtr >= numOfMessages else {
                        try await Task.sleep(for: .seconds(5)) // wait if new messages come here
                        continue
                    }
                    try await Task.sleep(for: .seconds(5)) // wait for extra messages
                    await serviceGroup1.triggerGracefulShutdown()
                    await serviceGroup2.triggerGracefulShutdown()
                    break
                }
            }

            try await group.next()
            try await group.next()
            try await group.next()

            // Wait for second Consumer Task to complete
            let totalCtr = sharedCtr.load(ordering: .relaxed)
            XCTAssertEqual(totalCtr, Int(numOfMessages))
        }
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
        return _createTestMessages(topic: topic, headers: headers, count: count)
    }

    private static func sendAndAcknowledgeMessages(
        producer: KafkaProducer,
        events: KafkaProducerEvents,
        messages: [KafkaProducerMessage<String, String>],
        skipConsistencyCheck: Bool = false
    ) async throws {
        return try await _sendAndAcknowledgeMessages(producer: producer, events: events, messages: messages, skipConsistencyCheck: skipConsistencyCheck)
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
