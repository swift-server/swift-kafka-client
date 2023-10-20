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

@testable import CoreMetrics // for MetricsSystem.bootstrapInternal
@testable import Kafka
import Logging
import Metrics
import MetricsTestKit
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

final class KafkaProducerTests: XCTestCase {
    // Read environment variables to get information about the test Kafka server
    let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!
    var bootstrapBrokerAddress: KafkaConfiguration.BrokerAddress!
    var config: KafkaProducerConfiguration!
    var metrics: TestMetrics! = TestMetrics()

    override func setUpWithError() throws {
        self.bootstrapBrokerAddress = KafkaConfiguration.BrokerAddress(
            host: self.kafkaHost,
            port: self.kafkaPort
        )

        self.config = KafkaProducerConfiguration(
            bootstrapBrokerAddresses: [self.bootstrapBrokerAddress]
        )
        self.config.broker.addressFamily = .v4

        MetricsSystem.bootstrapInternal(self.metrics)
    }

    override func tearDownWithError() throws {
        self.bootstrapBrokerAddress = nil
        self.config = nil

        self.metrics = nil
        MetricsSystem.bootstrapInternal(NOOPMetricsHandler.instance)
    }

    func testSend() async throws {
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            let expectedTopic = "test-topic"
            let message = KafkaProducerMessage(
                topic: expectedTopic,
                key: "key",
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
            XCTAssertEqual(ByteBuffer(string: message.key!), receivedMessage.key)
            XCTAssertEqual(ByteBuffer(string: message.value), receivedMessage.value)

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testSendEmptyMessage() async throws {
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            let expectedTopic = "test-topic"
            let message = KafkaProducerMessage(
                topic: expectedTopic,
                value: ByteBuffer()
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
            XCTAssertEqual(message.value, receivedMessage.value)

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testSendTwoTopics() async throws {
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

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

            messageIDs.insert(try producer.send(message1))
            messageIDs.insert(try producer.send(message2))

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

                if receivedDeliveryReports.count >= 2 {
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

            XCTAssertEqual(2, acknowledgedMessages.count)
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.topic == message1.topic }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.topic == message2.topic }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.key == ByteBuffer(string: message1.key!) }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.key == ByteBuffer(string: message2.key!) }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.value == ByteBuffer(string: message1.value) }))
            XCTAssertTrue(acknowledgedMessages.contains(where: { $0.value == ByteBuffer(string: message2.value) }))

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testProducerLog() async throws {
        let recorder = LogEventRecorder()
        let mockLogger = Logger(label: "kafka.test.producer.log") {
            _ in MockLogHandler(recorder: recorder)
        }

        // Set no bootstrap servers to trigger librdkafka configuration warning
        let config = KafkaProducerConfiguration(bootstrapBrokerAddresses: [])

        let producer = try KafkaProducer(configuration: config, logger: mockLogger)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Sleep for 1s to let poll loop receive log message
            try! await Task.sleep(for: .seconds(1))

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }

        let recordedEvents = recorder.recordedEvents
        XCTAssertEqual(1, recordedEvents.count)

        let expectedMessage = "[thrd:app]: No `bootstrap.servers` configured: client will not be able to connect to Kafka cluster"
        let expectedLevel = Logger.Level.notice
        let expectedSource = "CONFWARN"

        let receivedEvent = try XCTUnwrap(recordedEvents.first, "Expected log event, but found none")
        XCTAssertEqual(expectedMessage, receivedEvent.message.description)
        XCTAssertEqual(expectedLevel, receivedEvent.level)
        XCTAssertEqual(expectedSource, receivedEvent.source)
    }

    func testSendFailsAfterTerminatingAcknowledgementSequence() async throws {
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.config, logger: .kafkaTest)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            let message1 = KafkaProducerMessage(
                topic: "test-topic1",
                key: "key1",
                value: "Hello, Cupertino!"
            )
            let message2 = KafkaProducerMessage(
                topic: "test-topic2",
                key: "key2",
                value: "Hello, San Diego!"
            )

            try producer.send(message1)

            // Terminate the events sequence by deallocating its AsyncIterator
            var iterator: KafkaProducerEvents.AsyncIterator? = events.makeAsyncIterator()
            _ = iterator
            iterator = nil

            // Sending a new message should fail after the events sequence
            // has been terminated
            XCTAssertThrowsError(try producer.send(message2)) { error in
                let error = error as! KafkaError
                XCTAssertEqual(KafkaError.ErrorCode.shutdown, error.code)
            }

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testNoMemoryLeakAfterShutdown() async throws {
        var producer: KafkaProducer?
        var events: KafkaProducerEvents?
        (producer, events) = try KafkaProducer.makeProducerWithEvents(configuration: self.config, logger: .kafkaTest)
        _ = events

        weak var producerCopy = producer

        await withThrowingTaskGroup(of: Void.self) { group in
            // Initialize serviceGroup here so it gets dereferenced when this closure is complete
            let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer!], logger: .kafkaTest)
            let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            await serviceGroup.triggerGracefulShutdown()
        }

        producer = nil
        // Make sure to terminate the AsyncSequence
        events = nil

        XCTAssertNil(producerCopy)
    }

    func testProducerStatistics() async throws {
        self.config.metrics.updateInterval = .milliseconds(100)
        self.config.metrics.queuedOperation = .init(label: "operations")

        let producer = try KafkaProducer(
            configuration: self.config,
            logger: .kafkaTest
        )

        let svcGroupConfig = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: svcGroupConfig)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .seconds(1))

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }

        let value = try metrics.expectGauge("operations").lastValue
        XCTAssertNotNil(value)
    }
}
