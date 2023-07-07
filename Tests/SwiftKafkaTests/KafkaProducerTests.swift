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

import Logging
import NIOConcurrencyHelpers
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
        let (producer, acks) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.config, logger: .kafkaTest)

        let serviceGroup = ServiceGroup(
            services: [producer],
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

                let messageID = try producer.send(message)

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
        let (producer, acks) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.config, logger: .kafkaTest)

        let serviceGroup = ServiceGroup(
            services: [producer],
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

                let messageID = try producer.send(message)

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
        let (producer, acks) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.config, logger: .kafkaTest)

        let serviceGroup = ServiceGroup(
            services: [producer],
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

                messageIDs.insert(try producer.send(message1))
                messageIDs.insert(try producer.send(message2))

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

    func testProducerLog() async throws {
        let recorder = LogEventRecorder()
        let mockLogger = Logger(label: "kafka.test.producer.log") {
            _ in MockLogHandler(recorder: recorder)
        }

        // Set no bootstrap servers to trigger librdkafka configuration warning
        let config = KafkaProducerConfiguration(bootstrapServers: [])

        let producer = try KafkaProducer.makeProducer(config: config, logger: mockLogger)

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
        let (producer, acks) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.config, logger: .kafkaTest)

        let serviceGroup = ServiceGroup(
            services: [producer],
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
                    value: "Hello, Cupertino!"
                )
                let message2 = KafkaProducerMessage(
                    topic: "test-topic2",
                    key: "key2",
                    value: "Hello, San Diego!"
                )

                try producer.send(message1)

                // Terminate the acknowledgements sequence by deallocating its AsyncIterator
                var iter: KafkaMessageAcknowledgements.AsyncIterator? = acks.makeAsyncIterator()
                _ = iter
                iter = nil

                // Sending a new message should fail after the acknowledgements sequence
                // has been terminated
                XCTAssertThrowsError(try producer.send(message2)) { error in
                    let error = error as! KafkaError
                    XCTAssertEqual(KafkaError.ErrorCode.connectionClosed, error.code)
                }
            }

            // Wait for test task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testNoMemoryLeakAfterShutdown() async throws {
        var producer: KafkaProducer?
        var acks: KafkaMessageAcknowledgements?
        (producer, acks) = try KafkaProducer.makeProducerWithAcknowledgements(config: self.config, logger: .kafkaTest)
        _ = acks

        weak var producerCopy = producer

        await withThrowingTaskGroup(of: Void.self) { group in
            // Initialize serviceGroup here so it gets dereferenced when this closure is complete
            let serviceGroup = ServiceGroup(
                services: [producer!],
                configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
                logger: .kafkaTest
            )

            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            await serviceGroup.triggerGracefulShutdown()
        }

        producer = nil
        // Make sure to terminate the AsyncSequence
        acks = nil

        XCTAssertNil(producerCopy)
    }

    // MARK: - Mocks

    internal struct LogEvent {
        let level: Logger.Level
        let message: Logger.Message
        let source: String
    }

    internal struct LogEventRecorder {
        let _recordedEvents = NIOLockedValueBox<[LogEvent]>([])

        var recordedEvents: [LogEvent] {
            self._recordedEvents.withLockedValue { $0 }
        }

        func record(_ event: LogEvent) {
            self._recordedEvents.withLockedValue { $0.append(event) }
        }
    }

    internal struct MockLogHandler: LogHandler {
        let recorder: LogEventRecorder

        init(recorder: LogEventRecorder) {
            self.recorder = recorder
        }

        func log(
            level: Logger.Level,
            message: Logger.Message,
            metadata: Logger.Metadata?,
            source: String,
            file: String,
            function: String,
            line: UInt
        ) {
            self.recorder.record(LogEvent(level: level, message: message, source: source))
        }

        private var _logLevel: Logger.Level?
        var logLevel: Logger.Level {
            get {
                // get from config unless set
                return self._logLevel ?? .debug
            }
            set {
                self._logLevel = newValue
            }
        }

        private var _metadataSet = false
        private var _metadata = Logger.Metadata() {
            didSet {
                self._metadataSet = true
            }
        }

        public var metadata: Logger.Metadata {
            get {
                return self._metadata
            }
            set {
                self._metadata = newValue
            }
        }

        subscript(metadataKey metadataKey: Logger.Metadata.Key) -> Logger.Metadata.Value? {
            get {
                return self._metadata[metadataKey]
            }
            set {
                self._metadata[metadataKey] = newValue
            }
        }
    }
}
