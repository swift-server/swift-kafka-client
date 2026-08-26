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

import Logging
import NIOCore
import ServiceLifecycle
import Testing

@testable import Kafka

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

@Suite struct KafkaProducerTests {
    var config: KafkaProducerConfig

    init() throws {
        self.config = KafkaProducerConfig()
        self.config.useMockBroker()
        self.config.brokerAddressFamily = .v4
    }

    @Test func send() async throws {
        let (producer, events) = try KafkaProducer.makeProducer(
            config: self.config
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            let expectedTopic: KafkaTopic = "test-topic"
            let headers = [KafkaHeader(key: "some", value: Array("test".utf8))]
            let message = KafkaProducer.Message(
                topic: expectedTopic,
                headers: headers,
                key: "key",
                value: "Hello, World!"
            )

            let messageID = try producer.send(message)

            var receivedDeliveryReports = Set<KafkaProducer.DeliveryReport>()

            for await event in events {
                switch event {
                case .deliveryReports(let deliveryReports):
                    for deliveryReport in deliveryReports {
                        receivedDeliveryReports.insert(deliveryReport)
                    }
                default:
                    break  // Ignore any other events
                }

                if receivedDeliveryReports.count >= 1 {
                    break
                }
            }

            let receivedDeliveryReport = try #require(receivedDeliveryReports.first)
            #expect(messageID == receivedDeliveryReport.id)

            guard case .acknowledged(let receivedMessage) = receivedDeliveryReport.status else {
                Issue.record()
                return
            }

            #expect(expectedTopic == receivedMessage.topic)
            #expect(Array(message.key!.utf8) == receivedMessage.key)
            #expect(Array(message.value.utf8) == receivedMessage.value)
            #expect(headers == receivedMessage.headers)

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    @Test func sendEmptyMessage() async throws {
        let (producer, events) = try KafkaProducer.makeProducer(
            config: self.config
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            let expectedTopic: KafkaTopic = "test-topic"
            let message = KafkaProducer.Message(
                topic: expectedTopic,
                value: ByteBuffer()
            )

            let messageID = try producer.send(message)

            var receivedDeliveryReports = Set<KafkaProducer.DeliveryReport>()

            for await event in events {
                switch event {
                case .deliveryReports(let deliveryReports):
                    for deliveryReport in deliveryReports {
                        receivedDeliveryReports.insert(deliveryReport)
                    }
                default:
                    break  // Ignore any other events
                }

                if receivedDeliveryReports.count >= 1 {
                    break
                }
            }

            let receivedDeliveryReport = try #require(receivedDeliveryReports.first)
            #expect(messageID == receivedDeliveryReport.id)

            guard case .acknowledged(let receivedMessage) = receivedDeliveryReport.status else {
                Issue.record()
                return
            }

            #expect(expectedTopic == receivedMessage.topic)
            #expect(Array(message.value.readableBytesView) == receivedMessage.value)

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    @Test func sendTwoTopics() async throws {
        let (producer, events) = try KafkaProducer.makeProducer(
            config: self.config
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            let message1 = KafkaProducer.Message(
                topic: "test-topic1",
                key: "key1",
                value: "Hello, Munich!"
            )
            let message2 = KafkaProducer.Message(
                topic: "test-topic2",
                key: "key2",
                value: "Hello, London!"
            )

            var messageIDs = Set<KafkaProducer.MessageID>()

            messageIDs.insert(try producer.send(message1))
            messageIDs.insert(try producer.send(message2))

            var receivedDeliveryReports = Set<KafkaProducer.DeliveryReport>()

            for await event in events {
                switch event {
                case .deliveryReports(let deliveryReports):
                    for deliveryReport in deliveryReports {
                        receivedDeliveryReports.insert(deliveryReport)
                    }
                default:
                    break  // Ignore any other events
                }

                if receivedDeliveryReports.count >= 2 {
                    break
                }
            }

            #expect(Set(receivedDeliveryReports.map(\.id)) == messageIDs)

            let acknowledgedMessages: [KafkaProducer.AcknowledgedMessage] = receivedDeliveryReports.compactMap {
                guard case .acknowledged(let receivedMessage) = $0.status else {
                    return nil
                }
                return receivedMessage
            }

            #expect(acknowledgedMessages.count == 2)
            #expect(acknowledgedMessages.contains(where: { $0.topic == message1.topic }))
            #expect(acknowledgedMessages.contains(where: { $0.topic == message2.topic }))
            #expect(acknowledgedMessages.contains(where: { $0.key == Array(message1.key!.utf8) }))
            #expect(acknowledgedMessages.contains(where: { $0.key == Array(message2.key!.utf8) }))
            #expect(acknowledgedMessages.contains(where: { $0.value == Array(message1.value.utf8) }))
            #expect(acknowledgedMessages.contains(where: { $0.value == Array(message2.value.utf8) }))

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    @Test func producerLog() async throws {
        let recorder = LogEventRecorder()
        let mockLogger = Logger(label: "kafka.test.producer.log") {
            _ in MockLogHandler(recorder: recorder)
        }

        // Set no bootstrap servers to trigger librdkafka configuration warning
        let config = KafkaProducerConfig()

        let (producer, _) = try withLogger(mockLogger) { _ in
            try KafkaProducer.makeProducer(config: config)
        }

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
        #expect(recordedEvents.count >= 1)

        let expectedMessage =
            "[thrd:app]: No `bootstrap.servers` configured: client will not be able to connect to Kafka cluster"
        let expectedLevel = Logger.Level.notice
        let expectedSource = "CONFWARN"

        let receivedEvent = try #require(
            recordedEvents.first(where: { $0.source == expectedSource }),
            "Expected CONFWARN log event, but found none"
        )
        #expect(expectedMessage == receivedEvent.message.description)
        #expect(expectedLevel == receivedEvent.level)
        #expect(expectedSource == receivedEvent.source)
    }

    @Test func sendSucceedsAfterTerminatingAcknowledgementSequence() async throws {
        let (producer, events) = try KafkaProducer.makeProducer(
            config: self.config
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            let message1 = KafkaProducer.Message(
                topic: "test-topic1",
                key: "key1",
                value: "Hello, Cupertino!"
            )
            let message2 = KafkaProducer.Message(
                topic: "test-topic2",
                key: "key2",
                value: "Hello, San Diego!"
            )

            try producer.send(message1)

            // Terminate the events sequence by deallocating its AsyncIterator
            var iterator: KafkaProducer.Events.AsyncIterator? = events.makeAsyncIterator()
            _ = iterator
            iterator = nil

            // Sending a new message should succeed even after the events sequence
            // has been terminated
            try producer.send(message2)

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    @Test func noMemoryLeakAfterShutdown() async throws {
        var producer: KafkaProducer?
        var events: KafkaProducer.Events?
        (producer, events) = try KafkaProducer.makeProducer(config: self.config)
        _ = events

        weak var producerCopy: KafkaProducer?
        producerCopy = producer

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

        #expect(producerCopy == nil)
    }

    @Test func producerConstructDeinit() async throws {
        let config = KafkaProducerConfig()

        // deinit called before run
        _ = try KafkaProducer.makeProducer(config: config)

        // deinit called before run
        _ = try KafkaProducer.makeProducer(config: config)
    }

    @Test func producerEventsReadCancelledBeforeRun() async throws {
        let config = KafkaProducerConfig()

        let (producer, events) = try KafkaProducer.makeProducer(config: config)

        let svcGroupConfig = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: svcGroupConfig)

        // explicitly run and cancel message consuming task before serviceGroup.run()
        let producerEventsTask = Task {
            for try await event in events {
                Issue.record("Unexpected record \(event))")
            }
        }

        try await Task.sleep(for: .seconds(1))

        // explicitly cancel message consuming task before serviceGroup.run()
        producerEventsTask.cancel()

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .seconds(1))

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    // MARK: - KafkaProducer.Event.error Tests

    @Test func producerEventErrorPatternMatch() {
        let error = KafkaError.config(reason: "Authentication failed")
        let event = KafkaProducer.Event.error(error)

        switch event {
        case .error(let e):
            #expect(e.description.contains("Authentication failed"))
        default:
            Issue.record("Expected .error event")
        }
    }
}
