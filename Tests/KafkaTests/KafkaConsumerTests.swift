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
import struct Foundation.UUID
@testable import Kafka
import Logging
import Metrics
import MetricsTestKit
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

final class KafkaConsumerTests: XCTestCase {
    var metrics: TestMetrics! = TestMetrics()

    override func setUp() async throws {
        MetricsSystem.bootstrapInternal(self.metrics)
    }

    override func tearDown() async throws {
        self.metrics = nil
        MetricsSystem.bootstrapInternal(NOOPMetricsHandler.instance)
    }

    func testConsumerLog() async throws {
        let recorder = LogEventRecorder()
        let mockLogger = Logger(label: "kafka.test.consumer.log") {
            _ in MockLogHandler(recorder: recorder)
        }

        // Set no bootstrap servers to trigger librdkafka configuration warning
        let uniqueGroupID = UUID().uuidString
        var config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: uniqueGroupID, topics: ["this-topic-does-not-exist"]),
            bootstrapBrokerAddresses: []
        )
        config.securityProtocol = .plaintext
        config.debugOptions = [.all]

        let consumer = try KafkaConsumer(configuration: config, logger: mockLogger)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
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
        let expectedLogs: [(level: Logger.Level, source: String, message: String)] = [
            (Logger.Level.debug, "MEMBERID", uniqueGroupID),
        ]

        for expectedLog in expectedLogs {
            XCTAssertTrue(
                recordedEvents.contains(where: { event in
                    event.level == expectedLog.level &&
                        event.source == expectedLog.source &&
                        event.message.description.contains(expectedLog.message)
                }),
                "Expected log \(expectedLog) but was not found"
            )
        }
    }

    func testConsumerStatistics() async throws {
        let uniqueGroupID = UUID().uuidString
        var config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: uniqueGroupID, topics: ["this-topic-does-not-exist"]),
            bootstrapBrokerAddresses: []
        )

        config.metrics.updateInterval = .milliseconds(100)
        config.metrics.queuedOperation = .init(label: "operations")

        let consumer = try KafkaConsumer(configuration: config, logger: .kafkaTest)

        let svcGroupConfig = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
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
