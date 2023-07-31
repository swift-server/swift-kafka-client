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

import struct Foundation.UUID
import Logging
import NIOConcurrencyHelpers
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

final class KafkaConsumerTests: XCTestCase {
    func testConsumerLog() async throws {
        let recorder = LogEventRecorder()
        let mockLogger = Logger(label: "kafka.test.consumer.log") {
            _ in MockLogHandler(recorder: recorder)
        }

        // Set no bootstrap servers to trigger librdkafka configuration warning
        let uniqueGroupID = UUID().uuidString
        var config = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: uniqueGroupID, topics: ["this-topic-does-not-exist"])
        )
        config.bootstrapServers = []
        config.securityProtocol = .plaintext
        config.debug = [.all]

        let consumer = try KafkaConsumer(config: config, logger: mockLogger)

        let serviceGroup = ServiceGroup(
            services: [consumer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

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
            consumptionStrategy: .group(id: uniqueGroupID, topics: ["this-topic-does-not-exist"])
        )
        config.statisticsInterval = Duration.milliseconds(10)

        let statistics = NIOLockedValueBox<KafkaStatistics?>(nil)
        let (consumer, events) = try KafkaConsumer.makeConsumerWithEvents(config: config, logger: .kafkaTest)

        let serviceGroup = ServiceGroup(
            services: [consumer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: .kafkaTest
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // check for librdkafka statistics
            group.addTask {
                for try await event in events {
                    if case let .statistics(stat) = event {
                        statistics.withLockedValue {
                            $0 = stat
                        }
                        break
                    }
                }
            }

            try await group.next()

            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }

        let stats = statistics.withLockedValue { $0 }
        guard let stats else {
            XCTFail("stats are not occurred")
            return
        }
        XCTAssertFalse(stats.jsonString.isEmpty)
        XCTAssertNoThrow(try stats.json)
    }
}
