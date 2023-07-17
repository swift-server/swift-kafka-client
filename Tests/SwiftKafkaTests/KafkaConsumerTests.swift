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
        var config = KafkaConsumerConfiguration(
            consumptionStrategy: .partition(.unassigned, topic: "some topic")
        )
        config.bootstrapServers = []
        config.sasl.mechanism = .gssapi // This should trigger a configuration error

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
        XCTAssertEqual(1, recordedEvents.count)

        let expectedMessage = """
        [thrd:app]: Configuration property `sasl.mechanism` set to `GSSAPI` but `security.protocol` \
        is not configured for SASL: recommend setting `security.protocol` to SASL_SSL or SASL_PLAINTEXT
        """
        let expectedLevel = Logger.Level.warning
        let expectedSource = "CONFWARN"

        let receivedEvent = try XCTUnwrap(recordedEvents.first, "Expected log event, but found none")
        XCTAssertEqual(expectedMessage, receivedEvent.message.description)
        XCTAssertEqual(expectedLevel, receivedEvent.level)
        XCTAssertEqual(expectedSource, receivedEvent.source)
    }
}
