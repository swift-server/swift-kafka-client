//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2026 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging
import Metrics
import MetricsTestKit
import ServiceLifecycle
import Testing

import struct Foundation.UUID

@testable import CoreMetrics  // for MetricsSystem.bootstrapInternal
@testable import Kafka

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

@Suite(.serialized)
final class KafkaMetricsTests {
    var metrics: TestMetrics = TestMetrics()
    let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!

    init() async throws {
        MetricsSystem.bootstrapInternal(self.metrics)
    }

    deinit {
        MetricsSystem.bootstrapInternal(NOOPMetricsHandler.instance)
    }
    @Test func consumerStatistics() async throws {
        let uniqueGroupID = UUID().uuidString
        var config = KafkaConsumerConfig()
        config.consumptionStrategy = .group(
            id: uniqueGroupID,
            topics: ["this-topic-does-not-exist"]
        )
        config.metrics.updateInterval = .milliseconds(100)
        config.metrics.queuedOperation = .init(label: "operations")

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

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
        #expect(value != nil)
    }

    @Test func producerStatistics() async throws {
        let bootstrapBrokerAddress = KafkaConfiguration.BrokerAddress(
            host: self.kafkaHost,
            port: self.kafkaPort
        )

        var config = KafkaProducerConfiguration(
            bootstrapBrokerAddresses: [bootstrapBrokerAddress]
        )
        config.broker.addressFamily = .v4
        config.metrics.updateInterval = .milliseconds(100)
        config.metrics.queuedOperation = .init(label: "operations")

        let producer = try KafkaProducer(
            configuration: config,
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
        #expect(value != nil)
    }
}
