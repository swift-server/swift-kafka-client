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

import CoreMetrics
import Logging
import MetricsTestKit
import ServiceLifecycle
import Testing

import struct Foundation.UUID

@testable import Kafka

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Verifies the auto-register metrics model against an in-process mock broker.
///
/// - Important: Each test binds its own `TestMetrics` for the duration of client creation with
///   `withMetricsFactory(_:_:)` rather than bootstrapping the process-global `MetricsSystem`.
///   swift-testing runs suites in parallel, so a global factory swap would race with any other
///   metrics suite (for example, `KafkaMetricsIntegrationTests`); a task-local factory keeps each
///   test's instruments isolated. An instrument captures the active factory when it is created, so
///   wrapping only the `makeConsumer`/`makeProducer` call is sufficient — the eager instruments
///   keep recording into this test's `TestMetrics` from the run-loop task.
@Suite(.serialized)
struct KafkaMetricsTests {
    @Test func consumerMetricsAutoRegistered() async throws {
        let metrics = TestMetrics()
        let uniqueGroupID = UUID().uuidString
        var config = KafkaConsumerConfig()
        config.consumptionStrategy = .group(
            id: uniqueGroupID,
            topics: ["this-topic-does-not-exist"]
        )
        config.clientId = "metrics-test-consumer"
        config.metrics = .enabled(prefix: "kafka", updateInterval: .milliseconds(100))
        config.useMockBroker()
        config.brokerAddressFamily = .v4

        let (consumer, _, _) = try withMetricsFactory(metrics) {
            try KafkaConsumer.makeConsumer(config: config)
        }

        let svcGroupConfig = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: svcGroupConfig)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .seconds(1))

            await serviceGroup.triggerGracefulShutdown()
        }

        // The auto-register model emits under the configured prefix, without the
        // caller assigning any instruments.
        let value = try metrics.expectGauge(
            "kafka.consumer.queue.operations",
            [("client_id", "metrics-test-consumer")]
        ).lastValue
        #expect(value != nil)
    }

    @Test func producerMetricsAutoRegistered() async throws {
        let metrics = TestMetrics()
        var config = KafkaProducerConfig()
        config.useMockBroker()
        config.brokerAddressFamily = .v4
        config.metrics = .enabled(prefix: "kafka", updateInterval: .milliseconds(100))

        let (producer, _) = try withMetricsFactory(metrics) {
            try KafkaProducer.makeProducer(config: config)
        }

        let svcGroupConfig = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: svcGroupConfig)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .seconds(1))

            await serviceGroup.triggerGracefulShutdown()
        }

        // Without a configured `client.id`, the client is identified by librdkafka's handle name.
        let queueMessages = metrics.recorders.filter { $0.label == "kafka.producer.queue.messages" }
        #expect(queueMessages.count == 1)
        let clientID = queueMessages.first?.dimensions.first { $0.0 == "client_id" }?.1
        #expect(clientID?.hasPrefix("rdkafka#producer-") == true)
        #expect(queueMessages.first?.lastValue != nil)
    }

    @Test func metricsAreEnabledByDefault() {
        #expect(KafkaConsumerConfig().metrics.isEnabled)
        #expect(KafkaProducerConfig().metrics.isEnabled)
        #expect(KafkaConsumerConfig().metrics == .enabled())
        #expect(KafkaProducerConfig().metrics == .enabled())
        #expect(KafkaMetricsConfig.disabled.isEnabled == false)
    }
}
