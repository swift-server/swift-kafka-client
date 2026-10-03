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
import Kafka
import MetricsTestKit
import ServiceLifecycle
import Testing

import struct Foundation.UUID

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

private let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
private let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!

/// End-to-end verification that the auto-register metrics record against a real broker.
///
/// The unit-level `KafkaMetricsTests` only prove that a stats-derived gauge appears; these
/// tests drive an actual produce → consume → commit flow so the imperative hooks (delivery
/// reports, manual commits) and the statistics-sampling path are exercised together.
///
/// - Important: Each test binds its own `TestMetrics` for the duration of client creation with
///   `withMetricsFactory(_:_:)` instead of bootstrapping the process-global `MetricsSystem`.
///   swift-testing runs suites in parallel, and the unit `KafkaMetricsTests` suite swaps the
///   global factory; binding a task-local factory keeps this suite's instruments isolated so the
///   two suites cannot clobber each other's recordings. An instrument captures the active factory
///   when it is created, so wrapping only the `makeProducer`/`makeConsumer` calls is sufficient —
///   the eager instruments keep recording into this test's `TestMetrics` from the run-loop task.
@Suite(.timeLimit(.minutes(5)), .serialized)
struct KafkaMetricsIntegrationTests {
    @Test func recordsDeliveryCommitAndStatisticsMetrics() async throws {
        let metrics = TestMetrics()

        try await withTestTopic { testTopic in
            let messageCount = 10

            // MARK: Producer phase — sendAndAwait acknowledges every message.

            var producerConfig = KafkaProducerConfig()
            producerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            producerConfig.brokerAddressFamily = .v4
            producerConfig.clientId = "metrics-integration-producer"
            producerConfig.metrics = .enabled(prefix: "kafka", updateInterval: .milliseconds(100))

            let (producer, producerEvents) = try withMetricsFactory(metrics) {
                try KafkaProducer.makeProducer(config: producerConfig)
            }
            let producerGroup = ServiceGroup(
                configuration: ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
            )

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask { try await producerGroup.run() }
                // Drain events so they don't buffer.
                group.addTask { for await _ in producerEvents {} }

                for i in 0..<messageCount {
                    let message = KafkaProducer.Message(
                        topic: testTopic,
                        key: "key-\(i)",
                        value: "value-\(i)"
                    )
                    let report = try await producer.sendAndAwait(message)
                    if case .failure(let error) = report.status {
                        Issue.record("Message \(i) failed to deliver: \(error)")
                    }
                }

                await producerGroup.triggerGracefulShutdown()
            }

            let producerClient = [("client_id", "metrics-integration-producer")]
            // Each acknowledged delivery report increments the imperative counter exactly once.
            let deliveries = try metrics.expectCounter("kafka.producer.delivery.success.total", producerClient)
            #expect(deliveries.totalValue >= Int64(messageCount))
            // Every acknowledged sendAndAwait records its end-to-end latency once.
            let sendDuration = try metrics.expectTimer("kafka.producer.send.duration", producerClient)
            #expect(sendDuration.values.count >= messageCount)
            // No delivery failed, so the failure counter — if registered — stayed at zero.
            if let failures = try? metrics.expectCounter("kafka.producer.delivery.failure.total", producerClient) {
                #expect(failures.totalValue == 0)
            }

            // MARK: Consumer phase — manual commit exercises the commit hook.

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(id: UUID().uuidString, topics: [testTopic])
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.autoOffsetReset = .beginning
            consumerConfig.enableAutoCommit = false
            consumerConfig.brokerAddressFamily = .v4
            consumerConfig.clientId = "metrics-integration-consumer"
            consumerConfig.metrics = .enabled(prefix: "kafka", updateInterval: .milliseconds(100))

            let (consumer, consumerMessages, _) = try withMetricsFactory(metrics) {
                try KafkaConsumer.makeConsumer(config: consumerConfig)
            }
            let consumerGroup = ServiceGroup(
                configuration: ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
            )

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask { try await consumerGroup.run() }

                group.addTask {
                    var consumed = 0
                    for try await message in consumerMessages {
                        try await consumer.commit(message)
                        consumed += 1
                        if consumed >= messageCount {
                            break
                        }
                    }
                    // Give the statistics callback (100 ms interval) time to sample at least once
                    // so the stats-derived instrument below is populated.
                    try await Task.sleep(for: .seconds(1))
                }

                try await group.next()
                await consumerGroup.triggerGracefulShutdown()
            }

            let consumerClient = [("client_id", "metrics-integration-consumer")]
            // Each successful manual commit increments the counter and records a duration.
            let commits = try metrics.expectCounter("kafka.consumer.commits.total", consumerClient)
            #expect(commits.totalValue >= 1)
            let commitDuration = try metrics.expectTimer("kafka.consumer.commit.duration", consumerClient)
            #expect(!commitDuration.values.isEmpty)

            // The statistics-sampling path ran against the real broker: `queue.operations` is
            // recorded on every statistics callback, so a value must have landed.
            let queueOperations = try metrics.expectGauge("kafka.consumer.queue.operations", consumerClient)
            #expect(queueOperations.lastValue != nil)
            // The same sample sets librdkafka's cumulative received-message total on the meter.
            let messagesReceived = try metrics.expectMeter("kafka.consumer.messages.received", consumerClient)
            #expect((messagesReceived.lastValue ?? 0) >= Double(messageCount))
        }
    }
}
