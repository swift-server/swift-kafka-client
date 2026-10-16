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

                // The instruments are removed from the backend when the producer shuts down, so they
                // are checked while it is still running.
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

                await producerGroup.triggerGracefulShutdown()
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

                    // The instruments are removed from the backend when the consumer closes, so they
                    // are checked while it is still running.
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
                    // Bytes are message bytes (non-zero once messages were consumed).
                    let bytesReceived = try metrics.expectMeter("kafka.consumer.bytes.received", consumerClient)
                    #expect((bytesReceived.lastValue ?? 0) > 0)
                }

                try await group.next()
                await consumerGroup.triggerGracefulShutdown()
            }

            // A closed consumer must not keep exporting its last values.
            #expect(
                metrics.recorders.filter {
                    $0.dimensions.contains { $0 == ("client_id", "metrics-integration-consumer") }
                }
                .isEmpty
            )
        }
    }

    @Test func countsAutomaticCommitsAndRebalances() async throws {
        let metrics = TestMetrics()

        try await withTestTopic { testTopic in
            let messageCount = 5
            try await produce(messageCount, to: testTopic)

            var consumerConfig = KafkaConsumerConfig()
            consumerConfig.consumptionStrategy = .group(id: UUID().uuidString, topics: [testTopic])
            consumerConfig.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
            consumerConfig.autoOffsetReset = .beginning
            // Auto-commit is on (the default): the application never commits, librdkafka does.
            consumerConfig.autoCommitIntervalMs = 200
            consumerConfig.brokerAddressFamily = .v4
            consumerConfig.clientId = "metrics-integration-autocommit"
            consumerConfig.metrics = .enabled(prefix: "kafka", updateInterval: .milliseconds(100))

            let (consumer, messages, _) = try withMetricsFactory(metrics) {
                try KafkaConsumer.makeConsumer(config: consumerConfig)
            }
            let serviceGroup = ServiceGroup(
                configuration: ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
            )

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask { try await serviceGroup.run() }
                group.addTask {
                    var consumed = 0
                    for try await _ in messages {
                        consumed += 1
                        if consumed >= messageCount { break }
                    }
                    // Several commit intervals, including idle ones with nothing left to commit.
                    try await Task.sleep(for: .seconds(2))

                    let client = [("client_id", "metrics-integration-autocommit")]
                    let commits = try metrics.expectCounter("kafka.consumer.commits.total", client)
                    #expect(commits.totalValue >= 1, "automatic commits should be counted")
                    // Idle intervals report "no offset to commit"; that is not a failed commit.
                    let failed = try metrics.expectCounter("kafka.consumer.commits.failed", client)
                    #expect(failed.totalValue == 0)
                    // Nobody awaits an automatic commit, so there is no duration to record.
                    #expect(try metrics.expectTimer("kafka.consumer.commit.duration", client).values.isEmpty)
                    // One assignment is one rebalance (librdkafka's own counter would say 1 assign + more).
                    #expect(try metrics.expectMeter("kafka.consumer.rebalances", client).lastValue == 1)
                }

                try await group.next()
                await serviceGroup.triggerGracefulShutdown()
            }
        }
    }

    @Test func removesTheLagGaugeOfAPartitionThatMovedToAnotherConsumer() async throws {
        let metrics = TestMetrics()

        try await withTestTopic(partitions: 2) { testTopic in
            try await produce(10, to: testTopic, partitions: 2)
            let groupID = UUID().uuidString

            func makeConsumer(clientID: String) throws -> (KafkaConsumer, KafkaConsumer.Messages) {
                var config = KafkaConsumerConfig()
                config.consumptionStrategy = .group(id: groupID, topics: [testTopic])
                config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
                config.autoOffsetReset = .beginning
                config.autoCommitIntervalMs = 200
                config.brokerAddressFamily = .v4
                config.clientId = clientID
                config.metrics = .enabled(prefix: "kafka", updateInterval: .milliseconds(100))
                let (consumer, messages, _) = try withMetricsFactory(metrics) {
                    try KafkaConsumer.makeConsumer(config: config)
                }
                return (consumer, messages)
            }

            func lagPartitions(of clientID: String) -> Set<String> {
                Set(
                    metrics.recorders
                        .filter {
                            $0.label == "kafka.consumer.lag" && $0.dimensions.contains { $0 == ("client_id", clientID) }
                        }
                        .compactMap { $0.dimensions.first { $0.0 == "partition" }?.1 }
                )
            }

            func waitUntil(_ description: String, _ condition: () -> Bool) async throws {
                let deadline = ContinuousClock.now + .seconds(60)
                while !condition() {
                    if ContinuousClock.now > deadline {
                        Issue.record("Timed out waiting for: \(description)")
                        return
                    }
                    try await Task.sleep(for: .milliseconds(200))
                }
            }

            let (first, firstMessages) = try makeConsumer(clientID: "metrics-integration-lag-a")
            let (second, secondMessages) = try makeConsumer(clientID: "metrics-integration-lag-b")
            let firstGroup = ServiceGroup(configuration: .init(services: [first], logger: .kafkaTest))
            let secondGroup = ServiceGroup(configuration: .init(services: [second], logger: .kafkaTest))

            // The per-partition lag gauges are created on the consumers' run-loop tasks, not during
            // `makeConsumer`, so the task-local factory has to be active while the consumers run.
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask { try await withMetricsFactory(metrics) { try await firstGroup.run() } }
                group.addTask { for try await _ in firstMessages {} }

                // Alone in the group, the first consumer owns both partitions and reports both lags.
                try await waitUntil("the first consumer reports lag for both partitions") {
                    lagPartitions(of: "metrics-integration-lag-a") == ["0", "1"]
                }

                // The second consumer joins: one partition moves away from the first consumer.
                group.addTask { try await withMetricsFactory(metrics) { try await secondGroup.run() } }
                group.addTask { for try await _ in secondMessages {} }

                try await waitUntil("the first consumer drops the lag gauge of the partition it lost") {
                    lagPartitions(of: "metrics-integration-lag-a").count == 1
                        && lagPartitions(of: "metrics-integration-lag-b").count == 1
                }
                // The two consumers own different partitions, so each partition is reported once.
                let owned = lagPartitions(of: "metrics-integration-lag-a")
                    .union(lagPartitions(of: "metrics-integration-lag-b"))
                #expect(owned == ["0", "1"])

                await secondGroup.triggerGracefulShutdown()
                await firstGroup.triggerGracefulShutdown()
            }
        }
    }

    // MARK: Helpers

    /// Produces `count` messages without metrics, optionally spread over `partitions` partitions.
    private func produce(_ count: Int, to topic: KafkaTopic, partitions: Int = 1) async throws {
        var config = KafkaProducerConfig()
        config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
        config.brokerAddressFamily = .v4
        config.metrics = .disabled
        let (producer, events) = try KafkaProducer.makeProducer(config: config)
        let serviceGroup = ServiceGroup(
            configuration: ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        )
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await serviceGroup.run() }
            group.addTask { for await _ in events {} }
            for index in 0..<count {
                let message = KafkaProducer.Message(
                    topic: topic,
                    partition: KafkaPartition(rawValue: index % partitions),
                    key: "key-\(index)",
                    value: "value-\(index)"
                )
                _ = try await producer.sendAndAwait(message)
            }
            await serviceGroup.triggerGracefulShutdown()
        }
    }
}
