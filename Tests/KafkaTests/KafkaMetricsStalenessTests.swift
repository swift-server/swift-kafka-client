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
import Foundation
import MetricsTestKit
import Testing

@testable import Kafka

// MARK: - Statistics JSON builder

/// Builds librdkafka-shaped statistics JSON so a test can describe exactly the partitions, brokers
/// and topics it needs, then decode it into `RDKafkaStatistics`.
enum StatsJSON {
    struct Partition {
        var topic = "orders"
        var partition: Int
        var fetchState: String
        var lag: Int
    }

    struct Broker {
        var name: String
        /// Number of samples in each latency window. `0` means an idle window.
        var windowCount: Int
    }

    struct BatchTopic {
        var name: String
        var batchCount: Int
        var avgBytes: Int
    }

    static func window(count: Int, avg: Int = 2_000, p99: Int = 3_000, max: Int = 4_000) -> String {
        let empty = count == 0
        return """
            { "min": 0, "max": \(empty ? 0 : max), "avg": \(empty ? 0 : avg), "sum": 0, "stddev": 0,
              "p50": 0, "p75": 0, "p90": 0, "p95": 0, "p99": \(empty ? 0 : p99), "p99_99": 0,
              "outofrange": 0, "hdrsize": 0, "cnt": \(count) }
            """
    }

    static func partition(_ partition: Partition) -> String {
        """
        "\(partition.partition)": {
          "partition": \(partition.partition), "broker": 1, "leader": 1, "desired": false, "unknown": false,
          "msgq_cnt": 0, "msgq_bytes": 0, "xmit_msgq_cnt": 0, "xmit_msgq_bytes": 0,
          "fetchq_cnt": 0, "fetchq_size": 0, "fetch_state": "\(partition.fetchState)",
          "query_offset": 0, "next_offset": 0, "app_offset": 0, "stored_offset": 0,
          "stored_leader_epoch": -1, "committed_offset": 0, "committed_leader_epoch": -1,
          "eof_offset": -1001, "lo_offset": 0, "hi_offset": 0, "ls_offset": 0,
          "consumer_lag": \(partition.lag), "consumer_lag_stored": \(partition.lag), "leader_epoch": -1,
          "txmsgs": 0, "txbytes": 0, "rxmsgs": 0, "rxbytes": 0, "msgs": 0, "rx_ver_drops": 0,
          "msgs_inflight": 0, "next_ack_seq": 0, "next_err_seq": 0, "acked_msgid": 0
        }
        """
    }

    static func broker(_ broker: Broker) -> String {
        let window = Self.window(count: broker.windowCount)
        return """
            "\(broker.name)": {
              "name": "\(broker.name)", "nodeid": 1, "nodename": "localhost:9092", "source": "learned",
              "state": "UP", "stateage": 0, "outbuf_cnt": 0, "outbuf_msg_cnt": 0, "waitresp_cnt": 0,
              "waitresp_msg_cnt": 0, "tx": 0, "txbytes": 0, "txerrs": 0, "txretries": 0, "txidle": 0,
              "req_timeouts": 0, "rx": 0, "rxbytes": 0, "rxerrs": 0, "rxcorriderrs": 0, "rxpartial": 0,
              "rxidle": 0, "zbuf_grow": 0, "wakeups": 0, "connects": 0, "disconnects": 0,
              "int_latency": \(window), "outbuf_latency": \(window), "rtt": \(window), "throttle": \(window)
            }
            """
    }

    static func topic(name: String, partitions: [Partition], batch: BatchTopic? = nil) -> String {
        let batchSize = Self.window(count: batch?.batchCount ?? 0, avg: batch?.avgBytes ?? 0)
        let batchCount = Self.window(count: batch?.batchCount ?? 0)
        return """
            "\(name)": {
              "topic": "\(name)", "age": 0, "metadata_age": 0,
              "batchsize": \(batchSize), "batchcnt": \(batchCount),
              "partitions": { \(partitions.map(Self.partition).joined(separator: ",")) }
            }
            """
    }

    static func make(
        type: String,
        partitions: [Partition] = [],
        brokers: [Broker] = [],
        batchTopics: [BatchTopic] = [],
        messageBytes: Int = 0,
        wireBytes: Int = 0,
        rebalanceCount: Int? = nil
    ) throws -> RDKafkaStatistics {
        var topics = Dictionary(grouping: partitions, by: \.topic).map {
            Self.topic(name: $0.key, partitions: $0.value)
        }
        topics += batchTopics.map { Self.topic(name: $0.name, partitions: [], batch: $0) }
        let cgrp =
            rebalanceCount.map {
                ", \"cgrp\": { \"state\": \"up\", \"stateage\": 0, \"join_state\": \"steady\", "
                    + "\"rebalance_age\": 0, \"rebalance_cnt\": \($0), \"assignment_size\": 0 }"
            } ?? ""
        let json = """
            {
              "name": "rdkafka#\(type)-1", "client_id": "rdkafka", "type": "\(type)",
              "replyq": 0, "msg_cnt": 0, "msg_size": 0, "msg_max": 0, "msg_size_max": 0,
              "metadata_cache_cnt": 1,
              "brokers": { \(brokers.map(Self.broker).joined(separator: ",")) },
              "topics": { \(topics.joined(separator: ",")) },
              "tx": 0, "tx_bytes": \(wireBytes), "rx": 0, "rx_bytes": \(wireBytes),
              "txmsgs": 0, "txmsg_bytes": \(messageBytes), "rxmsgs": 0, "rxmsg_bytes": \(messageBytes)
              \(cgrp)
            }
            """
        return try JSONDecoder().decode(RDKafkaStatistics.self, from: Data(json.utf8))
    }
}

// MARK: - Tests

/// Covers metrics that used to keep reporting stale or misleading values.
///
/// Each test binds its own `TestMetrics` with `withMetricsFactory(_:_:)` (see `KafkaMetricsTests`
/// for why) and drives `updateFromStatistics` directly, so no broker is needed.
@Suite
struct KafkaMetricsStalenessTests {
    private static let client = [("client_id", "test-client")]

    private static func lagDimensions(_ partition: Int, topic: String = "orders") -> [(String, String)] {
        Self.client + [("topic", topic), ("partition", "\(partition)")]
    }

    // MARK: Consumer lag

    @Test func lagGaugeIsRemovedWhenThePartitionIsReleased() throws {
        let metrics = TestMetrics()
        let owned = try StatsJSON.make(
            type: "consumer",
            partitions: [
                .init(partition: 0, fetchState: "active", lag: 10),
                .init(partition: 1, fetchState: "active", lag: 20),
            ]
        )
        // Partition 0 was revoked: librdkafka keeps listing it with its frozen lag, but the
        // fetch state is no longer active.
        let afterRevoke = try StatsJSON.make(
            type: "consumer",
            partitions: [
                .init(partition: 0, fetchState: "stopped", lag: 10),
                .init(partition: 1, fetchState: "active", lag: 5),
            ]
        )

        withMetricsFactory(metrics) {
            let consumerMetrics = KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client")
            consumerMetrics.updateFromStatistics(owned)
            consumerMetrics.updateFromStatistics(afterRevoke)
        }

        #expect(try metrics.expectGauge("kafka.consumer.lag", Self.lagDimensions(1)).lastValue == 5)
        #expect((try? metrics.expectGauge("kafka.consumer.lag", Self.lagDimensions(0))) == nil)
        // The released partition's frozen lag of 10 must not feed the maximum either.
        #expect(try metrics.expectGauge("kafka.consumer.lag.max", Self.client).lastValue == 5)
    }

    @Test func lagGaugeIsRemovedWhenThePartitionIsStopping() throws {
        let metrics = TestMetrics()
        let owned = try StatsJSON.make(
            type: "consumer",
            partitions: [.init(partition: 0, fetchState: "active", lag: 10)]
        )
        let stopping = try StatsJSON.make(
            type: "consumer",
            partitions: [.init(partition: 0, fetchState: "stopping", lag: 10)]
        )

        withMetricsFactory(metrics) {
            let consumerMetrics = KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client")
            consumerMetrics.updateFromStatistics(owned)
            consumerMetrics.updateFromStatistics(stopping)
        }

        #expect((try? metrics.expectGauge("kafka.consumer.lag", Self.lagDimensions(0))) == nil)
    }

    @Test func lagGaugeComesBackWhenThePartitionIsReassigned() throws {
        let metrics = TestMetrics()
        let states = ["active", "stopped", "active"]
        let samples = try states.enumerated().map { index, state in
            try StatsJSON.make(
                type: "consumer",
                partitions: [.init(partition: 0, fetchState: state, lag: 10 + index)]
            )
        }

        withMetricsFactory(metrics) {
            let consumerMetrics = KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client")
            for sample in samples {
                consumerMetrics.updateFromStatistics(sample)
            }
        }

        #expect(try metrics.expectGauge("kafka.consumer.lag", Self.lagDimensions(0)).lastValue == 12)
    }

    @Test func neverOwnedPartitionDoesNotCreateALagGauge() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(
            type: "consumer",
            partitions: [
                .init(partition: 0, fetchState: "active", lag: 3),
                .init(partition: 1, fetchState: "none", lag: -1),
            ]
        )

        withMetricsFactory(metrics) {
            KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client").updateFromStatistics(stats)
        }

        #expect(try metrics.expectGauge("kafka.consumer.lag", Self.lagDimensions(0)).lastValue == 3)
        #expect((try? metrics.expectGauge("kafka.consumer.lag", Self.lagDimensions(1))) == nil)
    }

    @Test func lagMaxIsNaNWhenNoOwnedPartitionHasALag() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(
            type: "consumer",
            partitions: [.init(partition: 0, fetchState: "stopped", lag: 10)]
        )

        withMetricsFactory(metrics) {
            KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client").updateFromStatistics(stats)
        }

        // "No data" must not read as "fully caught up" (0).
        let lagMax = try #require(try metrics.expectGauge("kafka.consumer.lag.max", Self.client).lastValue)
        #expect(lagMax.isNaN)
    }

    @Test func lagMaxIsNaNWhenAnOwnedPartitionHasNoCommittedOffsetYet() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(
            type: "consumer",
            partitions: [.init(partition: 0, fetchState: "active", lag: -1)]
        )

        withMetricsFactory(metrics) {
            KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client").updateFromStatistics(stats)
        }

        let lagMax = try #require(try metrics.expectGauge("kafka.consumer.lag.max", Self.client).lastValue)
        #expect(lagMax.isNaN)
    }

    // MARK: Broker latency windows

    @Test func emptyConsumerLatencyWindowRecordsNaN() throws {
        let metrics = TestMetrics()
        let busy = try StatsJSON.make(type: "consumer", brokers: [.init(name: "b1:9092/1", windowCount: 5)])
        let idle = try StatsJSON.make(type: "consumer", brokers: [.init(name: "b1:9092/1", windowCount: 0)])

        withMetricsFactory(metrics) {
            let consumerMetrics = KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client")
            consumerMetrics.updateFromStatistics(busy)
            consumerMetrics.updateFromStatistics(idle)
        }

        let broker = Self.client + [("broker", "b1:9092/1")]
        for suffix in ["avg", "p99", "max"] {
            let value = try #require(
                try metrics.expectGauge("kafka.consumer.broker.rtt.\(suffix).ms", broker).lastValue
            )
            #expect(value.isNaN, "rtt.\(suffix) should be NaN for an empty window, not the previous value")
        }
    }

    @Test func emptyProducerLatencyWindowRecordsNaN() throws {
        let metrics = TestMetrics()
        let busy = try StatsJSON.make(type: "producer", brokers: [.init(name: "b1:9092/1", windowCount: 5)])
        let idle = try StatsJSON.make(type: "producer", brokers: [.init(name: "b1:9092/1", windowCount: 0)])

        withMetricsFactory(metrics) {
            let producerMetrics = KafkaProducerMetrics(prefix: "kafka", clientID: "test-client")
            producerMetrics.updateFromStatistics(busy)
            producerMetrics.updateFromStatistics(idle)
        }

        let broker = Self.client + [("broker", "b1:9092/1")]
        for base in ["rtt", "throttle", "queue.latency", "request.latency"] {
            let value = try #require(
                try metrics.expectGauge("kafka.producer.broker.\(base).avg.ms", broker).lastValue
            )
            #expect(value.isNaN, "\(base) should be NaN for an empty window")
        }
    }

    @Test func neverMeasuredLatencyWindowStillRecordsNaN() throws {
        let metrics = TestMetrics()
        let idle = try StatsJSON.make(type: "consumer", brokers: [.init(name: "b1:9092/1", windowCount: 0)])

        withMetricsFactory(metrics) {
            KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client").updateFromStatistics(idle)
        }

        let broker = Self.client + [("broker", "b1:9092/1")]
        let value = try #require(try metrics.expectGauge("kafka.consumer.broker.rtt.avg.ms", broker).lastValue)
        #expect(value.isNaN)
    }

    // MARK: Byte counters

    @Test func consumerBytesReceivedUseMessageBytesNotWireBytes() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(type: "consumer", messageBytes: 1_000, wireBytes: 9_999)

        withMetricsFactory(metrics) {
            KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client").updateFromStatistics(stats)
        }

        #expect(try metrics.expectMeter("kafka.consumer.bytes.received", Self.client).lastValue == 1_000)
    }

    @Test func producerBytesSentUseMessageBytesNotWireBytes() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(type: "producer", messageBytes: 1_000, wireBytes: 9_999)

        withMetricsFactory(metrics) {
            KafkaProducerMetrics(prefix: "kafka", clientID: "test-client").updateFromStatistics(stats)
        }

        #expect(try metrics.expectMeter("kafka.producer.bytes.sent", Self.client).lastValue == 1_000)
    }

    // MARK: Producer batch size

    @Test func batchSizeAverageIgnoresIdleTopics() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(
            type: "producer",
            batchTopics: [
                .init(name: "busy", batchCount: 10, avgBytes: 1_000),
                .init(name: "idle", batchCount: 0, avgBytes: 0),
            ]
        )

        withMetricsFactory(metrics) {
            KafkaProducerMetrics(prefix: "kafka", clientID: "test-client").updateFromStatistics(stats)
        }

        // The idle topic must not drag the average down to 500.
        #expect(try metrics.expectGauge("kafka.producer.batch.size.avg", Self.client).lastValue == 1_000)
    }

    @Test func batchSizeAverageIsNaNWhenNoTopicSentABatch() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(
            type: "producer",
            batchTopics: [.init(name: "idle", batchCount: 0, avgBytes: 0)]
        )

        withMetricsFactory(metrics) {
            KafkaProducerMetrics(prefix: "kafka", clientID: "test-client").updateFromStatistics(stats)
        }

        let value = try #require(try metrics.expectGauge("kafka.producer.batch.size.avg", Self.client).lastValue)
        #expect(value.isNaN)
    }

    // MARK: Unavailable statistics

    @Test func consumerStatisticsUnavailableMarksGaugesAsNoData() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(
            type: "consumer",
            partitions: [.init(partition: 0, fetchState: "active", lag: 7)],
            brokers: [.init(name: "b1:9092/1", windowCount: 5)],
            messageBytes: 100
        )

        withMetricsFactory(metrics) {
            let consumerMetrics = KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client")
            consumerMetrics.updateFromStatistics(stats)
            consumerMetrics.recordStatisticsUnavailable(last: stats)
        }

        let broker = Self.client + [("broker", "b1:9092/1")]
        for gauge in [
            ("kafka.consumer.lag", Self.lagDimensions(0)),
            ("kafka.consumer.lag.max", Self.client),
            ("kafka.consumer.queue.operations", Self.client),
            ("kafka.consumer.broker.rtt.avg.ms", broker),
            ("kafka.consumer.broker.rtt.p99.ms", broker),
            ("kafka.consumer.broker.throttle.max.ms", broker),
        ] {
            let value = try #require(try metrics.expectGauge(gauge.0, gauge.1).lastValue)
            #expect(value.isNaN, "\(gauge.0) should be NaN while statistics are unavailable")
        }
        // Cumulative totals keep their last value: NaN would break any rate computed over them.
        #expect(try metrics.expectMeter("kafka.consumer.bytes.received", Self.client).lastValue == 100)
    }

    @Test func consumerStatisticsUnavailableWithoutAPreviousSampleOnlyTouchesHeldGauges() throws {
        let metrics = TestMetrics()

        withMetricsFactory(metrics) {
            KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client").recordStatisticsUnavailable(last: nil)
        }

        let lagMax = try #require(try metrics.expectGauge("kafka.consumer.lag.max", Self.client).lastValue)
        #expect(lagMax.isNaN)
        #expect(metrics.recorders.allSatisfy { $0.label != "kafka.consumer.lag" })
    }

    @Test func producerStatisticsUnavailableMarksGaugesAsNoData() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(
            type: "producer",
            brokers: [.init(name: "b1:9092/1", windowCount: 5)],
            batchTopics: [.init(name: "busy", batchCount: 3, avgBytes: 500)],
            messageBytes: 100
        )

        withMetricsFactory(metrics) {
            let producerMetrics = KafkaProducerMetrics(prefix: "kafka", clientID: "test-client")
            producerMetrics.updateFromStatistics(stats)
            producerMetrics.recordStatisticsUnavailable(last: stats)
        }

        let broker = Self.client + [("broker", "b1:9092/1")]
        for gauge in [
            ("kafka.producer.queue.messages", Self.client),
            ("kafka.producer.queue.bytes", Self.client),
            ("kafka.producer.batch.size.avg", Self.client),
            ("kafka.producer.broker.rtt.avg.ms", broker),
            ("kafka.producer.broker.queue.latency.max.ms", broker),
        ] {
            let value = try #require(try metrics.expectGauge(gauge.0, gauge.1).lastValue)
            #expect(value.isNaN, "\(gauge.0) should be NaN while statistics are unavailable")
        }
        #expect(try metrics.expectMeter("kafka.producer.bytes.sent", Self.client).lastValue == 100)
    }

    // MARK: Close

    @Test func destroyingConsumerMetricsRemovesEveryInstrument() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(
            type: "consumer",
            partitions: [
                .init(partition: 0, fetchState: "active", lag: 7),
                .init(partition: 1, fetchState: "stopped", lag: 9),
            ],
            brokers: [.init(name: "b1:9092/1", windowCount: 5)]
        )

        withMetricsFactory(metrics) {
            let consumerMetrics = KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client")
            consumerMetrics.updateFromStatistics(stats)
            consumerMetrics.recordRebalances(total: 1)
            consumerMetrics.recordError()
            consumerMetrics.recordCommit(duration: .milliseconds(5))
            consumerMetrics.recordCommitFailure()
            #expect(!metrics.recorders.isEmpty && !metrics.meters.isEmpty && !metrics.counters.isEmpty)

            consumerMetrics.destroy(last: stats)
        }

        #expect(metrics.recorders.isEmpty, "gauges left: \(metrics.recorders.map(\.label))")
        #expect(metrics.meters.isEmpty, "meters left: \(metrics.meters.map(\.label))")
        #expect(metrics.counters.isEmpty, "counters left: \(metrics.counters.map(\.label))")
        #expect(metrics.timers.isEmpty, "timers left: \(metrics.timers.map(\.label))")
    }

    @Test func destroyingProducerMetricsRemovesEveryInstrument() throws {
        let metrics = TestMetrics()
        let stats = try StatsJSON.make(
            type: "producer",
            brokers: [.init(name: "b1:9092/1", windowCount: 5)],
            batchTopics: [.init(name: "busy", batchCount: 3, avgBytes: 500)]
        )

        withMetricsFactory(metrics) {
            let producerMetrics = KafkaProducerMetrics(prefix: "kafka", clientID: "test-client")
            producerMetrics.updateFromStatistics(stats)
            producerMetrics.recordError()
            producerMetrics.recordSendError()
            producerMetrics.recordDeliverySuccess()
            producerMetrics.recordDeliveryFailure()
            producerMetrics.recordSend(duration: .milliseconds(5))
            #expect(!metrics.recorders.isEmpty && !metrics.meters.isEmpty && !metrics.counters.isEmpty)

            producerMetrics.destroy(last: stats)
        }

        #expect(metrics.recorders.isEmpty, "gauges left: \(metrics.recorders.map(\.label))")
        #expect(metrics.meters.isEmpty, "meters left: \(metrics.meters.map(\.label))")
        #expect(metrics.counters.isEmpty, "counters left: \(metrics.counters.map(\.label))")
        #expect(metrics.timers.isEmpty, "timers left: \(metrics.timers.map(\.label))")
    }

    // MARK: Rebalances and commits

    @Test func rebalancesComeFromTheSwiftCountNotTheStatistics() throws {
        let metrics = TestMetrics()
        // The statistics report `rebalance_cnt`, which counts every assign and every revoke.
        let stats = try StatsJSON.make(type: "consumer", rebalanceCount: 6)

        withMetricsFactory(metrics) {
            let consumerMetrics = KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client")
            consumerMetrics.updateFromStatistics(stats)
            consumerMetrics.recordRebalances(total: 3)
        }

        #expect(try metrics.expectMeter("kafka.consumer.rebalances", Self.client).lastValue == 3)
    }

    @Test func automaticCommitsAreCounted() throws {
        let metrics = TestMetrics()

        withMetricsFactory(metrics) {
            let consumerMetrics = KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client")
            consumerMetrics.recordAutomaticCommit(succeeded: true)
            consumerMetrics.recordAutomaticCommit(succeeded: true)
            consumerMetrics.recordAutomaticCommit(succeeded: false)
        }

        #expect(try metrics.expectCounter("kafka.consumer.commits.total", Self.client).totalValue == 2)
        #expect(try metrics.expectCounter("kafka.consumer.commits.failed", Self.client).totalValue == 1)
        // The commit duration is only known for commits the application awaits.
        #expect(try metrics.expectTimer("kafka.consumer.commit.duration", Self.client).values.isEmpty)
    }
}
