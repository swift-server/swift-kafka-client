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

import Metrics

/// Metric instrument label suffixes (combined with the configured prefix) and dimension keys.
enum KafkaMetricLabels {
    // Consumer
    static let consumerLagMax = "consumer.lag.max"
    static let consumerLag = "consumer.lag"
    static let consumerErrors = "consumer.errors.total"
    static let consumerRebalances = "consumer.rebalances"
    static let consumerCommits = "consumer.commits.total"
    static let consumerCommitsFailed = "consumer.commits.failed"
    static let consumerCommitDuration = "consumer.commit.duration"
    static let consumerMessagesReceived = "consumer.messages.received"
    static let consumerBytesReceived = "consumer.bytes.received"
    static let consumerQueueOperations = "consumer.queue.operations"
    // Consumer per-broker latency windows (base names).
    static let consumerBrokerRTT = "consumer.broker.rtt"
    static let consumerBrokerThrottle = "consumer.broker.throttle"

    // Producer
    static let producerSendErrors = "producer.send.errors.total"
    static let producerSendDuration = "producer.send.duration"
    static let producerDeliverySuccess = "producer.delivery.success.total"
    static let producerDeliveryFailure = "producer.delivery.failure.total"
    static let producerErrors = "producer.errors.total"
    static let producerMessagesSent = "producer.messages.sent"
    static let producerBytesSent = "producer.bytes.sent"
    static let producerQueueMessages = "producer.queue.messages"
    static let producerQueueBytes = "producer.queue.bytes"
    static let producerBatchSizeAvg = "producer.batch.size.avg"
    // Producer per-broker latency windows (base names).
    static let producerBrokerRTT = "producer.broker.rtt"
    static let producerBrokerThrottle = "producer.broker.throttle"
    static let producerBrokerQueueLatency = "producer.broker.queue.latency"
    static let producerBrokerRequestLatency = "producer.broker.request.latency"

    // Latency-window sub-metric suffixes.
    static let windowAvg = "avg"
    static let windowP99 = "p99"
    static let windowMax = "max"
    /// Unit suffix for per-broker latency windows (values are recorded in milliseconds).
    static let unitMilliseconds = "ms"

    // Dimensions
    /// Identifies the client instance: the configured `client.id`, else librdkafka's unique handle name.
    static let clientIdDimension = "client_id"
    static let topicDimension = "topic"
    static let partitionDimension = "partition"
    static let brokerDimension = "broker"
}

/// Shared helpers for emitting librdkafka statistics into swift-metrics instruments.
enum KafkaMetricSupport {
    /// Emits a pre-aggregated latency window as `<baseLabel>.{avg,p99,max}.ms` gauges (milliseconds),
    /// dimensioned by client and broker. Skips windows with no measurements (`count == 0`).
    ///
    /// - Parameter divisorToMilliseconds: `1000` for microsecond sources, `1` for `throttle` (already ms).
    static func recordWindow(
        baseLabel: String,
        clientDimension: (String, String),
        broker: String,
        divisorToMilliseconds: Double,
        _ window: RDKafkaStatistics.WindowStats
    ) {
        guard window.count > 0 else { return }
        let dimensions = [clientDimension, (KafkaMetricLabels.brokerDimension, broker)]
        let unit = KafkaMetricLabels.unitMilliseconds
        Gauge(label: "\(baseLabel).\(KafkaMetricLabels.windowAvg).\(unit)", dimensions: dimensions)
            .record(Double(window.avg) / divisorToMilliseconds)
        Gauge(label: "\(baseLabel).\(KafkaMetricLabels.windowP99).\(unit)", dimensions: dimensions)
            .record(Double(window.p99) / divisorToMilliseconds)
        Gauge(label: "\(baseLabel).\(KafkaMetricLabels.windowMax).\(unit)", dimensions: dimensions)
            .record(Double(window.max) / divisorToMilliseconds)
    }
}
