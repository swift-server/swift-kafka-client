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

/// Holds all consumer metric instruments and provides thread-safe recording methods.
final class KafkaConsumerMetrics: Sendable {
    private let lagMax: Gauge
    private let lagLabel: String
    private let errors: Counter
    private let rebalances: Meter
    private let commits: Counter
    private let commitsFailed: Counter
    private let commitDuration: Timer
    private let messagesReceived: Meter
    private let bytesReceived: Meter
    private let queueOperations: Gauge
    // Base labels (already prefixed) for per-broker latency windows.
    private let brokerRTTLabel: String
    private let brokerThrottleLabel: String
    private let clientDimension: (String, String)

    /// - Parameters:
    ///   - prefix: Label prefix for every instrument.
    ///   - clientID: Value of the `client_id` dimension that identifies this client instance.
    init(prefix: String, clientID: String) {
        let clientDimension = (KafkaMetricLabels.clientIdDimension, clientID)
        let dimensions = [clientDimension]
        self.clientDimension = clientDimension
        self.lagMax = Gauge(label: "\(prefix).\(KafkaMetricLabels.consumerLagMax)", dimensions: dimensions)
        self.lagLabel = "\(prefix).\(KafkaMetricLabels.consumerLag)"
        self.errors = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerErrors)", dimensions: dimensions)
        self.rebalances = Meter(label: "\(prefix).\(KafkaMetricLabels.consumerRebalances)", dimensions: dimensions)
        self.commits = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerCommits)", dimensions: dimensions)
        self.commitsFailed = Counter(
            label: "\(prefix).\(KafkaMetricLabels.consumerCommitsFailed)",
            dimensions: dimensions
        )
        self.commitDuration = Timer(
            label: "\(prefix).\(KafkaMetricLabels.consumerCommitDuration)",
            dimensions: dimensions
        )
        self.messagesReceived = Meter(
            label: "\(prefix).\(KafkaMetricLabels.consumerMessagesReceived)",
            dimensions: dimensions
        )
        self.bytesReceived = Meter(
            label: "\(prefix).\(KafkaMetricLabels.consumerBytesReceived)",
            dimensions: dimensions
        )
        self.queueOperations = Gauge(
            label: "\(prefix).\(KafkaMetricLabels.consumerQueueOperations)",
            dimensions: dimensions
        )
        self.brokerRTTLabel = "\(prefix).\(KafkaMetricLabels.consumerBrokerRTT)"
        self.brokerThrottleLabel = "\(prefix).\(KafkaMetricLabels.consumerBrokerThrottle)"
    }

    func recordError() {
        self.errors.increment()
    }

    func recordCommit(duration: Duration) {
        self.commits.increment()
        self.commitDuration.record(duration: duration)
    }

    func recordCommitFailure() {
        self.commitsFailed.increment()
    }

    func updateFromStatistics(_ stats: RDKafkaStatistics) {
        self.queueOperations.record(stats.queueOperations)

        // librdkafka reports cumulative totals, so they are set as-is rather than accumulated.
        self.messagesReceived.set(stats.messagesReceivedTotal)
        self.bytesReceived.set(stats.bytesReceivedTotal)

        // Authoritative cgrp counter; counting assign/revoke events would double-count.
        if let rebalancesTotal = stats.consumerGroup?.rebalancesTotal {
            self.rebalances.set(rebalancesTotal)
        }

        var maxLag = 0
        for (topicName, topic) in stats.topics {
            for partition in (topic.partitions ?? [:]).values {
                let lag = partition.consumerLag
                guard lag >= 0 else { continue }
                Gauge(
                    label: self.lagLabel,
                    dimensions: [
                        self.clientDimension,
                        (KafkaMetricLabels.topicDimension, topicName),
                        (KafkaMetricLabels.partitionDimension, "\(partition.partition)"),
                    ]
                ).record(lag)
                maxLag = max(maxLag, lag)
            }
        }
        self.lagMax.record(maxLag)

        for broker in stats.brokers.values {
            KafkaMetricSupport.recordWindow(
                baseLabel: self.brokerRTTLabel,
                clientDimension: self.clientDimension,
                broker: broker.name,
                divisorToMilliseconds: 1000,
                broker.roundTripTime
            )
            KafkaMetricSupport.recordWindow(
                baseLabel: self.brokerThrottleLabel,
                clientDimension: self.clientDimension,
                broker: broker.name,
                divisorToMilliseconds: 1,
                broker.throttleTime
            )
        }
    }
}
