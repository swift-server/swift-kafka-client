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

/// State the consumer's event loop keeps between polls on behalf of ``KafkaConsumerMetrics``.
///
/// The event loop is the only place that serves statistics and rebalance events, so it owns this state
/// and no lock is needed.
struct KafkaConsumerMetricsState {
    /// The last statistics sample that decoded successfully. It identifies the per-partition and
    /// per-broker gauges that exist, so they can be marked "no data" or removed later.
    var lastStatistics: RDKafkaStatistics?
    /// The number of assignments received so far: one per completed rebalance.
    var rebalanceCount = 0
}

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

    /// The base labels of the per-broker latency windows this consumer emits.
    private var brokerWindowLabels: [String] {
        [self.brokerRTTLabel, self.brokerThrottleLabel]
    }

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

    /// Records the outcome of a commit librdkafka made on its own (`enable.auto.commit`).
    ///
    /// No duration is recorded: the application never awaits these commits, so there is no start time.
    func recordAutomaticCommit(succeeded: Bool) {
        if succeeded {
            self.commits.increment()
        } else {
            self.commitsFailed.increment()
        }
    }

    /// Sets the total number of rebalances this consumer completed (one per assignment received).
    ///
    /// librdkafka's `rebalance_cnt` is not used: it counts every assign and every revoke operation, so a
    /// single eager rebalance would count twice.
    func recordRebalances(total: Int) {
        self.rebalances.set(total)
    }

    /// Marks every gauge that derives from statistics as "no data" (`NaN`) after a statistics sample
    /// could not be decoded, instead of leaving the previous sample's values looking current.
    ///
    /// Cumulative totals are left untouched: `NaN` would break any rate computed over them.
    ///
    /// - Parameter last: The last successfully decoded sample, which identifies the partition and broker
    ///   gauges that exist. `nil` if there has been none.
    func recordStatisticsUnavailable(last: RDKafkaStatistics?) {
        self.queueOperations.record(Double.nan)
        self.lagMax.record(Double.nan)
        guard let last else { return }
        for (topicName, topic) in last.topics {
            for partition in (topic.partitions ?? [:]).values where partition.isOwnedByConsumer {
                self.lagGauge(topic: topicName, partition: partition.partition).record(Double.nan)
            }
        }
        KafkaMetricSupport.markWindowsUnavailable(
            baseLabels: self.brokerWindowLabels,
            clientDimension: self.clientDimension,
            brokers: Array(last.brokers.values)
        )
    }

    /// Removes every instrument from the metrics backend. Called when the consumer closes, so a closed
    /// consumer does not keep exporting its last values.
    ///
    /// - Parameter last: The last successfully decoded sample, which identifies the partition and broker
    ///   gauges that exist. `nil` if there has been none.
    func destroy(last: RDKafkaStatistics?) {
        self.lagMax.destroy()
        self.errors.destroy()
        self.rebalances.destroy()
        self.commits.destroy()
        self.commitsFailed.destroy()
        self.commitDuration.destroy()
        self.messagesReceived.destroy()
        self.bytesReceived.destroy()
        self.queueOperations.destroy()
        guard let last else { return }
        for (topicName, topic) in last.topics {
            for partition in (topic.partitions ?? [:]).values {
                self.lagGauge(topic: topicName, partition: partition.partition).destroy()
            }
        }
        KafkaMetricSupport.destroyWindows(
            baseLabels: self.brokerWindowLabels,
            clientDimension: self.clientDimension,
            brokers: Array(last.brokers.values)
        )
    }

    /// The lag gauge of one partition. The metrics backend returns the existing instrument for the same
    /// label and dimensions, so rebuilding it lets us update or `destroy()` it without keeping references.
    private func lagGauge(topic: String, partition: Int) -> Gauge {
        Gauge(
            label: self.lagLabel,
            dimensions: [
                self.clientDimension,
                (KafkaMetricLabels.topicDimension, topic),
                (KafkaMetricLabels.partitionDimension, "\(partition)"),
            ]
        )
    }

    func updateFromStatistics(_ stats: RDKafkaStatistics) {
        self.queueOperations.record(stats.queueOperations)

        // librdkafka reports cumulative totals, so they are set as-is rather than accumulated. Bytes are
        // message bytes (as in the Java client's `bytes-consumed-total`), not wire-level traffic.
        self.messagesReceived.set(stats.messagesReceivedTotal)
        self.bytesReceived.set(stats.messageBytesReceivedTotal)

        // Per-partition lag is only meaningful for partitions this consumer owns. librdkafka keeps
        // listing a revoked or lost partition with its frozen lag, so its gauge is destroyed instead
        // of being republished, and it is left out of the maximum.
        var maxLag: Int?
        for (topicName, topic) in stats.topics {
            for partition in (topic.partitions ?? [:]).values {
                if partition.isOwnedByConsumer {
                    let lag = partition.consumerLag
                    guard lag >= 0 else { continue }
                    self.lagGauge(topic: topicName, partition: partition.partition).record(lag)
                    maxLag = max(maxLag ?? lag, lag)
                }
            }
        }
        // With no lag to report there is no data, which must not read as "fully caught up" (0).
        self.lagMax.record(maxLag.map(Double.init) ?? Double.nan)

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

    /// Removes the lag gauge for partitions that are no longer owned by this consumer.
    func removePartitions(_ partitions: [KafkaTopicPartition]) {
        for partition in partitions {
            self.lagGauge(topic: partition.topic.description, partition: partition.partition.rawValue).destroy()
        }
    }
}
