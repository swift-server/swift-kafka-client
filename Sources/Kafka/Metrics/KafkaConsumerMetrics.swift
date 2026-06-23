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
import NIOConcurrencyHelpers

/// Holds all consumer metric instruments and provides thread-safe recording methods.
final class KafkaConsumerMetrics: Sendable {
    private let lagMax: Gauge
    private let lagLabel: String
    private let errors: Counter
    private let rebalances: Counter
    private let commits: Counter
    private let commitsFailed: Counter
    private let commitDuration: Timer
    private let messagesReceived: Counter
    private let bytesReceived: Counter
    private let queueOperations: Gauge

    private let previousMessagesReceived: NIOLockedValueBox<Int>
    private let previousBytesReceived: NIOLockedValueBox<Int>

    init(prefix: String) {
        self.lagMax = Gauge(label: "\(prefix).\(KafkaMetricLabels.consumerLagMax)")
        self.lagLabel = "\(prefix).\(KafkaMetricLabels.consumerLag)"
        self.errors = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerErrors)")
        self.rebalances = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerRebalances)")
        self.commits = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerCommits)")
        self.commitsFailed = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerCommitsFailed)")
        self.commitDuration = Timer(label: "\(prefix).\(KafkaMetricLabels.consumerCommitDuration)")
        self.messagesReceived = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerMessagesReceived)")
        self.bytesReceived = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerBytesReceived)")
        self.queueOperations = Gauge(label: "\(prefix).\(KafkaMetricLabels.consumerQueueOperations)")
        self.previousMessagesReceived = NIOLockedValueBox(0)
        self.previousBytesReceived = NIOLockedValueBox(0)
    }

    func recordError() {
        self.errors.increment()
    }

    func recordRebalance() {
        self.rebalances.increment()
    }

    @available(macOS 13, iOS 16, tvOS 16, watchOS 9, *)
    func recordCommit(duration: Duration) {
        self.commits.increment()
        self.commitDuration.record(duration: duration)
    }

    func recordCommitFailure() {
        self.commitsFailed.increment()
    }

    func updateFromStatistics(_ stats: RDKafkaStatistics) {
        if let replyq = stats.queuedOperation {
            self.queueOperations.record(replyq)
        }

        if let current = stats.totalKafkaBrokerMessagesReceived {
            let delta = self.previousMessagesReceived.withLockedValue { prev in
                let d = current - prev
                prev = current
                return d
            }
            if delta > 0 { self.messagesReceived.increment(by: delta) }
        }

        if let current = stats.totalKafkaBrokerResponsesSize {
            let delta = self.previousBytesReceived.withLockedValue { prev in
                let d = current - prev
                prev = current
                return d
            }
            if delta > 0 { self.bytesReceived.increment(by: delta) }
        }

        var maxLag: Int = 0
        for (_, topic) in stats.topics ?? [:] {
            for (_, partition) in topic.partitions ?? [:] {
                if let lag = partition.consumerLag, lag >= 0 {
                    Gauge(
                        label: self.lagLabel,
                        dimensions: [
                            (KafkaMetricLabels.topicDimension, topic.topic),
                            (KafkaMetricLabels.partitionDimension, "\(partition.partition)"),
                        ]
                    ).record(lag)
                    if lag > maxLag { maxLag = lag }
                }
            }
        }
        self.lagMax.record(maxLag)
    }
}
