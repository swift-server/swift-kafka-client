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

/// Holds all producer metric instruments and provides thread-safe recording methods.
final class KafkaProducerMetrics: Sendable {
    private let sendErrors: Counter
    private let sendDuration: Timer
    private let deliverySuccess: Counter
    private let deliveryFailure: Counter
    private let errors: Counter
    private let messagesSent: Counter
    private let bytesSent: Counter
    private let queueMessages: Gauge
    private let queueBytes: Gauge
    private let batchSizeAvg: Gauge

    private let previousMessagesSent: NIOLockedValueBox<Int>
    private let previousBytesSent: NIOLockedValueBox<Int>

    init(prefix: String) {
        self.sendErrors = Counter(label: "\(prefix).\(KafkaMetricLabels.producerSendErrors)")
        self.sendDuration = Timer(label: "\(prefix).\(KafkaMetricLabels.producerSendDuration)")
        self.deliverySuccess = Counter(label: "\(prefix).\(KafkaMetricLabels.producerDeliverySuccess)")
        self.deliveryFailure = Counter(label: "\(prefix).\(KafkaMetricLabels.producerDeliveryFailure)")
        self.errors = Counter(label: "\(prefix).\(KafkaMetricLabels.producerErrors)")
        self.messagesSent = Counter(label: "\(prefix).\(KafkaMetricLabels.producerMessagesSent)")
        self.bytesSent = Counter(label: "\(prefix).\(KafkaMetricLabels.producerBytesSent)")
        self.queueMessages = Gauge(label: "\(prefix).\(KafkaMetricLabels.producerQueueMessages)")
        self.queueBytes = Gauge(label: "\(prefix).\(KafkaMetricLabels.producerQueueBytes)")
        self.batchSizeAvg = Gauge(label: "\(prefix).\(KafkaMetricLabels.producerBatchSizeAvg)")
        self.previousMessagesSent = NIOLockedValueBox(0)
        self.previousBytesSent = NIOLockedValueBox(0)
    }

    func recordSendError() {
        self.sendErrors.increment()
    }

    @available(macOS 13, iOS 16, tvOS 16, watchOS 9, *)
    func recordSend(duration: Duration) {
        self.sendDuration.record(duration: duration)
    }

    func recordDeliverySuccess() {
        self.deliverySuccess.increment()
    }

    func recordDeliveryFailure() {
        self.deliveryFailure.increment()
    }

    func recordError() {
        self.errors.increment()
    }

    func updateFromStatistics(_ stats: RDKafkaStatistics) {
        if let queuedMessages = stats.queuedProducerMessages {
            self.queueMessages.record(queuedMessages)
        }

        if let queuedBytes = stats.queuedProducerMessagesSize {
            self.queueBytes.record(queuedBytes)
        }

        if let current = stats.totalKafkaBrokerMessagesSent {
            let delta = self.previousMessagesSent.withLockedValue { prev in
                let d = current - prev
                prev = current
                return d
            }
            if delta > 0 { self.messagesSent.increment(by: delta) }
        }

        if let current = stats.totalKafkaBrokerBytesSent {
            let delta = self.previousBytesSent.withLockedValue { prev in
                let d = current - prev
                prev = current
                return d
            }
            if delta > 0 { self.bytesSent.increment(by: delta) }
        }

        var totalBatchSize: Int = 0
        var topicCount: Int = 0
        for (_, topic) in stats.topics ?? [:] {
            if let avg = topic.batchSize?.avg {
                totalBatchSize += avg
                topicCount += 1
            }
        }
        if topicCount > 0 {
            self.batchSizeAvg.record(totalBatchSize / topicCount)
        }
    }
}
