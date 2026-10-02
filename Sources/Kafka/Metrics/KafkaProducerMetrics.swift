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

/// Holds all producer metric instruments and provides thread-safe recording methods.
final class KafkaProducerMetrics: Sendable {
    private let sendErrors: Counter
    private let sendDuration: Timer
    private let deliverySuccess: Counter
    private let deliveryFailure: Counter
    private let errors: Counter
    private let messagesSent: Meter
    private let bytesSent: Meter
    private let queueMessages: Gauge
    private let queueBytes: Gauge
    private let batchSizeAvg: Gauge
    // Base labels (already prefixed) for per-broker latency windows.
    private let brokerRTTLabel: String
    private let brokerThrottleLabel: String
    private let brokerQueueLatencyLabel: String
    private let brokerRequestLatencyLabel: String
    private let clientDimension: (String, String)

    /// - Parameters:
    ///   - prefix: Label prefix for every instrument.
    ///   - clientID: Value of the `client_id` dimension that identifies this client instance.
    init(prefix: String, clientID: String) {
        let clientDimension = (KafkaMetricLabels.clientIdDimension, clientID)
        let dimensions = [clientDimension]
        self.clientDimension = clientDimension
        self.sendErrors = Counter(label: "\(prefix).\(KafkaMetricLabels.producerSendErrors)", dimensions: dimensions)
        self.sendDuration = Timer(label: "\(prefix).\(KafkaMetricLabels.producerSendDuration)", dimensions: dimensions)
        self.deliverySuccess = Counter(
            label: "\(prefix).\(KafkaMetricLabels.producerDeliverySuccess)",
            dimensions: dimensions
        )
        self.deliveryFailure = Counter(
            label: "\(prefix).\(KafkaMetricLabels.producerDeliveryFailure)",
            dimensions: dimensions
        )
        self.errors = Counter(label: "\(prefix).\(KafkaMetricLabels.producerErrors)", dimensions: dimensions)
        self.messagesSent = Meter(label: "\(prefix).\(KafkaMetricLabels.producerMessagesSent)", dimensions: dimensions)
        self.bytesSent = Meter(label: "\(prefix).\(KafkaMetricLabels.producerBytesSent)", dimensions: dimensions)
        self.queueMessages = Gauge(
            label: "\(prefix).\(KafkaMetricLabels.producerQueueMessages)",
            dimensions: dimensions
        )
        self.queueBytes = Gauge(label: "\(prefix).\(KafkaMetricLabels.producerQueueBytes)", dimensions: dimensions)
        self.batchSizeAvg = Gauge(label: "\(prefix).\(KafkaMetricLabels.producerBatchSizeAvg)", dimensions: dimensions)
        self.brokerRTTLabel = "\(prefix).\(KafkaMetricLabels.producerBrokerRTT)"
        self.brokerThrottleLabel = "\(prefix).\(KafkaMetricLabels.producerBrokerThrottle)"
        self.brokerQueueLatencyLabel = "\(prefix).\(KafkaMetricLabels.producerBrokerQueueLatency)"
        self.brokerRequestLatencyLabel = "\(prefix).\(KafkaMetricLabels.producerBrokerRequestLatency)"
    }

    func recordSendError() {
        self.sendErrors.increment()
    }

    /// Records the end-to-end latency of an acknowledged `sendAndAwait(_:)` (enqueue → delivery report).
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
        self.queueMessages.record(stats.queueMessages)
        self.queueBytes.record(stats.queueMessagesSize)

        // librdkafka reports cumulative totals, so they are set as-is rather than accumulated.
        self.messagesSent.set(stats.messagesSentTotal)
        self.bytesSent.set(stats.bytesSentTotal)

        var totalBatchSize = 0
        var topicCount = 0
        for topic in stats.topics.values {
            if let avg = topic.batchBytes?.avg {
                totalBatchSize += avg
                topicCount += 1
            }
        }
        if topicCount > 0 {
            self.batchSizeAvg.record(totalBatchSize / topicCount)
        }

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
            KafkaMetricSupport.recordWindow(
                baseLabel: self.brokerQueueLatencyLabel,
                clientDimension: self.clientDimension,
                broker: broker.name,
                divisorToMilliseconds: 1000,
                broker.internalLatency
            )
            KafkaMetricSupport.recordWindow(
                baseLabel: self.brokerRequestLatencyLabel,
                clientDimension: self.clientDimension,
                broker: broker.name,
                divisorToMilliseconds: 1000,
                broker.outbufLatency
            )
        }
    }
}
