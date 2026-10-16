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

    /// The base labels of the per-broker latency windows this producer emits.
    private var brokerWindowLabels: [String] {
        [
            self.brokerRTTLabel, self.brokerThrottleLabel, self.brokerQueueLatencyLabel,
            self.brokerRequestLatencyLabel,
        ]
    }

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

    /// Marks every gauge that derives from statistics as "no data" (`NaN`) after a statistics sample
    /// could not be decoded, instead of leaving the previous sample's values looking current.
    ///
    /// Cumulative totals are left untouched: `NaN` would break any rate computed over them.
    ///
    /// - Parameter last: The last successfully decoded sample, which identifies the broker gauges that
    ///   exist. `nil` if there has been none.
    func recordStatisticsUnavailable(last: RDKafkaStatistics?) {
        self.queueMessages.record(Double.nan)
        self.queueBytes.record(Double.nan)
        self.batchSizeAvg.record(Double.nan)
        guard let last else { return }
        KafkaMetricSupport.markWindowsUnavailable(
            baseLabels: self.brokerWindowLabels,
            clientDimension: self.clientDimension,
            brokers: Array(last.brokers.values)
        )
    }

    /// Removes every instrument from the metrics backend. Called when the producer shuts down, so a
    /// stopped producer does not keep exporting its last values.
    ///
    /// - Parameter last: The last successfully decoded sample, which identifies the broker gauges that
    ///   exist. `nil` if there has been none.
    func destroy(last: RDKafkaStatistics?) {
        self.sendErrors.destroy()
        self.sendDuration.destroy()
        self.deliverySuccess.destroy()
        self.deliveryFailure.destroy()
        self.errors.destroy()
        self.messagesSent.destroy()
        self.bytesSent.destroy()
        self.queueMessages.destroy()
        self.queueBytes.destroy()
        self.batchSizeAvg.destroy()
        guard let last else { return }
        KafkaMetricSupport.destroyWindows(
            baseLabels: self.brokerWindowLabels,
            clientDimension: self.clientDimension,
            brokers: Array(last.brokers.values)
        )
    }

    func updateFromStatistics(_ stats: RDKafkaStatistics) {
        self.queueMessages.record(stats.queueMessages)
        self.queueBytes.record(stats.queueMessagesSize)

        // librdkafka reports cumulative totals, so they are set as-is rather than accumulated. Bytes are
        // message bytes, not wire-level traffic.
        self.messagesSent.set(stats.messagesSentTotal)
        self.bytesSent.set(stats.messageBytesSentTotal)

        // Topics that sent no batch in this interval have nothing to average, so they are left out
        // instead of pulling the average towards 0. With no batches at all there is no data.
        var totalBatchSize = 0
        var topicCount = 0
        for topic in stats.topics.values {
            if let batchBytes = topic.batchBytes, batchBytes.count > 0 {
                totalBatchSize += batchBytes.avg
                topicCount += 1
            }
        }
        self.batchSizeAvg.record(topicCount > 0 ? Double(totalBatchSize) / Double(topicCount) : Double.nan)

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
