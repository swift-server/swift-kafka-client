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

public struct KafkaMetrics: Sendable {
    /// Prefix used for all metric labels.
    public let prefix: String

    /// Enable export of per-broker metrics.
    public var enableBrokerMetrics: Bool

    /// Enable export of per-topic and per-partition metrics.
    public var enableTopicMetrics: Bool

    /// Configure which window statistics percentiles/aggregations to export.
    public var windowMetrics: Set<WindowMetric>

    /// List of metrics to export for rolling window statistics (e.g. broker latency or batch sizes).
    public enum WindowMetric: Sendable, Hashable {
        case min
        case max
        case avg
        case sum
        case count
        case stddev
        case hdrsize
        case p50
        case p75
        case p90
        case p95
        case p99
        case p99_99
        case outOfRange
    }

    private let factory: any MetricsFactory

    /// Initialize a ``KafkaMetrics`` instance for a Kafka client.
    ///
    /// If `.statisticsIntervalMs` is set on the client configuration `librdkafka` statistics
    /// will be exported as metrics.
    ///
    /// - Parameters:
    ///   - prefix: Prefix for all metric labels. Default: `"kafka"`.
    ///   - enableBrokerMetrics: Export per-broker statistics as metrics. Default: `true`.
    ///   - enableTopicMetrics: Export per-topic and per-partition statistics as metrics. Default: `true`.
    ///   - windowMetrics: Which window statistics to export. Default: min, max, avg, count, p50, p95, p99.
    ///   - factory: An optional custom `MetricsFactory`. If `nil`, uses `MetricsSystem.factory`.
    public init(
        prefix: String = "kafka",
        enableBrokerMetrics: Bool = true,
        enableTopicMetrics: Bool = true,
        windowMetrics: Set<WindowMetric> = [.min, .max, .avg, .count, .p95, .p99],
        factory: (any MetricsFactory)? = nil
    ) {
        self.prefix = prefix
        self.enableBrokerMetrics = enableBrokerMetrics
        self.enableTopicMetrics = enableTopicMetrics
        self.windowMetrics = windowMetrics
        self.factory = factory ?? MetricsSystem.factory
    }

    internal func update(with stats: RDKafkaStatistics) {
        let clientDimension: [(String, String)] = [("client", stats.clientId)]

        self.recordTopLevel(stats: stats, dimensions: clientDimension)

        if self.enableBrokerMetrics {
            for (brokerName, broker) in stats.brokers {
                let brokerDimensions = clientDimension + [("broker", brokerName)]
                self.recordBroker(broker: broker, dimensions: brokerDimensions)
            }
        }

        if self.enableTopicMetrics {
            for (topicName, topic) in stats.topics {
                let topicDimensions = clientDimension + [("topic", topicName)]
                self.recordTopic(topic: topic, dimensions: topicDimensions)

                if let partitions = topic.partitions {
                    for (partId, partition) in partitions {
                        let partitionDimensions = clientDimension + [("topic", topicName), ("partition", partId)]
                        self.recordPartition(partition: partition, dimensions: partitionDimensions)
                    }
                }
            }
        }

        // Consumer group metrics
        if let cgrp = stats.consumerGroup {
            self.recordConsumerGroup(cgrp: cgrp, dimensions: clientDimension)
        }
    }

    private func recordTopLevel(stats: RDKafkaStatistics, dimensions: [(String, String)]) {
        record("queue_operations", stats.queueOperations, dimensions)
        record("queue_messages", stats.queueMessages, dimensions)
        record("queue_messages_size", stats.queueMessagesSize, dimensions)
        record("queue_messages_max", stats.queueMessagesMax, dimensions)
        record("queue_messages_size_max", stats.queueMessagesSizeMax, dimensions)
        record("requests_sent_total", stats.requestsSentTotal, dimensions)
        record("bytes_sent_total", stats.bytesSentTotal, dimensions)
        record("responses_received_total", stats.responsesReceivedTotal, dimensions)
        record("bytes_received_total", stats.bytesReceivedTotal, dimensions)
        record("messages_sent_total", stats.messagesSentTotal, dimensions)
        record("message_bytes_sent_total", stats.messageBytesSentTotal, dimensions)
        record("messages_received_total", stats.messagesReceivedTotal, dimensions)
        record("message_bytes_received_total", stats.messageBytesReceivedTotal, dimensions)
    }

    private func recordBroker(broker: RDKafkaStatistics.BrokerStats, dimensions: [(String, String)]) {
        record("broker_state_age", broker.stateAge, dimensions)
        record("broker_outbuf_requests", broker.outbufRequests, dimensions)
        record("broker_outbuf_messages", broker.outbufMessages, dimensions)
        record("broker_waitresp_requests", broker.waitrespRequests, dimensions)
        record("broker_waitresp_messages", broker.waitrespMessages, dimensions)
        record("broker_requests_sent_total", broker.requestsSentTotal, dimensions)
        record("broker_bytes_sent_total", broker.bytesSentTotal, dimensions)
        record("broker_transmit_errors_total", broker.transmitErrorsTotal, dimensions)
        record("broker_request_retries_total", broker.requestRetriesTotal, dimensions)
        record("broker_transmit_idle_time", broker.transmitIdleTime, dimensions)
        record("broker_request_timeouts_total", broker.requestTimeoutsTotal, dimensions)
        record("broker_responses_received_total", broker.responsesReceivedTotal, dimensions)
        record("broker_bytes_received_total", broker.bytesReceivedTotal, dimensions)
        record("broker_receive_errors_total", broker.receiveErrorsTotal, dimensions)
        record("broker_correlation_id_errors_total", broker.correlationIdErrorsTotal, dimensions)
        record("broker_partial_responses_total", broker.partialResponsesTotal, dimensions)
        record("broker_receive_idle_time", broker.receiveIdleTime, dimensions)
        record("broker_decompress_buffer_grow_total", broker.decompressBufferGrowTotal, dimensions)
        record("broker_wakeups_total", broker.wakeupsTotal, dimensions)
        record("broker_connects_total", broker.connectsTotal, dimensions)
        record("broker_disconnects_total", broker.disconnectsTotal, dimensions)

        recordWindowStats("broker_internal_latency", broker.internalLatency, dimensions)
        recordWindowStats("broker_outbuf_latency", broker.outbufLatency, dimensions)
        recordWindowStats("broker_round_trip_time", broker.roundTripTime, dimensions)
        recordWindowStats("broker_throttle_time", broker.throttleTime, dimensions)
    }

    private func recordTopic(topic: RDKafkaStatistics.TopicStats, dimensions: [(String, String)]) {
        record("topic_age", topic.age, dimensions)
        record("topic_metadata_age", topic.metadataAge, dimensions)

        recordWindowStats("topic_batch_bytes", topic.batchBytes, dimensions)
        recordWindowStats("topic_batch_messages", topic.batchMessages, dimensions)
    }

    private func recordPartition(partition: RDKafkaStatistics.PartitionStats, dimensions: [(String, String)]) {
        record("partition_queue_messages", partition.queueMessages, dimensions)
        record("partition_queue_bytes", partition.queueBytes, dimensions)
        record("partition_transmit_queue_messages", partition.transmitQueueMessages, dimensions)
        record("partition_transmit_queue_bytes", partition.transmitQueueBytes, dimensions)
        record("partition_fetch_queue_messages", partition.fetchQueueMessages, dimensions)
        record("partition_fetch_queue_size", partition.fetchQueueSize, dimensions)
        record("partition_query_offset", partition.queryOffset, dimensions)
        record("partition_next_offset", partition.nextOffset, dimensions)
        record("partition_app_offset", partition.appOffset, dimensions)
        record("partition_stored_offset", partition.storedOffset, dimensions)
        record("partition_committed_offset", partition.committedOffset, dimensions)
        record("partition_eof_offset", partition.eofOffset, dimensions)
        record("partition_low_watermark_offset", partition.lowWatermarkOffset, dimensions)
        record("partition_high_watermark_offset", partition.highWatermarkOffset, dimensions)
        record("partition_last_stable_offset", partition.lastStableOffset, dimensions)
        record("partition_consumer_lag", partition.consumerLag, dimensions)
        record("partition_consumer_lag_stored", partition.consumerLagStored, dimensions)
        record("partition_messages_sent_total", partition.messagesSentTotal, dimensions)
        record("partition_bytes_sent_total", partition.bytesSentTotal, dimensions)
        record("partition_messages_received_total", partition.messagesReceivedTotal, dimensions)
        record("partition_bytes_received_total", partition.bytesReceivedTotal, dimensions)
        record("partition_messages_total", partition.messagesTotal, dimensions)
        record("partition_received_version_drops_total", partition.receivedVersionDropsTotal, dimensions)
        record("partition_messages_in_flight", partition.messagesInFlight, dimensions)
        record("partition_next_ack_sequence", partition.nextAckSequence, dimensions)
        record("partition_next_error_sequence", partition.nextErrorSequence, dimensions)
    }

    private func recordConsumerGroup(cgrp: RDKafkaStatistics.ConsumerGroupStats, dimensions: [(String, String)]) {
        record("cgrp_state_age", cgrp.stateAge, dimensions)
        record("cgrp_rebalance_age", cgrp.rebalanceAge, dimensions)
        record("cgrp_rebalances_total", cgrp.rebalancesTotal, dimensions)
        record("cgrp_assignment_size", cgrp.assignmentSize, dimensions)
    }

    private func record(_ label: String, _ value: Int, _ dimensions: [(String, String)]) {
        Gauge(label: "\(prefix)_\(label)", dimensions: dimensions, factory: self.factory).record(value)
    }

    private func recordWindowStats(
        _ base: String,
        _ stats: RDKafkaStatistics.WindowStats?,
        _ dimensions: [(String, String)]
    ) {
        guard let stats else { return }
        for ws in self.windowMetrics {
            switch ws {
            case .min:
                record("\(base)_min", stats.min, dimensions)
            case .max:
                record("\(base)_max", stats.max, dimensions)
            case .avg:
                record("\(base)_avg", stats.avg, dimensions)
            case .sum:
                record("\(base)_sum", stats.sum, dimensions)
            case .count:
                record("\(base)_count", stats.count, dimensions)
            case .stddev:
                record("\(base)_stddev", stats.stddev, dimensions)
            case .hdrsize:
                record("\(base)_hdrsize", stats.hdrsize, dimensions)
            case .p50:
                record("\(base)_p50", stats.p50, dimensions)
            case .p75:
                record("\(base)_p75", stats.p75, dimensions)
            case .p90:
                record("\(base)_p90", stats.p90, dimensions)
            case .p95:
                record("\(base)_p95", stats.p95, dimensions)
            case .p99:
                record("\(base)_p99", stats.p99, dimensions)
            case .p99_99:
                record("\(base)_p99_99", stats.p99_99, dimensions)
            case .outOfRange:
                record("\(base)_outofrange", stats.outOfRange, dimensions)
            }
        }
    }
}
