//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Metrics

extension KafkaConfiguration {
    // MARK: - Metrics

    /// Configuration for the consumer metrics emitted by `SwiftKafka`.
    public struct ConsumerMetrics: Sendable {
        internal var enabled: Bool {
            self.updateInterval != nil &&
                (self.queuedOperation != nil ||
                    self.totalKafkaBrokerRequests != nil ||
                    self.totalKafkaBrokerBytesSent != nil ||
                    self.totalKafkaBrokerResponses != nil ||
                    self.totalKafkaBrokerResponsesSize != nil ||
                    self.totalKafkaBrokerMessagesBytesRecieved != nil ||
                    self.topicsInMetadataCache != nil)
        }

        /// Update interval for statistics.
        public var updateInterval: Duration?

        /// Number of operations (callbacks, events, etc) waiting in the queue.
        public var queuedOperation: Gauge?

        /// Total number of requests sent to Kafka brokers.
        public var totalKafkaBrokerRequests: Gauge?
        /// Total number of bytes transmitted to Kafka brokers.
        public var totalKafkaBrokerBytesSent: Gauge?
        /// Total number of responses received from Kafka brokers.
        public var totalKafkaBrokerResponses: Gauge?
        /// Total number of bytes received from Kafka brokers.
        public var totalKafkaBrokerResponsesSize: Gauge?

        /// Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers.
        public var totalKafkaBrokerMessagesRecieved: Gauge?
        /// Total number of message bytes (including framing) received from Kafka brokers.
        public var totalKafkaBrokerMessagesBytesRecieved: Gauge?

        /// Number of topics in the metadata cache.
        public var topicsInMetadataCache: Gauge?

        private static func record<T: BinaryInteger>(_ value: T?, to: Gauge?) {
            guard let value,
                  let to else {
                return
            }
            to.record(value)
        }

        internal func update(with rdKafkaStatistics: RDKafkaStatistics) {
            Self.record(rdKafkaStatistics.queuedOperation, to: self.queuedOperation)

            Self.record(rdKafkaStatistics.totalKafkaBrokerRequests, to: self.totalKafkaBrokerRequests)
            Self.record(rdKafkaStatistics.totalKafkaBrokerBytesSent, to: self.totalKafkaBrokerBytesSent)
            Self.record(rdKafkaStatistics.totalKafkaBrokerResponses, to: self.totalKafkaBrokerResponses)
            Self.record(rdKafkaStatistics.totalKafkaBrokerResponsesSize, to: self.totalKafkaBrokerResponsesSize)

            Self.record(rdKafkaStatistics.totalKafkaBrokerMessagesRecieved, to: self.totalKafkaBrokerMessagesRecieved)
            Self.record(rdKafkaStatistics.totalKafkaBrokerMessagesBytesRecieved, to: self.totalKafkaBrokerMessagesBytesRecieved)

            Self.record(rdKafkaStatistics.topicsInMetadataCache, to: self.topicsInMetadataCache)
        }
    }

    /// Configuration for the producer metrics emitted by `SwiftKafka`.
    public struct ProducerMetrics: Sendable {
        internal var enabled: Bool {
            self.updateInterval != nil &&
                (self.queuedOperation != nil ||
                    self.queuedProducerMessages != nil ||
                    self.queuedProducerMessagesSize != nil ||
                    self.totalKafkaBrokerRequests != nil ||
                    self.totalKafkaBrokerBytesSent != nil ||
                    self.totalKafkaBrokerResponses != nil ||
                    self.totalKafkaBrokerResponsesSize != nil ||
                    self.totalKafkaBrokerMessagesSent != nil ||
                    self.totalKafkaBrokerMessagesBytesSent != nil ||
                    self.topicsInMetadataCache != nil)
        }

        /// Update interval for statistics.
        public var updateInterval: Duration?

        /// Number of operations (callbacks, events, etc) waiting in the queue.
        public var queuedOperation: Gauge?
        /// Current number of queued producer messages.
        public var queuedProducerMessages: Gauge?
        /// Current total size in bytes of queued producer messages.
        public var queuedProducerMessagesSize: Gauge?

        /// Total number of requests sent to Kafka brokers.
        public var totalKafkaBrokerRequests: Gauge?
        /// Total number of bytes transmitted to Kafka brokers.
        public var totalKafkaBrokerBytesSent: Gauge?
        /// Total number of responses received from Kafka brokers.
        public var totalKafkaBrokerResponses: Gauge?
        /// Total number of bytes received from Kafka brokers.
        public var totalKafkaBrokerResponsesSize: Gauge?

        /// Total number of messages transmitted (produced) to Kafka brokers.
        public var totalKafkaBrokerMessagesSent: Gauge?
        /// Total number of message bytes (including framing, such as per-Message framing and MessageSet/batch framing) transmitted to Kafka brokers.
        public var totalKafkaBrokerMessagesBytesSent: Gauge?

        /// Number of topics in the metadata cache.
        public var topicsInMetadataCache: Gauge?

        private static func record<T: BinaryInteger>(_ value: T?, to: Gauge?) {
            guard let value,
                  let to else {
                return
            }
            to.record(value)
        }

        internal func update(with rdKafkaStatistics: RDKafkaStatistics) {
            Self.record(rdKafkaStatistics.queuedOperation, to: self.queuedOperation)
            Self.record(rdKafkaStatistics.queuedProducerMessages, to: self.queuedProducerMessages)
            Self.record(rdKafkaStatistics.queuedProducerMessagesSize, to: self.queuedProducerMessagesSize)

            Self.record(rdKafkaStatistics.totalKafkaBrokerRequests, to: self.totalKafkaBrokerRequests)
            Self.record(rdKafkaStatistics.totalKafkaBrokerBytesSent, to: self.totalKafkaBrokerBytesSent)
            Self.record(rdKafkaStatistics.totalKafkaBrokerResponses, to: self.totalKafkaBrokerResponses)
            Self.record(rdKafkaStatistics.totalKafkaBrokerResponsesSize, to: self.totalKafkaBrokerResponsesSize)

            Self.record(rdKafkaStatistics.totalKafkaBrokerMessagesSent, to: self.totalKafkaBrokerMessagesSent)
            Self.record(rdKafkaStatistics.totalKafkaBrokerMessagesBytesSent, to: self.totalKafkaBrokerMessagesBytesSent)

            Self.record(rdKafkaStatistics.topicsInMetadataCache, to: self.topicsInMetadataCache)
        }
    }
}
