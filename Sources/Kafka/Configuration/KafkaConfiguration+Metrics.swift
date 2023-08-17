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

    /// Use to configure metrics.
    public struct MetricsOptions: Sendable {
        /// librdkafka's internal monotonic clock (microseconds)
        public var ts: Gauge?
        /// Wall clock time in seconds since the epoch
        public var time: Timer?
        /// Time since this client instance was created
        public var age: Timer?
        /// Number of ops (callbacks, events, etc) waiting in queue for application to serve
        public var replyQueue: Gauge?
        /// Current number of messages in producer queues
        public var msgCount: Gauge?
        /// Current total size of messages in producer queues
        public var msgSize: Gauge?
        /// Threshold: maximum number of messages allowed allowed on the producer queues
        public var msgMax: Gauge?
        /// Threshold: maximum total size of messages allowed on the producer queues
        public var msgSizeMax: Gauge?
        
        /// Total number of requests sent to Kafka brokers
        public var tx: Gauge?
        /// Total number of bytes transmitted to Kafka brokers
        public var txBytex: Gauge?
        /// Total number of responses received from Kafka brokers
        public var rx: Gauge?
        /// Total number of bytes received from Kafka brokers
        public var rxBytex: Gauge?
        
        /// Total number of messages transmitted (produced) to Kafka brokers
        public var txMessages: Gauge?
        /// Total number of message bytes (including framing, such as per-Message framing and MessageSet/batch framing) transmitted to Kafka brokers
        public var txMessagesBytex: Gauge?
        /// Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers.
        public var rxMessages: Gauge?
        /// Total number of message bytes (including framing) received from Kafka brokers
        public var rxMessagesBytex: Gauge?
        
        /// Number of topics in the metadata cache.
        public var metadataCacheCount: Gauge?
    }
    
    public enum Metrics: Sendable {
        case disable
        case enable(updateInterval: KafkaConfiguration.KeyRefreshAttempts, options: MetricsOptions)
    }
}
