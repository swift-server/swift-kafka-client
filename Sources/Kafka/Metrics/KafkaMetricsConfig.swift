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

/// Configuration for Kafka client metrics.
///
/// Use `.enabled(prefix:updateInterval:)` to activate all metrics with a consistent
/// naming prefix, or `.disabled` (the default) for zero-overhead operation.
public struct KafkaMetricsConfig: Sendable, Hashable {
    /// Whether metrics collection is active.
    internal let isEnabled: Bool
    /// Label prefix for all metric instruments.
    internal let prefix: String
    /// How often librdkafka emits statistics events.
    internal let updateInterval: Duration

    private init() {
        self.isEnabled = false
        self.prefix = ""
        self.updateInterval = .seconds(5)
    }

    private init(prefix: String, updateInterval: Duration) {
        self.isEnabled = true
        self.prefix = prefix
        self.updateInterval = updateInterval
    }

    /// Disabled metrics — no instruments are created and no statistics events are polled.
    public static let disabled = KafkaMetricsConfig()

    /// Enable all metrics with the given prefix and statistics update interval.
    ///
    /// - Parameters:
    ///   - prefix: Label prefix for all metrics. Default: `"kafka"`.
    ///   - updateInterval: How often librdkafka emits statistics. Default: 5 seconds.
    public static func enabled(
        prefix: String = "kafka",
        updateInterval: Duration = .seconds(5)
    ) -> KafkaMetricsConfig {
        KafkaMetricsConfig(prefix: prefix, updateInterval: updateInterval)
    }
}
