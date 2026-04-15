//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@testable import Kafka

/// Helpers for configuring librdkafka's built-in mock cluster.
///
/// Setting `test.mock.num.brokers` causes librdkafka to create an in-process
/// mock Kafka cluster, eliminating the need for a real broker in unit tests.
enum MockBrokerConfig {

    /// Apply mock broker configuration to a ``KafkaProducerConfig``.
    static func apply(to config: inout KafkaProducerConfig, brokerCount: Int) {
        config.additionalConfig["test.mock.num.brokers"] = "\(brokerCount)"
    }

    /// Apply mock broker configuration to a ``KafkaConsumerConfig``.
    static func apply(to config: inout KafkaConsumerConfig, brokerCount: Int) {
        config.additionalConfig["test.mock.num.brokers"] = "\(brokerCount)"
    }
}
