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

/// Extensions for configuring librdkafka's built-in mock cluster.
///
/// Setting `test.mock.num.brokers` causes librdkafka to create an in-process
/// mock Kafka cluster, eliminating the need for a real broker in unit tests.

extension KafkaProducerConfig {
    /// Configure this producer to use librdkafka's in-process mock broker.
    mutating func useMockBroker(count: Int = 1) {
        self.additionalConfig["test.mock.num.brokers"] = "\(count)"
    }
}

extension KafkaConsumerConfig {
    /// Configure this consumer to use librdkafka's in-process mock broker.
    mutating func useMockBroker(count: Int = 1) {
        self.additionalConfig["test.mock.num.brokers"] = "\(count)"
    }
}
