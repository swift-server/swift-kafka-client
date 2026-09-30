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

import Kafka
import Testing

/// Checks that public value types can be created and modified from outside the module.
///
/// - Important: This file must import `Kafka` without `@testable` (unlike `KafkaTests.swift`), so it
///   only compiles if the initializers and property setters are public. The tests don't need a broker.
@Suite struct KafkaPublicValueTypeTests {
    @Test func rebalanceValuesCanBeModified() {
        var rebalance = KafkaConsumer.Rebalance(
            kind: .assign,
            partitions: [KafkaTopicPartition(topic: "orders", partition: KafkaPartition(rawValue: 0))]
        )

        rebalance.kind = .revoke
        rebalance.partitions.append(KafkaTopicPartition(topic: "orders", partition: KafkaPartition(rawValue: 1)))

        #expect(rebalance.kind == .revoke)
        #expect(rebalance.partitions.map(\.partition.rawValue) == [0, 1])
    }
}
