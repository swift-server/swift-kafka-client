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

import Crdkafka

/// Type for representing the id of a Kafka Partition.
public struct KafkaPartition: RawRepresentable {
    public var rawValue: Int {
        didSet {
            precondition(
                0...Int(Int32.max) ~= self.rawValue || self.rawValue == RD_KAFKA_PARTITION_UA,
                "Partition ID outside of valid range \(0...Int32.max)"
            )
        }
    }

    public init(rawValue: Int) {
        precondition(
            0...Int(Int32.max) ~= rawValue || rawValue == RD_KAFKA_PARTITION_UA,
            "Partition ID outside of valid range \(0...Int32.max)"
        )
        self.rawValue = rawValue
    }

    /// Automatically assign a partition using the topic's partitioner function.
    public static let unassigned = KafkaPartition(rawValue: Int(RD_KAFKA_PARTITION_UA))
}

// MARK: KafkaPartition + Hashable

extension KafkaPartition: Hashable {}

// MARK: KafkaPartition + Sendable

extension KafkaPartition: Sendable {}
