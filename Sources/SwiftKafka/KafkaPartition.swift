//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka

/// Type for representing the number of a Kafka Partition.
public struct KafkaPartition: RawRepresentable {
    public var rawValue: Int32

    public init(rawValue: Int32) {
        self.rawValue = rawValue
    }

    public static let unassigned = KafkaPartition(rawValue: RD_KAFKA_PARTITION_UA)
}

// MARK: KafkaPartition + Hashable

extension KafkaPartition: Hashable {}

// MARK: KafkaPartition + Sendable

extension KafkaPartition: Sendable {}
