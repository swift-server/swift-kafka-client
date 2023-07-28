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

/// Swift wrapper type for `rd_kafka_topic_partition_list_t`.
final class RDKafkaTopicPartitionList {
    private let _internal: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>

    /// Create a new topic+partition list.
    ///
    /// - Parameter size: Initial allocated size used when the number of allocated elements can be estimated.
    init(size: Int32 = 1) {
        self._internal = rd_kafka_topic_partition_list_new(size)
    }

    deinit {
        rd_kafka_topic_partition_list_destroy(self._internal)
    }

    /// Add topic+partition pair to list.
    func add(topic: String, partition: KafkaPartition) {
        precondition(
            0...Int(Int32.max) ~= partition.rawValue || partition == .unassigned,
            "Partition ID outside of valid range \(0...Int32.max)"
        )

        rd_kafka_topic_partition_list_add(
            self._internal,
            topic,
            Int32(partition.rawValue)
        )
    }

    /// Manually set read offset for a given topic+partition pair.
    func setOffset(topic: String, partition: KafkaPartition, offset: Int64) {
        precondition(
            0...Int(Int32.max) ~= partition.rawValue || partition == .unassigned,
            "Partition ID outside of valid range \(0...Int32.max)"
        )

        guard let partitionPointer = rd_kafka_topic_partition_list_add(
            self._internal,
            topic,
            Int32(partition.rawValue)
        ) else {
            fatalError("rd_kafka_topic_partition_list_add returned invalid pointer")
        }
        partitionPointer.pointee.offset = offset
    }

    /// Scoped accessor that enables safe access to the pointer of the underlying `rd_kafka_topic_partition_t`.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the pointer.
    @discardableResult
    func withListPointer<T>(_ body: (UnsafeMutablePointer<rd_kafka_topic_partition_list_t>) throws -> T) rethrows -> T {
        return try body(self._internal)
    }
}
