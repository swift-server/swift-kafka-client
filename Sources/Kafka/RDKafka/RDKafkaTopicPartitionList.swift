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
public final class RDKafkaTopicPartitionList {
    private let _internal: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>

    /// Create a new topic+partition list.
    ///
    /// - Parameter size: Initial allocated size used when the number of allocated elements can be estimated.
    init(size: Int32 = 1) {
        self._internal = rd_kafka_topic_partition_list_new(size)
    }
    
    init(from: UnsafePointer<rd_kafka_topic_partition_list_t>) {
        self._internal = rd_kafka_topic_partition_list_copy(from)
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
    func setOffset(topic: String, partition: KafkaPartition, offset: KafkaOffset) {
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
        partitionPointer.pointee.offset = Int64(offset.rawValue)
    }

    /// Scoped accessor that enables safe access to the pointer of the underlying `rd_kafka_topic_partition_t`.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the pointer.
    @discardableResult
    func withListPointer<T>(_ body: (UnsafeMutablePointer<rd_kafka_topic_partition_list_t>) throws -> T) rethrows -> T {
        return try body(self._internal)
    }

    /// Scoped accessor that enables safe access to the pointer of the underlying `rd_kafka_topic_partition_t`.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the pointer.
    @discardableResult
    func withListPointer<T>(_ body: (UnsafeMutablePointer<rd_kafka_topic_partition_list_t>) async throws -> T) async rethrows -> T {
        return try await body(self._internal)
    }
    
    func getByIdx(idx: Int) -> TopicPartition? {
        withListPointer { list -> TopicPartition? in
            guard list.pointee.cnt > idx else {
                return nil
            }
            let elem = list.pointee.elems[idx]
            let topicName = String(cString: elem.topic)
            let partition = KafkaPartition(rawValue: Int(elem.partition))
            let offset = KafkaOffset(rawValue: Int(elem.offset))
            return TopicPartition(topicName, partition, offset)
        }
    }
    
    var count: Int {
        withListPointer { list in
            Int(list.pointee.cnt)
        }
    }
}

extension RDKafkaTopicPartitionList: Sendable {}
extension RDKafkaTopicPartitionList: Hashable {
    
    public func hash(into hasher: inout Hasher) {
        for idx in 0..<self.count {
            hasher.combine(self.getByIdx(idx: idx))
        }
    }
    
    public static func == (lhs: RDKafkaTopicPartitionList, rhs: RDKafkaTopicPartitionList) -> Bool {
        if lhs.count != rhs.count {
            return false
        }
        
        for idx in 0..<lhs.count {
            guard lhs.getByIdx(idx: idx) == rhs.getByIdx(idx: idx) else {
                return false
            }
        }
        return true
    }
}

extension RDKafkaTopicPartitionList: CustomStringConvertible {
    public var description: String {
        var str = "RDKafkaTopicPartitionList {"
        for idx in 0..<count {
            str += "\(getByIdx(idx: idx)!)"
        }
        str += "}"
        return str
    }
}
