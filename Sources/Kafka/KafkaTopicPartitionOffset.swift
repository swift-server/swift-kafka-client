//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2024 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// A topic-partition pair with an associated offset.
///
/// Used as both input (for example, ``KafkaConsumer/seek(topicPartitionOffsets:timeout:)``) and output
/// (for example, ``KafkaConsumer/committed(topicPartitions:timeout:)`` and ``KafkaConsumer/position(topicPartitions:)``).
public struct KafkaTopicPartitionOffset: Sendable, Hashable {
    /// The topic and partition.
    public var topicPartition: KafkaTopicPartition

    /// The offset for this topic-partition.
    ///
    /// When returned from ``KafkaConsumer/committed(topicPartitions:timeout:)`` or ``KafkaConsumer/position(topicPartitions:)``,
    /// a `nil` value indicates that no committed offset or position exists.
    public var offset: KafkaOffset?

    /// The name of the Kafka topic.
    ///
    /// A convenience accessor for the underlying ``KafkaTopicPartition/topic``.
    public var topic: String { self.topicPartition.topic }

    /// The partition within the topic.
    ///
    /// A convenience accessor for the underlying ``KafkaTopicPartition/partition``.
    public var partition: KafkaPartition { self.topicPartition.partition }

    /// Creates a topic-partition-offset triple from a topic name, partition, and offset.
    ///
    /// - Parameters:
    ///   - topic: The name of the Kafka topic.
    ///   - partition: The partition within the topic.
    ///   - offset: The offset for this topic-partition.
    public init(topic: String, partition: KafkaPartition, offset: KafkaOffset?) {
        self.topicPartition = KafkaTopicPartition(topic: topic, partition: partition)
        self.offset = offset
    }

    /// Creates a topic-partition-offset triple from an existing topic-partition pair and an offset.
    ///
    /// Use this initializer when you already have a ``KafkaTopicPartition`` value.
    ///
    /// - Parameters:
    ///   - topicPartition: The topic and partition.
    ///   - offset: The offset for this topic-partition.
    public init(topicPartition: KafkaTopicPartition, offset: KafkaOffset?) {
        self.topicPartition = topicPartition
        self.offset = offset
    }
}
