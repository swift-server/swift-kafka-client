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

/// A topic and partition pair identifying a specific Kafka partition.
public struct KafkaTopicPartition: Sendable, Hashable {
    /// The name of the Kafka topic.
    public var topic: String

    /// The partition within the topic.
    public var partition: KafkaPartition

    /// Create a new ``KafkaTopicPartition``.
    ///
    /// - Parameters:
    ///   - topic: The name of the Kafka topic.
    ///   - partition: The partition within the topic.
    public init(topic: String, partition: KafkaPartition) {
        self.topic = topic
        self.partition = partition
    }
}

/// A topic-partition pair with an associated offset.
///
/// Used as both input (e.g., for ``KafkaConsumer/seek``) and output
/// (e.g., from ``KafkaConsumer/committed`` and ``KafkaConsumer/position``).
public struct KafkaTopicPartitionOffset: Sendable, Hashable {
    /// The topic and partition.
    public var topicPartition: KafkaTopicPartition

    /// The offset for this topic-partition.
    ///
    /// When returned from ``KafkaConsumer/committed`` or ``KafkaConsumer/position``,
    /// a `nil` value indicates that no committed offset or position exists.
    public var offset: KafkaOffset?

    /// The name of the Kafka topic (convenience accessor).
    public var topic: String { self.topicPartition.topic }

    /// The partition within the topic (convenience accessor).
    public var partition: KafkaPartition { self.topicPartition.partition }

    /// Create a new ``KafkaTopicPartitionOffset``.
    ///
    /// - Parameters:
    ///   - topic: The name of the Kafka topic.
    ///   - partition: The partition within the topic.
    ///   - offset: The offset for this topic-partition.
    public init(topic: String, partition: KafkaPartition, offset: KafkaOffset?) {
        self.topicPartition = KafkaTopicPartition(topic: topic, partition: partition)
        self.offset = offset
    }

    /// Create a new ``KafkaTopicPartitionOffset`` from an existing ``KafkaTopicPartition``.
    ///
    /// - Parameters:
    ///   - topicPartition: The topic and partition.
    ///   - offset: The offset for this topic-partition.
    public init(topicPartition: KafkaTopicPartition, offset: KafkaOffset?) {
        self.topicPartition = topicPartition
        self.offset = offset
    }
}