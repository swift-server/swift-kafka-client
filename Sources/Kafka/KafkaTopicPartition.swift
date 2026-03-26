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
