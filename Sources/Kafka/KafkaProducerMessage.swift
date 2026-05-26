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
import NIOCore

/// A message a producer sends to the Kafka cluster.
public struct KafkaProducerMessage<Key: KafkaContiguousBytes, Value: KafkaContiguousBytes> {
    /// The topic the producer sends the message to.
    public var topic: String

    /// The partition the producer sends the message to.
    ///
    /// Defaults to ``KafkaPartition/unassigned``.
    /// When unassigned, the topic's partitioner function selects a partition automatically.
    public var partition: KafkaPartition

    /// The headers of the message.
    public var headers: [KafkaHeader]

    /// The optional key associated with the message.
    ///
    /// If the ``KafkaPartition`` is ``KafkaPartition/unassigned``, the producer uses
    /// ``KafkaProducerMessage/key`` to ensure that two ``KafkaProducerMessage``s with
    /// the same key route to the same ``KafkaPartition``.
    public var key: Key?

    /// The value of the message to send.
    public var value: Value

    /// Creates a producer message with key and value content.
    ///
    /// Both `key` and `value` must conform to ``KafkaContiguousBytes``.
    ///
    /// - Parameters:
    ///     - topic: The topic the producer sends the message to. The ``KafkaProducer`` creates the topic if it doesn't exist.
    ///     - partition: The topic partition the producer sends the message to. If you don't set this explicitly, the producer assigns the partition automatically.
    ///     - headers: The headers of the message.
    ///     - key: Guarantees that messages with the same key go to the same partition, preserving their order.
    ///     - value: The message's value.
    public init(
        topic: String,
        partition: KafkaPartition = .unassigned,
        headers: [KafkaHeader] = [],
        key: Key?,
        value: Value
    ) {
        self.topic = topic
        self.partition = partition
        self.headers = headers
        self.key = key
        self.value = value
    }
}

extension KafkaProducerMessage where Key == Never {
    /// Creates a producer message with value content and no key.
    ///
    /// The `value` must conform to ``KafkaContiguousBytes``.
    ///
    /// - Parameters:
    ///     - topic: The topic the producer sends the message to. The ``KafkaProducer`` creates the topic if it doesn't exist.
    ///     - partition: The topic partition the producer sends the message to. If you don't set this explicitly, the producer assigns the partition automatically.
    ///     - headers: The headers of the message.
    ///     - value: The message body.
    public init(
        topic: String,
        partition: KafkaPartition = .unassigned,
        headers: [KafkaHeader] = [],
        value: Value
    ) {
        self.topic = topic
        self.partition = partition
        self.headers = headers
        self.key = nil
        self.value = value
    }
}

extension KafkaProducerMessage: Hashable where Key: Hashable, Value: Hashable {}

extension KafkaProducerMessage: Equatable where Key: Equatable, Value: Equatable {}

extension KafkaProducerMessage: Sendable where Key: Sendable, Value: Sendable {}
