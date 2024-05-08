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

/// Message that is sent by the `KafkaProducer`
public struct KafkaProducerMessage<Key: KafkaContiguousBytes, Value: KafkaContiguousBytes> {
    /// The topic to which the message will be sent.
    public var topic: String

    /// The partition to which the message will be sent.
    /// Defaults to ``KafkaPartition/unassigned``.
    /// This means the message will be automatically assigned a partition using the topic's partitioner function.
    public var partition: KafkaPartition

    /// The headers of the message.
    public var headers: [KafkaHeader]

    /// The optional key associated with the message.
    /// If the ``KafkaPartition`` is ``KafkaPartition/unassigned``, the ``KafkaProducerMessage/key`` is used to ensure
    /// that two ``KafkaProducerMessage``s with the same key still get sent to the same ``KafkaPartition``.
    public var key: Key?

    /// The value of the message to be sent.
    public var value: Value

    /// Create a new `KafkaProducerMessage` with a ``KafkaContiguousBytes`` key and value.
    ///
    /// - Parameters:
    ///     - topic: The topic the message will be sent to. Topics may be created by the `KafkaProducer` if non-existent.
    ///     - partition: The topic partition the message will be sent to. If not set explicitly, the partition will be assigned automatically.
    ///     - headers: The headers of the message.
    ///     - key: Used to guarantee that messages with the same key will be sent to the same partition so that their order is preserved.
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
    /// Create a new `KafkaProducerMessage` with a ``KafkaContiguousBytes`` value.
    ///
    /// - Parameters:
    ///     - topic: The topic the message will be sent to. Topics may be created by the `KafkaProducer` if non-existent.
    ///     - partition: The topic partition the message will be sent to. If not set explicitly, the partition will be assigned automatically.
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
