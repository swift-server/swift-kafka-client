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

extension KafkaProducer {
    /// A message a producer sends to the Kafka cluster.
    ///
    /// A message owns its key and value as byte arrays, so you can keep it, queue it, or send it from
    /// another task. To send bytes you hold in another type (for example `Foundation.Data`) without
    /// copying them into an array first, use ``KafkaProducer/send(topic:partition:headers:key:value:)``
    /// with a `RawSpan`.
    public struct Message: Sendable, Hashable {
        /// The topic the producer sends the message to.
        public var topic: KafkaTopic

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
        /// ``KafkaProducer/Message/key`` to ensure that two ``KafkaProducer/Message``s with
        /// the same key route to the same ``KafkaPartition``.
        public var key: [UInt8]?

        /// The value of the message to send.
        public var value: [UInt8]

        /// Creates a producer message with a key and value as byte arrays.
        ///
        /// - Parameters:
        ///     - topic: The topic the producer sends the message to. The ``KafkaProducer`` creates the topic if it doesn't exist.
        ///     - partition: The topic partition the producer sends the message to. If you don't set this explicitly, the producer assigns the partition automatically.
        ///     - headers: The headers of the message.
        ///     - key: Guarantees that messages with the same key go to the same partition, preserving their order.
        ///     - value: The message's value.
        public init(
            topic: KafkaTopic,
            partition: KafkaPartition = .unassigned,
            headers: [KafkaHeader] = [],
            key: [UInt8]?,
            value: [UInt8]
        ) {
            self.topic = topic
            self.partition = partition
            self.headers = headers
            self.key = key
            self.value = value
        }

        /// Creates a producer message with a value as a byte array and no key.
        ///
        /// - Parameters:
        ///     - topic: The topic the producer sends the message to. The ``KafkaProducer`` creates the topic if it doesn't exist.
        ///     - partition: The topic partition the producer sends the message to. If you don't set this explicitly, the producer assigns the partition automatically.
        ///     - headers: The headers of the message.
        ///     - value: The message body.
        public init(
            topic: KafkaTopic,
            partition: KafkaPartition = .unassigned,
            headers: [KafkaHeader] = [],
            value: [UInt8]
        ) {
            self.init(topic: topic, partition: partition, headers: headers, key: nil, value: value)
        }

        /// Creates a producer message with a key and value as strings, sent as their UTF-8 bytes.
        ///
        /// - Parameters:
        ///     - topic: The topic the producer sends the message to. The ``KafkaProducer`` creates the topic if it doesn't exist.
        ///     - partition: The topic partition the producer sends the message to. If you don't set this explicitly, the producer assigns the partition automatically.
        ///     - headers: The headers of the message.
        ///     - key: Guarantees that messages with the same key go to the same partition, preserving their order.
        ///     - value: The message's value.
        public init(
            topic: KafkaTopic,
            partition: KafkaPartition = .unassigned,
            headers: [KafkaHeader] = [],
            key: String?,
            value: String
        ) {
            self.init(
                topic: topic,
                partition: partition,
                headers: headers,
                key: key.map { Array($0.utf8) },
                value: Array(value.utf8)
            )
        }

        /// Creates a producer message with a value as a string, sent as its UTF-8 bytes, and no key.
        ///
        /// - Parameters:
        ///     - topic: The topic the producer sends the message to. The ``KafkaProducer`` creates the topic if it doesn't exist.
        ///     - partition: The topic partition the producer sends the message to. If you don't set this explicitly, the producer assigns the partition automatically.
        ///     - headers: The headers of the message.
        ///     - value: The message body.
        public init(
            topic: KafkaTopic,
            partition: KafkaPartition = .unassigned,
            headers: [KafkaHeader] = [],
            value: String
        ) {
            self.init(topic: topic, partition: partition, headers: headers, key: nil, value: Array(value.utf8))
        }
    }
}
