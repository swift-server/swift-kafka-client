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
import NIOCore

/// Message that is sent by the `KafkaProducer`
public struct KafkaProducerMessage<Key: KafkaBuffer, Value: KafkaBuffer> {
    public var topic: String
    public var partition: KafkaPartition
    public var key: Key?
    public var value: Value

    /// Create a new `KafkaProducerMessage` with a `ByteBuffer` key and value
    /// - Parameter topic: The topic the message will be sent to. Topics may be created by the `KafkaProducer` if non-existent.
    /// - Parameter partition: The topic partition the message will be sent to. If not set explicitly, the partiotion will be assigned automatically.
    /// - Parameter key: Used to guarantee that messages with the same key will be sent to the same partittion so that their order is preserved.
    /// - Parameter value: The message body.
    public init(
        topic: String,
        partition: KafkaPartition? = nil,
        key: Key? = nil,
        value: Value
    ) {
        self.topic = topic
        self.key = key
        self.value = value

        if let partition = partition {
            self.partition = partition
        } else {
            self.partition = .unassigned
        }
    }
}
