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
import Foundation

/// Message that is sent by the `KafkaProducer`
public struct KafkaProducerMessage {
    let topic: String
    let partition: Int32
    let key: ContiguousBytes?
    let value: ContiguousBytes

    /// Create a new `KafkaProducerMessage` with any keys and values pair that conform to the `ContiguousBytes` protocol
    /// - Parameter topic: The topic the message will be sent to. Topics may be created by the `KafkaProducer` if non-existent.
    /// - Parameter partition: The topic partition the message will be sent to. If not set explicitly, the partiotion will be assigned automatically.
    /// - Parameter key: Used to guarantee that messages with the same key will be sent to the same partittion so that their order is preserved.
    /// - Parameter value: The message body.
    public init(
        topic: String,
        partition: Int32? = nil,
        key: ContiguousBytes? = nil,
        value: ContiguousBytes
    ) {
        self.topic = topic
        self.key = key
        self.value = value

        if let partition = partition {
            self.partition = partition
        } else {
            self.partition = RD_KAFKA_PARTITION_UA
        }
    }

    /// Create a new `KafkaProducerMessage` with a `String` key and value
    /// - Parameter topic: The topic the message will be sent to. Topics may be created by the `KafkaProducer` if non-existent.
    /// - Parameter partition: The topic partition the message will be sent to. If not set explicitly, the partiotion will be assigned automatically.
    /// - Parameter key: Used to guarantee that messages with the same key will be sent to the same partittion so that their order is preserved.
    /// - Parameter value: The message body.
    public init(
        topic: String,
        partition: Int32? = nil,
        key: String? = nil,
        value: String
    ) {
        self.init(
            topic: topic,
            partition: partition,
            key: key?.data(using: .utf8),
            value: Data(value.utf8)
        )
    }
}
