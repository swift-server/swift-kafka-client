//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka

/// Message offset on a Kafka partition queue.
public struct KafkaOffset: RawRepresentable {
    public var rawValue: Int

    public init(rawValue: Int) {
        self.rawValue = rawValue
    }

    /// Start consuming from the beginning of the Kafka partition queue i.e. the oldest message.
    public static let beginning = KafkaOffset(rawValue: Int(RD_KAFKA_OFFSET_BEGINNING))

    /// Start consuming from the end of the Kafka partition queue i.e. wait for next message to be produced.
    public static let end = KafkaOffset(rawValue: Int(RD_KAFKA_OFFSET_END))

    /// Start consuming from offset retrieved from offset store.
    public static let storedOffset = KafkaOffset(rawValue: Int(RD_KAFKA_OFFSET_STORED))

    /// Start consuming with the `count` latest messages.
    /// Example: Current end offset is at `12345` and `count = 200`.
    /// This means start reading offset from offset `12345 - 200 = 12145`.
    public static func tail(_ count: Int) -> KafkaOffset {
        return KafkaOffset(rawValue: Int(RD_KAFKA_OFFSET_TAIL_BASE) - count)
    }
}

// MARK: KafkaOffset + Hashable

extension KafkaOffset: Hashable {}

// MARK: KafkaOffset + Sendable

extension KafkaOffset: Sendable {}
