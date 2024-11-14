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

/// ID of message produced by the ``KafkaProducer``.
/// The ``KafkaProducerMessageID`` can be used to relate incoming ``KafkaDeliveryReport``s
/// with their corresponding ``KafkaProducer/send(_:)`` invocation.
public struct KafkaProducerMessageID {
    internal var rawValue: UInt

    internal init(rawValue: UInt) {
        self.rawValue = rawValue
    }
}

// MARK: - KafkaProducerMessageID + CustomStringConvertible

extension KafkaProducerMessageID: CustomStringConvertible {
    public var description: String {
        String(self.rawValue)
    }
}

// MARK: - KafkaProducerMessageID + Hashable

extension KafkaProducerMessageID: Hashable {}

// MARK: - KafkaProducerMessageID + Comparable

extension KafkaProducerMessageID: Comparable {
    public static func < (lhs: KafkaProducerMessageID, rhs: KafkaProducerMessageID) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

// MARK: - KafkaProducerMessageID + Sendable

extension KafkaProducerMessageID: Sendable {}
