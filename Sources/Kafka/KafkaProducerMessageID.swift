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

/// An identifier for a message produced by a ``KafkaProducer``.
///
/// Use a ``KafkaProducerMessageID`` to correlate an incoming ``KafkaDeliveryReport`` with the
/// corresponding ``KafkaProducer/send(_:)`` call that produced it.
public struct KafkaProducerMessageID {
    internal var rawValue: UInt

    internal init(rawValue: UInt) {
        self.rawValue = rawValue
    }
}

// MARK: - KafkaProducerMessageID + CustomStringConvertible

extension KafkaProducerMessageID: CustomStringConvertible {
    /// A textual representation of the producer message identifier.
    public var description: String {
        String(self.rawValue)
    }
}

// MARK: - KafkaProducerMessageID + Hashable

extension KafkaProducerMessageID: Hashable {}

// MARK: - KafkaProducerMessageID + Comparable

extension KafkaProducerMessageID: Comparable {
    /// Returns a Boolean value that indicates whether the first identifier is ordered before the second.
    public static func < (lhs: KafkaProducerMessageID, rhs: KafkaProducerMessageID) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

// MARK: - KafkaProducerMessageID + Sendable

extension KafkaProducerMessageID: Sendable {}
