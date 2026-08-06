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

extension KafkaProducer {
    /// An identifier for a message produced by a Kafka producer.
    ///
    /// Use a ``KafkaProducer/MessageID`` to correlate an incoming ``KafkaProducer/DeliveryReport`` with the
    /// corresponding ``KafkaProducer/send(_:)`` call that produced it.
    public struct MessageID {
        internal var rawValue: UInt

        internal init(rawValue: UInt) {
            self.rawValue = rawValue
        }
    }
}

// MARK: - KafkaProducer.MessageID + CustomStringConvertible

extension KafkaProducer.MessageID: CustomStringConvertible {
    /// A textual representation of the producer message identifier.
    public var description: String {
        String(self.rawValue)
    }
}

// MARK: - KafkaProducer.MessageID + Hashable

extension KafkaProducer.MessageID: Hashable {}

// MARK: - KafkaProducer.MessageID + Comparable

extension KafkaProducer.MessageID: Comparable {
    /// Returns a Boolean value that indicates whether the first identifier is ordered before the second.
    public static func < (lhs: KafkaProducer.MessageID, rhs: KafkaProducer.MessageID) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

// MARK: - KafkaProducer.MessageID + Sendable

extension KafkaProducer.MessageID: Sendable {}
