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

/// An event received through the ``KafkaConsumerEvents`` asynchronous sequence.
///
/// Use the ``kind`` property to determine what type of event occurred,
/// and the ``partitions`` property to access the affected topic-partitions.
public struct KafkaConsumerEvent: Sendable, Hashable {
    /// The kind of consumer event.
    public struct Kind: Sendable, Hashable, CustomStringConvertible {
        fileprivate enum BackingKind {
            case assignedPartitions
            case revokedPartitions
        }

        fileprivate let backingKind: BackingKind

        fileprivate init(_ backingKind: BackingKind) {
            self.backingKind = backingKind
        }

        /// Partitions have been assigned to this consumer as part of a rebalance.
        public static let assignedPartitions = Kind(.assignedPartitions)

        /// Partitions have been revoked from this consumer as part of a rebalance.
        public static let revokedPartitions = Kind(.revokedPartitions)

        public var description: String {
            switch self.backingKind {
            case .assignedPartitions:
                return "assignedPartitions"
            case .revokedPartitions:
                return "revokedPartitions"
            }
        }
    }

    /// The kind of consumer event (assigned or revoked partitions).
    public let kind: Kind

    /// The list of topic-partitions affected by this event.
    public let partitions: [KafkaTopicPartition]

    private init(kind: Kind, partitions: [KafkaTopicPartition]) {
        self.kind = kind
        self.partitions = partitions
    }

    /// Create a new ``KafkaConsumerEvent`` indicating partitions were assigned.
    ///
    /// - Parameter partitions: The assigned topic-partitions.
    public static func assignedPartitions(_ partitions: [KafkaTopicPartition]) -> KafkaConsumerEvent {
        KafkaConsumerEvent(kind: .assignedPartitions, partitions: partitions)
    }

    /// Create a new ``KafkaConsumerEvent`` indicating partitions were revoked.
    ///
    /// - Parameter partitions: The revoked topic-partitions.
    public static func revokedPartitions(_ partitions: [KafkaTopicPartition]) -> KafkaConsumerEvent {
        KafkaConsumerEvent(kind: .revokedPartitions, partitions: partitions)
    }

}
