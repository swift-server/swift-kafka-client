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

/// Describes a consumer group rebalance event.
///
/// When a consumer joins or leaves a consumer group, Kafka redistributes
/// partitions among the group members. The library handles the assign/unassign
/// operations automatically — this event is informational only.
public struct KafkaConsumerRebalance: Sendable, Hashable {
    /// The kind of rebalance that occurred.
    public enum Kind: Sendable, Hashable {
        /// New partitions have been assigned to this consumer.
        case assign
        /// Partitions have been revoked from this consumer.
        case revoke
        /// An unexpected error occurred during rebalance. All partitions have been
        /// unassigned as a recovery measure. The associated string describes the error.
        case error(String)
    }

    /// Whether this is an assignment, revocation, or error.
    public let kind: Kind

    /// The partitions involved in this rebalance.
    ///
    /// For ``Kind/assign``: the partitions newly assigned to this consumer.
    /// For ``Kind/revoke``: the partitions being revoked from this consumer.
    /// For ``Kind/error(_:)``: empty.
    public let partitions: [KafkaTopicPartition]

    /// Create a rebalance event description.
    /// - Parameters:
    ///   - kind: Whether this is an assignment, revocation, or error.
    ///   - partitions: The partitions involved in this rebalance.
    public init(kind: Kind, partitions: [KafkaTopicPartition]) {
        self.kind = kind
        self.partitions = partitions
    }
}
