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

extension KafkaConsumer {
    /// An event reported by a Kafka consumer, such as a rebalance notification or an error.
    ///
    /// Delivered through ``KafkaConsumer/Events``.
    @nonexhaustive
    public enum Event: Sendable, Hashable {
        /// A consumer group rebalance occurred.
        ///
        /// The library has already performed the necessary assign/unassign
        /// operations — this notification is informational. Use it to perform
        /// application-level bookkeeping such as committing offsets on revoke
        /// or initializing state on assign.
        case rebalance(Rebalance)

        /// An error reported by the Kafka client (for example, broker disconnection or authentication failure).
        case error(KafkaError)
    }
}
