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

/// An enumeration representing events that can be received through the ``KafkaConsumerEvents`` asynchronous sequence.
public enum KafkaConsumerEvent: Sendable, Hashable {
    /// A consumer group rebalance occurred.
    ///
    /// The library has already performed the necessary assign/unassign
    /// operations — this notification is informational. Use it to perform
    /// application-level bookkeeping such as committing offsets on revoke
    /// or initializing state on assign.
    case rebalance(KafkaConsumerRebalance)

    /// An error reported by the Kafka client (e.g., broker disconnection, authentication failure).
    case error(KafkaError)

    /// - Important: Always provide a `default` case when switching over this `enum`.
    case DO_NOT_SWITCH_OVER_THIS_EXHAUSITVELY
}
