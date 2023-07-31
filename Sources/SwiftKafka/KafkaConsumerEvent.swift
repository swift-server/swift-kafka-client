//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// An enumeration representing events that can be received through the ``KafkaConsumerEvents`` asynchronous sequence.
public enum KafkaConsumerEvent: Sendable, Hashable {
    /// Statistics from librdkafka
    case statistics(KafkaStatistics)
    /// - Important: Always provide a `default` case when switiching over this `enum`.
    case DO_NOT_SWITCH_OVER_THIS_EXHAUSITVELY

    internal init?(_ event: RDKafkaClient.KafkaEvent) {
        switch event {
        case .statistics(let stat):
            self = .statistics(stat)
        case .deliveryReport:
            fatalError("Cannot cast \(event) to KafkaConsumerEvent")
        case .consumerMessages:
            fatalError("Consumer messages should be handled in the KafkaConsumerMessages asynchronous sequence")
        }
    }
}
