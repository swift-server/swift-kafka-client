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

/// An enumeration representing events that can be received through the ``KafkaProducerEvents`` asynchronous sequence.
public enum KafkaProducerEvent: Sendable, Hashable {
    /// A collection of delivery reports received from the Kafka cluster indicating the status of produced messages.
    case deliveryReports([KafkaDeliveryReport])
    /// - Important: Always provide a `default` case when switching over this `enum`.
    case DO_NOT_SWITCH_OVER_THIS_EXHAUSITVELY

    internal init(_ event: RDKafkaClient.KafkaEvent) {
        switch event {
        case .deliveryReport(results: let results):
            self = .deliveryReports(results)
        case .statistics:
            fatalError("Cannot cast \(event) to KafkaProducerEvent")
        }
    }
}
