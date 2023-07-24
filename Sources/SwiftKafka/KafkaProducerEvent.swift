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

/// An enumeration representing events that can be received through the ``KafkaProducerEvents`` asynchronous sequence.
public enum KafkaProducerEvent: Hashable, Sendable {
    /// A delivery report received from the Kafka cluster indicating the status of a produced message.
    ///
    /// Parameters:
    ///    - results: Array of message acknowledgement results.
    case deliveryReport(results: [Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>])
    /// - Important: Always provide a `default` case when switiching over this `enum`.
    case DO_NOT_SWITCH_OVER_THIS_EXHAUSITVELY

    /// Down-cast ``RDKafkaClient/KafkaEvent`` to ``KafkaProducerEvent``.
    static func fromKafkaEvent(_ event: RDKafkaClient.KafkaEvent) -> KafkaProducerEvent {
        switch event {
        case .deliveryReport(results: let results):
            return .deliveryReport(results: results)
        case .consumerMessages:
            fatalError("Cannot cast \(event) to KafkaProducerEvent")
        }
    }
}
