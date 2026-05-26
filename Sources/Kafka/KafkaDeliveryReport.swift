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

import Crdkafka

/// A delivery report for a message the producer sent to the Kafka cluster.
public struct KafkaDeliveryReport: Sendable, Hashable {
    /// The outcome of a producer's attempt to deliver a message to the Kafka cluster.
    public enum Status: Sendable, Hashable {
        /// The Kafka cluster successfully acknowledged the message.
        case acknowledged(message: KafkaAcknowledgedMessage)
        /// The Kafka cluster failed to acknowledge the message and encountered an error.
        case failure(KafkaError)
    }

    /// The ``Status`` of a Kafka producer message after attempting to send it.
    public var status: Status

    /// The unique identifier the ``KafkaProducer`` assigned when sending the message to Kafka.
    ///
    /// ``KafkaProducer/send(_:)`` returns the same identifier, which correlates
    /// a sent message with a delivery report.
    public var id: KafkaProducerMessageID

    internal init?(messagePointer: UnsafePointer<rd_kafka_message_t>?) {
        guard let messagePointer else {
            return nil
        }

        self.id = KafkaProducerMessageID(rawValue: UInt(bitPattern: messagePointer.pointee._private))

        do {
            let message = try KafkaAcknowledgedMessage(messagePointer: messagePointer)
            self.status = .acknowledged(message: message)
        } catch {
            guard let error = error as? KafkaError else {
                fatalError("Caught error that is not of type \(KafkaError.self)")
            }
            self.status = .failure(error)
        }
    }
}
