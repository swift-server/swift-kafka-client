//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka

/// A delivery report for a message that was sent to the Kafka cluster.
public struct KafkaDeliveryReport: Sendable, Hashable {
    public enum Status: Sendable, Hashable {
        /// The message has been successfully acknowledged by the Kafka cluster.
        case acknowledged(message: KafkaAcknowledgedMessage)
        /// The message failed to be acknowledged by the Kafka cluster and encountered an error.
        case failure(KafkaError)
    }

    /// The status of a Kafka producer message after attempting to send it.
    public var status: Status

    /// The unique identifier assigned by the ``KafkaProducer`` when the message was sent to Kafka.
    /// The same identifier is returned by ``KafkaProducer/send(_:)`` and can be used to correlate
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
