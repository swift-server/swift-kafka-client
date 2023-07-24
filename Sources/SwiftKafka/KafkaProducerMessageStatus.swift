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

/// Represents the status of a Kafka producer message after attempting to send it.
public enum KafkaProducerMessageStatus: Sendable, Hashable {
    /// The message has been successfully acknowledged by the Kafka cluster.
    case acknowledged(message: KafkaAcknowledgedMessage)
    /// The message failed to be acknowledged by the Kafka cluster and encountered an error.
    case failure(KafkaAcknowledgedMessageError)

    internal init?(messagePointer: UnsafePointer<rd_kafka_message_t>?) {
        guard let messagePointer else {
            return nil
        }

        let messageID = KafkaProducerMessageID(rawValue: UInt(bitPattern: messagePointer.pointee._private))

        do {
            let message = try KafkaAcknowledgedMessage(messagePointer: messagePointer, id: messageID)
            self = .acknowledged(message: message)
        } catch {
            guard let error = error as? KafkaAcknowledgedMessageError else {
                fatalError("Caught error that is not of type \(KafkaAcknowledgedMessageError.self)")
            }
            self = .failure(error)
        }
    }
}
