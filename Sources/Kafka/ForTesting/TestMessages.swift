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
import NIOCore

import struct Foundation.Date
import struct Foundation.UUID

/// An error thrown by the SPI test helpers when delivery report verification fails.
@_spi(Internal)
public enum _TestMessagesError: Error {
    /// The set of received delivery report IDs doesn't match the IDs of the messages that were sent.
    case deliveryReportsIdsIncorrect
    /// The number of acknowledged messages doesn't match the number of messages that were sent.
    case deliveryReportsNotAllMessagesAcknoledged
    /// One or more delivery reports reference content that doesn't match the original messages.
    case deliveryReportsIncorrect
}

/// Builds an array of test producer messages with unique keys for the given topic.
@_spi(Internal)
public func _createTestMessages(
    topic: String,
    headers: [KafkaHeader] = [],
    count: UInt
) -> [KafkaProducerMessage<String, String>] {
    Array(0..<count).map {
        KafkaProducerMessage(
            topic: topic,
            headers: headers,
            key: UUID().uuidString,
            value: "Hello, World! \($0) - \(Date().description)"
        )
    }
}

/// Sends the given messages and verifies that each one is acknowledged via the producer's events sequence.
@_spi(Internal)
public func _sendAndAcknowledgeMessages(
    producer: KafkaProducer,
    events: KafkaProducerEvents,
    messages: [KafkaProducerMessage<String, String>],
    skipConsistencyCheck: Bool = false
) async throws {
    var messageIDs = Set<KafkaProducerMessageID>()
    messageIDs.reserveCapacity(messages.count)

    for message in messages {
        while true {
            do {
                messageIDs.insert(try producer.send(message))
                break
            } catch let error as KafkaError where error.description.contains("Queue full") {
                // That means we have to flush queue immediately but there is no interface for that
                // producer.flush()
            }
        }
    }

    var receivedDeliveryReports = Set<KafkaDeliveryReport>()
    receivedDeliveryReports.reserveCapacity(messages.count)

    for await event in events {
        switch event {
        case .deliveryReports(let deliveryReports):
            for deliveryReport in deliveryReports {
                receivedDeliveryReports.insert(deliveryReport)
            }
        default:
            break  // Ignore any other events
        }

        if receivedDeliveryReports.count >= messages.count {
            break
        }
    }

    guard Set(receivedDeliveryReports.map(\.id)) == messageIDs else {
        throw _TestMessagesError.deliveryReportsIdsIncorrect
    }

    let acknowledgedMessages: [KafkaAcknowledgedMessage] = receivedDeliveryReports.compactMap {
        guard case .acknowledged(let receivedMessage) = $0.status else {
            return nil
        }
        return receivedMessage
    }

    guard messages.count == acknowledgedMessages.count else {
        throw _TestMessagesError.deliveryReportsNotAllMessagesAcknoledged
    }
    if skipConsistencyCheck {
        return
    }
    for message in messages {
        guard acknowledgedMessages.contains(where: { $0.topic == message.topic }),
            acknowledgedMessages.contains(where: { $0.key == ByteBuffer(string: message.key!) }),
            acknowledgedMessages.contains(where: { $0.value == ByteBuffer(string: message.value) })
        else {
            throw _TestMessagesError.deliveryReportsIncorrect
        }
    }
}
