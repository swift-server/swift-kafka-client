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
import struct Foundation.Date
import NIOCore

@_spi(Internal)
public enum _TestMessagesError: Error {
    case deliveryReportsIdsIncorrect
    case deliveryReportsNotAllMessagesAcknoledged
    case deliveryReportsIncorrect
}

@_spi(Internal)
public func _createTestMessages(
    topic: String,
    headers: [KafkaHeader] = [],
    count: UInt
) -> [KafkaProducerMessage<String, String>] {
    return Array(0..<count).map {
        KafkaProducerMessage(
            topic: topic,
            headers: headers,
            key: "key",
            value: "Hello, World! \($0) - \(Date().description)"
        )
    }
}

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
            break // Ignore any other events
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
              acknowledgedMessages.contains(where: { $0.value == ByteBuffer(string: message.value) }) else {
            throw _TestMessagesError.deliveryReportsIncorrect
        }
    }
}
