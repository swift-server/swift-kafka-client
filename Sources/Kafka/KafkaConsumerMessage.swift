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
import NIOCore

/// The type of timestamp on a Kafka message.
public struct KafkaTimestampType: Hashable, Sendable, CustomStringConvertible {
    /// The raw value corresponding to the librdkafka timestamp type.
    public let rawValue: Int32

    public init(rawValue: Int32) {
        self.rawValue = rawValue
    }

    /// Timestamp not available.
    public static let notAvailable = KafkaTimestampType(
        rawValue: Int32(RD_KAFKA_TIMESTAMP_NOT_AVAILABLE.rawValue)
    )
    /// Timestamp set by the producer (message creation time).
    public static let createTime = KafkaTimestampType(
        rawValue: Int32(RD_KAFKA_TIMESTAMP_CREATE_TIME.rawValue)
    )
    /// Timestamp set by the broker (log append time).
    public static let logAppendTime = KafkaTimestampType(
        rawValue: Int32(RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME.rawValue)
    )

    public var description: String {
        switch self {
        case .createTime: return "createTime"
        case .logAppendTime: return "logAppendTime"
        case .notAvailable: return "notAvailable"
        default: return "unknown(\(self.rawValue))"
        }
    }
}

/// A message received from the Kafka cluster.
public struct KafkaConsumerMessage {
    /// The topic that the message was received from.
    public var topic: String
    /// The partition that the message was received from.
    public var partition: KafkaPartition
    /// The headers of the message.
    public var headers: [KafkaHeader]
    /// The key of the message.
    public var key: ByteBuffer?
    /// The body of the message.
    public var value: ByteBuffer
    /// The offset of the message in its partition.
    public var offset: KafkaOffset
    /// The timestamp of the message in milliseconds since epoch, or `nil` if not available.
    public var timestamp: Int64?
    /// The type of timestamp on this message.
    public var timestampType: KafkaTimestampType

    /// Initialize ``KafkaConsumerMessage`` from `rd_kafka_message_t` pointer.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
    internal init(messagePointer: UnsafePointer<rd_kafka_message_t>) throws {
        let rdKafkaMessage = messagePointer.pointee

        guard let valuePointer = rdKafkaMessage.payload else {
            fatalError("Could not resolve payload of consumer message")
        }

        let valueBufferPointer = UnsafeRawBufferPointer(start: valuePointer, count: rdKafkaMessage.len)

        guard rdKafkaMessage.err == RD_KAFKA_RESP_ERR_NO_ERROR else {
            var errorStringBuffer = ByteBuffer(bytes: valueBufferPointer)
            let errorString = errorStringBuffer.readString(length: errorStringBuffer.readableBytes)

            if let errorString {
                throw KafkaError.messageConsumption(reason: errorString)
            } else {
                throw KafkaError.rdKafkaError(wrapping: rdKafkaMessage.err)
            }
        }

        guard let topic = String(validatingCString: rd_kafka_topic_name(rdKafkaMessage.rkt)) else {
            fatalError("Received topic name that is non-valid UTF-8")
        }

        self.topic = topic

        self.partition = KafkaPartition(rawValue: Int(rdKafkaMessage.partition))

        self.headers = try RDKafkaClient.getHeaders(for: messagePointer)

        if let keyPointer = rdKafkaMessage.key {
            let keyBufferPointer = UnsafeRawBufferPointer(
                start: keyPointer,
                count: rdKafkaMessage.key_len
            )
            self.key = .init(bytes: keyBufferPointer)
        } else {
            self.key = nil
        }

        self.value = ByteBuffer(bytes: valueBufferPointer)

        self.offset = KafkaOffset(rawValue: Int(rdKafkaMessage.offset))

        // Extract timestamp and type
        var tsType = rd_kafka_timestamp_type_t(rawValue: 0)
        let tsValue = rd_kafka_message_timestamp(messagePointer, &tsType)
        self.timestampType = KafkaTimestampType(rawValue: Int32(tsType.rawValue))
        self.timestamp = tsValue != -1 ? tsValue : nil
    }
}

// MARK: - KafkaConsumerMessage + Hashable

extension KafkaConsumerMessage: Hashable {}

// MARK: - KafkaConsumerMessage + Sendable

extension KafkaConsumerMessage: Sendable {}
