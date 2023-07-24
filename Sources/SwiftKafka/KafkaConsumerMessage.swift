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
import NIOCore

/// A message received from the Kafka cluster.
public struct KafkaConsumerMessage {
    /// The topic that the message was received from.
    public var topic: String
    /// The partition that the message was received from.
    public var partition: KafkaPartition
    /// The key of the message.
    public var key: ByteBuffer?
    /// The body of the message.
    public var value: ByteBuffer
    /// The offset of the message in its partition.
    public var offset: Int

    /// Initialize ``KafkaConsumerMessage`` from `rd_kafka_message_t` pointer.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
    init(messagePointer: UnsafePointer<rd_kafka_message_t>) throws {
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

        guard let topic = String(validatingUTF8: rd_kafka_topic_name(rdKafkaMessage.rkt)) else {
            fatalError("Received topic name that is non-valid UTF-8")
        }
        self.topic = topic

        self.partition = KafkaPartition(rawValue: rdKafkaMessage.partition)

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

        self.offset = Int(rdKafkaMessage.offset)
    }
}

// MARK: - KafkaConsumerMessage + Hashable

extension KafkaConsumerMessage: Hashable {}

// MARK: - KafkaConsumerMessage + Sendable

extension KafkaConsumerMessage: Sendable {}
