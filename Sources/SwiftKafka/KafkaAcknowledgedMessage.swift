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

/// A message produced by the client and acknowledged by the Kafka cluster.
public struct KafkaAcknowledgedMessage: Hashable {
    /// The unique identifier assigned by the ``KafkaProducer`` when the message was send to Kafka.
    /// The same identifier is returned by ``KafkaProducer/sendAsync(message:)`` and can be used to correlate
    /// a sent message and an acknowledged message.
    public var id: UInt
    /// The topic that the message was sent to.
    public var topic: String
    /// The partition that the message was sent to.
    public var partition: Int32
    /// The key of the message.
    public var key: ByteBuffer?
    /// The body of the message.
    public var value: ByteBuffer
    /// The offset of the message in its partition.
    public var offset: Int64

    /// Initialize `KafkaAckedMessage` from `rd_kafka_message_t` pointer.
    init(messagePointer: UnsafePointer<rd_kafka_message_t>, id: UInt) throws {
        self.id = id

        let rdKafkaMessage = messagePointer.pointee

        let valueBufferPointer = UnsafeRawBufferPointer(start: rdKafkaMessage.payload, count: rdKafkaMessage.len)
        self.value = ByteBuffer(bytes: valueBufferPointer)

        guard rdKafkaMessage.err.rawValue == 0 else {
            var errorStringBuffer = self.value
            let errorString = errorStringBuffer.readString(length: errorStringBuffer.readableBytes)

            throw KafkaAcknowledgedMessageError(
                rawValue: rdKafkaMessage.err.rawValue,
                description: errorString,
                messageID: self.id
            )
        }

        guard let topic = String(validatingUTF8: rd_kafka_topic_name(rdKafkaMessage.rkt)) else {
            fatalError("Received topic name that is non-valid UTF-8")
        }
        self.topic = topic

        self.partition = rdKafkaMessage.partition

        if let keyPointer = rdKafkaMessage.key {
            let keyBufferPointer = UnsafeRawBufferPointer(
                start: keyPointer,
                count: rdKafkaMessage.key_len
            )
            self.key = .init(bytes: keyBufferPointer)
        } else {
            self.key = nil
        }

        self.offset = Int64(rdKafkaMessage.offset)
    }
}
