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

/// A message acknowledged by the Kafka cluster.
public struct KafkaAcknowledgedMessage {
    /// The topic that the message was sent to.
    public var topic: String
    /// The partition that the message was sent to.
    public var partition: KafkaPartition
    /// The key of the message.
    public var key: ByteBuffer?
    /// The body of the message.
    public var value: ByteBuffer
    /// The offset of the message in its partition.
    public var offset: KafkaOffset

    /// Initialize ``KafkaAcknowledgedMessage`` from `rd_kafka_message_t` pointer.
    /// - Throws: A ``KafkaAcknowledgedMessageError`` for failed acknowledgements or malformed messages.
    internal init(messagePointer: UnsafePointer<rd_kafka_message_t>) throws {
        let rdKafkaMessage = messagePointer.pointee

        let valueBufferPointer = UnsafeRawBufferPointer(start: rdKafkaMessage.payload, count: rdKafkaMessage.len)
        self.value = ByteBuffer(bytes: valueBufferPointer)

        guard rdKafkaMessage.err == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.rdKafkaError(wrapping: rdKafkaMessage.err)
        }

        #if swift(>=6.0)
        guard let topic = String(validatingCString: rd_kafka_topic_name(rdKafkaMessage.rkt)) else {
            fatalError("Received topic name that is non-valid UTF-8")
        }
        #else
        guard let topic = String(validatingUTF8: rd_kafka_topic_name(rdKafkaMessage.rkt)) else {
            fatalError("Received topic name that is non-valid UTF-8")
        }
        #endif

        self.topic = topic

        self.partition = KafkaPartition(rawValue: Int(rdKafkaMessage.partition))

        if let keyPointer = rdKafkaMessage.key {
            let keyBufferPointer = UnsafeRawBufferPointer(
                start: keyPointer,
                count: rdKafkaMessage.key_len
            )
            self.key = .init(bytes: keyBufferPointer)
        } else {
            self.key = nil
        }

        self.offset = KafkaOffset(rawValue: Int(rdKafkaMessage.offset))
    }
}

// MARK: KafkaAcknowledgedMessage + Hashable

extension KafkaAcknowledgedMessage: Hashable {}

// MARK: KafkaAcknowledgedMessage + Sendable

extension KafkaAcknowledgedMessage: Sendable {}
