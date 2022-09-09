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
public struct KafkaConsumerMessage: Hashable {
    /// The topic that the message was sent to.
    public var topic: String
    /// The partition that the message was sent to.
    public var partition: KafkaPartition
    /// The key of the message.
    public var key: ByteBuffer?
    /// The body of the message.
    public var value: ByteBuffer
    /// The offset of the message in its partition.
    public var offset: Int64

    /// Initialize `KafkaAckedMessage` from `rd_kafka_message_t` pointer.
    init(messagePointer: UnsafePointer<rd_kafka_message_t>) throws {
        let rdKafkaMessage = messagePointer.pointee

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

        guard let valuePointer = rdKafkaMessage.payload else {
            fatalError("Could not resolve payload of acknowledged message")
        }

        let valueBufferPointer = UnsafeRawBufferPointer(start: valuePointer, count: rdKafkaMessage.len)
        self.value = .init(bytes: valueBufferPointer)

        self.offset = Int64(rdKafkaMessage.offset)

        guard rdKafkaMessage.err == RD_KAFKA_RESP_ERR_NO_ERROR else {
            var errorStringBuffer = ByteBuffer(bytes: valueBufferPointer)
            let errorString = errorStringBuffer.readString(length: errorStringBuffer.readableBytes)

            // TODO: what to do with error string?
            // TODO: handle errors here or in consumer?
            throw KafkaError(rawValue: rdKafkaMessage.err.rawValue)
        }
    }
}
