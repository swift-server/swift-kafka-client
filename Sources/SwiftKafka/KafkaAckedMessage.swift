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
public struct KafkaAckedMessage {
    let topic: String
    let partition: Int32
    let key: ByteBuffer?
    let value: ByteBuffer
    let offset: Int64

    /// Initialize `KafkaAckedMessage` from `rd_kafka_message_t` pointer.
    init(messagePointer: UnsafePointer<rd_kafka_message_t>) throws {
        let rdKafkaMessage = messagePointer.pointee

        guard rdKafkaMessage.err.rawValue == 0 else {
            throw KafkaError(error: rdKafkaMessage.err)
        }

        guard let topic = String(validatingUTF8: rd_kafka_topic_name(rdKafkaMessage.rkt)) else {
            throw KafkaError(description: "Topic name not UTF8 encoded")
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

        guard let valuePointer = rdKafkaMessage.payload else {
            throw KafkaError(description: "Message payload could not be read")
        }

        let valueBufferPointer = UnsafeRawBufferPointer(start: valuePointer, count: rdKafkaMessage.len)
        self.value = .init(bytes: valueBufferPointer)

        self.offset = Int64(rdKafkaMessage.offset)
    }
}
