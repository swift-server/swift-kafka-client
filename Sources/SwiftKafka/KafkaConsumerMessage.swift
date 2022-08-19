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
import struct Foundation.Data

/// Messages that are received from the Kafka cluster.
public struct KafkaConsumerMessage {
    let topic: String
    let partition: Int32
    let key: Data?
    let value: Data
    let offset: Int64

    // TODO: copy on write, does this even make sense as we read-only?
    // TODO: deallocate rd_kafka_message_t on deinit

    /// Initialize `KafkaConsumerMessage` from `rd_kafka_message_t` pointer.
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
            self.key = Data(
                bytes: keyPointer.assumingMemoryBound(to: UInt8.self),
                count: rdKafkaMessage.key_len
            )
        } else {
            self.key = nil
        }

        guard let valuePointer = rdKafkaMessage.payload else {
            throw KafkaError(description: "Message payload could not be read")
        }

        self.value = Data(
            bytes: valuePointer.assumingMemoryBound(to: UInt8.self),
            count: rdKafkaMessage.len
        )

        self.offset = Int64(rdKafkaMessage.offset)
    }

    /// Optional String representation of the message's key.
    public var keyString: String? {
        guard let key = self.key else {
            return nil
        }

        return String(data: key, encoding: .utf8)
    }

    /// Optional String representation of the message's value.
    public var valueString: String? {
        return String(data: value, encoding: .utf8)
    }
}
