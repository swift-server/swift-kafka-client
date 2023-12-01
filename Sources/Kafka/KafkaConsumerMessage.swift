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

/// A message received from the Kafka cluster.
public struct KafkaConsumerMessage {
    /// Internal enum for EOF, required to allow empty message
    internal enum MessageContent: Hashable, Sendable {
        case buffer(ByteBuffer)
        case eof
    }
    
    internal var _value: MessageContent
    
    /// The topic that the message was received from.
    public var topic: String
    /// The partition that the message was received from.
    public var partition: KafkaPartition
    /// The headers of the message.
    public var headers: [KafkaHeader]
    /// The key of the message.
    public var key: ByteBuffer?
    /// The body of the message.
    public var value: ByteBuffer {
        switch _value {
        case .buffer(let byteBuffer):
            return byteBuffer
        case .eof:
            return ByteBuffer()
        }
    }
    /// The offset of the message in its partition.
    public var offset: KafkaOffset

    /// If ``true``, means it is not a message but partition EOF event
    public var eof: Bool {
        switch _value {
        case .buffer:
            return false
        case .eof:
            return true
        }
    }

    /// Initialize ``KafkaConsumerMessage`` as EOF from `rd_kafka_topic_partition_t` pointer.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
//    internal init(topicPartitionPointer: UnsafePointer<rd_kafka_topic_partition_t>) {
//        let topicPartition = topicPartitionPointer.pointee
//        guard let topic = String(validatingUTF8: topicPartition.topic) else {
//            fatalError("Received topic name that is non-valid UTF-8")
//        }
//        self.topic = topic
//        self.partition = KafkaPartition(rawValue: Int(topicPartition.partition))
//        self.offset = KafkaOffset(rawValue: Int(topicPartition.offset))
//        self.value = ByteBuffer()
//        self.headers = [KafkaHeader]()
//    }
    
    /// Initialize ``KafkaConsumerMessage`` from `rd_kafka_message_t` pointer.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
    internal init(messagePointer: UnsafePointer<rd_kafka_message_t>) throws {
        let rdKafkaMessage = messagePointer.pointee

        guard let valuePointer = rdKafkaMessage.payload else {
            fatalError("Could not resolve payload of consumer message")
        }

        let valueBufferPointer = UnsafeRawBufferPointer(start: valuePointer, count: rdKafkaMessage.len)

        guard rdKafkaMessage.err == RD_KAFKA_RESP_ERR_NO_ERROR || rdKafkaMessage.err == RD_KAFKA_RESP_ERR__PARTITION_EOF else {
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

        self.partition = KafkaPartition(rawValue: Int(rdKafkaMessage.partition))

        if rdKafkaMessage.err != RD_KAFKA_RESP_ERR__PARTITION_EOF {
            self.headers = try Self.getHeaders(for: messagePointer)
            
            if let keyPointer = rdKafkaMessage.key {
                let keyBufferPointer = UnsafeRawBufferPointer(
                    start: keyPointer,
                    count: rdKafkaMessage.key_len
                )
                self.key = .init(bytes: keyBufferPointer)
            } else {
                self.key = nil
            }
            
            self._value = .buffer(ByteBuffer(bytes: valueBufferPointer))
        } else {
            self._value = .eof
            self.key = .init()
            self.headers = .init()
        }

        self.offset = KafkaOffset(rawValue: Int(rdKafkaMessage.offset))
    }
}

// MARK: - KafkaConsumerMessage + Hashable

extension KafkaConsumerMessage: Hashable {}

// MARK: - KafkaConsumerMessage + Sendable

extension KafkaConsumerMessage: Sendable {}

// MARK: - Helpers

extension KafkaConsumerMessage {
    /// Extract ``KafkaHeader``s from a `rd_kafka_message_t` pointer.
    ///
    /// - Parameters:
    ///    - for: Pointer to the `rd_kafka_message_t` object to extract the headers from.
    private static func getHeaders(
        for messagePointer: UnsafePointer<rd_kafka_message_t>
    ) throws -> [KafkaHeader] {
        var result: [KafkaHeader] = []
        var headers: OpaquePointer?

        var readStatus = rd_kafka_message_headers(messagePointer, &headers)

        if readStatus == RD_KAFKA_RESP_ERR__NOENT {
            // No Header Entries
            return result
        }

        guard readStatus == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.rdKafkaError(wrapping: readStatus)
        }

        guard let headers else {
            return result
        }

        let headerCount = rd_kafka_header_cnt(headers)
        result.reserveCapacity(headerCount)

        var headerIndex = 0

        while readStatus != RD_KAFKA_RESP_ERR__NOENT && headerIndex < headerCount {
            var headerKeyPointer: UnsafePointer<CChar>?
            var headerValuePointer: UnsafeRawPointer?
            var headerValueSize = 0

            readStatus = rd_kafka_header_get_all(
                headers,
                headerIndex,
                &headerKeyPointer,
                &headerValuePointer,
                &headerValueSize
            )

            if readStatus == RD_KAFKA_RESP_ERR__NOENT {
                // No Header Entries
                return result
            }

            guard readStatus == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: readStatus)
            }

            guard let headerKeyPointer else {
                fatalError("Found null pointer when reading KafkaConsumerMessage header key")
            }
            let headerKey = String(cString: headerKeyPointer)

            var headerValue: ByteBuffer?
            if let headerValuePointer, headerValueSize > 0 {
                let headerValueBufferPointer = UnsafeRawBufferPointer(
                    start: headerValuePointer,
                    count: headerValueSize
                )
                headerValue = ByteBuffer(bytes: headerValueBufferPointer)
            }

            let newHeader = KafkaHeader(key: headerKey, value: headerValue)
            result.append(newHeader)

            headerIndex += 1
        }

        return result
    }
}
