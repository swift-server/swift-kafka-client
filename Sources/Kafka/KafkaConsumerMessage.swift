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
import struct Foundation.Date
import typealias Foundation.TimeInterval

extension FixedWidthInteger {
    func roundUpToMultipleOf(_ multiple: Self) -> Self {
        ((self + multiple - 1) / multiple) * multiple
    }
}

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

    public enum Timestamp: Sendable, Hashable, Equatable {
        case createTime(Date)
        case logAppendTime(Date)
    }

    /// The timestamp of the Kafka message (see `rd_kafka_message_timestamp()`), if available.
    public let timestamp: Timestamp?

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
    
    private static let bufferAlignment: Int = MemoryLayout<UInt64>.size

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

        var timestamp: Timestamp? = nil

        if rdKafkaMessage.err != RD_KAFKA_RESP_ERR__PARTITION_EOF {

            var bufferSize = 0
            var headersCount = 0
            try Self.forEachHeader(inMessage: messagePointer) { _, value in
                headersCount += 1
                bufferSize += Int(value.count).roundUpToMultipleOf(Self.bufferAlignment)
            }
            bufferSize += Int(rdKafkaMessage.key_len).roundUpToMultipleOf(Self.bufferAlignment)
            bufferSize += Int(rdKafkaMessage.len)

            var buffer = ByteBufferAllocator().buffer(capacity: bufferSize)

            try Self.forEachHeader(inMessage: messagePointer) { _, value in
                if value.count > 0 {
                    buffer.writeBytes(value)
                    let alignment = Int(value.count).roundUpToMultipleOf(Self.bufferAlignment) - value.count
                    buffer.moveWriterIndex(forwardBy: alignment)
                }
            }

            if rdKafkaMessage.key_len > 0 {
                let keyBufferPointer = UnsafeRawBufferPointer(start: rdKafkaMessage.key, count: rdKafkaMessage.key_len)
                buffer.writeBytes(keyBufferPointer)
                let alignment = Int(rdKafkaMessage.key_len).roundUpToMultipleOf(Self.bufferAlignment) - rdKafkaMessage.key_len
                buffer.moveWriterIndex(forwardBy: alignment)
            }

            buffer.writeBytes(valueBufferPointer)

            var headers = [KafkaHeader]()
            headers.reserveCapacity(headersCount)
            try Self.forEachHeader(inMessage: messagePointer) { key, value in
                let valueBuffer: ByteBuffer? = {
                    if value.count > 0 {
                        buffer.moveWriterIndex(to: buffer.readerIndex + value.count)
                        let ret = buffer.slice()
                        let newIndex = buffer.readerIndex + Int(value.count).roundUpToMultipleOf(Self.bufferAlignment)
                        buffer.moveWriterIndex(to: newIndex)
                        buffer.moveReaderIndex(to: newIndex)
                        return ret
                    } else {
                        return nil
                    }
                }()
                let header = KafkaHeader(key: String(cString: key), value: valueBuffer)
                headers.append(header)
            }
            self.headers = headers

            if rdKafkaMessage.key_len > 0 {
                buffer.moveWriterIndex(to: buffer.readerIndex + rdKafkaMessage.key_len)
                self.key = buffer.slice()
                let newIndex = buffer.readerIndex + Int(rdKafkaMessage.key_len).roundUpToMultipleOf(Self.bufferAlignment)
                buffer.moveWriterIndex(to: newIndex)
                buffer.moveReaderIndex(to: newIndex)
            } else {
                self.key = nil
            }

            buffer.moveWriterIndex(to: buffer.readerIndex + valueBufferPointer.count)
            self._value = .buffer(buffer)

            var timestampType = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE
            let kafkaTimestamp = rd_kafka_message_timestamp(messagePointer, &timestampType)
            if kafkaTimestamp != -1 {
                if timestampType == RD_KAFKA_TIMESTAMP_CREATE_TIME {
                    timestamp = .createTime(.init(timeIntervalSince1970: TimeInterval(kafkaTimestamp)/1000.0))
                } else if timestampType == RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME {
                    timestamp = .logAppendTime(.init(timeIntervalSince1970: TimeInterval(kafkaTimestamp)/1000.0))
                }
            }
        } else {
            self._value = .eof
            self.key = .init()
            self.headers = .init()
        }

        self.offset = KafkaOffset(rawValue: Int(rdKafkaMessage.offset))
        self.timestamp = timestamp
    }
}

// MARK: - KafkaConsumerMessage + Hashable

extension KafkaConsumerMessage: Hashable {}

// MARK: - KafkaConsumerMessage + Sendable

extension KafkaConsumerMessage: Sendable {}

// MARK: - Helpers

extension KafkaConsumerMessage {
    /// Iterates ``KafkaHeader``s from a `rd_kafka_message_t` pointer
    /// applying the `body` function for each header.
    ///
    /// - Parameters:
    ///    - inMessage: Pointer to the `rd_kafka_message_t` object to extract the headers from.
    ///    - body: Function to be called for each header.
    static func forEachHeader(
        inMessage messagePointer: UnsafePointer<rd_kafka_message_t>,
        _ body: (UnsafePointer<CChar>, UnsafeRawBufferPointer) -> Void
    ) throws {
        var headers: OpaquePointer?
        var readStatus = rd_kafka_message_headers(messagePointer, &headers)

        if readStatus == RD_KAFKA_RESP_ERR__NOENT {
            // No Header Entries
            return
        }

        guard readStatus == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.rdKafkaError(wrapping: readStatus)
        }

        guard let headers else {
            return
        }

        let count = rd_kafka_header_cnt(headers)
        var index = 0

        while readStatus != RD_KAFKA_RESP_ERR__NOENT && (index < count) {
            var keyPointer: UnsafePointer<CChar>?
            var valuePointer: UnsafeRawPointer?
            var valueSize = 0

            readStatus = rd_kafka_header_get_all(
                headers,
                index,
                &keyPointer,
                &valuePointer,
                &valueSize
            )

            if readStatus == RD_KAFKA_RESP_ERR__NOENT {
                // No Header Entries
                return
            }

            if readStatus != RD_KAFKA_RESP_ERR_NO_ERROR {
                throw KafkaError.rdKafkaError(wrapping: readStatus)
            }

            guard let keyPointer else {
                fatalError("Found null pointer when reading KafkaConsumerMessage header key")
            }

            body(keyPointer, UnsafeRawBufferPointer(start: valuePointer, count: valueSize))

            index += 1
        }
    }
}
