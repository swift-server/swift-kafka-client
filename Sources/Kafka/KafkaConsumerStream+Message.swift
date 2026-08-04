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
import struct NIOCore.ByteBuffer
import struct Foundation.Date
import typealias Foundation.TimeInterval

extension KafkaConsumerStream {
    public struct Message: ~Escapable {
        @usableFromInline
        let messagePointer: UnsafePointer<rd_kafka_message_t>

        // MARK: - Nested types

        /// A non-escapable, zero-copy view over the headers of a ``KafkaConsumerMessage``.
        ///
        /// Valid only for the duration of the enclosing ``withHeaders(_:)`` call.
        /// Provides `count`, `isEmpty`, and subscript access without allocating a `[KafkaHeader]` array.
        public struct Headers: ~Escapable {
            @usableFromInline
            let pointer: OpaquePointer?  // rd_kafka_headers_t*, nil when there are no headers

            @_lifetime(immortal)
            @usableFromInline
            init(_ pointer: OpaquePointer?) {
                self.pointer = pointer
            }

            /// The number of headers in the message.
            @inlinable
            public var count: Int {
                guard let pointer else { return 0 }
                return rd_kafka_header_cnt(pointer)
            }

            /// `true` if the message has no headers.
            @inlinable
            public var isEmpty: Bool { count == 0 }

            /// Returns a ``Header`` view for the header at `index`.
            ///
            /// - Precondition: `index >= 0 && index < count`
            @inlinable
            public subscript(index: Int) -> Header {
                @_lifetime(borrow self)
                get {
                    guard let pointer else {
                        preconditionFailure("index \(index) out of range: Headers is empty")
                    }
                    var namePointer: UnsafePointer<CChar>?
                    var valuePointer: UnsafeRawPointer?
                    var valueSize = 0
                    let err = rd_kafka_header_get_all(pointer, index, &namePointer, &valuePointer, &valueSize)
                    guard err == RD_KAFKA_RESP_ERR_NO_ERROR, let namePointer else {
                        preconditionFailure("Failed to read Kafka header at index \(index): \(err)")
                    }
                    let header = Header(name: namePointer, value: valuePointer, valueSize: valueSize)
                    return _overrideLifetime(header, borrowing: self)
                }
            }
        }

        /// A non-escapable, zero-copy view over a single Kafka header's name and value bytes.
        ///
        /// Obtained via ``Headers/subscript(_:)``; valid only within the enclosing ``withHeaders(_:)`` call.
        public struct Header: ~Escapable {
            @usableFromInline
            let _name: UnsafePointer<CChar>
            @usableFromInline
            let value: UnsafeRawPointer?
            @usableFromInline
            let valueSize: Int
            
            @_lifetime(immortal)
            @usableFromInline
            init(name: UnsafePointer<CChar>, value: UnsafeRawPointer?, valueSize: Int) {
                self._name = name
                self.value = value
                self.valueSize = valueSize
            }
            
            /// The header name, copied into a `String`.
            @inlinable
            public var name: String {
                String(cString: _name)
            }
            
            /// The header name as a zero-copy UTF-8 span.
            ///
            /// librdkafka header names are always null-terminated and valid UTF-8.
            @available(macOS 26.0, *)
            @inlinable
            public var nameBytes: UTF8Span {
                @_lifetime(borrow self)
                get {
                    let nameUInt8 = UnsafeRawPointer(_name).assumingMemoryBound(to: UInt8.self)
                    let buf = UnsafeBufferPointer<UInt8>(start: nameUInt8, count: strlen(_name))
                    let span = Span<UInt8>(_unsafeElements: buf)
                    let utf8 = try! UTF8Span(validating: span)
                    return _overrideLifetime(utf8, borrowing: self)
                }
            }
            
            /// The header value as raw bytes, or `nil` if the header has no value.
            @inlinable
            public var valueBytes: RawSpan? {
                @_lifetime(borrow self)
                get {
                    guard let value, valueSize > 0 else { return nil }
                    let buf = UnsafeRawBufferPointer(start: value, count: valueSize)
                    return _overrideLifetime(RawSpan(_unsafeBytes: buf), borrowing: self)
                }
            }
        }

        public enum Timestamp: Sendable, Hashable {
            case createTime(Date)
            case logAppendTime(Date)
        }

        init(messagePointer: UnsafePointer<rd_kafka_message_t>) {
            self.messagePointer = messagePointer
        }

        /// If ``true``, means it is not a message but tombstone.
        @inlinable
        public var tombstone: Bool {
            messagePointer.pointee.payload == nil
        }

        /// The topic that the message was received from.
        @inlinable
        public var topic: String {
            guard let topic = String(validatingCString: rd_kafka_topic_name(messagePointer.pointee.rkt)) else {
                fatalError("Received topic name that is non-valid UTF-8")
            }
            return topic
        }

        /// The partition that the message was received from.
        @inlinable
        public var partition: KafkaPartition {
            KafkaPartition(rawValue: Int(messagePointer.pointee.partition))
        }

        /// The offset of the message in its partition.
        @inlinable
        public var offset: KafkaOffset {
            KafkaOffset(rawValue: Int(messagePointer.pointee.offset))
        }

        /// The key of the message.
        @inlinable
        public var key: ByteBuffer? {
            let msg = messagePointer.pointee
            guard msg.key_len > 0 else { return nil }
            return ByteBuffer(bytes: UnsafeRawBufferPointer(start: msg.key, count: msg.key_len))
        }

        /// The body of the message.
        @inlinable
        public var value: ByteBuffer? {
            guard !tombstone else { return nil }
            let msg = messagePointer.pointee
            guard let payload = msg.payload else { return nil }
            return ByteBuffer(bytes: UnsafeRawBufferPointer(start: payload, count: msg.len))
        }

        /// Calls `body` with a ``RawSpan`` over the raw payload bytes of the message,
        /// avoiding a copy. The span is only valid for the duration of the call.
        ///
        /// Returns an empty span for messages with no payload.
        @inlinable
        @discardableResult
        public func withValueBytes<Result>(_ body: (RawSpan) throws -> Result) rethrows -> Result {
            let msg = messagePointer.pointee
            guard let payload = msg.payload else {
                return try body(RawSpan(_unsafeBytes: UnsafeRawBufferPointer(start: nil, count: 0)))
            }
            return try body(RawSpan(_unsafeBytes: UnsafeRawBufferPointer(start: payload, count: msg.len)))
        }

        /// Calls `body` with an optional ``RawSpan`` over the raw key bytes of the message,
        /// avoiding a copy. The span is only valid for the duration of the call.
        ///
        /// Passes `nil` if the message has no key.
        @inlinable
        @discardableResult
        public func withKeyBytes<Result>(_ body: (RawSpan?) throws -> Result) rethrows -> Result {
            let msg = messagePointer.pointee
            guard msg.key_len > 0 else { return try body(nil) }
            return try body(RawSpan(_unsafeBytes: UnsafeRawBufferPointer(start: msg.key, count: msg.key_len)))
        }

        /*
        /// The headers of the message, copied into an array.
        public var headers: [KafkaHeader] {
            return (try? Self.extractHeaders(fromMessage: messagePointer)) ?? []
        }
        */
        
        /// Calls `body` with a zero-copy ``Headers`` view over the message's headers.
        ///
        /// The ``Headers`` value — and any ``Header`` or ``RawSpan`` derived from it —
        /// must not escape the closure.
        @inlinable
        @discardableResult
        public func withHeaders<R>(_ body: (borrowing Headers) throws -> R) rethrows -> R {
            var headersPtr: OpaquePointer?
            let status = rd_kafka_message_headers(messagePointer, &headersPtr)
            guard status == RD_KAFKA_RESP_ERR_NO_ERROR || status == RD_KAFKA_RESP_ERR__NOENT else {
                return try body(_overrideLifetime(Headers(nil), borrowing: self))
            }
            return try body(_overrideLifetime(Headers(headersPtr), borrowing: self))
        }

        /// The timestamp of the Kafka message (see `rd_kafka_message_timestamp()`), if available.
        @inlinable
        public var timestamp: Timestamp? {
            var timestampType = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE
            let kafkaTimestamp = rd_kafka_message_timestamp(messagePointer, &timestampType)
            guard kafkaTimestamp != -1 else { return nil }
            if timestampType == RD_KAFKA_TIMESTAMP_CREATE_TIME {
                return .createTime(.init(timeIntervalSince1970: TimeInterval(kafkaTimestamp) / 1000.0))
            } else if timestampType == RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME {
                return .logAppendTime(.init(timeIntervalSince1970: TimeInterval(kafkaTimestamp) / 1000.0))
            }
            return nil
        }
    }
}
