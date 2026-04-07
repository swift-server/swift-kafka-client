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
#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif

extension FixedWidthInteger {
    func roundUpToMultipleOf(_ multiple: Self) -> Self {
        ((self + multiple - 1) / multiple) * multiple
    }
}

/// A message received from the Kafka cluster.
public struct KafkaConsumerMessage {
    @usableFromInline
    final class Storage: @unchecked Sendable {
        @usableFromInline
        let messagePointer: UnsafeMutablePointer<rd_kafka_message_t>

        init(messagePointer: UnsafeMutablePointer<rd_kafka_message_t>) {
            self.messagePointer = messagePointer

            // Force lazy init now, single-threaded, before this becomes Sendable
            var headersPtr: OpaquePointer?
            _ = rd_kafka_message_headers(messagePointer, &headersPtr)
        }

        deinit {
            rd_kafka_message_destroy(messagePointer)
        }
    }

    @usableFromInline
    let storage: Storage

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

    public enum Timestamp: Sendable, Hashable, Equatable {
        case createTime(Date)
        case logAppendTime(Date)
    }

    /// Initialize ``KafkaConsumerMessage`` from `rd_kafka_message_t` pointer.
    /// - Throws: A ``KafkaError`` if the received message is an error message or malformed.
    internal init(messagePointer: UnsafeMutablePointer<rd_kafka_message_t>) throws {
        let rdKafkaMessage = messagePointer.pointee

        guard rdKafkaMessage.err == RD_KAFKA_RESP_ERR_NO_ERROR || rdKafkaMessage.err == RD_KAFKA_RESP_ERR__PARTITION_EOF else {
            defer { rd_kafka_message_destroy(messagePointer) }
            if let valuePointer = rdKafkaMessage.payload {
                let valueBufferPointer = UnsafeRawBufferPointer(start: valuePointer, count: rdKafkaMessage.len)
                var errorStringBuffer = ByteBuffer(bytes: valueBufferPointer)
                if let errorString = errorStringBuffer.readString(length: errorStringBuffer.readableBytes) {
                    throw KafkaError.messageConsumption(reason: errorString)
                }
            }
            throw KafkaError.rdKafkaError(wrapping: rdKafkaMessage.err)
        }

        self.storage = Storage(messagePointer: messagePointer)
    }

    /// If ``true``, means it is not a message but partition EOF event.
    @inlinable
    public var eof: Bool {
        storage.messagePointer.pointee.err == RD_KAFKA_RESP_ERR__PARTITION_EOF
    }

    /// The topic that the message was received from.
    @inlinable
    public var topic: String {
        guard let topic = String(validatingCString: rd_kafka_topic_name(storage.messagePointer.pointee.rkt)) else {
            fatalError("Received topic name that is non-valid UTF-8")
        }
        return topic
    }

    /// The partition that the message was received from.
    @inlinable
    public var partition: KafkaPartition {
        KafkaPartition(rawValue: Int(storage.messagePointer.pointee.partition))
    }

    /// The offset of the message in its partition.
    @inlinable
    public var offset: KafkaOffset {
        KafkaOffset(rawValue: Int(storage.messagePointer.pointee.offset))
    }

    /// The key of the message.
    @inlinable
    public var key: ByteBuffer? {
        guard !eof else { return nil }
        let msg = storage.messagePointer.pointee
        guard msg.key_len > 0 else { return nil }
        return ByteBuffer(bytes: UnsafeRawBufferPointer(start: msg.key, count: msg.key_len))
    }

    /// The body of the message.
    @inlinable
    public var value: ByteBuffer {
        guard !eof else { return ByteBuffer() }
        let msg = storage.messagePointer.pointee
        guard let payload = msg.payload else { return ByteBuffer() }
        return ByteBuffer(bytes: UnsafeRawBufferPointer(start: payload, count: msg.len))
    }

    /// Calls `body` with a ``RawSpan`` over the raw payload bytes of the message,
    /// avoiding a copy. The span is only valid for the duration of the call.
    ///
    /// Returns an empty span for EOF messages or messages with no payload.
    @inlinable
    @discardableResult
    public func withValueBytes<Result>(_ body: (RawSpan) throws -> Result) rethrows -> Result {
        guard !eof else {
            return try body(RawSpan(_unsafeBytes: UnsafeRawBufferPointer(start: nil, count: 0)))
        }
        let msg = storage.messagePointer.pointee
        guard let payload = msg.payload else {
            return try body(RawSpan(_unsafeBytes: UnsafeRawBufferPointer(start: nil, count: 0)))
        }
        return try body(RawSpan(_unsafeBytes: UnsafeRawBufferPointer(start: payload, count: msg.len)))
    }

    /// Calls `body` with an optional ``RawSpan`` over the raw key bytes of the message,
    /// avoiding a copy. The span is only valid for the duration of the call.
    ///
    /// Passes `nil` if the message has no key or is an EOF message.
    @inlinable
    @discardableResult
    public func withKeyBytes<Result>(_ body: (RawSpan?) throws -> Result) rethrows -> Result {
        guard !eof else { return try body(nil) }
        let msg = storage.messagePointer.pointee
        guard msg.key_len > 0 else { return try body(nil) }
        return try body(RawSpan(_unsafeBytes: UnsafeRawBufferPointer(start: msg.key, count: msg.key_len)))
    }

    /// The headers of the message, copied into an array.
    public var headers: [KafkaHeader] {
        guard !eof else { return [] }
        return (try? Self.extractHeaders(fromMessage: storage.messagePointer)) ?? []
    }

    /// Calls `body` with a zero-copy ``Headers`` view over the message's headers.
    ///
    /// The ``Headers`` value — and any ``Header`` or ``RawSpan`` derived from it —
    /// must not escape the closure.
    @inlinable
    @discardableResult
    public func withHeaders<R>(_ body: (borrowing Headers) throws -> R) rethrows -> R {
        guard !eof else {
            return try body(_overrideLifetime(Headers(nil), borrowing: self))
        }
        var headersPtr: OpaquePointer?
        let status = rd_kafka_message_headers(storage.messagePointer, &headersPtr)
        guard status == RD_KAFKA_RESP_ERR_NO_ERROR || status == RD_KAFKA_RESP_ERR__NOENT else {
            return try body(_overrideLifetime(Headers(nil), borrowing: self))
        }
        return try body(_overrideLifetime(Headers(headersPtr), borrowing: self))
    }

    /// The timestamp of the Kafka message (see `rd_kafka_message_timestamp()`), if available.
    @inlinable
    public var timestamp: Timestamp? {
        guard !eof else { return nil }
        var timestampType = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE
        let kafkaTimestamp = rd_kafka_message_timestamp(storage.messagePointer, &timestampType)
        guard kafkaTimestamp != -1 else { return nil }
        if timestampType == RD_KAFKA_TIMESTAMP_CREATE_TIME {
            return .createTime(.init(timeIntervalSince1970: TimeInterval(kafkaTimestamp) / 1000.0))
        } else if timestampType == RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME {
            return .logAppendTime(.init(timeIntervalSince1970: TimeInterval(kafkaTimestamp) / 1000.0))
        }
        return nil
    }
}

// MARK: - KafkaConsumerMessage + Hashable

extension KafkaConsumerMessage: Hashable {
    @inlinable
    public static func == (lhs: KafkaConsumerMessage, rhs: KafkaConsumerMessage) -> Bool {
        lhs.topic == rhs.topic && lhs.partition == rhs.partition && lhs.offset == rhs.offset
    }

    @inlinable
    public func hash(into hasher: inout Hasher) {
        hasher.combine(topic)
        hasher.combine(partition)
        hasher.combine(offset)
    }
}

// MARK: - KafkaConsumerMessage + Sendable

extension KafkaConsumerMessage: Sendable {}

// MARK: - Helpers

extension KafkaConsumerMessage {
    static let bufferAlignment: Int = MemoryLayout<UInt64>.size

    /// Extracts headers from a `rd_kafka_message_t` pointer.
    static func extractHeaders(
        fromMessage messagePointer: UnsafePointer<rd_kafka_message_t>
    ) throws -> [KafkaHeader] {
        var headers = [KafkaHeader]()
        try Self.forEachHeader(inMessage: messagePointer) { key, value in
            let valueBuffer: ByteBuffer? = value.count > 0 ? ByteBuffer(bytes: value) : nil
            headers.append(KafkaHeader(key: String(cString: key), value: valueBuffer))
        }
        return headers
    }

    static func extractContent(
        fromMessage messagePointer: UnsafePointer<rd_kafka_message_t>
    ) throws -> (key: ByteBuffer?, value: ByteBuffer, headers: [KafkaHeader]) {
        let rdKafkaMessage = messagePointer.pointee

        let valueBufferPointer = UnsafeRawBufferPointer(start: rdKafkaMessage.payload, count: rdKafkaMessage.len)

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

        var key: ByteBuffer?
        if rdKafkaMessage.key_len > 0 {
            buffer.moveWriterIndex(to: buffer.readerIndex + rdKafkaMessage.key_len)
            key = buffer.slice()
            let newIndex = buffer.readerIndex + Int(rdKafkaMessage.key_len).roundUpToMultipleOf(Self.bufferAlignment)
            buffer.moveWriterIndex(to: newIndex)
            buffer.moveReaderIndex(to: newIndex)
        }

        buffer.moveWriterIndex(to: buffer.readerIndex + valueBufferPointer.count)

        return (key: key, value: buffer, headers: headers)
    }

    /// Iterates over ``KafkaHeader``s from a `rd_kafka_message_t` pointer
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
