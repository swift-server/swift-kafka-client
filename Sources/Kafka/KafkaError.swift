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

/// An error that can occur on `Kafka` operations
///
/// - Note: `Hashable` conformance considers both the ``KafkaError/code``
///   and the ``KafkaError/rdKafkaCode``.
public struct KafkaError: Error, CustomStringConvertible, @unchecked Sendable {
    // Note: @unchecked because we use a backing class for storage (copy-on-write).

    // MARK: - RDKafkaCode

    /// A type-safe wrapper around librdkafka's `rd_kafka_resp_err_t` error codes.
    ///
    /// Use the static constants (e.g., `.allBrokersDown`, `.authentication`) to match
    /// against the ``KafkaError/rdKafkaCode`` property without importing Crdkafka.
    public struct RDKafkaCode: Hashable, Sendable, CustomStringConvertible {
        /// The raw `Int32` value corresponding to the librdkafka error code.
        public let rawValue: Int32

        public init(rawValue: Int32) {
            self.rawValue = rawValue
        }

        // MARK: - Common error codes

        /// All broker connections are down.
        public static let allBrokersDown = RDKafkaCode(rawValue: -187)
        /// Authentication failure.
        public static let authentication = RDKafkaCode(rawValue: -169)
        /// Broker transport failure.
        public static let transport = RDKafkaCode(rawValue: -195)
        /// Operation timed out.
        public static let timedOut = RDKafkaCode(rawValue: -185)
        /// SSL error.
        public static let ssl = RDKafkaCode(rawValue: -181)
        /// Message timed out.
        public static let messageTimedOut = RDKafkaCode(rawValue: -192)
        /// Queue full.
        public static let queueFull = RDKafkaCode(rawValue: -184)
        /// Fatal error.
        public static let fatal = RDKafkaCode(rawValue: -150)
        /// Maximum poll interval exceeded.
        public static let maxPollExceeded = RDKafkaCode(rawValue: -147)
        /// Invalid argument.
        public static let invalidArgument = RDKafkaCode(rawValue: -186)
        /// Unknown topic.
        public static let unknownTopic = RDKafkaCode(rawValue: -188)
        /// Unknown partition.
        public static let unknownPartition = RDKafkaCode(rawValue: -190)
        /// No error.
        public static let noError = RDKafkaCode(rawValue: 0)

        public var description: String {
            let name = String(cString: rd_kafka_err2str(rd_kafka_resp_err_t(rawValue: self.rawValue)))
            return "\(name) (code: \(self.rawValue))"
        }
    }

    private var backing: Backing

    /// Represents the kind of error that was encountered.
    public var code: ErrorCode {
        get {
            self.backing.code
        }
        set {
            self.makeUnique()
            self.backing.code = newValue
        }
    }

    /// The underlying librdkafka error code, if this error originated from librdkafka.
    ///
    /// Returns `nil` for errors that do not wrap a `rd_kafka_resp_err_t`
    /// (e.g., pure configuration or lifecycle errors).
    public var rdKafkaCode: RDKafkaCode? {
        self.backing.rdKafkaCode
    }

    /// Whether this error is fatal to the client instance.
    ///
    /// A fatal error means the client instance is no longer usable and must be
    /// destroyed and re-created. Always `false` for errors that do not originate
    /// from librdkafka's rich error type (`rd_kafka_error_t`).
    public var isFatal: Bool {
        self.backing.isFatal
    }

    /// Whether this error is retriable.
    ///
    /// A retriable error indicates the operation may succeed if retried.
    /// Always `false` for errors that do not originate from librdkafka's
    /// rich error type (`rd_kafka_error_t`).
    public var isRetriable: Bool {
        self.backing.isRetriable
    }

    private var reason: String {
        self.backing.reason
    }

    private var file: String {
        self.backing.file
    }

    private var line: UInt {
        self.backing.line
    }

    public var description: String {
        "KafkaError.\(self.code): \(self.reason) \(self.file):\(self.line)"
    }

    private mutating func makeUnique() {
        if !isKnownUniquelyReferenced(&self.backing) {
            self.backing = self.backing.copy()
        }
    }

    static func rdKafkaError(
        wrapping error: rd_kafka_resp_err_t,
        file: String = #fileID,
        line: UInt = #line
    ) -> KafkaError {
        let errorMessage = String(cString: rd_kafka_err2str(error))
        return KafkaError(
            backing: .init(
                code: .underlying,
                reason: errorMessage,
                file: file,
                line: line,
                rdKafkaCode: RDKafkaCode(rawValue: error.rawValue)
            )
        )
    }

    static func rdKafkaError(
        wrapping error: rd_kafka_resp_err_t,
        reason: String,
        file: String = #fileID,
        line: UInt = #line
    ) -> KafkaError {
        KafkaError(
            backing: .init(
                code: .underlying,
                reason: reason,
                file: file,
                line: line,
                rdKafkaCode: RDKafkaCode(rawValue: error.rawValue)
            )
        )
    }

    /// Create a ``KafkaError`` from a rich `rd_kafka_error_t*` error.
    ///
    /// Extracts the error code, reason string, isFatal, and isRetriable flags
    /// before destroying the error object. This preserves the full error metadata
    /// that is only available on the rich error type.
    ///
    /// - Parameter error: A non-nil `rd_kafka_error_t*` pointer. Will be destroyed after extraction.
    static func rdKafkaError(
        wrapping error: OpaquePointer,
        file: String = #fileID,
        line: UInt = #line
    ) -> KafkaError {
        let code = rd_kafka_error_code(error)
        let reason = String(cString: rd_kafka_error_string(error))
        let isFatal = rd_kafka_error_is_fatal(error) == 1
        let isRetriable = rd_kafka_error_is_retriable(error) == 1
        rd_kafka_error_destroy(error)

        return KafkaError(
            backing: .init(
                code: .underlying,
                reason: reason,
                file: file,
                line: line,
                rdKafkaCode: RDKafkaCode(rawValue: code.rawValue),
                isFatal: isFatal,
                isRetriable: isRetriable
            )
        )
    }

    static func config(
        reason: String,
        file: String = #fileID,
        line: UInt = #line
    ) -> KafkaError {
        KafkaError(
            backing: .init(
                code: .config,
                reason: reason,
                file: file,
                line: line
            )
        )
    }

    static func topicConfig(
        reason: String,
        file: String = #fileID,
        line: UInt = #line
    ) -> KafkaError {
        KafkaError(
            backing: .init(
                code: .topicConfig,
                reason: reason,
                file: file,
                line: line
            )
        )
    }

    static func client(
        reason: String,
        file: String = #fileID,
        line: UInt = #line
    ) -> KafkaError {
        KafkaError(
            backing: .init(
                code: .connectionFailed,
                reason: reason,
                file: file,
                line: line
            )
        )
    }

    static func connectionClosed(
        reason: String,
        file: String = #fileID,
        line: UInt = #line
    ) -> KafkaError {
        KafkaError(
            backing: .init(
                code: .shutdown,
                reason: reason,
                file: file,
                line: line
            )
        )
    }

    static func messageConsumption(
        reason: String,
        file: String = #fileID,
        line: UInt = #line
    ) -> KafkaError {
        KafkaError(
            backing: .init(
                code: .messageConsumptionFailed,
                reason: reason,
                file: file,
                line: line
            )
        )
    }

    static func topicCreation(
        reason: String,
        file: String = #fileID,
        line: UInt = #line
    ) -> KafkaError {
        KafkaError(
            backing: .init(
                code: .topicCreationFailed,
                reason: reason,
                file: file,
                line: line
            )
        )
    }

    static func topicDeletion(
        reason: String,
        file: String = #fileID,
        line: UInt = #line
    ) -> KafkaError {
        KafkaError(
            backing: .init(
                code: .topicDeletionFailed,
                reason: reason,
                file: file,
                line: line
            )
        )
    }
}

extension KafkaError {
    /// Represents the kind of error.
    ///
    /// The same error may be thrown from more than one place for more than one reason.
    /// This type represents only a relatively high-level error:
    /// use the string representation of ``KafkaError`` to get more details about the specific cause.
    public struct ErrorCode: Hashable, Sendable, CustomStringConvertible {
        fileprivate enum BackingCode {
            case rdKafkaError
            case config
            case topicConfig
            case connectionClosed
            case client
            case messageConsumption
            case topicCreation
            case topicDeletion
        }

        fileprivate var backingCode: BackingCode

        fileprivate init(_ backingCode: BackingCode) {
            self.backingCode = backingCode
        }

        /// Errors caused in the underlying transport.
        public static let underlying = ErrorCode(.rdKafkaError)
        /// There is an error in the Kafka client configuration.
        public static let config = ErrorCode(.config)
        /// There is an error in the Kafka topic configuration.
        public static let topicConfig = ErrorCode(.topicConfig)
        /// The Kafka connection is already shutdown.
        public static let shutdown = ErrorCode(.connectionClosed)
        /// Establishing a connection to Kafka failed.
        public static let connectionFailed = ErrorCode(.client)
        /// Consuming a message failed.
        public static let messageConsumptionFailed = ErrorCode(.messageConsumption)
        /// Creating a topic failed.
        public static let topicCreationFailed = ErrorCode(.topicCreation)
        /// Deleting a topic failed.
        public static let topicDeletionFailed = ErrorCode(.topicDeletion)

        public var description: String {
            String(describing: self.backingCode)
        }
    }
}

// MARK: - KafkaError + Backing

extension KafkaError {
    final class Backing: Hashable {
        var code: KafkaError.ErrorCode

        let reason: String

        let file: String

        let line: UInt

        let rdKafkaCode: RDKafkaCode?

        let isFatal: Bool

        let isRetriable: Bool

        fileprivate init(
            code: KafkaError.ErrorCode,
            reason: String,
            file: String,
            line: UInt,
            rdKafkaCode: RDKafkaCode? = nil,
            isFatal: Bool = false,
            isRetriable: Bool = false
        ) {
            self.code = code
            self.reason = reason
            self.file = file
            self.line = line
            self.rdKafkaCode = rdKafkaCode
            self.isFatal = isFatal
            self.isRetriable = isRetriable
        }

        static func == (lhs: Backing, rhs: Backing) -> Bool {
            lhs.code == rhs.code && lhs.rdKafkaCode == rhs.rdKafkaCode
        }

        func hash(into hasher: inout Hasher) {
            hasher.combine(self.code)
            hasher.combine(self.rdKafkaCode)
        }

        fileprivate func copy() -> Backing {
            Backing(
                code: self.code,
                reason: self.reason,
                file: self.file,
                line: self.line,
                rdKafkaCode: self.rdKafkaCode,
                isFatal: self.isFatal,
                isRetriable: self.isRetriable
            )
        }
    }
}

// MARK: - KafkaError + Hashable

extension KafkaError: Hashable {
    public static func == (lhs: KafkaError, rhs: KafkaError) -> Bool {
        lhs.backing == rhs.backing
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.backing)
    }
}
