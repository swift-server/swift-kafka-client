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

/// An error that can occur on `Kafka` operations
///
/// - Note: `Hashable` conformance only considers the ``KafkaError/code``.
public struct KafkaError: Error, CustomStringConvertible {
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
        wrapping error: rd_kafka_resp_err_t, isFatal: Bool = false, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        let errorMessage = String(cString: rd_kafka_err2str(error))
        return KafkaError(
            backing: .init(
                code: .rdKafkaError, reason: errorMessage, file: file, line: line
            )
        )
    }

    static func config(
        reason: String, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        return KafkaError(
            backing: .init(
                code: .config, reason: reason, file: file, line: line
            )
        )
    }

    static func topicConfig(
        reason: String, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        return KafkaError(
            backing: .init(
                code: .topicConfig, reason: reason, file: file, line: line
            )
        )
    }

    static func client(
        reason: String, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        return KafkaError(
            backing: .init(
                code: .client, reason: reason, file: file, line: line
            )
        )
    }

    static func connectionClosed(
        reason: String, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        return KafkaError(
            backing: .init(
                code: .connectionClosed, reason: reason, file: file, line: line
            )
        )
    }

    static func messageConsumption(
        reason: String, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        return KafkaError(
            backing: .init(
                code: .messageConsumption, reason: reason, file: file, line: line
            )
        )
    }

    static func topicCreation(
        reason: String, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        return KafkaError(
            backing: .init(
                code: .topicCreation, reason: reason, file: file, line: line
            )
        )
    }

    static func topicDeletion(
        reason: String, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        return KafkaError(
            backing: .init(
                code: .topicDeletion, reason: reason, file: file, line: line
            )
        )
    }

    static func transactionAborted(
        reason: String, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        return KafkaError(
            backing: .init(
                code: .transactionAborted, reason: reason, file: file, line: line
            )
        )
    }

    static func transactionIncomplete(
        reason: String, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        return KafkaError(
            backing: .init(
                code: .transactionIncomplete, reason: reason, file: file, line: line
            )
        )
    }

    static func transactionOutOfAttempts(
        numOfAttempts: UInt64, file: String = #fileID, line: UInt = #line
    ) -> KafkaError {
        return KafkaError(
            backing: .init(
                code: .transactionOutOfAttempts, reason: "Out of \(numOfAttempts) attempts", file: file, line: line
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
            case transactionAborted
            case transactionIncomplete
            case notInTransaction // FIXME: maybe add subcode ?
            case transactionOutOfAttempts
        }

        fileprivate var backingCode: BackingCode

        fileprivate init(_ backingCode: BackingCode) {
            self.backingCode = backingCode
        }

        /// Errors caused by the underlying `librdkafka` library.
        public static let rdKafkaError = ErrorCode(.rdKafkaError)
        /// There is an error in the Kafka client configuration.
        public static let config = ErrorCode(.config)
        /// There is an error in the Kafka topic configuration.
        public static let topicConfig = ErrorCode(.topicConfig)
        /// Something or somebody tried to access a client that ended its connection to Kafka.
        public static let connectionClosed = ErrorCode(.connectionClosed)
        /// Establishing a connection to Kafka failed.
        public static let client = ErrorCode(.client)
        /// Consuming a message failed.
        public static let messageConsumption = ErrorCode(.messageConsumption)
        /// Creating a topic failed.
        public static let topicCreation = ErrorCode(.topicCreation)
        /// Deleting a topic failed.
        public static let topicDeletion = ErrorCode(.topicDeletion)
        /// Transaction was aborted (can be re-tried from scratch).
        public static let transactionAborted = ErrorCode(.transactionAborted)
        /// Transaction could not be completed
        public static let transactionIncomplete = ErrorCode(.transactionIncomplete)
        /// Out of provided number of attempts
        public static let transactionOutOfAttempts = ErrorCode(.transactionOutOfAttempts)

        public var description: String {
            return String(describing: self.backingCode)
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

        let isFatal: Bool

        fileprivate init(
            code: KafkaError.ErrorCode,
            reason: String,
            file: String,
            line: UInt,
            isFatal: Bool = false
        ) {
            self.code = code
            self.reason = reason
            self.file = file
            self.line = line
            self.isFatal = isFatal
        }

        // Only the error code matters for equality.
        static func == (lhs: Backing, rhs: Backing) -> Bool {
            return lhs.code == rhs.code
        }

        func hash(into hasher: inout Hasher) {
            hasher.combine(self.code)
        }

        fileprivate func copy() -> Backing {
            return Backing(code: self.code, reason: self.reason, file: self.file, line: self.line)
        }
    }
}

// MARK: - KafkaError + Hashable

extension KafkaError: Hashable {
    public static func == (lhs: KafkaError, rhs: KafkaError) -> Bool {
        return lhs.backing == rhs.backing
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.backing)
    }
}
