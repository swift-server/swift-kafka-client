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

/// Error caused by the Kafka cluster when trying to process a message produced by ``KafkaProducer``.
///
/// - Note: `Hashable` conformance only considers the underlying ``KafkaAcknowledgedMessageError/error``'s
/// ``KafkaError/code``.
public struct KafkaAcknowledgedMessageError: Error, CustomStringConvertible {
    /// Identifier of the message that caused the error.
    public var messageID: KafkaProducerMessageID
    /// The underlying ``KafkaError``.
    public let error: KafkaError

    init(messageID: KafkaProducerMessageID, error: KafkaError) {
        self.messageID = messageID
        self.error = error
    }

    public var description: String {
        self.error.description
    }

    static func fromRDKafkaError(
        messageID: KafkaProducerMessageID,
        error: rd_kafka_resp_err_t,
        file: String = #fileID,
        line: UInt = #line
    ) -> Self {
        .init(
            messageID: messageID,
            error: .rdKafkaError(
                wrapping: error,
                file: file,
                line: line
            )
        )
    }

    static func fromMessage(
        messageID: KafkaProducerMessageID,
        message: String,
        file: String = #fileID,
        line: UInt = #line
    ) -> Self {
        .init(
            messageID: messageID,
            error: .acknowledgement(
                reason: message,
                file: file,
                line: line
            )
        )
    }
}

// MARK: - KafkaAcknowledgedMessageError + Hashable

extension KafkaAcknowledgedMessageError: Hashable {
    public static func == (lhs: KafkaAcknowledgedMessageError, rhs: KafkaAcknowledgedMessageError) -> Bool {
        return lhs.error == rhs.error
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.error)
    }
}
