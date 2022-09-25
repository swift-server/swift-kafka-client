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
public struct KafkaAcknowledgedMessageError: Error, CustomStringConvertible {
    /// Identifier of the message that caused the error.
    public var messageID: UInt
    /// A string describing the error.
    public var description: String

    var file: String

    var line: Int

    init(messageID: UInt, description: String, file: String, line: Int) {
        self.messageID = messageID
        self.description = description
        self.file = file
        self.line = line
    }

    static func fromRDKafkaError(
        messageID: UInt,
        error: rd_kafka_resp_err_t,
        file: String = #fileID,
        line: Int = #line)
    -> Self {
        .init(
            messageID: messageID,
            description: String(cString: rd_kafka_err2str(error)),
            file: file,
            line: line
        )
    }

    static func fromMessage(
        messageID: UInt,
        message: String,
        file: String = #fileID,
        line: Int = #line)
    -> Self {
        .init(
            messageID: messageID,
            description: "Acknowledgement Error: \(message)",
            file: file,
            line: line
        )
    }
}
