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

public struct KafkaError: Error, CustomStringConvertible {

    internal enum _Code: CustomStringConvertible {
        case rdKafkaError(rd_kafka_resp_err_t)
        case config(String)
        case topicConfig(String)
        case connectionClosed
        case client(String)
        case messageConsumption(String)
        case topicCreation(String)
        case topicDeletion(String)

        var description: String {
            switch self {
            case .rdKafkaError(let error):
                return String(cString: rd_kafka_err2str(error))
            case .config(let message):
                return "Configuration Error: \(message)"
            case .topicConfig(let message):
                return "Topic Configuration Error: \(message)"
            case .connectionClosed:
                return "Connection closed"
            case .client(let message):
                return "Client Error: \(message)"
            case .messageConsumption(let message):
                return "Message Consumption Error: \(message)"
            case .topicCreation(let message):
                return "Topic Creation Error: \(message)"
            case .topicDeletion(let message):
                return "Topic Deletion Error: \(message)"
            }
        }
    }

    let _code: _Code

    var file: String

    var line: Int

    init(_code: _Code, file: String, line: Int) {
        self._code = _code
        self.file = file
        self.line = line
    }

    static func rdKafkaError(_ error: rd_kafka_resp_err_t, file: String = #fileID, line: Int = #line) -> Self {
        .init(_code: .rdKafkaError(error), file: file, line: line)
    }

    static func config(_ message: String, file: String = #fileID, line: Int = #line) -> Self {
        .init(_code: .config(message), file: file, line: line)
    }

    static func topicConfig(_ message: String, file: String = #fileID, line: Int = #line) -> Self {
        .init(_code: .topicConfig(message), file: file, line: line)
    }

    static func connectionClosed(file: String = #fileID, line: Int = #line) -> Self {
        .init(_code: .connectionClosed, file: file, line: line)
    }

    static func client(_ message: String, file: String = #fileID, line: Int = #line) -> Self {
        .init(_code: .client(message), file: file, line: line)
    }

    static func messageConsumption(_ message: String, file: String = #fileID, line: Int = #line) -> Self {
        .init(_code: .messageConsumption(message), file: file, line: line)
    }

    static func topicCreation(_ message: String, file: String = #fileID, line: Int = #line) -> Self {
        .init(_code: .topicCreation(message), file: file, line: line)
    }

    static func topicDeletion(_ message: String, file: String = #fileID, line: Int = #line) -> Self {
        .init(_code: .topicDeletion(message), file: file, line: line)
    }

    public var description: String {
        self._code.description
    }
}
