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
        case anyError(description: String) // TODO: replace

        var description: String {
            switch self {
            case .rdKafkaError(let error):
                return String(cString: rd_kafka_err2str(error))
            case .anyError(description: let description):
                return description
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

    public static func rdKafkaError(_ error: rd_kafka_resp_err_t, file: String = #fileID, line: Int = #line) -> Self {
        .init(_code: .rdKafkaError(error), file: file, line: line)
    }

    public static func anyError(description: String, file: String = #fileID, line: Int = #line) -> Self {
        .init(_code: .anyError(description: description), file: file, line: line)
    }

    public var description: String {
        self._code.description
    }
}
