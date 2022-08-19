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

public struct KafkaError: Error {
    // Preliminary Implementation
    public let rawValue: Int32
    public let description: String

    public init(rawValue: Int32) {
        self.rawValue = rawValue
        self.description = "" // TODO: error PR
    }

    init(description: String) {
        self.rawValue = -1
        self.description = description
    }

    init(error: rd_kafka_resp_err_t) {
        self.rawValue = error.rawValue
        self.description = String(cString: rd_kafka_err2str(error))
    }
}
