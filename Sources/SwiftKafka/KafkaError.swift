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

struct KafkaError: Error {
    // Preliminary Implementation
    var rawValue: Int32
    var description: String

    init(
        rawValue: Int32 = -1,
        description: String = ""
    ) {
        self.rawValue = rawValue
        self.description = description // TODO: https://github.com/swift-server/swift-kafka-gsoc/issues/4
    }

    init(error: rd_kafka_resp_err_t) {
        self.rawValue = error.rawValue
        self.description = String(cString: rd_kafka_err2str(error))
    }
}
