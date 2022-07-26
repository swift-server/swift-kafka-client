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

import Foundation
import Crdkafka

struct KafkaError: Error {
    let code: Int32?
    let description: String
    
    init(code: Int32?, message: String) {
        self.code = code
        self.description = message
    }
    
    init(error: rd_kafka_resp_err_t) {
        code = error.rawValue
        message = String(cString: rd_kafka_err2str(error))
    }
}
