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

@main
struct ErrorPrinter {
    static func main() {
        print("""
        This program is for testing purposes.
        It prints the names of all available Kafka errors.
        """)

        var errDescs: UnsafePointer<rd_kafka_err_desc>?
        var count = 0

        rd_kafka_get_err_descs(&errDescs, &count)

        for index in 0..<count {
            if var errorPointer = errDescs {
                errorPointer += index

                let kafkaError = errorPointer.pointee
                let name = String(cString: rd_kafka_err2name(kafkaError.code))
                print(name)
            }
        }
    }
}
