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

public class KafkaConfig {
    // Preliminary implementation
    private var pointer: OpaquePointer

    func getPointerDuplicate() -> OpaquePointer {
        return rd_kafka_conf_dup(self.pointer)
    }

    public init() {
        self.pointer = rd_kafka_conf_new()
    }

    deinit {
        rd_kafka_conf_destroy(pointer)
    }

    public subscript(key: String) -> String? {
        get {
            let value = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
            defer { value.deallocate() }

            var valueSize = KafkaClient.stringSize
            let configResult = rd_kafka_conf_get(
                pointer,
                key,
                value,
                &valueSize
            )

            if configResult == RD_KAFKA_CONF_OK {
                return String(cString: value)
            }
            return nil
        }

        set {
            let errorString = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
            defer { errorString.deallocate() }

            let configResult = rd_kafka_conf_set(
                pointer,
                key,
                newValue,
                errorString,
                KafkaClient.stringSize
            )

            // TODO: what shall we do when the value could not be set?
        }
    }
}
