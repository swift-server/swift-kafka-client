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
import Logging

/// Base class for ``KafkaProducer`` and ``KafkaConsumer``, which is used to handle the connection to the Kafka ecosystem.
public class KafkaClient {
    // Default size for Strings returned from C API
    static let stringSize = 1024

    private var _kafkaHandle: OpaquePointer?
    private var _clientType: rd_kafka_type_t
    private var _config: KafkaConfig

    static let logger = Logger(label: "SwiftKafkaLogger")

    /// Determines if client is a producer or a consumer
    enum `Type` {
        case producer
        case consumer
    }

    init(type: Type, config: KafkaConfig) throws {
        self._clientType = type == .producer ? RD_KAFKA_PRODUCER : RD_KAFKA_CONSUMER
        self._config = config

        try self.initializeKafkaHandle()
    }

    deinit {
        if let handle = _kafkaHandle {
            rd_kafka_destroy(handle)
        }
    }

    func initializeKafkaHandle() throws {
        let errorString = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
        defer { errorString.deallocate() }

        if self._kafkaHandle == nil {
            guard let handle = rd_kafka_new(
                _clientType,
                _config.getPointerDuplicate(),
                errorString,
                KafkaClient.stringSize
            ) else {
                throw KafkaError()
            }
            self._kafkaHandle = handle
        }
    }

    func connectAdditional(brokers: [String]) {
        fatalError("Not implemented")
    }
}
