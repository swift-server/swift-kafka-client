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
class KafkaClient {
    // Default size for Strings returned from C API
    static let stringSize = 1024

    let logger: Logger

    private let _clientType: rd_kafka_type_t
    private let _config: KafkaConfig
    private let _kafkaHandle: OpaquePointer

    /// Determines if client is a producer or a consumer
    enum `Type` {
        case producer
        case consumer
    }

    init(type: Type, config: KafkaConfig, logger: Logger) throws {
        self.logger = logger
        self._clientType = type == .producer ? RD_KAFKA_PRODUCER : RD_KAFKA_CONSUMER
        self._config = config.createDuplicate()

        let errorString = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
        defer { errorString.deallocate() }

        guard let handle = rd_kafka_new(
            _clientType,
            _config.pointer,
            errorString,
            KafkaClient.stringSize
        ) else {
            throw KafkaError()
        }
        self._kafkaHandle = handle
    }

    deinit {
        rd_kafka_destroy(_kafkaHandle)
    }

    func connectAdditional(brokers: [String]) {
        fatalError("Not implemented")
    }
}
