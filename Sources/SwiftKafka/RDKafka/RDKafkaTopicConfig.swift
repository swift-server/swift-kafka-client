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

/// A collection of helper functions wrapping common `rd_kafka_topic_conf_*` functions in Swift.
struct RDKafkaTopicConfig {

    /// Create a new `rd_kafka_topic_conf_t` object in memory and initialize it with the given configuration properties.
    /// - Parameter topicConfig: The ``KafkaTopicConfig`` used to initialize the `rd_kafka_topic_conf_t` object.
    /// - Returns: An `OpaquePointer` pointing to the newly created `rd_kafka_topic_conf_t` object in memory.
    /// - Throws: A ``KafkaError`` if setting a config value failed.
    static func createFrom(topicConfig: KafkaTopicConfig) throws -> OpaquePointer {
        let configPointer: OpaquePointer = rd_kafka_topic_conf_new()
        try topicConfig.dictionary.forEach { key, value in
            try Self.set(configPointer: configPointer, key: key, value: value)
        }

        return configPointer
    }

    /// A Swift wrapper for `rd_kafka_topic_conf_set`.
    /// - Parameter configPointer: An `OpaquePointer` pointing to the `rd_kafka_topic_conf_t` object in memory.
    /// - Parameter key: The configuration property to be changed.
    /// - Parameter value: The new value of the configuration property to be changed.
    /// - Throws: A ``KafkaError`` if setting the value failed.
    static func set(configPointer: OpaquePointer, key: String, value: String) throws {
        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
        defer { errorChars.deallocate() }

        let configResult = rd_kafka_topic_conf_set(
            configPointer,
            key,
            value,
            errorChars,
            KafkaClient.stringSize
        )

        if configResult != RD_KAFKA_CONF_OK {
            let errorString = String(cString: errorChars)
            throw KafkaError.config(reason: errorString)
        }
    }
}
