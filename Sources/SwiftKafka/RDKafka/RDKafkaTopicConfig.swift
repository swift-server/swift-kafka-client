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
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used to initialize the `rd_kafka_topic_conf_t` object.
    /// - Returns: An `OpaquePointer` pointing to the newly created `rd_kafka_topic_conf_t` object in memory.
    /// - Throws: A ``KafkaError`` if setting a config value failed.
    static func createFrom(topicConfig: KafkaTopicConfiguration) throws -> OpaquePointer {
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
        var size: Int = RDKafkaClient.stringSize
        let configValue = UnsafeMutablePointer<CChar>.allocate(capacity: size)
        defer { configValue.deallocate() }

        if RD_KAFKA_CONF_OK == rd_kafka_topic_conf_get(configPointer, key, configValue, &size) {
            let sizeNoNullTerm = size - 1
            let wasVal = String(unsafeUninitializedCapacity: sizeNoNullTerm) {
                let buf = UnsafeRawBufferPointer(
                    UnsafeMutableRawBufferPointer(
                        start: configValue,
                        count: sizeNoNullTerm
                    ))
                _ = $0.initialize(from: buf)
                return sizeNoNullTerm
            }
            if wasVal == value {
                return // Values are equal, avoid changing (not mark config as modified)
            }
        }

        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: RDKafkaClient.stringSize)
        defer { errorChars.deallocate() }

        let configResult = rd_kafka_topic_conf_set(
            configPointer,
            key,
            value,
            errorChars,
            RDKafkaClient.stringSize
        )

        if configResult != RD_KAFKA_CONF_OK {
            let errorString = String(cString: errorChars)
            throw KafkaError.topicConfig(reason: errorString)
        }
    }
}
