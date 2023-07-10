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

/// A collection of helper functions wrapping common `rd_kafka_conf_*` functions in Swift.
struct RDKafkaConfig {
    /// Create a new `rd_kafka_conf_t` object in memory and initialize it with the given configuration properties.
    /// - Parameter configDictionary: A dictionary containing the Kafka client configurations.
    /// - Returns: An `OpaquePointer` pointing to the newly created `rd_kafka_conf_t` object in memory.
    /// - Throws: A ``KafkaError`` if setting a config value failed.
    static func createFrom(configDictionary: [String: String]) throws -> OpaquePointer {
        let configPointer: OpaquePointer = rd_kafka_conf_new()
        try configDictionary.forEach { key, value in
            try Self.set(configPointer: configPointer, key: key, value: value)
        }

        return configPointer
    }

    /// A Swift wrapper for `rd_kafka_conf_set`.
    /// - Parameter configPointer: An `OpaquePointer` pointing to the `rd_kafka_conf_t` object in memory.
    /// - Parameter key: The configuration property to be changed.
    /// - Parameter value: The new value of the configuration property to be changed.
    /// - Throws: A ``KafkaError`` if setting the value failed.
    static func set(configPointer: OpaquePointer, key: String, value: String) throws {
        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
        defer { errorChars.deallocate() }

        let configResult = rd_kafka_conf_set(
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

    /// Enable event sourcing.
    ///
    /// - Parameter events: a bitmask of `RD_KAFKA_EVENT_*` events to enable
    /// for consumption by `rd_kafka_queue_poll()`.
    static func setEvents(configPointer: OpaquePointer, events: Int32) {
        rd_kafka_conf_set_events(configPointer, events)
    }
}
