//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
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
        do {
            var size: Int = RDKafkaClient.stringSize
            let configValue = UnsafeMutablePointer<CChar>.allocate(capacity: size)
            defer { configValue.deallocate() }

            if RD_KAFKA_CONF_OK == rd_kafka_conf_get(configPointer, key, configValue, &size) {
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

        }
        
        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: RDKafkaClient.stringSize)
        defer { errorChars.deallocate() }

        let configResult = rd_kafka_conf_set(
            configPointer,
            key,
            value,
            errorChars,
            RDKafkaClient.stringSize
        )

        if configResult != RD_KAFKA_CONF_OK {
            let errorString = String(cString: errorChars)
            throw KafkaError.config(reason: errorString)
        }
    }

    /// Enable event sourcing.
    ///
    /// - Parameter events: a bitmask of ``RDKafkaEvent``s to enable
    /// for consumption by `rd_kafka_queue_poll()`.
    static func setEvents(configPointer: OpaquePointer, events: [RDKafkaEvent]) {
        let events = events.map(\.rawValue).reduce(0) { $0 | $1 }
        rd_kafka_conf_set_events(configPointer, events)
    }
}
