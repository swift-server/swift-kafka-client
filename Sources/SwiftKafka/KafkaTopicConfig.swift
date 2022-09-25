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

/// `KafkaTopicConfig` is a `struct` that points to a topic configuration in memory.
/// Once a property of the `KafkaTopicConfig` is changed, a duplicate in-memory config is created using the
/// copy-on-write mechanism.
/// For more information on how to configure Kafka topics, see
/// [all available configurations](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#topic-configuration-properties).
public struct KafkaTopicConfig: Hashable, Equatable {
    private final class _Internal: Hashable, Equatable {
        /// Pointer to the `rd_kafka_topic_conf_t` object managed by `librdkafka`
        private var pointer: OpaquePointer

        /// Initialize internal `KafkaTopicConfig` object with default configuration
        init() {
            self.pointer = rd_kafka_topic_conf_new()
        }

        /// Initialize internal `KafkaTopicConfig` object through a given `rd_kafka_topic_conf_t` pointer
        init(pointer: OpaquePointer) {
            self.pointer = pointer
        }

        deinit {
            rd_kafka_topic_conf_destroy(pointer)
        }

        func value(forKey key: String) -> String? {
            let value = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
            defer { value.deallocate() }

            var valueSize = KafkaClient.stringSize
            let configResult = rd_kafka_topic_conf_get(
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

        func set(_ value: String, forKey key: String) throws {
            let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
            defer { errorChars.deallocate() }

            let configResult = rd_kafka_topic_conf_set(
                pointer,
                key,
                value,
                errorChars,
                KafkaClient.stringSize
            )

            if configResult != RD_KAFKA_CONF_OK {
                let errorString = String(cString: errorChars)
                throw KafkaError.topicConfig(errorString)
            }
        }

        func createDuplicatePointer() -> OpaquePointer {
            rd_kafka_topic_conf_dup(self.pointer)
        }

        func createDuplicate() -> _Internal {
            return .init(pointer: self.createDuplicatePointer())
        }

        // MARK: Hashable

        func hash(into hasher: inout Hasher) {
            hasher.combine(self.pointer)
        }

        // MARK: Equatable

        static func == (lhs: _Internal, rhs: _Internal) -> Bool {
            return lhs.pointer == rhs.pointer
        }
    }

    private var _internal: _Internal

    public init() {
        self._internal = .init()
    }

    /// Retrieve value of topic configuration property for `key`
    public func value(forKey key: String) -> String? {
        return self._internal.value(forKey: key)
    }

    /// Set topic configuration `value` for `key`
    public mutating func set(_ value: String, forKey key: String) throws {
        // Copy-on-write mechanism
        if !isKnownUniquelyReferenced(&(self._internal)) {
            self._internal = self._internal.createDuplicate()
        }

        try self._internal.set(value, forKey: key)
    }

    /// Create a duplicate topic configuration object in memory and access it through a scoped accessor.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the `OpaquePointer` to the duplicate `rd_kafka_topic_conf_t` object in memory.
    @discardableResult
    func withDuplicatePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        return try body(self._internal.createDuplicatePointer())
    }
}
