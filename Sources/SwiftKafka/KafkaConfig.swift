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

/// Used to configure producers and consumers.
/// `KafkaConfig` is a `struct` that points to a configuration in memory.
/// Once a property of the `KafkaConfig` is changed, a duplicate in-memory config is created using the
/// copy-on-write mechanism.
/// For more information on how to configure Kafka, see
/// [all available configurations](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
public struct KafkaConfig: Hashable, Equatable {
    private final class _Internal: Hashable, Equatable {
        /// Pointer to the `rd_kafka_conf_t` object managed by `librdkafka`
        private(set) var pointer: OpaquePointer

        /// Initialize internal `KafkaConfig` object through a given `rd_kafka_conf_t` pointer
        init(pointer: OpaquePointer) {
            self.pointer = pointer
        }

        /// Initialize internal `KafkaConfig` object with default configuration
        convenience init() {
            self.init(pointer: rd_kafka_conf_new())
        }

        deinit {
            rd_kafka_conf_destroy(pointer)
        }

        func value(forKey key: String) -> String? {
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

        func set(_ value: String, forKey key: String) throws {
            let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
            defer { errorChars.deallocate() }

            let configResult = rd_kafka_conf_set(
                pointer,
                key,
                value,
                errorChars,
                KafkaClient.stringSize
            )

            if configResult != RD_KAFKA_CONF_OK {
                let errorString = String(cString: errorChars)
                throw KafkaError(description: errorString)
            }
        }

        func setDeliveryReportCallback(
            callback: @escaping (@convention(c) (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, UnsafeMutableRawPointer?) -> Void)
        ) {
            rd_kafka_conf_set_dr_msg_cb(
                self.pointer,
                callback
            )
        }

        func setCallbackOpaque(opaque: UnsafeMutableRawPointer) {
            rd_kafka_conf_set_opaque(
                self.pointer,
                opaque
            )
        }

        func createDuplicatePointer() -> OpaquePointer {
            rd_kafka_conf_dup(self.pointer)
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

    var pointer: OpaquePointer {
        return self._internal.pointer
    }

    /// Retrieve value of configuration property for `key`
    public func value(forKey key: String) -> String? {
        return self._internal.value(forKey: key)
    }

    /// Set configuration `value` for `key`
    public mutating func set(_ value: String, forKey key: String) throws {
        // Copy-on-write mechanism
        if !isKnownUniquelyReferenced(&(self._internal)) {
            self._internal = self._internal.createDuplicate()
        }

        try self._internal.set(value, forKey: key)
    }

    /// Define a function that is called upon every message acknowledgement.
    /// - Parameter callback: C compatible function.
    mutating func setDeliveryReportCallback(
        callback: @escaping (@convention(c) (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, UnsafeMutableRawPointer?) -> Void)
    ) {
        // Copy-on-write mechanism
        if !isKnownUniquelyReferenced(&(self._internal)) {
            self._internal = self._internal.createDuplicate()
        }

        self._internal.setDeliveryReportCallback(callback: callback)
    }

    /// Set an opaque pointer that will be passed to callbacks of the `KafkaClient` using this configuration.
    /// - Parameter pointer: pointer that will be passed to callbacks.
    mutating func setCallbackOpaque(opaque: UnsafeMutableRawPointer) {
        // Copy-on-write mechanism
        if !isKnownUniquelyReferenced(&(self._internal)) {
            self._internal = self._internal.createDuplicate()
        }

        self._internal.setCallbackOpaque(opaque: opaque)
    }

    /// Create a duplicate configuration object in memory.
    /// - Returns: `OpaquePointer` to the duplicate `rd_kafka_conf_t` object in memory.
    func createDuplicatePointer() -> OpaquePointer {
        return self._internal.createDuplicatePointer()
    }
}
