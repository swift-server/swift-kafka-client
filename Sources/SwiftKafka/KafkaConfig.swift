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
    /// Class that wraps both the opaque object and the delivery callback callback passed to this config.
    /// C closures cannot capture context. Therefore we save our context in this wrapper.
    class Opaque {
        let actual: AnyObject?
        let callback: (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, AnyObject?) -> Void

        init(
            actual: AnyObject? = nil,
            callback: @escaping (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, AnyObject?) -> Void
        ) {
            self.actual = actual
            self.callback = callback
        }
    }

    private final class _Internal: Hashable, Equatable {
        /// Pointer to the `rd_kafka_conf_t` object managed by `librdkafka`.
        private(set) var pointer: OpaquePointer

        /// References the opaque object passed to the config to ensure ARC retains it as long as the config exists.
        private var opaque: Opaque?

        /// Initialize internal `KafkaConfig` object through a given `rd_kafka_conf_t` pointer.
        init(
            pointer: OpaquePointer,
            opaque: Opaque?
        ) {
            self.pointer = pointer
            self.opaque = opaque
        }

        /// Initialize internal `KafkaConfig` object with default configuration.
        convenience init() {
            self.init(
                pointer: rd_kafka_conf_new(),
                opaque: nil
            )
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
            opaque: AnyObject?,
            callback: @escaping (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, AnyObject?) -> Void
        ) {
            self.opaque = Opaque(actual: opaque, callback: callback)

            let opaquePointer: UnsafeMutableRawPointer? = self.opaque.map { Unmanaged.passUnretained($0).toOpaque() }
            rd_kafka_conf_set_opaque(
                self.pointer,
                opaquePointer
            )

            let callbackWrapper: (
                @convention(c) (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, UnsafeMutableRawPointer?) -> Void
            ) = { kafkaHandle, messagePointer, opaquePointer in

                guard let opaquePointer = opaquePointer else {
                    fatalError("Could not resolve reference to KafkaProducer instance")
                }
                let opaque = Unmanaged<Opaque>.fromOpaque(opaquePointer).takeUnretainedValue()

                let actualCallback = opaque.callback
                let actualOpaque = opaque.actual
                actualCallback(kafkaHandle, messagePointer, actualOpaque)
            }

            rd_kafka_conf_set_dr_msg_cb(
                self.pointer,
                callbackWrapper
            )
        }

        func createDuplicatePointer() -> OpaquePointer {
            rd_kafka_conf_dup(self.pointer)
        }

        func createDuplicate() -> _Internal {
            return .init(
                pointer: self.createDuplicatePointer(),
                opaque: self.opaque
            )
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
    /// - Parameter opaque: An object that shall be passed to the C callback closure.
    /// - Parameter callback: A closure that is invoked upon message acknowledgement.
    mutating func setDeliveryReportCallback(
        opaque: AnyObject? = nil,
        callback: @escaping (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, AnyObject?) -> Void
    ) {
        // Copy-on-write mechanism
        if !isKnownUniquelyReferenced(&(self._internal)) {
            self._internal = self._internal.createDuplicate()
        }

        self._internal.setDeliveryReportCallback(
            opaque: opaque,
            callback: callback
        )
    }

    /// Create a duplicate configuration object in memory and access it through a scoped accessor.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the `OpaquePointer` to the duplicate `rd_kafka_conf_t` object in memory.
    @discardableResult
    func withDuplicatePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        return try body(self._internal.createDuplicatePointer())
    }
}
