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

/// Base class for ``KafkaProducer`` and ``KafkaConsumer``,
/// which is used to handle the connection to the Kafka ecosystem.
final class KafkaClient {
    // Default size for Strings returned from C API
    static let stringSize = 1024

    /// Handle for the C library's Kafka instance.
    private let kafkaHandle: OpaquePointer
    /// References the opaque object passed to the config to ensure ARC retains it as long as the client exists.
    private let opaque: RDKafkaConfig.CapturedClosure?
    /// A logger.
    private let logger: Logger

    init(
        kafkaHandle: OpaquePointer,
        opaque: RDKafkaConfig.CapturedClosure?,
        logger: Logger
    ) {
        self.kafkaHandle = kafkaHandle
        self.opaque = opaque
        self.logger = logger
    }

    deinit {
        rd_kafka_destroy(kafkaHandle)
    }

    /// Polls the Kafka client for events.
    ///
    /// Events will cause application-provided callbacks to be called.
    ///
    /// - Parameter timeout: Specifies the maximum amount of time
    /// (in milliseconds) that the call will block waiting for events.
    /// For non-blocking calls, provide 0 as `timeout`.
    /// To wait indefinitely for an event, provide -1.
    /// - Returns: The number of events served.
    @discardableResult
    func poll(timeout: Int32) -> Int32 {
        return rd_kafka_poll(self.kafkaHandle, timeout)
    }

    /// Scoped accessor that enables safe access to the pointer of the client's Kafka handle.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter body: The closure will use the Kafka handle pointer.
    @discardableResult
    func withKafkaHandlePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        return try body(self.kafkaHandle)
    }
}
