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
import NIOConcurrencyHelpers

/// Swift class that matches topic names with their respective `rd_kafka_topic_t` handles.
internal final class RDKafkaTopicHandles: Sendable {
    private let _internal: NIOLockedValueBox<[String: OpaquePointer]>

    // Note: we retain the client to ensure it does not get
    // deinitialized before rd_kafka_topic_destroy() is invoked (required)
    private let client: RDKafkaClient

    init(client: RDKafkaClient) {
        self._internal = NIOLockedValueBox([:])
        self.client = client
    }

    deinit {
        self._internal.withLockedValue { dict in
            for (_, topicHandle) in dict {
                rd_kafka_topic_destroy(topicHandle)
            }
        }
    }

    /// Scoped accessor that enables safe access to the pointer of the topic's handle.
    /// - Warning: Do not escape the pointer from the closure for later use.
    /// - Parameter topic: The name of the topic that is addressed.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter body: The closure will use the topic handle pointer.
    @discardableResult
    func withTopicHandlePointer<T>(
        topic: String,
        topicConfig: KafkaTopicConfiguration,
        _ body: (OpaquePointer) throws -> T
    ) throws -> T {
        let topicHandle = try self.createTopicHandleIfNeeded(topic: topic, topicConfig: topicConfig)
        return try body(topicHandle)
    }

    /// Check `topicHandles` for a handle matching the topic name and create a new handle if needed.
    /// - Parameter topic: The name of the topic that is addressed.
    private func createTopicHandleIfNeeded(
        topic: String,
        topicConfig: KafkaTopicConfiguration
    ) throws -> OpaquePointer {
        try self._internal.withLockedValue { dict in
            if let handle = dict[topic] {
                return handle
            } else {
                let rdTopicConf = try RDKafkaTopicConfig.createFrom(topicConfig: topicConfig)
                let newHandle = self.client.withKafkaHandlePointer { kafkaHandle in
                    rd_kafka_topic_new(
                        kafkaHandle,
                        topic,
                        rdTopicConf
                    )
                    // rd_kafka_topic_new deallocates topic config object
                }

                guard let newHandle else {
                    // newHandle is nil, so we can retrieve error through rd_kafka_last_error()
                    let error = KafkaError.rdKafkaError(wrapping: rd_kafka_last_error())
                    throw error
                }
                dict[topic] = newHandle
                return newHandle
            }
        }
    }
}
