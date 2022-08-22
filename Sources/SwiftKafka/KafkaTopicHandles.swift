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

/// Actor that  contains and manages all Kafka topic handles of a producer.
actor KafkaTopicHandles {

    private let kafkaHandle: OpaquePointer
    private var topicHandleCache: [String: OpaquePointer]

    init(kafkaHandle: OpaquePointer) {
        self.kafkaHandle = kafkaHandle
        self.topicHandleCache = [:]
    }

    deinit {
        for (_, topicHandle) in self.topicHandleCache {
            rd_kafka_topic_destroy(topicHandle)
        }
    }

    /// Get topic handle for a given topic name.
    /// Creates a new topic handle if no matching handle could be found in cache.
    /// - Parameter topic: The name of the Kafka topic.
    /// - Parameter topicConfig: The ``KafkaTopicConfig`` used for newly created topics.
    func getTopicHandle(
        topic: String,
        topicConfig: KafkaTopicConfig = KafkaTopicConfig()
    ) -> OpaquePointer? {

        if let handle = self.topicHandleCache[topic] {
            return handle
        } else {
            let newHandle = rd_kafka_topic_new(
                self.kafkaHandle,
                topic,
                topicConfig.pointer
            )
            if newHandle != nil {
                self.topicHandleCache[topic] = newHandle
            }
            return newHandle
        }
    }
}
