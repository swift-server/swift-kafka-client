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
import struct Foundation.UUID
import Logging

@_spi(Internal)
extension RDKafkaClient {
    /// Create a topic with a unique name (`UUID`).
    /// Blocks for a maximum of `timeout` milliseconds.
    /// - Parameter partitions: Partitions in topic (default: -1 - default for broker)
    /// - Parameter timeout: Timeout in milliseconds.
    /// - Returns: Name of newly created topic.
    /// - Throws: A ``KafkaError`` if the topic creation failed.
    public func _createUniqueTopic(partitions: Int32 = -1, timeout: Int32) throws -> String {
        let uniqueTopicName = UUID().uuidString

        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: RDKafkaClient.stringSize)
        defer { errorChars.deallocate() }

        guard let newTopic = rd_kafka_NewTopic_new(
            uniqueTopicName,
            partitions,
            -1, // use default replication_factor
            errorChars,
            RDKafkaClient.stringSize
        ) else {
            let errorString = String(cString: errorChars)
            throw KafkaError.topicCreation(reason: errorString)
        }
        defer { rd_kafka_NewTopic_destroy(newTopic) }

        try self.withKafkaHandlePointer { kafkaHandle in
            let resultQueue = rd_kafka_queue_new(kafkaHandle)
            defer { rd_kafka_queue_destroy(resultQueue) }

            var newTopicsArray: [OpaquePointer?] = [newTopic]
            rd_kafka_CreateTopics(
                kafkaHandle,
                &newTopicsArray,
                1,
                nil,
                resultQueue
            )

            guard let resultEvent = rd_kafka_queue_poll(resultQueue, timeout) else {
                throw KafkaError.topicCreation(reason: "No CreateTopics result after 10s timeout")
            }
            defer { rd_kafka_event_destroy(resultEvent) }

            let resultCode = rd_kafka_event_error(resultEvent)
            guard resultCode == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: resultCode)
            }

            guard let topicsResultEvent = rd_kafka_event_CreateTopics_result(resultEvent) else {
                throw KafkaError.topicCreation(reason: "Received event that is not of type rd_kafka_CreateTopics_result_t")
            }

            var resultTopicCount = 0
            let topicResults = rd_kafka_CreateTopics_result_topics(
                topicsResultEvent,
                &resultTopicCount
            )

            guard resultTopicCount == 1, let topicResult = topicResults?[0] else {
                throw KafkaError.topicCreation(reason: "Received less/more than one topic result")
            }

            let topicResultError = rd_kafka_topic_result_error(topicResult)
            guard topicResultError == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: topicResultError)
            }

            let receivedTopicName = String(cString: rd_kafka_topic_result_name(topicResult))
            guard receivedTopicName == uniqueTopicName else {
                throw KafkaError.topicCreation(reason: "Received topic result for topic with different name")
            }
        }

        return uniqueTopicName
    }

    /// Delete a topic.
    /// Blocks for a maximum of `timeout` milliseconds.
    /// - Parameter topic: Topic to delete.
    /// - Parameter timeout: Timeout in milliseconds.
    /// - Throws: A ``KafkaError`` if the topic deletion failed.
    public func _deleteTopic(_ topic: String, timeout: Int32) throws {
        let deleteTopic = rd_kafka_DeleteTopic_new(topic)
        defer { rd_kafka_DeleteTopic_destroy(deleteTopic) }

        try self.withKafkaHandlePointer { kafkaHandle in
            let resultQueue = rd_kafka_queue_new(kafkaHandle)
            defer { rd_kafka_queue_destroy(resultQueue) }

            var deleteTopicsArray: [OpaquePointer?] = [deleteTopic]
            rd_kafka_DeleteTopics(
                kafkaHandle,
                &deleteTopicsArray,
                1,
                nil,
                resultQueue
            )

            guard let resultEvent = rd_kafka_queue_poll(resultQueue, timeout) else {
                throw KafkaError.topicDeletion(reason: "No DeleteTopics result after 10s timeout")
            }
            defer { rd_kafka_event_destroy(resultEvent) }

            let resultCode = rd_kafka_event_error(resultEvent)
            guard resultCode == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: resultCode)
            }

            guard let topicsResultEvent = rd_kafka_event_DeleteTopics_result(resultEvent) else {
                throw KafkaError.topicDeletion(reason: "Received event that is not of type rd_kafka_DeleteTopics_result_t")
            }

            var resultTopicCount = 0
            let topicResults = rd_kafka_DeleteTopics_result_topics(
                topicsResultEvent,
                &resultTopicCount
            )

            guard resultTopicCount == 1, let topicResult = topicResults?[0] else {
                throw KafkaError.topicDeletion(reason: "Received less/more than one topic result")
            }

            let topicResultError = rd_kafka_topic_result_error(topicResult)
            guard topicResultError == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: topicResultError)
            }

            let receivedTopicName = String(cString: rd_kafka_topic_result_name(topicResult))
            guard receivedTopicName == topic else {
                throw KafkaError.topicDeletion(reason: "Received topic result for topic with different name")
            }
        }
    }

    public static func makeClientForTopics(config: KafkaConsumerConfiguration, logger: Logger) throws -> RDKafkaClient {
        return try Self.makeClient(type: .consumer, configDictionary: config.dictionary, events: [], logger: logger)
    }
}
