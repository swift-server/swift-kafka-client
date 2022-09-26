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
import struct Foundation.UUID
import Logging
@testable import SwiftKafka

extension Logger {
    static var kafkaTest: Logger {
        var logger = Logger(label: "kafka.test")
        logger.logLevel = .info
        return logger
    }
}

extension KafkaClient {
//    func createUniqueTopic(timeout: Int32 = 10000) async throws -> String {
//        try await withCheckedThrowingContinuation { continuation in
//            do {
//                let uniqueTopic = try self._createUniqueTopic(timeout: timeout)
//                continuation.resume(returning: uniqueTopic)
//            } catch {
//                continuation.resume(throwing: error)
//            }
//        }
//    }

    /// Create a topic with a unique name (`UUID`).
    /// Blocks for a maximum of `timeout` milliseconds.
    /// - Parameter timeout: Timeout in milliseconds.
    /// - Returns: Name of newly created topic.
    func _createUniqueTopic(timeout: Int32) throws -> String {
        let uniqueTopicName = UUID().uuidString

        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaClient.stringSize)
        defer { errorChars.deallocate() }

        guard let newTopic = rd_kafka_NewTopic_new(
            uniqueTopicName,
            -1, // use default num_partitions
            -1, // use default replication_factor
            errorChars,
            KafkaClient.stringSize
        ) else {
            let errorString = String(cString: errorChars)
            throw KafkaError(description: errorString)
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
                throw KafkaError(description: "No CreateTopics result after 10s timeout")
            }
            defer { rd_kafka_event_destroy(resultEvent) }

            let resultCode = rd_kafka_event_error(resultEvent)
            guard resultCode == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError(rawValue: resultCode.rawValue)
            }

            guard let topicsResultEvent = rd_kafka_event_CreateTopics_result(resultEvent) else {
                throw KafkaError(description: "Received event that is not of type rd_kafka_CreateTopics_result_t")
            }

            var resultTopicCount = 0
            let topicResults = rd_kafka_CreateTopics_result_topics(
                topicsResultEvent,
                &resultTopicCount
            )

            guard resultTopicCount == 1, let topicResult = topicResults?[0] else {
                throw KafkaError(description: "Received less/more than one topic result")
            }

            let topicResultError = rd_kafka_topic_result_error(topicResult)
            guard topicResultError == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError(rawValue: topicResultError.rawValue)
            }

            let receivedTopicName = String(cString: rd_kafka_topic_result_name(topicResult))
            guard receivedTopicName == uniqueTopicName else {
                throw KafkaError(description: "Received topic result for topic with different name")
            }
        }

        return uniqueTopicName
    }

//    func deleteTopic(_ topic: String, timeout: Int32 = 10000) async throws {
//        try await withCheckedThrowingContinuation { continuation in
//            do {
//                try self._deleteTopic(topic, timeout: timeout)
//                continuation.resume()
//            } catch {
//                continuation.resume(throwing: error)
//            }
//        }
//    }

    /// Delete a topic.
    /// Blocks for a maximum of `timeout` milliseconds.
    /// - Parameter topic: Topic to delete.
    /// - Parameter timeout: Timeout in milliseconds.
    func _deleteTopic(_ topic: String, timeout: Int32) throws {
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
                throw KafkaError(description: "No DeleteTopics result after 10s timeout")
            }
            defer { rd_kafka_event_destroy(resultEvent) }

            let resultCode = rd_kafka_event_error(resultEvent)
            guard resultCode == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError(rawValue: resultCode.rawValue)
            }

            guard let topicsResultEvent = rd_kafka_event_DeleteTopics_result(resultEvent) else {
                throw KafkaError(description: "Received event that is not of type rd_kafka_DeleteTopics_result_t")
            }

            var resultTopicCount = 0
            let topicResults = rd_kafka_DeleteTopics_result_topics(
                topicsResultEvent,
                &resultTopicCount
            )

            guard resultTopicCount == 1, let topicResult = topicResults?[0] else {
                throw KafkaError(description: "Received less/more than one topic result")
            }

            let topicResultError = rd_kafka_topic_result_error(topicResult)
            guard topicResultError == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError(rawValue: topicResultError.rawValue)
            }

            let receivedTopicName = String(cString: rd_kafka_topic_result_name(topicResult))
            guard receivedTopicName == topic else {
                throw KafkaError(description: "Received topic result for topic with different name")
            }
        }
    }
}
