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
import Foundation
import Kafka
import Logging

extension Logger {
    static var testLogger: Logger {
        var logger = Logger(label: "bench_log")
        #if DEBUG
            logger.logLevel = .debug
        #else
            logger.logLevel = .info
        #endif
        return logger
    }
}

let logger: Logger = .testLogger
let stringSize = 1024
let kafkaHost: String = kafkaHostFromEnv()
let kafkaPort: Int = kafkaPortFromEnv()

func benchLog(_ msg: @autoclosure () -> Logger.Message) {
//    Just in case for debug
    #if DEBUG
    logger.debug(msg())
    #endif
}

enum RDKafkaClientHolderError: Error {
    case generic(String)
}

class RDKafkaClientHolder {
    let kafkaHandle: OpaquePointer
    
    enum HandleType {
        case producer
        case consumer
    }
    
    init(configDictionary: [String: String], type: HandleType) {
        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: stringSize)
        defer { errorChars.deallocate() }

        let config: OpaquePointer = rd_kafka_conf_new()
        configDictionary.forEach { key, value in
            let res = rd_kafka_conf_set(
                config,
                key,
                value,
                errorChars,
                stringSize
            )
            if res != RD_KAFKA_CONF_OK {
                fatalError("Could not set \(key) with \(value)")
            }
        }
        
        guard let handle = rd_kafka_new(
            type == .consumer ? RD_KAFKA_CONSUMER : RD_KAFKA_PRODUCER,
            config,
            errorChars,
            stringSize
        ) else {
            fatalError("Could not create client")
        }
        self.kafkaHandle = handle
    }
    
    deinit {
        rd_kafka_poll(self.kafkaHandle, 0)
        rd_kafka_destroy(self.kafkaHandle)
    }
    
    /// Create a topic with a unique name (`UUID`).
    /// Blocks for a maximum of `timeout` milliseconds.
    /// - Parameter timeout: Timeout in milliseconds.
    /// - Returns: Name of newly created topic.
    /// - Throws: A ``KafkaError`` if the topic creation failed.
    func _createUniqueTopic(timeout: Int32) throws -> String {
        let uniqueTopicName = UUID().uuidString

        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: stringSize)
        defer { errorChars.deallocate() }

        guard let newTopic = rd_kafka_NewTopic_new(
            uniqueTopicName,
            6, // use default num_partitions
            -1, // use default replication_factor
            errorChars,
            stringSize
        ) else {
            let errorString = String(cString: errorChars)
            throw RDKafkaClientHolderError.generic(errorString)
        }
        defer { rd_kafka_NewTopic_destroy(newTopic) }

        
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
            throw RDKafkaClientHolderError.generic("No CreateTopics result after 10s timeout")
        }
        defer { rd_kafka_event_destroy(resultEvent) }

        let resultCode = rd_kafka_event_error(resultEvent)
        guard resultCode == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw RDKafkaClientHolderError.generic("code: \(resultCode)")
        }

        guard let topicsResultEvent = rd_kafka_event_CreateTopics_result(resultEvent) else {
            throw RDKafkaClientHolderError.generic("Received event that is not of type rd_kafka_CreateTopics_result_t")
        }

        var resultTopicCount = 0
        let topicResults = rd_kafka_CreateTopics_result_topics(
            topicsResultEvent,
            &resultTopicCount
        )

        guard resultTopicCount == 1, let topicResult = topicResults?[0] else {
            throw RDKafkaClientHolderError.generic("Received less/more than one topic result")
        }

        let topicResultError = rd_kafka_topic_result_error(topicResult)
        guard topicResultError == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw RDKafkaClientHolderError.generic("code: \(topicResultError)")
        }

        let receivedTopicName = String(cString: rd_kafka_topic_result_name(topicResult))
        guard receivedTopicName == uniqueTopicName else {
            throw RDKafkaClientHolderError.generic("Received topic result for topic with different name")
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
    /// - Throws: A ``KafkaError`` if the topic deletion failed.
    func _deleteTopic(_ topic: String, timeout: Int32) throws {
        let deleteTopic = rd_kafka_DeleteTopic_new(topic)
        defer { rd_kafka_DeleteTopic_destroy(deleteTopic) }

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
            throw RDKafkaClientHolderError.generic("No DeleteTopics result after 10s timeout")
        }
        defer { rd_kafka_event_destroy(resultEvent) }

        let resultCode = rd_kafka_event_error(resultEvent)
        guard resultCode == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw RDKafkaClientHolderError.generic("code: \(resultCode)")
        }

        guard let topicsResultEvent = rd_kafka_event_DeleteTopics_result(resultEvent) else {
            throw RDKafkaClientHolderError.generic("Received event that is not of type rd_kafka_DeleteTopics_result_t")
        }

        var resultTopicCount = 0
        let topicResults = rd_kafka_DeleteTopics_result_topics(
            topicsResultEvent,
            &resultTopicCount
        )

        guard resultTopicCount == 1, let topicResult = topicResults?[0] else {
            throw RDKafkaClientHolderError.generic("Received less/more than one topic result")
        }

        let topicResultError = rd_kafka_topic_result_error(topicResult)
        guard topicResultError == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw RDKafkaClientHolderError.generic("code: \(topicResultError)")
        }

        let receivedTopicName = String(cString: rd_kafka_topic_result_name(topicResult))
        guard receivedTopicName == topic else {
            throw RDKafkaClientHolderError.generic("Received topic result for topic with different name")
        }
    }
}

func sendAndAcknowledgeMessages(
    producer: KafkaProducer,
    events: KafkaProducerEvents,
    messages: [KafkaProducerMessage<String, String>]
) async throws {
    for message in messages {
        while true { // Note: this is an example of queue full
            do {
                try producer.send(message)
                break
            } catch let error as KafkaError where error.description.contains("Queue full") {
                continue
            } catch {
                benchLog("Caught some error: \(error)")
                throw error
            }
        }
    }

    var receivedDeliveryReportsCtr = 0
    var prevPercent = 0

    for await event in events {
        switch event {
        case .deliveryReports(let deliveryReports):
            receivedDeliveryReportsCtr += deliveryReports.count
        default:
            break // Ignore any other events
        }
        let curPercent = receivedDeliveryReportsCtr * 100 / messages.count
        if curPercent >= prevPercent + 10 {
            benchLog("Delivered \(curPercent)% of messages")
            prevPercent = curPercent
        }

        if receivedDeliveryReportsCtr >= messages.count {
            break
        }
    }
}

func createTestMessages(
    topic: String,
    headers: [KafkaHeader] = [],
    count: UInt
) -> [KafkaProducerMessage<String, String>] {
    return Array(0..<count).map {
        KafkaProducerMessage(
            topic: topic,
            headers: headers,
            key: "key \($0)",
            value: "Hello, World! \($0) - \(Date().description)"
        )
    }
}

func getFromEnv(_ key: String) -> String? {
    ProcessInfo.processInfo.environment[key]
}

func kafkaHostFromEnv() -> String {
    getFromEnv("KAFKA_HOST") ?? "localhost"
}

func kafkaPortFromEnv() -> Int {
    .init(getFromEnv("KAFKA_PORT") ?? "9092")!
}

func bootstrapBrokerAddress() -> KafkaConfiguration.BrokerAddress {
    .init(
        host: kafkaHost,
        port: kafkaPort
    )
}
