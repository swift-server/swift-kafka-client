//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2026 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation
import Testing

@testable import Kafka

@Suite struct RDKafkaStatisticsTests {
    // MARK: - decodesTopLevelFields

    @Test func decodesTopLevelFields() throws {
        let json = """
            {
                "replyq": 1,
                "msg_cnt": 2,
                "msg_size": 3,
                "metadata_cache_cnt": 4,
                "tx": 5,
                "tx_bytes": 6,
                "rx": 7,
                "rx_bytes": 8,
                "txmsgs": 9,
                "txmsg_bytes": 10,
                "rxmsgs": 11,
                "rxmsg_bytes": 12
            }
            """
        let data = try #require(json.data(using: .utf8))
        let stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: data)

        #expect(stats.queuedOperation == 1)
        #expect(stats.queuedProducerMessages == 2)
        #expect(stats.queuedProducerMessagesSize == 3)
        #expect(stats.topicsInMetadataCache == 4)
        #expect(stats.totalKafkaBrokerRequests == 5)
        #expect(stats.totalKafkaBrokerBytesSent == 6)
        #expect(stats.totalKafkaBrokerResponses == 7)
        #expect(stats.totalKafkaBrokerResponsesSize == 8)
        #expect(stats.totalKafkaBrokerMessagesSent == 9)
        #expect(stats.totalKafkaBrokerMessagesBytesSent == 10)
        #expect(stats.totalKafkaBrokerMessagesReceived == 11)
        #expect(stats.totalKafkaBrokerMessagesBytesReceived == 12)
        #expect(stats.topics == nil)
    }

    // MARK: - decodesTopicAndPartitionStats

    @Test func decodesTopicAndPartitionStats() throws {
        let json = """
            {
                "replyq": 0,
                "msg_cnt": 0,
                "msg_size": 0,
                "metadata_cache_cnt": 0,
                "tx": 0,
                "tx_bytes": 0,
                "rx": 0,
                "rx_bytes": 0,
                "txmsgs": 0,
                "txmsg_bytes": 0,
                "rxmsgs": 0,
                "rxmsg_bytes": 0,
                "topics": {
                    "test-topic": {
                        "topic": "test-topic",
                        "batchsize": {
                            "min": 100,
                            "max": 900,
                            "avg": 450,
                            "sum": 4500,
                            "cnt": 10
                        },
                        "batchcnt": {
                            "min": 1,
                            "max": 5,
                            "avg": 3,
                            "sum": 30,
                            "cnt": 10
                        },
                        "partitions": {
                            "0": {
                                "partition": 0,
                                "consumer_lag": 42
                            }
                        }
                    }
                }
            }
            """
        let data = try #require(json.data(using: .utf8))
        let stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: data)

        let topics = try #require(stats.topics)
        #expect(topics.count == 1)

        let topicStats = try #require(topics["test-topic"])
        #expect(topicStats.topic == "test-topic")

        let batchSize = try #require(topicStats.batchSize)
        #expect(batchSize.min == 100)
        #expect(batchSize.max == 900)
        #expect(batchSize.avg == 450)
        #expect(batchSize.sum == 4500)
        #expect(batchSize.cnt == 10)

        let batchCount = try #require(topicStats.batchCount)
        #expect(batchCount.min == 1)
        #expect(batchCount.max == 5)
        #expect(batchCount.avg == 3)
        #expect(batchCount.sum == 30)
        #expect(batchCount.cnt == 10)

        let partitions = try #require(topicStats.partitions)
        #expect(partitions.count == 1)

        let partition0 = try #require(partitions["0"])
        #expect(partition0.partition == 0)
        #expect(partition0.consumerLag == 42)
    }

    // MARK: - handlesNullConsumerLag

    /// librdkafka uses -1 to signal that consumer_lag is not yet available
    /// (e.g. the committed offset has never been fetched). Verify it round-trips
    /// through the decoder as-is rather than being mapped to nil.
    @Test func handlesNullConsumerLag() throws {
        let json = """
            {
                "replyq": 0,
                "msg_cnt": 0,
                "msg_size": 0,
                "metadata_cache_cnt": 0,
                "tx": 0,
                "tx_bytes": 0,
                "rx": 0,
                "rx_bytes": 0,
                "txmsgs": 0,
                "txmsg_bytes": 0,
                "rxmsgs": 0,
                "rxmsg_bytes": 0,
                "topics": {
                    "test-topic": {
                        "topic": "test-topic",
                        "partitions": {
                            "0": {
                                "partition": 0,
                                "consumer_lag": -1
                            }
                        }
                    }
                }
            }
            """
        let data = try #require(json.data(using: .utf8))
        let stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: data)

        let topics = try #require(stats.topics)
        let topicStats = try #require(topics["test-topic"])
        let partitions = try #require(topicStats.partitions)
        let partition0 = try #require(partitions["0"])

        // -1 is librdkafka's sentinel meaning "not available" — it must be
        // preserved exactly so callers can distinguish "lag is zero" from
        // "lag is unknown".
        #expect(partition0.consumerLag == -1)
    }

    // MARK: - handlesMissingTopics

    @Test func handlesMissingTopics() throws {
        let json = """
            {
                "replyq": 0,
                "msg_cnt": 0,
                "msg_size": 0,
                "metadata_cache_cnt": 0,
                "tx": 0,
                "tx_bytes": 0,
                "rx": 0,
                "rx_bytes": 0,
                "txmsgs": 0,
                "txmsg_bytes": 0,
                "rxmsgs": 0,
                "rxmsg_bytes": 0
            }
            """
        let data = try #require(json.data(using: .utf8))
        let stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: data)

        #expect(stats.topics == nil)
    }
}
