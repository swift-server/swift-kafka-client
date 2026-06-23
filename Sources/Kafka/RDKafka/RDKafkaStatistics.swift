//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

// MARK: - RDKafkaStatistics

struct RDKafkaStatistics: Hashable, Codable {
    let queuedOperation: Int?
    let queuedProducerMessages: Int?
    let queuedProducerMessagesSize: Int?
    let topicsInMetadataCache: Int?
    let totalKafkaBrokerRequests: Int?
    let totalKafkaBrokerBytesSent: Int?
    let totalKafkaBrokerResponses: Int?
    let totalKafkaBrokerResponsesSize: Int?
    let totalKafkaBrokerMessagesSent: Int?
    let totalKafkaBrokerMessagesBytesSent: Int?
    let totalKafkaBrokerMessagesReceived: Int?
    let totalKafkaBrokerMessagesBytesReceived: Int?

    let topics: [String: RDKafkaTopicStatistics]?

    enum CodingKeys: String, CodingKey {
        case queuedOperation = "replyq"
        case queuedProducerMessages = "msg_cnt"
        case queuedProducerMessagesSize = "msg_size"
        case topicsInMetadataCache = "metadata_cache_cnt"
        case totalKafkaBrokerRequests = "tx"
        case totalKafkaBrokerBytesSent = "tx_bytes"
        case totalKafkaBrokerResponses = "rx"
        case totalKafkaBrokerResponsesSize = "rx_bytes"
        case totalKafkaBrokerMessagesSent = "txmsgs"
        case totalKafkaBrokerMessagesBytesSent = "txmsg_bytes"
        case totalKafkaBrokerMessagesReceived = "rxmsgs"
        case totalKafkaBrokerMessagesBytesReceived = "rxmsg_bytes"
        case topics
    }
}

struct RDKafkaTopicStatistics: Hashable, Codable {
    let topic: String
    let batchSize: RDKafkaWindowStatistics?
    let batchCount: RDKafkaWindowStatistics?
    let partitions: [String: RDKafkaPartitionStatistics]?

    enum CodingKeys: String, CodingKey {
        case topic
        case batchSize = "batchsize"
        case batchCount = "batchcnt"
        case partitions
    }
}

struct RDKafkaPartitionStatistics: Hashable, Codable {
    let partition: Int
    let consumerLag: Int?

    enum CodingKeys: String, CodingKey {
        case partition
        case consumerLag = "consumer_lag"
    }
}

struct RDKafkaWindowStatistics: Hashable, Codable {
    let min: Int
    let max: Int
    let avg: Int
    let sum: Int
    let cnt: Int
}
