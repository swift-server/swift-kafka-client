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
    let totalKafkaBrokerMessagesRecieved: Int?
    let totalKafkaBrokerMessagesBytesRecieved: Int?

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
        case totalKafkaBrokerMessagesRecieved = "rxmsgs"
        case totalKafkaBrokerMessagesBytesRecieved = "rxmsg_bytes"
    }
}
