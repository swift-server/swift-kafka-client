//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import ExtrasJSON

struct KafkaStatistics: Sendable, Hashable {
    let jsonString: String

    func fill(_ options: KafkaConfiguration.KafkaMetrics) {
        do {
            let json = try XJSONDecoder().decode(KafkaStatisticsJson.self, from: self.jsonString.utf8)
            if let timestamp = options.timestamp,
               let jsonTimestamp = json.ts {
                timestamp.record(jsonTimestamp)
            }

            if let time = options.time,
               let jsonTime = json.time {
                time.record(.init(jsonTime))
            }

            if let age = options.age,
               let jsonAge = json.age {
                age.recordMicroseconds(jsonAge)
            }

            if let replyQueue = options.replyQueue,
               let jsonReplyQueue = json.replyq {
                replyQueue.record(jsonReplyQueue)
            }

            if let messageCount = options.messageCount,
               let jsonMessageCount = json.msgCnt {
                messageCount.record(jsonMessageCount)
            }

            if let messageSize = options.messageSize,
               let jsonMessageSize = json.msgSize {
                messageSize.record(jsonMessageSize)
            }

            if let messageMax = options.messageMax,
               let jsonMessageMax = json.msgMax {
                messageMax.record(jsonMessageMax)
            }

            if let messageSizeMax = options.messageSizeMax,
               let jsonMessageSizeMax = json.msgSizeMax {
                messageSizeMax.record(jsonMessageSizeMax)
            }

            if let totalRequestsSent = options.totalRequestsSent,
               let jsonTx = json.tx {
                totalRequestsSent.record(jsonTx)
            }

            if let totalBytesSent = options.totalBytesSent,
               let jsonTxBytes = json.txBytes {
                totalBytesSent.record(jsonTxBytes)
            }

            if let totalResponsesRecieved = options.totalResponsesRecieved,
               let jsonRx = json.rx {
                totalResponsesRecieved.record(jsonRx)
            }

            if let totalBytesReceived = options.totalBytesReceived,
               let jsonRxBytes = json.rxBytes {
                totalBytesReceived.record(jsonRxBytes)
            }

            if let totalMessagesSent = options.totalMessagesSent,
               let jsonTxMessages = json.txmsgs {
                totalMessagesSent.record(jsonTxMessages)
            }

            if let totalMessagesBytesSent = options.totalMessagesBytesSent,
               let jsonTxMessagesBytes = json.txmsgBytes {
                totalMessagesBytesSent.record(jsonTxMessagesBytes)
            }

            if let totalMessagesRecieved = options.totalMessagesRecieved,
               let jsonRxMessages = json.rxmsgs {
                totalMessagesRecieved.record(jsonRxMessages)
            }

            if let totalMessagesBytesRecieved = options.totalMessagesBytesRecieved,
               let jsonRxMessagesBytes = json.rxmsgBytes {
                totalMessagesBytesRecieved.record(jsonRxMessagesBytes)
            }

            if let metadataCacheCount = options.metadataCacheCount,
               let jsonMetaDataCacheCount = json.metadataCacheCnt {
                metadataCacheCount.record(jsonMetaDataCacheCount)
            }
        } catch {
            fatalError("Statistics json decode error \(error)")
        }
    }
}
