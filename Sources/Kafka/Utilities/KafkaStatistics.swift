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
import Metrics

struct KafkaStatistics: Sendable, Hashable {
    let jsonString: String
    
    private func record<T: BinaryInteger>(_ value: T?, to: Gauge?) {
        guard let value,
              let to else {
            return
        }
        to.record(value)
    }
    private func recordMircoseconds<T: BinaryInteger>(_ value: T?, to: Timer?) {
        guard let value,
              let to else {
            return
        }
        to.recordMicroseconds(value)
    }

    func fill(_ options: KafkaConfiguration.KafkaMetrics) {
        do {
            let json = try XJSONDecoder().decode(KafkaStatisticsJson.self, from: self.jsonString.utf8)
            
            record(json.timestamp, to: options.timestamp)
            recordMircoseconds(json.time, to: options.time)
            recordMircoseconds(json.age, to: options.age)
            record(json.replyQueue, to: options.replyQueue)
            record(json.messageCount, to: options.messageCount)
            record(json.messageSize, to: options.messageSize)
            record(json.messageMax, to: options.messageMax)
            record(json.messageSizeMax, to: options.messageSizeMax)
            record(json.totalRequestsSent, to: options.totalRequestsSent)
            record(json.totalBytesSent, to: options.totalBytesSent)
            
            record(json.totalResponsesRecieved, to: options.totalResponsesRecieved)
            record(json.totalBytesReceived, to: options.totalBytesReceived)
            record(json.totalMessagesSent, to: options.totalMessagesSent)
            record(json.totalBytesSent, to: options.totalBytesSent)
            
            record(json.totalMessagesBytesSent, to: options.totalMessagesBytesSent)
            record(json.totalMessagesRecieved, to: options.totalMessagesRecieved)
            record(json.totalMessagesBytesRecieved, to: options.totalMessagesBytesRecieved)
            record(json.metadataCacheCount, to: options.metadataCacheCount)

        } catch {
            fatalError("Statistics json decode error \(error)")
        }
    }
}
