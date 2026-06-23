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

enum KafkaMetricLabels {
    // Consumer
    static let consumerLagMax = "consumer.lag.max"
    static let consumerLag = "consumer.lag"
    static let consumerErrors = "consumer.errors.total"
    static let consumerRebalances = "consumer.rebalances.total"
    static let consumerCommits = "consumer.commits.total"
    static let consumerCommitsFailed = "consumer.commits.failed"
    static let consumerCommitDuration = "consumer.commit.duration"
    static let consumerMessagesReceived = "consumer.messages.received.total"
    static let consumerBytesReceived = "consumer.bytes.received.total"
    static let consumerQueueOperations = "consumer.queue.operations"

    // Producer
    static let producerSendErrors = "producer.send.errors.total"
    static let producerSendDuration = "producer.send.duration"
    static let producerDeliverySuccess = "producer.delivery.success.total"
    static let producerDeliveryFailure = "producer.delivery.failure.total"
    static let producerErrors = "producer.errors.total"
    static let producerMessagesSent = "producer.messages.sent.total"
    static let producerBytesSent = "producer.bytes.sent.total"
    static let producerQueueMessages = "producer.queue.messages"
    static let producerQueueBytes = "producer.queue.bytes"
    static let producerBatchSizeAvg = "producer.batch.size.avg"

    // Dimensions
    static let topicDimension = "topic"
    static let partitionDimension = "partition"
}
