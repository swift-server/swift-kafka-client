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

enum KafkaLoggingKeys {
    static let clientId = "kafka.client.id"
    static let clientType = "kafka.client.type"
    static let groupId = "kafka.group.id"
    static let bootstrapServers = "kafka.bootstrap.servers"
    static let topics = "kafka.topics"
    static let topic = "kafka.topic"
    static let rebalanceKind = "kafka.rebalance.kind"
    static let partitions = "kafka.partitions"
    static let timeoutMs = "kafka.timeout.ms"
    static let messageId = "kafka.message.id"
}
