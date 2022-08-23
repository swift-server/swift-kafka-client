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
import Logging

/// Send messages to the Kafka cluster.
/// - Note: When messages get published to a non-existent topic, a new topic is created using the ``KafkaTopicConfig``
/// configuration object (only works if server has `auto.create.topics.enable` property set).
public struct KafkaProducer {
    private let client: KafkaClient
    private let topicConfig: KafkaTopicConfig

    // Preliminary implementation
    public init(
        config: KafkaConfig = KafkaConfig(),
        topicConfig: KafkaTopicConfig = KafkaTopicConfig(),
        logger: Logger
    ) throws {
        self.client = try KafkaClient(type: .producer, config: config, logger: logger)
        self.topicConfig = topicConfig
    }
}
