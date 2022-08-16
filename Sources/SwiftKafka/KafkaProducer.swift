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

public struct KafkaProducer {
    let client: KafkaClient
    let topicConfig: KafkaTopicConfig

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
