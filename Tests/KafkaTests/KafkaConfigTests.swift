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

import Testing

@testable import Kafka

@Suite struct KafkaConfigTests {
    @Test func consumerConfig() async throws {
        var config = KafkaConsumerConfig()
        config.bootstrapServers = ["a.example.com:9052", "b.example.com:9052"]
        config.debug = [.consumer, .broker]
        config.enableAutoOffsetStore = false
        config.socketTimeoutMs = 5000
        config.pollInterval = .milliseconds(100)

        #expect(
            config.config == [
                "bootstrap.servers": "a.example.com:9052,b.example.com:9052",
                "debug": "consumer,broker",
                "enable.auto.offset.store": "false",
                "socket.timeout.ms": "5000",
            ]
        )
    }

    @Test func consumptionStrategySetsGroupId() async throws {
        var config = KafkaConsumerConfig()
        config.consumptionStrategy = .group(id: "test-group", topics: [])

        #expect(
            config.config == [
                "group.id": "test-group"
            ]
        )

        // Explicit config takes priority
        config.groupId = "explicit-id"
        #expect(
            config.config == [
                "group.id": "explicit-id"
            ]
        )
    }

    @available(*, deprecated, message: "Use KafkaConsumerConfig instead")
    @Test func testConsumerConfigFromConfiguration() async throws {
        var configuration = KafkaConsumerConfiguration(
            consumptionStrategy: .group(id: "test", topics: []),
            bootstrapBrokerAddresses: [.init(host: "example.com", port: 9052)]
        )
        configuration.isAutoCommitEnabled = false
        configuration.debugOptions = [.broker, .metadata]

        let config = configuration.asKafkaConsumerConfig
        #expect(
            config.config == configuration.dictionary
        )
    }
}
