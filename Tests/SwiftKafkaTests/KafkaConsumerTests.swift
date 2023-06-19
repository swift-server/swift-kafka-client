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

import NIOCore
@testable import SwiftKafka
import XCTest

// TODO: move to SwiftKafkaTests?
final class KafkaConsumerTests: XCTestCase {
    // Read environment variables to get information about the test Kafka server
    let kafkaHost = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort = ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092"
    var bootstrapServer: String!
    var config: KafkaConsumerConfig!

    override func setUpWithError() throws {
        self.bootstrapServer = "\(self.kafkaHost):\(self.kafkaPort)"

        self.config = KafkaConsumerConfig(
            consumptionStrategy: .group(groupID: "test-group", topics: ["test-topic"]),
            bootstrapServers: [self.bootstrapServer],
            brokerAddressFamily: .v4
        )
    }

    override func tearDownWithError() throws {
        self.bootstrapServer = nil
        self.config = nil
    }

    func testNoMemoryLeakAfterShutdown() async throws {
        var consumer: KafkaConsumer? = try KafkaConsumer(config: config, logger: .kafkaTest)
        weak var consumerCopy = consumer

        // TODO: one consuming version, one without consuming -> also non consuming test for producer

        consumer = nil
        XCTAssertNil(consumerCopy)
    }

    // MARK: See SwiftKafkaTests (Integration Tests) for actual message consumption tests
}
