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

@testable import SwiftKafka
import XCTest

/// - Note: Please see IntegrationTests/SwiftKafkaTests for tests concerning the consumption of sent messages.
final class KafkaConsumerTests: XCTestCase {
    // Read environment variables to get information about the test Kafka server
    let kafkaHost = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort = ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092"
    var bootstrapServer: String!
    var config: KafkaConfig!

    override func setUpWithError() throws {
        self.bootstrapServer = "\(self.kafkaHost):\(self.kafkaPort)"

        self.config = KafkaConfig()
        try self.config.set(self.bootstrapServer, forKey: "bootstrap.servers")
        try self.config.set("v4", forKey: "broker.address.family")
    }

    override func tearDownWithError() throws {
        self.bootstrapServer = nil
        self.config = nil
    }

    func testSettingAmbigousGroupIDFails() throws {
        try self.config.set("some-group-id", forKey: "group.id")

        XCTAssertThrowsError(
            _ = try KafkaConsumer(
                topics: ["test-topic"],
                groupID: "another-group-id",
                config: self.config,
                logger: .kafkaTest
            )
        )
    }

    func testSettingDuplicateIdenticalGroupIDSucceeds() throws {
        try self.config.set("some-group-id", forKey: "group.id")

        XCTAssertNoThrow(
            _ = try KafkaConsumer(
                topics: ["test-topic"],
                groupID: "some-group-id",
                config: self.config,
                logger: .kafkaTest
            )
        )
    }

    func testClosingClosedConsumerReturns() async throws {
        let consumer = try KafkaConsumer(
            topics: ["test-topic"],
            groupID: "some-group-id",
            config: self.config,
            logger: .kafkaTest
        )
        try await consumer.close()
        try await consumer.close()
    }
}
