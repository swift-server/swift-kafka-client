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

    func testSettingAmbigousGroupIDFails() async throws {
        try self.config.set("some-group-id", forKey: "group.id")

        do {
            _ = try await KafkaConsumer(
                topics: ["test-topic"],
                groupID: "another-group-id",
                config: self.config,
                logger: .kafkaTest
            )
            XCTFail("Method should have thrown error")
        } catch {}
    }

    func testSettingDuplicateIdenticalGroupIDSucceeds() async throws {
        try self.config.set("some-group-id", forKey: "group.id")

        do {
            _ = try await KafkaConsumer(
                topics: ["test-topic"],
                groupID: "some-group-id",
                config: self.config,
                logger: .kafkaTest
            )
        } catch {
            XCTFail("Method should not have thrown error")
        }
    }
}
