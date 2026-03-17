//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Benchmark
import Kafka
@_spi(Internal) import Kafka
import Logging
import ServiceLifecycle

import class Foundation.ProcessInfo
import struct Foundation.UUID

let kafkaHost = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
let bootstrapServer = "\(kafkaHost):9092"

// swift-format-ignore: DontRepeatTypeInStaticProperties
extension Logger {
    static let perfLogger = {
        var logger = Logger(label: "perf logger")
        logger.logLevel = .critical
        return logger
    }()
}

// For perf tests debugging
func benchLog(_ log: @autoclosure () -> Logger.Message) {
    #if DEBUG
    Logger.perfLogger.info(log())
    #endif
}

func createTopic(partitions: Int32) async throws -> String {
    var basicConfig = KafkaConsumerConfig()
    basicConfig.consumptionStrategy = .group(id: "no-group", topics: [])
    basicConfig.bootstrapServers = [bootstrapServer]
    basicConfig.brokerAddressFamily = .v4

    let client = try RDKafkaClient.makeClientForTopics(config: basicConfig, logger: .perfLogger)
    return try await client._createUniqueTopic(partitions: partitions)
}

func deleteTopic(_ topic: String) async throws {
    var basicConfig = KafkaConsumerConfig()
    basicConfig.consumptionStrategy = .group(id: "no-group", topics: [])
    basicConfig.bootstrapServers = [bootstrapServer]
    basicConfig.brokerAddressFamily = .v4

    let client = try RDKafkaClient.makeClientForTopics(config: basicConfig, logger: .perfLogger)
    try await client._deleteTopic(topic)
}

func prepareTopic(messagesCount: UInt, partitions: Int32 = -1, logger: Logger = .perfLogger) async throws -> String {
    let uniqueTestTopic = try await createTopic(partitions: partitions)

    benchLog("Created topic \(uniqueTestTopic)")

    benchLog("Generating \(messagesCount) messages")
    let testMessages = _createTestMessages(topic: uniqueTestTopic, count: messagesCount)
    benchLog("Finish generating \(messagesCount) messages")

    var producerConfig = KafkaProducerConfig()
    producerConfig.bootstrapServers = [bootstrapServer]
    producerConfig.brokerAddressFamily = .v4

    let (producer, acks) = try KafkaProducer.makeProducerWithEvents(config: producerConfig, logger: logger)

    let serviceGroupConfiguration = ServiceGroupConfiguration(
        services: [producer],
        gracefulShutdownSignals: [.sigterm, .sigint],
        logger: logger
    )
    let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

    try await withThrowingTaskGroup(of: Void.self) { group in
        benchLog("Start producing \(messagesCount) messages")
        defer {
            benchLog("Finish producing")
        }
        // Run Task
        group.addTask {
            try await serviceGroup.run()
        }

        // Producer Task
        group.addTask {
            try await _sendAndAcknowledgeMessages(
                producer: producer,
                events: acks,
                messages: testMessages,
                skipConsistencyCheck: true
            )
        }

        // Wait for Producer Task to complete
        try await group.next()
        await serviceGroup.triggerGracefulShutdown()
    }

    return uniqueTestTopic
}

// swift-format-ignore: AmbiguousTrailingClosureOverload
extension Benchmark {
    @discardableResult
    func withMeasurement<T>(_ body: () throws -> T) rethrows -> T {
        self.startMeasurement()
        defer {
            self.stopMeasurement()
        }
        return try body()
    }

    @discardableResult
    func withMeasurement<T>(_ body: () async throws -> T) async rethrows -> T {
        self.startMeasurement()
        defer {
            self.stopMeasurement()
        }
        return try await body()
    }
}
