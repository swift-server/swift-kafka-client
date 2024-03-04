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
import class Foundation.ProcessInfo
import struct Foundation.UUID
import Kafka
@_spi(Internal) import Kafka
import Logging
import ServiceLifecycle

let brokerAddress = KafkaConfiguration.BrokerAddress(
    host: ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost",
    port: 9092
)

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

func createTopic(partitions: Int32) throws -> String {
    var basicConfig = KafkaConsumerConfiguration(
        consumptionStrategy: .group(id: "no-group", topics: []),
        bootstrapBrokerAddresses: [brokerAddress]
    )
    basicConfig.broker.addressFamily = .v4

    let client = try RDKafkaClient.makeClientForTopics(config: basicConfig, logger: .perfLogger)
    return try client._createUniqueTopic(partitions: partitions, timeout: 10 * 1000)
}

func deleteTopic(_ topic: String) throws {
    var basicConfig = KafkaConsumerConfiguration(
        consumptionStrategy: .group(id: "no-group", topics: []),
        bootstrapBrokerAddresses: [brokerAddress]
    )
    basicConfig.broker.addressFamily = .v4

    let client = try RDKafkaClient.makeClientForTopics(config: basicConfig, logger: .perfLogger)
    try client._deleteTopic(topic, timeout: 10 * 1000)
}

func prepareTopic(messagesCount: UInt, partitions: Int32 = -1, logger: Logger = .perfLogger) async throws -> String {
    let uniqueTestTopic = try createTopic(partitions: partitions)

    benchLog("Created topic \(uniqueTestTopic)")

    benchLog("Generating \(messagesCount) messages")
    let testMessages = _createTestMessages(topic: uniqueTestTopic, count: messagesCount)
    benchLog("Finish generating \(messagesCount) messages")

    var producerConfig = KafkaProducerConfiguration(bootstrapBrokerAddresses: [brokerAddress])
    producerConfig.broker.addressFamily = .v4

    let (producer, acks) = try KafkaProducer.makeProducerWithEvents(configuration: producerConfig, logger: logger)

    let serviceGroupConfiguration = ServiceGroupConfiguration(services: [producer], gracefulShutdownSignals: [.sigterm, .sigint], logger: logger)
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

private let stableBenchmarkMetrics: [BenchmarkMetric] = [
    .allocatedResidentMemory,
]

private let allMetricsToMeasure: [BenchmarkMetric] = [
    .wallClock,
    .cpuTotal,
    .contextSwitches,
    .throughput,
] + stableBenchmarkMetrics + .arc

private let useStableMetrics = Bool(ProcessInfo.processInfo.environment["KAFKA_USE_STABLE_BENCHMARK_METRICS"] ?? "false") == true

let metricsToMeasure: [BenchmarkMetric] =
    useStableMetrics
        ? stableBenchmarkMetrics
        : allMetricsToMeasure

let stableMetricsThreasholds: [BenchmarkMetric: BenchmarkThresholds] = [
    .allocatedResidentMemory: .init(relative: [.p90: 20]),
    .objectAllocCount: .init(relative: [.p90: 20]),
    .retainCount: .init(relative: [.p90: 20]),
    .releaseCount: .init(relative: [.p90: 20]),
    .retainReleaseDelta: .init(relative: [.p90: 20]),
]

let allMetricsThreasholds: [BenchmarkMetric: BenchmarkThresholds] = [
    .wallClock: .init(relative: [.p90: 35]),
    .cpuTotal: .init(relative: [.p90: 35]),
    .allocatedResidentMemory: .init(relative: [.p90: 20]),
    .contextSwitches: .init(relative: [.p90: 35]),
    .throughput: .init(relative: [.p90: 35]),
    .objectAllocCount: .init(relative: [.p90: 20]),
    .retainCount: .init(relative: [.p90: 20]),
    .releaseCount: .init(relative: [.p90: 20]),
    .retainReleaseDelta: .init(relative: [.p90: 20]),
]

let metricsThreasholds: [BenchmarkMetric: BenchmarkThresholds] =
    useStableMetrics
        ? stableMetricsThreasholds
        : allMetricsThreasholds
