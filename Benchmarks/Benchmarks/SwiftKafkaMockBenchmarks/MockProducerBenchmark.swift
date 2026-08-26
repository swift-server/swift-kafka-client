//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2025 Apple Inc. and the swift-kafka-client project authors
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

// Benchmark that runs against librdkafka's in-process mock cluster
// (`test.mock.num.brokers`), exercising the real produce path end-to-end without
// an external Kafka broker.
//
// The mock cluster is a full in-process broker (its own threads, timing-dependent
// batching), so this benchmark is too noisy for absolute-threshold gating. It is
// therefore opt-in via `KAFKA_MOCK_BENCHMARK` and only runs on a manual workflow
// dispatch — never in the PR-gated benchmark set.
//
// Note: the mock cluster is per-client, so a message produced by one client is
// not visible to a different client. That makes the *consumer* decode path
// impossible to benchmark this way (a real `rd_kafka_message_t` requires a real
// broker), so only the producer send path is covered here.

// swift-format-ignore: DontRepeatTypeInStaticProperties
extension Logger {
    static let benchLogger: Logger = {
        var logger = Logger(label: "benchmark")
        logger.logLevel = .critical
        return logger
    }()
}

let benchmarks: @Sendable () -> Void = {
    // Opt-in only (manual workflow dispatch). Registers nothing otherwise, so it
    // never runs in the PR-gated benchmark set.
    guard ProcessInfo.processInfo.environment["KAFKA_MOCK_BENCHMARK"] != nil else {
        return
    }

    let messageCount: UInt = 1000

    Benchmark.defaultConfiguration = .init(
        metrics: [
            .mallocCountTotal,
            .instructions,
            .allocatedResidentMemory,
            .throughput,
        ] + .arc,
        warmupIterations: 1,
        scalingFactor: .one,
        maxDuration: .seconds(5),
        maxIterations: 100,
        thresholds: [
            // Swift-level ARC counts are fairly stable for this path.
            .objectAllocCount: .init(relative: [.p90: 15]),
            .retainCount: .init(relative: [.p90: 15]),
            .releaseCount: .init(relative: [.p90: 15]),
            .retainReleaseDelta: .init(relative: [.p90: 15]),
            .instructions: .init(relative: [.p90: 20]),
            // Total mallocs and resident memory include librdkafka's mock-cluster
            // internals and are noisy, so keep these tolerances wide.
            .mallocCountTotal: .init(relative: [.p90: 40]),
            .allocatedResidentMemory: .init(relative: [.p90: 30]),
            .throughput: .init(relative: [.p90: 40]),
        ]
    )

    Benchmark("Producer_sendAck_mockCluster_\(messageCount)") { benchmark in
        var producerConfig = KafkaProducerConfig()
        // Spin up an in-process mock broker instead of connecting to a real one.
        producerConfig.additionalConfig["test.mock.num.brokers"] = "1"
        producerConfig.brokerAddressFamily = .v4

        let (producer, events) = try KafkaProducer.makeProducer(
            config: producerConfig
        )
        let messages = _createTestMessages(topic: "benchmark-topic", count: messageCount)

        let serviceGroupConfiguration = ServiceGroupConfiguration(
            services: [producer],
            gracefulShutdownSignals: [.sigterm, .sigint],
            logger: .benchLogger
        )
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            // Run the producer's poll loop so delivery reports are served.
            group.addTask {
                try await serviceGroup.run()
            }

            // Send all messages and await their acknowledgements from the mock.
            // Measured in the parent task so `benchmark` isn't captured by a child task.
            benchmark.startMeasurement()
            try await _sendAndAcknowledgeMessages(
                producer: producer,
                events: events,
                messages: messages,
                skipConsistencyCheck: true
            )
            benchmark.stopMeasurement()

            await serviceGroup.triggerGracefulShutdown()
        }
    }
}
