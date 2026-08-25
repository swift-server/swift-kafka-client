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
import NIOCore

// Deterministic, broker-free micro-benchmarks that measure per-operation
// allocations and CPU instructions for the library's hot paths. Because these
// don't touch a broker or the network, their allocation/instruction counts are
// reproducible, which makes them suitable for gating on every pull request.
//
// The threshold tolerances below are compared against the base branch by the
// benchmark CI workflow; allocation/retain counts are deterministic for
// pure-Swift paths, so any change is treated as a real regression.

let benchmarks: @Sendable () -> Void = {
    Benchmark.defaultConfiguration = .init(
        metrics: [
            .mallocCountTotal,
            .objectAllocCount,
            .instructions,
        ],
        warmupIterations: 1,
        scalingFactor: .kilo,
        maxDuration: .seconds(5),
        maxIterations: 100,
        thresholds: [
            // Gate only on the allocation counts, with a small *absolute*
            // tolerance (the swift-nio approach). Absolute rather than relative
            // because the smallest benchmarks have near-zero counts where a
            // percentage is meaningless. The retain/release counts are
            // intentionally not measured: they have run-to-run jitter unrelated to
            // real allocation changes. Instructions are only recorded where perf
            // is available (not on the Linux CI runners).
            .mallocCountTotal: .init(absolute: [.p90: 10]),
            .objectAllocCount: .init(absolute: [.p90: 10]),
            .instructions: .init(relative: [.p90: 5]),
        ]
    )

    let topic: KafkaTopic = "benchmark-topic"

    // MARK: - Producer message construction (P1–P3)
    //
    // `KafkaProducerMessage` is generic over `KafkaContiguousBytes`, so the same
    // construction is measured for each supported payload representation. Each
    // has a different `withUnsafeBytes` cost downstream, but here we isolate the
    // message-value allocation itself.

    Benchmark("ProducerMessage_build_UInt8_256B") { benchmark in
        let key: [UInt8] = Array("benchmark-key".utf8)
        let value = [UInt8](repeating: 0xAB, count: 256)
        benchmark.startMeasurement()
        for _ in benchmark.scaledIterations {
            blackHole(KafkaProducerMessage(topic: topic, key: key, value: value))
        }
        benchmark.stopMeasurement()
    }

    Benchmark("ProducerMessage_build_ByteBuffer_256B") { benchmark in
        let key = ByteBuffer(string: "benchmark-key")
        let value = ByteBuffer(repeating: 0xAB, count: 256)
        benchmark.startMeasurement()
        for _ in benchmark.scaledIterations {
            blackHole(KafkaProducerMessage(topic: topic, key: key, value: value))
        }
        benchmark.stopMeasurement()
    }

    Benchmark("ProducerMessage_build_String_256B") { benchmark in
        let key = "benchmark-key"
        let value = String(repeating: "a", count: 256)
        benchmark.startMeasurement()
        for _ in benchmark.scaledIterations {
            blackHole(KafkaProducerMessage(topic: topic, key: key, value: value))
        }
        benchmark.stopMeasurement()
    }

    // MARK: - Message with headers (S3)

    Benchmark("ProducerMessage_build_8_headers") { benchmark in
        let value = [UInt8](repeating: 0xAB, count: 64)
        let headerKeys = (0..<8).map { "header-\($0)" }
        let headerValue = Array("header-value".utf8)
        benchmark.startMeasurement()
        for _ in benchmark.scaledIterations {
            var headers: [KafkaHeader] = []
            headers.reserveCapacity(headerKeys.count)
            for headerKey in headerKeys {
                headers.append(KafkaHeader(key: headerKey, value: headerValue))
            }
            blackHole(KafkaProducerMessage(topic: topic, headers: headers, value: value))
        }
        benchmark.stopMeasurement()
    }

    // MARK: - Topic-partition input lists (S1–S2)
    //
    // Commit/seek/assign build these lists per call, one entry per partition.

    Benchmark("KafkaTopicPartition_list_100") { benchmark in
        benchmark.startMeasurement()
        for _ in benchmark.scaledIterations {
            var list: [KafkaTopicPartition] = []
            list.reserveCapacity(100)
            for partition in 0..<100 {
                list.append(KafkaTopicPartition(topic: topic, partition: KafkaPartition(rawValue: partition)))
            }
            blackHole(list)
        }
        benchmark.stopMeasurement()
    }

    Benchmark("KafkaTopicPartitionOffset_list_100") { benchmark in
        benchmark.startMeasurement()
        for _ in benchmark.scaledIterations {
            var list: [KafkaTopicPartitionOffset] = []
            list.reserveCapacity(100)
            for partition in 0..<100 {
                list.append(
                    KafkaTopicPartitionOffset(
                        topic: topic,
                        partition: KafkaPartition(rawValue: partition),
                        offset: .end
                    )
                )
            }
            blackHole(list)
        }
        benchmark.stopMeasurement()
    }

    // MARK: - Configuration construction (C1–C2)

    Benchmark("KafkaProducerConfig_build") { benchmark in
        benchmark.startMeasurement()
        for _ in benchmark.scaledIterations {
            var config = KafkaProducerConfig()
            config.bootstrapServers = ["localhost:9092"]
            config.brokerAddressFamily = .v4
            blackHole(config)
        }
        benchmark.stopMeasurement()
    }

    Benchmark("KafkaConsumerConfig_build") { benchmark in
        benchmark.startMeasurement()
        for _ in benchmark.scaledIterations {
            var config = KafkaConsumerConfig()
            config.consumptionStrategy = .group(id: "benchmark-group", topics: [topic])
            config.bootstrapServers = ["localhost:9092"]
            config.brokerAddressFamily = .v4
            blackHole(config)
        }
        benchmark.stopMeasurement()
    }
}
