//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Benchmark
import Crdkafka
import Dispatch
import Kafka
import Logging
import ServiceLifecycle

import struct Foundation.Date
import struct Foundation.UUID

let benchmarks = {
    var uniqueTestTopic: String!
    let numberOfPartitions: Int32 = 4
    // We perform every benchmark this many times
    let numberOfBatches: UInt = 1000
    // In every benchmark iteration, we consume this many messages
    let messageCountPerBatch: UInt = 1000

    Benchmark.defaultConfiguration = .init(
        metrics: [
            .wallClock,
            .cpuTotal,
            .contextSwitches,
            .throughput,
            .allocatedResidentMemory,
        ] + .arc,
        // We need to tell the benchmarking framework how often we are running the benchmark.
        scalingFactor: .kilo,
        maxDuration: .seconds(10_000_000),
        maxIterations: 10,
        thresholds: [
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
    )

    Benchmark.setup = {
        uniqueTestTopic = try await prepareTopic(
            messagesCount: messageCountPerBatch * numberOfBatches,
            partitions: numberOfPartitions
        )
    }

    Benchmark.teardown = {
        if let uniqueTestTopic {
            try deleteTopic(uniqueTestTopic)
        }
        uniqueTestTopic = nil
    }

    Benchmark("SwiftKafkaConsumer_basic_consumer_messages_\(messageCountPerBatch)") { benchmark in
        let uniqueGroupID = UUID().uuidString
        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: uniqueGroupID,
                topics: [uniqueTestTopic]
            ),
            bootstrapBrokerAddresses: [brokerAddress]
        )
        consumerConfig.autoOffsetReset = .beginning
        consumerConfig.broker.addressFamily = .v4
        // We must specify it at least 10 otherwise CI will timeout
        consumerConfig.pollInterval = .milliseconds(1)

        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: .perfLogger
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(
            services: [consumer],
            gracefulShutdownSignals: [.sigterm, .sigint],
            logger: .perfLogger
        )
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            benchLog("Start consuming")
            defer {
                benchLog("Finish consuming")
            }
            // Run task
            group.addTask {
                try await serviceGroup.run()
            }

            // Consumer task
            group.addTask {
                var counter: UInt64 = 0
                var tmpCounter: UInt64 = 0
                let interval: UInt64 = Swift.max(UInt64(messageCountPerBatch / 20), 1)
                let totalStartDate = Date.timeIntervalSinceReferenceDate
                var totalBytes: UInt64 = 0

                try await benchmark.withMeasurement {
                    for _ in benchmark.scaledIterations {
                        for try await record in consumer.messages {
                            counter += 1
                            totalBytes += UInt64(record.value.readableBytes)

                            tmpCounter += 1
                            if tmpCounter >= interval {
                                benchLog("read \(counter * 100 / UInt64(messageCountPerBatch))%")
                                tmpCounter = 0
                            }
                            if counter >= messageCountPerBatch {
                                // Reset counters for next iteration
                                counter = 0
                                tmpCounter = 0
                                break
                            }
                        }
                    }
                }

                let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
                let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
                benchLog(
                    "All read up to counter: \(counter), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec"
                )
            }

            // Wait for consumer task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    Benchmark("SwiftKafkaConsumer_with_offset_commit_messages_\(messageCountPerBatch)") { benchmark in
        let uniqueGroupID = UUID().uuidString
        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: uniqueGroupID,
                topics: [uniqueTestTopic]
            ),
            bootstrapBrokerAddresses: [brokerAddress]
        )
        consumerConfig.autoOffsetReset = .beginning
        consumerConfig.broker.addressFamily = .v4
        consumerConfig.isAutoCommitEnabled = false
        // We must specify it at least 10 otherwise CI will timeout
        consumerConfig.pollInterval = .milliseconds(1)

        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: .perfLogger
        )

        let serviceGroupConfiguration = ServiceGroupConfiguration(
            services: [consumer],
            gracefulShutdownSignals: [.sigterm, .sigint],
            logger: .perfLogger
        )
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            benchLog("Start consuming")
            defer {
                benchLog("Finish consuming")
            }
            // Run task
            group.addTask {
                try await serviceGroup.run()
            }

            // Consumer task
            group.addTask {
                var counter: UInt64 = 0
                var tmpCounter: UInt64 = 0
                let interval: UInt64 = Swift.max(UInt64(messageCountPerBatch / 20), 1)
                let totalStartDate = Date.timeIntervalSinceReferenceDate
                var totalBytes: UInt64 = 0

                try await benchmark.withMeasurement {
                    for _ in benchmark.scaledIterations {
                        for try await record in consumer.messages {
                            try consumer.scheduleCommit(record)

                            counter += 1
                            totalBytes += UInt64(record.value.readableBytes)

                            tmpCounter += 1
                            if tmpCounter >= interval {
                                benchLog("read \(counter * 100 / UInt64(messageCountPerBatch))%")
                                tmpCounter = 0
                            }
                            if counter >= messageCountPerBatch {
                                // Reset counters for next iteration
                                counter = 0
                                tmpCounter = 0
                                break
                            }
                        }
                    }
                }

                let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
                let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
                benchLog(
                    "All read up to counter: \(counter), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec"
                )
            }

            // Wait for consumer cask to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    Benchmark("librdkafka_basic_consumer_messages_\(messageCountPerBatch)") { benchmark in
        let uniqueGroupID = UUID().uuidString
        let rdKafkaConsumerConfig: [String: String] = [
            "group.id": uniqueGroupID,
            "bootstrap.servers": "\(brokerAddress.host):\(brokerAddress.port)",
            "broker.address.family": "v4",
            "auto.offset.reset": "beginning",
        ]

        let configPointer: OpaquePointer = rd_kafka_conf_new()
        for (key, value) in rdKafkaConsumerConfig {
            precondition(rd_kafka_conf_set(configPointer, key, value, nil, 0) == RD_KAFKA_CONF_OK)
        }

        let kafkaHandle = rd_kafka_new(RD_KAFKA_CONSUMER, configPointer, nil, 0)
        guard let kafkaHandle else {
            preconditionFailure("Kafka handle was not created")
        }
        defer {
            rd_kafka_destroy(kafkaHandle)
        }

        rd_kafka_poll_set_consumer(kafkaHandle)
        let subscriptionList = rd_kafka_topic_partition_list_new(1)
        defer {
            rd_kafka_topic_partition_list_destroy(subscriptionList)
        }
        rd_kafka_topic_partition_list_add(
            subscriptionList,
            uniqueTestTopic,
            RD_KAFKA_PARTITION_UA
        )
        rd_kafka_subscribe(kafkaHandle, subscriptionList)
        rd_kafka_poll(kafkaHandle, 0)

        var counter: UInt64 = 0
        var tmpCounter: UInt64 = 0

        let interval: UInt64 = Swift.max(UInt64(messageCountPerBatch / 20), 1)
        let totalStartDate = Date.timeIntervalSinceReferenceDate
        var totalBytes: UInt64 = 0

        benchmark.withMeasurement {
            for _ in benchmark.scaledIterations {
                while counter < messageCountPerBatch {
                    guard let record = rd_kafka_consumer_poll(kafkaHandle, 10) else {
                        continue
                    }
                    defer {
                        rd_kafka_message_destroy(record)
                    }
                    counter += 1
                    totalBytes += UInt64(record.pointee.len)

                    tmpCounter += 1
                    if tmpCounter >= interval {
                        benchLog("read \(counter * 100 / UInt64(messageCountPerBatch))%")
                        tmpCounter = 0
                    }
                }

                // Reset counters for next iteration
                counter = 0
                tmpCounter = 0
            }
        }

        rd_kafka_consumer_close(kafkaHandle)

        let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
        let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
        benchLog(
            "All read up to counter: \(counter), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec"
        )
    }

    // Benchmark("librdkafka_with_offset_commit_messages_\(messageCountPerBatch)") { benchmark in
    //     let uniqueGroupID = UUID().uuidString
    //     let rdKafkaConsumerConfig: [String: String] = [
    //         "group.id": uniqueGroupID,
    //         "bootstrap.servers": "\(brokerAddress.host):\(brokerAddress.port)",
    //         "broker.address.family": "v4",
    //         "auto.offset.reset": "beginning",
    //         "enable.auto.commit": "false",
    //     ]

    //     let configPointer: OpaquePointer = rd_kafka_conf_new()
    //     for (key, value) in rdKafkaConsumerConfig {
    //         precondition(rd_kafka_conf_set(configPointer, key, value, nil, 0) == RD_KAFKA_CONF_OK)
    //     }

    //     let kafkaHandle = rd_kafka_new(RD_KAFKA_CONSUMER, configPointer, nil, 0)
    //     guard let kafkaHandle else {
    //         preconditionFailure("Kafka handle was not created")
    //     }
    //     defer {
    //         rd_kafka_destroy(kafkaHandle)
    //     }

    //     rd_kafka_poll_set_consumer(kafkaHandle)
    //     let subscriptionList = rd_kafka_topic_partition_list_new(1)
    //     defer {
    //         rd_kafka_topic_partition_list_destroy(subscriptionList)
    //     }
    //     rd_kafka_topic_partition_list_add(
    //         subscriptionList,
    //         uniqueTestTopic,
    //         RD_KAFKA_PARTITION_UA
    //     )
    //     rd_kafka_subscribe(kafkaHandle, subscriptionList)
    //     rd_kafka_poll(kafkaHandle, 0)

    //     var counter: UInt64 = 0
    //     var tmpCounter: UInt64 = 0

    //     let interval: UInt64 = Swift.max(UInt64(messageCountPerBatch / 20), 1)
    //     let totalStartDate = Date.timeIntervalSinceReferenceDate
    //     var totalBytes: UInt64 = 0

    //     benchmark.withMeasurement {
    //         var myCtr: UInt64 = 0
    //         for _ in benchmark.scaledIterations {
    //             myCtr += 1
    //             print(myCtr)
    //             while counter < messageCountPerBatch {
    //                 guard let record = rd_kafka_consumer_poll(kafkaHandle, 10) else {
    //                     continue
    //                 }
    //                 defer {
    //                     rd_kafka_message_destroy(record)
    //                 }
    //                 guard record.pointee.err != RD_KAFKA_RESP_ERR__PARTITION_EOF else {
    //                     continue
    //                 }
    //                 let result = rd_kafka_commit_message(kafkaHandle, record, 1)
    //                 precondition(result == RD_KAFKA_RESP_ERR_NO_ERROR)

    //                 counter += 1
    //                 totalBytes += UInt64(record.pointee.len)

    //                 tmpCounter += 1
    //                 if tmpCounter >= interval {
    //                     benchLog("read \(counter * 100 / UInt64(messageCountPerBatch))%")
    //                     tmpCounter = 0
    //                 }
    //             }

    //             // Reset counters for next iteration
    //             counter = 0
    //             tmpCounter = 0
    //         }
    //     }

    //     rd_kafka_consumer_close(kafkaHandle)

    //     let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
    //     let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
    //     benchLog(
    //         "All read up to counter: \(counter), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec"
    //     )
    // }
}
