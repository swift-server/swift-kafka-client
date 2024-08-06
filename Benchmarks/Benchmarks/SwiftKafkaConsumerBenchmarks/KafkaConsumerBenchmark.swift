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
import struct Foundation.Date
import struct Foundation.UUID
import Kafka
import Logging
import ServiceLifecycle

let benchmarks = {
    var uniqueTestTopic: String!
    let messageCount: UInt = 1000

    Benchmark.defaultConfiguration = .init(
        metrics: [
            .wallClock,
            .cpuTotal,
            .contextSwitches,
            .throughput,
            .allocatedResidentMemory,
        ] + .arc,
        warmupIterations: 0,
        scalingFactor: .one,
        maxDuration: .seconds(5),
        maxIterations: 100,
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
        uniqueTestTopic = try await prepareTopic(messagesCount: messageCount, partitions: 6)
    }

    Benchmark.teardown = {
        if let uniqueTestTopic {
            try deleteTopic(uniqueTestTopic)
        }
        uniqueTestTopic = nil
    }

    Benchmark("SwiftKafkaConsumer_basic_consumer_messages_\(messageCount)") { benchmark in
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

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], gracefulShutdownSignals: [.sigterm, .sigint], logger: .perfLogger)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            benchLog("Start consuming")
            defer {
                benchLog("Finish consuming")
            }
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Second Consumer Task
            group.addTask {
                var ctr: UInt64 = 0
                var tmpCtr: UInt64 = 0
                let interval: UInt64 = Swift.max(UInt64(messageCount / 20), 1)
                let totalStartDate = Date.timeIntervalSinceReferenceDate
                var totalBytes: UInt64 = 0

                try await benchmark.withMeasurement {
                    for try await record in consumer.messages {
                        ctr += 1
                        totalBytes += UInt64(record.value.readableBytes)

                        tmpCtr += 1
                        if tmpCtr >= interval {
                            benchLog("read \(ctr * 100 / UInt64(messageCount))%")
                            tmpCtr = 0
                        }
                        if ctr >= messageCount {
                            break
                        }
                    }
                }

                let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
                let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
                benchLog("All read up to ctr: \(ctr), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec")
            }

            // Wait for second Consumer Task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    Benchmark("SwiftKafkaConsumer_with_offset_commit_messages_\(messageCount)") { benchmark in
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

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], gracefulShutdownSignals: [.sigterm, .sigint], logger: .perfLogger)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            benchLog("Start consuming")
            defer {
                benchLog("Finish consuming")
            }
            // Run Task
            group.addTask {
                try await serviceGroup.run()
            }

            // Second Consumer Task
            group.addTask {
                var ctr: UInt64 = 0
                var tmpCtr: UInt64 = 0
                let interval: UInt64 = Swift.max(UInt64(messageCount / 20), 1)
                let totalStartDate = Date.timeIntervalSinceReferenceDate
                var totalBytes: UInt64 = 0

                try await benchmark.withMeasurement {
                    for try await record in consumer.messages {
                        try consumer.scheduleCommit(record)

                        ctr += 1
                        totalBytes += UInt64(record.value.readableBytes)

                        tmpCtr += 1
                        if tmpCtr >= interval {
                            benchLog("read \(ctr * 100 / UInt64(messageCount))%")
                            tmpCtr = 0
                        }
                        if ctr >= messageCount {
                            break
                        }
                    }
                }

                let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
                let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
                benchLog("All read up to ctr: \(ctr), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec")
            }

            // Wait for second Consumer Task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    Benchmark("librdkafka_basic_consumer_messages_\(messageCount)") { benchmark in
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

        var ctr: UInt64 = 0
        var tmpCtr: UInt64 = 0

        let interval: UInt64 = Swift.max(UInt64(messageCount / 20), 1)
        let totalStartDate = Date.timeIntervalSinceReferenceDate
        var totalBytes: UInt64 = 0

        benchmark.withMeasurement {
            while ctr < messageCount {
                guard let record = rd_kafka_consumer_poll(kafkaHandle, 10) else {
                    continue
                }
                defer {
                    rd_kafka_message_destroy(record)
                }
                ctr += 1
                totalBytes += UInt64(record.pointee.len)

                tmpCtr += 1
                if tmpCtr >= interval {
                    benchLog("read \(ctr * 100 / UInt64(messageCount))%")
                    tmpCtr = 0
                }
            }
        }

        rd_kafka_consumer_close(kafkaHandle)

        let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
        let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
        benchLog("All read up to ctr: \(ctr), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec")
    }

    Benchmark("librdkafka_with_offset_commit_messages_\(messageCount)") { benchmark in
        let uniqueGroupID = UUID().uuidString
        let rdKafkaConsumerConfig: [String: String] = [
            "group.id": uniqueGroupID,
            "bootstrap.servers": "\(brokerAddress.host):\(brokerAddress.port)",
            "broker.address.family": "v4",
            "auto.offset.reset": "beginning",
            "enable.auto.commit": "false",
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

        var ctr: UInt64 = 0
        var tmpCtr: UInt64 = 0

        let interval: UInt64 = Swift.max(UInt64(messageCount / 20), 1)
        let totalStartDate = Date.timeIntervalSinceReferenceDate
        var totalBytes: UInt64 = 0

        benchmark.withMeasurement {
            while ctr < messageCount {
                guard let record = rd_kafka_consumer_poll(kafkaHandle, 10) else {
                    continue
                }
                defer {
                    rd_kafka_message_destroy(record)
                }
                guard record.pointee.err != RD_KAFKA_RESP_ERR__PARTITION_EOF else {
                    continue
                }
                let result = rd_kafka_commit_message(kafkaHandle, record, 0)
                precondition(result == RD_KAFKA_RESP_ERR_NO_ERROR)

                ctr += 1
                totalBytes += UInt64(record.pointee.len)

                tmpCtr += 1
                if tmpCtr >= interval {
                    benchLog("read \(ctr * 100 / UInt64(messageCount))%")
                    tmpCtr = 0
                }
            }
        }

        rd_kafka_consumer_close(kafkaHandle)

        let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
        let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
        benchLog("All read up to ctr: \(ctr), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec")
    }
}
