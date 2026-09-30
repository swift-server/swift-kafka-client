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

import Crdkafka
import Logging
import ServiceLifecycle
import Testing

import struct Foundation.UUID

@testable import Kafka

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

@Suite struct KafkaClusterMetadataTests {
    // MARK: - Conversion from librdkafka

    // These build real `rd_kafka_metadata` structs in memory, so the copy logic is covered
    // without a broker. The broker-backed behaviour lives in the integration tests.

    @Test func conversionCopiesBrokersTopicsAndPartitions() {
        let brokerHost = makeCString("broker-1.example.com")
        let topicName = makeCString("orders")
        let originName = makeCString("broker-1.example.com:9092/1")
        defer {
            brokerHost.deallocate()
            topicName.deallocate()
            originName.deallocate()
        }

        let replicas = UnsafeMutablePointer<Int32>.allocate(capacity: 3)
        replicas.initialize(from: [1, 2, 3], count: 3)
        let isrs = UnsafeMutablePointer<Int32>.allocate(capacity: 2)
        isrs.initialize(from: [1, 3], count: 2)
        defer {
            replicas.deallocate()
            isrs.deallocate()
        }

        let partitions = UnsafeMutablePointer<rd_kafka_metadata_partition>.allocate(capacity: 2)
        partitions.initialize(
            from: [
                rd_kafka_metadata_partition(
                    id: 0,
                    err: RD_KAFKA_RESP_ERR_NO_ERROR,
                    leader: 1,
                    replica_cnt: 3,
                    replicas: replicas,
                    isr_cnt: 2,
                    isrs: isrs
                ),
                // A leaderless partition: librdkafka reports leader -1 and no replicas.
                rd_kafka_metadata_partition(
                    id: 1,
                    err: RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE,
                    leader: -1,
                    replica_cnt: 0,
                    replicas: nil,
                    isr_cnt: 0,
                    isrs: nil
                ),
            ],
            count: 2
        )
        defer { partitions.deallocate() }

        var broker = rd_kafka_metadata_broker(id: 1, host: brokerHost, port: 9092)
        var topic = rd_kafka_metadata_topic(
            topic: topicName,
            partition_cnt: 2,
            partitions: partitions,
            err: RD_KAFKA_RESP_ERR_NO_ERROR
        )

        let metadata = withUnsafeMutablePointer(to: &broker) { brokerPointer in
            withUnsafeMutablePointer(to: &topic) { topicPointer in
                var raw = rd_kafka_metadata(
                    broker_cnt: 1,
                    brokers: brokerPointer,
                    topic_cnt: 1,
                    topics: topicPointer,
                    orig_broker_id: 1,
                    orig_broker_name: originName
                )
                return KafkaClusterMetadata(&raw)
            }
        }

        #expect(metadata.brokers == [.init(id: 1, host: "broker-1.example.com", port: 9092)])
        #expect(metadata.originatingBroker == .init(id: 1, name: "broker-1.example.com:9092/1"))

        #expect(metadata.topics.count == 1)
        let orders = metadata.topics[0]
        #expect(orders.name == "orders")
        #expect(orders.error == nil)
        #expect(
            orders.partitions == [
                .init(
                    id: KafkaPartition(rawValue: 0),
                    leader: 1,
                    replicas: [1, 2, 3],
                    inSyncReplicas: [1, 3],
                    error: nil
                ),
                .init(
                    id: KafkaPartition(rawValue: 1),
                    leader: nil,
                    replicas: [],
                    inSyncReplicas: [],
                    error: KafkaError.RDKafkaCode(rawValue: RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE.rawValue)
                ),
            ]
        )
    }

    @Test func conversionReportsTopicErrorsAndEmptyResults() {
        let topicName = makeCString("missing-topic")
        defer { topicName.deallocate() }

        var topic = rd_kafka_metadata_topic(
            topic: topicName,
            partition_cnt: 0,
            partitions: nil,
            err: RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART
        )
        let metadata = withUnsafeMutablePointer(to: &topic) { topicPointer in
            var raw = rd_kafka_metadata(
                broker_cnt: 0,
                brokers: nil,
                topic_cnt: 1,
                topics: topicPointer,
                orig_broker_id: -1,
                orig_broker_name: nil
            )
            return KafkaClusterMetadata(&raw)
        }

        #expect(metadata.brokers.isEmpty)
        // librdkafka's -1 (bootstrap connection) maps to a nil ID.
        #expect(metadata.originatingBroker == .init(id: nil, name: ""))
        #expect(metadata.topics.map(\.name) == ["missing-topic"])
        #expect(metadata.topics[0].partitions.isEmpty)
        #expect(metadata.topics[0].error == .unknownTopicOrPartition)
    }

    @Test func unknownTopicOrPartitionMatchesLibrdkafkaCode() {
        #expect(
            KafkaError.RDKafkaCode.unknownTopicOrPartition.rawValue == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART.rawValue
        )
    }

    // MARK: - KafkaConsumer.metadata(topic:timeout:)

    @Test func metadataFromMockClusterListsBrokers() async throws {
        var config = KafkaConsumerConfig()
        config.groupId = UUID().uuidString
        config.useMockBroker(count: 3)
        config.brokerAddressFamily = .v4
        let (consumer, _, _) = try KafkaConsumer.makeConsumer(config: config)

        // Called before `run()`: the metadata request doesn't need the consumer's poll loop.
        let metadata = try await consumer.metadata(timeout: .seconds(10))

        #expect(metadata.brokers.count == 3)
        #expect(Set(metadata.brokers.map(\.id)).count == 3)
        #expect(metadata.brokers.allSatisfy { !$0.host.isEmpty && $0.port > 0 })
        // The answering connection may be a bootstrap connection with no broker ID yet (nil);
        // a known ID must belong to one of the listed brokers.
        #expect(!metadata.originatingBroker.name.isEmpty)
        if let originatingID = metadata.originatingBroker.id {
            #expect(metadata.brokers.map(\.id).contains(originatingID))
        }
    }

    @Test func metadataFailsOnClosedConsumer() async throws {
        var config = KafkaConsumerConfig()
        config.groupId = UUID().uuidString
        config.useMockBroker()
        config.brokerAddressFamily = .v4
        let (consumer, _, _) = try KafkaConsumer.makeConsumer(config: config)

        let serviceGroupConfiguration = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: serviceGroupConfiguration)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(500), tolerance: .zero)
            await serviceGroup.triggerGracefulShutdown()
            try await group.waitForAll()
        }

        await #expect(throws: KafkaError.self) {
            _ = try await consumer.metadata(timeout: .milliseconds(1000))
        }
    }
}

/// Copies `string` into a newly allocated NUL-terminated C string; release it with `deallocate()`.
///
/// Avoids `strdup`/`free`, which aren't in scope on Linux without importing the platform C library.
private func makeCString(_ string: String) -> UnsafeMutablePointer<CChar> {
    let bytes = Array(string.utf8CString)
    let pointer = UnsafeMutablePointer<CChar>.allocate(capacity: bytes.count)
    pointer.initialize(from: bytes, count: bytes.count)
    return pointer
}
