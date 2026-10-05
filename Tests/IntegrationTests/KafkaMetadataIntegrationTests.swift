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

import Kafka
import ServiceLifecycle
import Testing

import struct Foundation.UUID

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

private let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
private let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!

private struct MetadataNotReady: Error {
    let topic: KafkaTopic
}

@Suite(.timeLimit(.minutes(5)), .serialized)
struct KafkaMetadataIntegrationTests {
    private func makeConsumerConfig() -> KafkaConsumerConfig {
        var config = KafkaConsumerConfig()
        config.groupId = UUID().uuidString
        config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
        config.brokerAddressFamily = .v4
        return config
    }

    /// Returns metadata for `topic` once every partition has a leader.
    ///
    /// Brokers learn about a newly created topic asynchronously, so the first requests after
    /// `CreateTopics` can briefly report no leaders. This polls for a bounded time instead of
    /// asserting on the first response.
    private func metadataOnceLeadersAreAssigned(
        _ consumer: KafkaConsumer,
        topic: KafkaTopic?,
        expecting expectedTopic: KafkaTopic,
        partitions: Int
    ) async throws -> (metadata: KafkaClusterMetadata, topic: KafkaClusterMetadata.Topic) {
        for _ in 0..<50 {
            let metadata = try await consumer.metadata(topic: topic, timeout: .seconds(10))
            if let described = metadata.topics.first(where: { $0.name == expectedTopic }),
                described.error == nil,
                described.partitions.count == partitions,
                described.partitions.allSatisfy({ $0.leader != nil })
            {
                return (metadata, described)
            }
            try await Task.sleep(for: .milliseconds(200))
        }
        Issue.record("Topic \(expectedTopic) never reported \(partitions) partitions with leaders")
        throw MetadataNotReady(topic: expectedTopic)
    }

    @Test func metadataDescribesTopicPartitionsLeadersAndReplicas() async throws {
        try await withTestTopic(partitions: 3) { testTopic in
            let (consumer, _, _) = try KafkaConsumer.makeConsumer(config: self.makeConsumerConfig())

            // Called before `run()`: metadata requests don't depend on the consumer's poll loop.
            let (metadata, topic) = try await self.metadataOnceLeadersAreAssigned(
                consumer,
                topic: testTopic,
                expecting: testTopic,
                partitions: 3
            )

            // Asking for one topic returns only that topic.
            #expect(metadata.topics.map(\.name) == [testTopic])

            #expect(!metadata.brokers.isEmpty)
            #expect(metadata.brokers.allSatisfy { !$0.host.isEmpty && $0.port > 0 })
            let brokerIDs = Set(metadata.brokers.map(\.id))

            #expect(topic.partitions.map(\.id.rawValue).sorted() == [0, 1, 2])
            for partition in topic.partitions {
                #expect(partition.error == nil)
                let leader = try #require(partition.leader)
                // The leader is a known broker and holds one of the partition's replicas.
                #expect(brokerIDs.contains(leader))
                #expect(partition.replicas.contains(leader))
                // In-sync replicas are a non-empty subset of the replicas.
                #expect(!partition.inSyncReplicas.isEmpty)
                #expect(Set(partition.inSyncReplicas).isSubset(of: Set(partition.replicas)))
                #expect(Set(partition.replicas).isSubset(of: brokerIDs))
            }

            if let originatingID = metadata.originatingBroker.id {
                #expect(brokerIDs.contains(originatingID))
            }
        }
    }

    @Test func metadataForAllTopicsIncludesCreatedTopicWhileRunning() async throws {
        try await withTestTopic(partitions: 2) { testTopic in
            let (consumer, _, _) = try KafkaConsumer.makeConsumer(config: self.makeConsumerConfig())
            let serviceGroup = ServiceGroup(
                configuration: ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
            )

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask { try await serviceGroup.run() }

                // `topic: nil` describes every topic in the cluster, including the one just created.
                let (metadata, topic) = try await self.metadataOnceLeadersAreAssigned(
                    consumer,
                    topic: nil,
                    expecting: testTopic,
                    partitions: 2
                )
                #expect(metadata.topics.count >= 1)
                #expect(topic.partitions.map(\.id.rawValue).sorted() == [0, 1])

                await serviceGroup.triggerGracefulShutdown()
                try await group.waitForAll()
            }
        }
    }

    // This file imports `Kafka` without `@testable`, so this only compiles if the initializers are
    // public. It doesn't need a broker.
    @Test func metadataValuesCanBeCreatedAndModified() {
        var metadata = KafkaClusterMetadata(
            brokers: [.init(id: 1, host: "localhost", port: 9092)],
            topics: [
                .init(
                    name: "orders",
                    partitions: [
                        .init(
                            id: KafkaPartition(rawValue: 0),
                            leader: 1,
                            replicas: [1],
                            inSyncReplicas: [1],
                            error: nil
                        )
                    ],
                    error: nil
                )
            ],
            originatingBroker: .init(id: 1, name: "localhost:9092/1")
        )

        // Properties are mutable, for example to simulate a leaderless partition.
        metadata.topics[0].partitions[0].leader = nil
        metadata.topics[0].partitions[0].inSyncReplicas = []

        #expect(metadata.topics[0].partitions[0].leader == nil)
        #expect(metadata.topics[0].partitions[0].inSyncReplicas.isEmpty)
        #expect(metadata.brokers == [.init(id: 1, host: "localhost", port: 9092)])
        #expect(metadata.originatingBroker == .init(id: 1, name: "localhost:9092/1"))
    }

    @Test func metadataForMissingTopicReportsUnknownTopicWithoutCreatingIt() async throws {
        let (consumer, _, _) = try KafkaConsumer.makeConsumer(config: self.makeConsumerConfig())
        let missingTopic = KafkaTopic(rawValue: "missing-\(UUID().uuidString)")

        let metadata = try await consumer.metadata(topic: missingTopic, timeout: .seconds(10))

        // A missing topic isn't an error for the call itself; the topic entry carries the broker's code.
        let described = try #require(metadata.topics.first { $0.name == missingTopic })
        #expect(described.error == .unknownTopicOrPartition)
        #expect(described.partitions.isEmpty)

        // The broker allows auto-creation, but the consumer doesn't request it by default,
        // so asking about the topic must not have created it.
        let allTopics = try await consumer.metadata(timeout: .seconds(10))
        #expect(!allTopics.topics.map(\.name).contains(missingTopic))
    }
}
