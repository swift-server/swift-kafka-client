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

/// A snapshot of Kafka cluster metadata, as reported by a broker.
///
/// Retrieve it with ``KafkaConsumer/metadata(topic:timeout:)``. The metadata reflects the
/// responding broker's view of the cluster at the time of the request, so leadership and
/// in-sync replica information can change immediately afterwards (for example, during a
/// leader election). You can also create values directly, for example to stand in for a
/// broker's response in tests.
public struct KafkaClusterMetadata: Sendable, Hashable {
    /// A broker in the cluster.
    public struct Broker: Sendable, Hashable {
        /// The broker ID.
        public var id: Int
        /// The broker's advertised hostname.
        public var host: String
        /// The broker's advertised port.
        public var port: Int

        /// Creates a broker description with the ID and address you provide.
        ///
        /// - Parameters:
        ///   - id: The broker ID.
        ///   - host: The broker's advertised hostname.
        ///   - port: The broker's advertised port.
        public init(id: Int, host: String, port: Int) {
            self.id = id
            self.host = host
            self.port = port
        }
    }

    /// A topic and its partitions.
    public struct Topic: Sendable, Hashable {
        /// The topic name.
        public var name: KafkaTopic
        /// The topic's partitions, including leader and replica assignments.
        public var partitions: [Partition]
        /// The error the broker reported for this topic, or `nil` if there was none.
        ///
        /// For example, a topic that doesn't exist reports
        /// ``KafkaError/RDKafkaCode/unknownTopicOrPartition``.
        public var error: KafkaError.RDKafkaCode?

        /// Creates a topic description with the name, partitions, and error you provide.
        ///
        /// - Parameters:
        ///   - name: The topic name.
        ///   - partitions: The topic's partitions.
        ///   - error: The error reported for the topic, or `nil` if there was none.
        public init(name: KafkaTopic, partitions: [Partition], error: KafkaError.RDKafkaCode?) {
            self.name = name
            self.partitions = partitions
            self.error = error
        }
    }

    /// A partition of a topic.
    public struct Partition: Sendable, Hashable {
        /// The partition ID.
        public var id: KafkaPartition
        /// The ID of the broker that leads this partition, or `nil` if the partition has no leader.
        public var leader: Int?
        /// The IDs of the brokers that hold a replica of this partition.
        public var replicas: [Int]
        /// The IDs of the brokers whose replicas are in sync with the leader.
        public var inSyncReplicas: [Int]
        /// The error the broker reported for this partition, or `nil` if there was none.
        public var error: KafkaError.RDKafkaCode?

        /// Creates a partition description with the leader, replicas, and error you provide.
        ///
        /// - Parameters:
        ///   - id: The partition ID.
        ///   - leader: The ID of the leading broker, or `nil` if the partition has no leader.
        ///   - replicas: The IDs of the brokers that hold a replica.
        ///   - inSyncReplicas: The IDs of the brokers whose replicas are in sync with the leader.
        ///   - error: The error reported for the partition, or `nil` if there was none.
        public init(
            id: KafkaPartition,
            leader: Int?,
            replicas: [Int],
            inSyncReplicas: [Int],
            error: KafkaError.RDKafkaCode?
        ) {
            self.id = id
            self.leader = leader
            self.replicas = replicas
            self.inSyncReplicas = inSyncReplicas
            self.error = error
        }
    }

    /// The broker connection that answered a metadata request.
    public struct OriginatingBroker: Sendable, Hashable {
        /// The broker ID, or `nil` when a bootstrap connection answered before the client
        /// learned that broker's ID.
        public var id: Int?
        /// The librdkafka name of the connection (for example, `localhost:9092/1`, or
        /// `localhost:9092/bootstrap` for a bootstrap connection).
        public var name: String

        /// Creates an originating-broker description with the ID and name you provide.
        ///
        /// - Parameters:
        ///   - id: The broker ID, or `nil` if it isn't known.
        ///   - name: The librdkafka name of the connection.
        public init(id: Int?, name: String) {
            self.id = id
            self.name = name
        }
    }

    /// The brokers in the cluster.
    public var brokers: [Broker]
    /// The topics included in this snapshot.
    public var topics: [Topic]
    /// The broker connection that answered the metadata request.
    public var originatingBroker: OriginatingBroker

    /// Creates cluster metadata with the brokers, topics, and originating broker you provide.
    ///
    /// - Parameters:
    ///   - brokers: The brokers in the cluster.
    ///   - topics: The topics included in the snapshot.
    ///   - originatingBroker: The broker connection that answered the request.
    public init(brokers: [Broker], topics: [Topic], originatingBroker: OriginatingBroker) {
        self.brokers = brokers
        self.topics = topics
        self.originatingBroker = originatingBroker
    }
}

// MARK: - Conversion from librdkafka

// The conversion initializers delegate to the public initializers above.

extension KafkaClusterMetadata {
    /// Copies a librdkafka metadata result into Swift values.
    ///
    /// The caller still owns `metadata` and must release it with `rd_kafka_metadata_destroy`.
    init(_ metadata: UnsafePointer<rd_kafka_metadata>) {
        let metadata = metadata.pointee
        self.init(
            brokers: copyArray(metadata.brokers, count: metadata.broker_cnt, Broker.init),
            topics: copyArray(metadata.topics, count: metadata.topic_cnt, Topic.init),
            originatingBroker: OriginatingBroker(
                // librdkafka reports -1 for a bootstrap connection whose broker ID isn't known yet.
                id: metadata.orig_broker_id >= 0 ? Int(metadata.orig_broker_id) : nil,
                name: metadata.orig_broker_name.map { String(cString: $0) } ?? ""
            )
        )
    }
}

extension KafkaClusterMetadata.Broker {
    init(_ broker: rd_kafka_metadata_broker) {
        self.init(
            id: Int(broker.id),
            host: broker.host.map { String(cString: $0) } ?? "",
            port: Int(broker.port)
        )
    }
}

extension KafkaClusterMetadata.Topic {
    init(_ topic: rd_kafka_metadata_topic) {
        self.init(
            name: KafkaTopic(rawValue: topic.topic.map { String(cString: $0) } ?? ""),
            partitions: copyArray(
                topic.partitions,
                count: topic.partition_cnt,
                KafkaClusterMetadata.Partition.init
            ),
            error: KafkaError.RDKafkaCode(reportedBy: topic.err)
        )
    }
}

extension KafkaClusterMetadata.Partition {
    init(_ partition: rd_kafka_metadata_partition) {
        self.init(
            id: KafkaPartition(rawValue: Int(partition.id)),
            // librdkafka reports -1 when the partition has no leader.
            leader: partition.leader >= 0 ? Int(partition.leader) : nil,
            replicas: copyArray(partition.replicas, count: partition.replica_cnt) { Int($0) },
            inSyncReplicas: copyArray(partition.isrs, count: partition.isr_cnt) { Int($0) },
            error: KafkaError.RDKafkaCode(reportedBy: partition.err)
        )
    }
}

extension KafkaError.RDKafkaCode {
    /// Wraps a broker-reported error code, returning `nil` for `RD_KAFKA_RESP_ERR_NO_ERROR`.
    fileprivate init?(reportedBy error: rd_kafka_resp_err_t) {
        guard error != RD_KAFKA_RESP_ERR_NO_ERROR else { return nil }
        self.init(rawValue: error.rawValue)
    }
}

/// Copies `count` elements of a C array, tolerating a `NULL` base pointer for empty arrays.
private func copyArray<Element, Result>(
    _ base: UnsafeMutablePointer<Element>?,
    count: Int32,
    _ transform: (Element) -> Result
) -> [Result] {
    guard let base, count > 0 else { return [] }
    return UnsafeBufferPointer(start: base, count: Int(count)).map(transform)
}
