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

// TODO: docs are busted -> use doc comments from API Review
// TODO: README

import Crdkafka
import struct Foundation.UUID

public struct KafkaConsumerConfiguration {
    // MARK: - SwiftKafka-specific Config properties

    /// The time between two consecutive polls.
    /// Effectively controls the rate at which incoming events and messages are consumed.
    public var pollInterval: Duration = .milliseconds(100)

    /// The consumption strategy for the consumer.
    /// See ``KafkaSharedConfiguration/ConsumptionStrategy`` for more information.
    public var consumptionStrategy: KafkaConfiguration.ConsumptionStrategy = .group(groupID: "", topics: [])

    // MARK: - Consumer-specific Config Properties

    /// Client group session options.
    public var session: KafkaConfiguration.SessionOptions = .init()

    /// Heartbeat interval in milliseconds.
    public var heartbeatIntervalMilliseconds: Int = 3000

    /// Maximum allowed time between calls to consume messages for high-level consumers.
    public var maxPollInvervalMilliseconds: Int = 300_000

    /// If true, automatically commit offsets periodically.
    public var enableAutoCommit: Bool = true

    /// The frequency in milliseconds at which to auto-commit offsets when enableAutoCommit is true.
    public var autoCommitIntervalMilliseconds: Int = 5000

    /// Action to take when there is no initial offset in offset store or the offset is out of range.
    public var autoOffsetReset: KafkaConfiguration.AutoOffsetReset = .largest

    /// If true, automatically store offset of last message provided to application.
    public var enableAutoOffsetStore: Bool = true

    /// If true, automatically create topics when consuming.
    public var allowAutoCreateTopics: Bool = false

    // MARK: - Common Client Config Properties

    /// Client identifier string.
    public var clientID: String = "rdkafka"

    /// Bootstrap broker(s) (host[:port]) for initial connection.
    public var bootstrapServers: [String] = []

    /// Message options.
    public var message: KafkaConfiguration.MessageOptions = .init()

    /// Maximum receive message size for network requests.
    public var receiveMessageMaxBytes: Int = 100_000_000

    /// Maximum number of in-flight requests per broker connection.
    public var maxInFlightRequestsPerConnection: Int = 1_000_000

    /// Maximum time, in milliseconds, that broker metadata can be cached.
    public var metadataMaxAgeMilliseconds: Int = 900_000

    /// Topic metadata options.
    public var topicMetadata: KafkaConfiguration.TopicMetadataOptions = .init()

    /// Topic denylist.
    public var topicDenylist: [String] = []

    /// Debug options.
    public var debug: [KafkaConfiguration.DebugOption] = []

    /// Socket options.
    public var socket: KafkaConfiguration.SocketOptions = .init()

    /// Broker options.
    public var broker: KafkaConfiguration.BrokerOptions = .init()

    /// Reconnect options.
    public var reconnect: KafkaConfiguration.ReconnectOptions = .init()

    /// Security protocol to use (plaintext, ssl, sasl_plaintext, sasl_ssl).
    public var securityProtocol: KafkaConfiguration.SecurityProtocol = .plaintext

    /// SSL options.
    public var ssl: KafkaConfiguration.SSLOptions = .init()

    /// SASL options.
    public var sasl: KafkaConfiguration.SASLOptions = .init()

    public init() {}
}

// MARK: - KafkaConsumerConfiguration + Dictionary

extension KafkaConsumerConfiguration {
    internal var dictionary: [String: String] {
        var resultDict: [String: String] = [:]

        switch self.consumptionStrategy._internal {
        case .partition:
            // Although an assignment is not related to a consumer group,
            // librdkafka requires us to set a `group.id`.
            // This is a known issue:
            // https://github.com/edenhill/librdkafka/issues/3261
            resultDict["group.id"] = UUID().uuidString
        case .group(groupID: let groupID, topics: _):
            resultDict["group.id"] = groupID
        }

        resultDict["session.timeout.ms"] = String(session.timeoutMilliseconds)
        resultDict["heartbeat.interval.ms"] = String(heartbeatIntervalMilliseconds)
        resultDict["max.poll.interval.ms"] = String(maxPollInvervalMilliseconds)
        resultDict["enable.auto.commit"] = String(enableAutoCommit)
        resultDict["auto.commit.interval.ms"] = String(autoCommitIntervalMilliseconds)
        resultDict["auto.offset.reset"] = autoOffsetReset.description
        resultDict["enable.auto.offset.store"] = String(enableAutoOffsetStore)
        resultDict["allow.auto.create.topics"] = String(allowAutoCreateTopics)

        resultDict["client.id"] = clientID
        resultDict["bootstrap.servers"] = bootstrapServers.joined(separator: ",")
        resultDict["message.max.bytes"] = String(message.maxBytes)
        resultDict["message.copy.max.bytes"] = String(message.copyMaxBytes)
        resultDict["receive.message.max.bytes"] = String(receiveMessageMaxBytes)
        resultDict["max.in.flight.requests.per.connection"] = String(maxInFlightRequestsPerConnection)
        resultDict["metadata.max.age.ms"] = String(metadataMaxAgeMilliseconds)
        resultDict["topic.metadata.refresh.interval.ms"] = String(topicMetadata.refreshIntervalMilliseconds)
        resultDict["topic.metadata.refresh.fast.interval.ms"] = String(topicMetadata.refreshFastIntervalMilliseconds)
        resultDict["topic.metadata.refresh.sparse"] = String(topicMetadata.refreshSparse)
        resultDict["topic.metadata.propagation.max.ms"] = String(topicMetadata.propagationMaxMilliseconds)
        resultDict["topic.blacklist"] = topicDenylist.joined(separator: ",")
        if !debug.isEmpty {
            resultDict["debug"] = debug.map(\.description).joined(separator: ",")
        }
        resultDict["socket.timeout.ms"] = String(socket.timeoutMilliseconds)
        resultDict["socket.send.buffer.bytes"] = String(socket.sendBufferBytes)
        resultDict["socket.receive.buffer.bytes"] = String(socket.receiveBufferBytes)
        resultDict["socket.keepalive.enable"] = String(socket.keepaliveEnable)
        resultDict["socket.nagle.disable"] = String(socket.nagleDisable)
        resultDict["socket.max.fails"] = String(socket.maxFails)
        resultDict["socket.connection.setup.timeout.ms"] = String(socket.connectionSetupTimeoutMilliseconds)
        resultDict["broker.address.ttl"] = String(broker.addressTTL)
        resultDict["broker.address.family"] = broker.addressFamily.description
        resultDict["reconnect.backoff.ms"] = String(reconnect.backoffMilliseconds)
        resultDict["reconnect.backoff.max.ms"] = String(reconnect.backoffMaxMilliseconds)
        resultDict["security.protocol"] = securityProtocol.description
        resultDict["ssl.key.location"] = ssl.keyLocation
        resultDict["ssl.key.password"] = ssl.keyPassword
        resultDict["ssl.certificate.location"] = ssl.certificateLocation
        resultDict["ssl.ca.location"] = ssl.CALocation
        resultDict["ssl.crl.location"] = ssl.CRLLocation
        resultDict["ssl.keystore.location"] = ssl.keystoreLocation
        resultDict["ssl.keystore.password"] = ssl.keystorePassword
        if let saslMechnism = sasl.mechanism {
            resultDict["sasl.mechanism"] = saslMechnism.description
        }
        if let saslUsername = sasl.username {
            resultDict["sasl.username"] = saslUsername
        }
        if let saslPassword = sasl.password {
            resultDict["sasl.password"] = saslPassword
        }

        return resultDict
    }
}

// MARK: - KafkaConsumerConfiguration + Hashable

extension KafkaConsumerConfiguration: Hashable {}

// MARK: - KafkaConsumerConfiguration + Sendable

extension KafkaConsumerConfiguration: Sendable {}

// MARK: - KafkaConfiguration + Consumer Additions

extension KafkaConfiguration {
    /// Client group session options.
    public struct SessionOptions: Sendable, Hashable {
        /// Client group session and failure detection timeout. The consumer sends periodic heartbeats (heartbeat.interval.Milliseconds) to indicate its liveness to the broker. If no hearts are received by the broker for a group member within the session timeout, the broker will remove the consumer from the group and trigger a rebalance. The allowed range is configured with the broker configuration properties group.min.session.timeout.ms and group.max.session.timeout.ms. Also see max.poll.interval.ms.
        public var timeoutMilliseconds: Int = 45000
    }

    /// A struct representing the different Kafka message consumption strategies.
    public struct ConsumptionStrategy: Sendable, Hashable {
        enum _ConsumptionStrategy: Sendable, Hashable {
            case partition(topic: String, partition: KafkaPartition, offset: Int)
            case group(groupID: String, topics: [String])
        }

        let _internal: _ConsumptionStrategy

        private init(consumptionStrategy: _ConsumptionStrategy) {
            self._internal = consumptionStrategy
        }

        /// A consumption strategy based on partition assignment.
        /// The consumer reads from a specific partition of a topic at a given offset.
        ///
        /// - Parameter topic: The name of the Kafka topic.
        /// - Parameter partition: The partition of the topic to consume from.
        /// - Parameter offset: The offset to start consuming from.
        /// Defaults to the end of the Kafka partition queue (meaning wait for next produced message).
        public static func partition(
            topic: String,
            partition: KafkaPartition,
            offset: Int = Int(RD_KAFKA_OFFSET_END)
        ) -> ConsumptionStrategy {
            return .init(consumptionStrategy: .partition(topic: topic, partition: partition, offset: offset))
        }

        /// A consumption strategy based on consumer group membership.
        /// The consumer joins a consumer group identified by a group ID and consumes from multiple topics.
        ///
        /// - Parameter groupID: The ID of the consumer group to join.
        /// - Parameter topics: An array of topic names to consume from.
        public static func group(groupID: String, topics: [String]) -> ConsumptionStrategy {
            return .init(consumptionStrategy: .group(groupID: groupID, topics: topics))
        }
    }

    /// Available actions to take when there is no initial offset in offset store / offset is out of range.
    public struct AutoOffsetReset: Sendable, Hashable, CustomStringConvertible {
        public let description: String

        /// Automatically reset the offset to the smallest offset.
        public static let smallest = AutoOffsetReset(description: "smallest")
        /// Automatically reset the offset to the earliest offset.
        public static let earliest = AutoOffsetReset(description: "earliest")
        /// Automatically reset the offset to the beginning of a topic.
        public static let beginning = AutoOffsetReset(description: "beginning")
        /// Automatically reset the offset to the largest offset.
        public static let largest = AutoOffsetReset(description: "largest")
        /// Automatically reset the offset to the latest offset.
        public static let latest = AutoOffsetReset(description: "latest")
        /// Automatically reset the offset to the end offset.
        public static let end = AutoOffsetReset(description: "end")
        /// Trigger an error when there is no initial offset / offset is out of range.
        public static let error = AutoOffsetReset(description: "error")
    }
}
