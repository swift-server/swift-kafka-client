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

import Crdkafka
import struct Foundation.UUID

public struct KafkaConsumerConfiguration {
    // MARK: - SwiftKafka-specific Config properties

    /// The time between two consecutive polls.
    /// Effectively controls the rate at which incoming events and messages are consumed.
    /// Default: `.milliseconds(100)`
    public var pollInterval: Duration = .milliseconds(100)

    /// Interval for librdkafka statistics reports
    /// 0ms - disabled
    /// >= 1ms - statistics provided every specified interval
    public var statisticsInterval: Duration = .zero {
        didSet {
            precondition(
                self.statisticsInterval.totalMilliseconds > 0 || self.statisticsInterval == .zero /*self.statisticsInterval.canBeRepresentedAsMilliseconds*/,
                "Lowest granularity is milliseconds"
            )
        }
    }

    /// The strategy used for consuming messages.
    /// See ``KafkaConfiguration/ConsumptionStrategy`` for more information.
    public var consumptionStrategy: KafkaConfiguration.ConsumptionStrategy

    // MARK: - Consumer-specific Config Properties

    /// Client group session options.
    public var session: KafkaConfiguration.SessionOptions = .init()

    /// Group session keepalive heartbeat interval.
    /// Default: `3000`
    public var heartbeatIntervalMilliseconds: Int = 3000

    /// Maximum allowed time between calls to consume messages. If this interval is exceeded the consumer is considered failed and the group will rebalance in order to reassign the partitions to another consumer group member. Warning: Offset commits may be not possible at this point. Note: It is recommended to set enable.auto.offset.store=false for long-time processing applications and then explicitly store offsets (using offsets_store()) after message processing, to make sure offsets are not auto-committed prior to processing has finished. The interval is checked two times per second. See KIP-62 for more information.
    /// Default: `300_000`
    public var maxPollInvervalMilliseconds: Int = 300_000

    /// Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets.
    /// Default: `true`
    public var enableAutoCommit: Bool = true

    /// The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. (0 = disable).
    /// Default: `5000`
    public var autoCommitIntervalMilliseconds: Int = 5000

    /// Action to take when there is no initial offset in offset store or the desired offset is out of range. See ``KafkaConfiguration/AutoOffsetReset`` for more information.
    /// Default: `.largest`
    public var autoOffsetReset: KafkaConfiguration.AutoOffsetReset = .largest

    /// Allow automatic topic creation on the broker when subscribing to or assigning non-existent topics.
    /// The broker must also be configured with auto.create.topics.enable=true for this configuration to take effect.
    /// Default: `false`
    public var allowAutoCreateTopics: Bool = false

    // MARK: - Common Client Config Properties

    /// Client identifier.
    /// Default: `"rdkafka"`
    public var clientID: String = "rdkafka"

    /// Initial list of brokers.
    /// Default: `[]`
    public var bootstrapServers: [KafkaConfiguration.Broker] = []

    /// Message options.
    public var message: KafkaConfiguration.MessageOptions = .init()

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hickups. This value must be at least fetch.max.bytes + 512 to allow for protocol overhead; the value is adjusted automatically unless the configuration property is explicitly set.
    /// Default: `100_000_000`
    public var receiveMessageMaxBytes: Int = 100_000_000

    /// Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one.
    /// Default: `1_000_000`
    public var maxInFlightRequestsPerConnection: Int = 1_000_000

    /// Metadata cache max age.
    /// Default: `900_000`
    public var metadataMaxAgeMilliseconds: Int = 900_000

    /// Topic metadata options.
    public var topicMetadata: KafkaConfiguration.TopicMetadataOptions = .init()

    /// Topic denylist.
    /// Default: `[]`
    public var topicDenylist: [String] = []

    /// Debug options.
    /// Default: `[]`
    public var debug: [KafkaConfiguration.DebugOption] = []

    /// Socket options.
    public var socket: KafkaConfiguration.SocketOptions = .init()

    /// Broker options.
    public var broker: KafkaConfiguration.BrokerOptions = .init()

    /// Reconnect options.
    public var reconnect: KafkaConfiguration.ReconnectOptions = .init()

    /// Security protocol to use (plaintext, ssl, sasl_plaintext, sasl_ssl).
    /// Default: `.plaintext`
    public var securityProtocol: KafkaConfiguration.SecurityProtocol = .plaintext

    public init(consumptionStrategy: KafkaConfiguration.ConsumptionStrategy) {
        self.consumptionStrategy = consumptionStrategy
    }
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

        resultDict["statistics.interval.ms"] = String(self.statisticsInterval.totalMilliseconds)
        resultDict["session.timeout.ms"] = String(session.timeoutMilliseconds)
        resultDict["heartbeat.interval.ms"] = String(heartbeatIntervalMilliseconds)
        resultDict["max.poll.interval.ms"] = String(maxPollInvervalMilliseconds)
        resultDict["enable.auto.commit"] = String(enableAutoCommit)
        resultDict["auto.commit.interval.ms"] = String(autoCommitIntervalMilliseconds)
        resultDict["auto.offset.reset"] = autoOffsetReset.description
        resultDict["allow.auto.create.topics"] = String(allowAutoCreateTopics)

        resultDict["client.id"] = clientID
        resultDict["bootstrap.servers"] = bootstrapServers.map(\.description).joined(separator: ",")
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

        // Merge with SecurityProtocol configuration dictionary
        resultDict.merge(securityProtocol.dictionary) { _, _ in
            fatalError("securityProtocol and \(#file) should not have duplicate keys")
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
        /// Client group session and failure detection timeout. The consumer sends periodic heartbeats (heartbeat.interval.ms) to indicate its liveness to the broker. If no hearts are received by the broker for a group member within the session timeout, the broker will remove the consumer from the group and trigger a rebalance. The allowed range is configured with the broker configuration properties group.min.session.timeout.ms and group.max.session.timeout.ms. Also see max.poll.interval.ms.
        /// Default: `45000`
        public var timeoutMilliseconds: Int = 45000

        public init(timeoutMilliseconds: Int = 45000) {
            self.timeoutMilliseconds = timeoutMilliseconds
        }
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
        /// - Parameters:
        ///     - partition: The partition of the topic to consume from.
        ///     - topic: The name of the Kafka topic.
        ///     - offset: The offset to start consuming from. Defaults to the end of the Kafka partition queue (meaning wait for next produced message).
        ///       Defaults to the end of the Kafka partition queue (meaning wait for next produced message).
        public static func partition(
            _ partition: KafkaPartition,
            topic: String,
            offset: Int = Int(RD_KAFKA_OFFSET_END)
        ) -> ConsumptionStrategy {
            return .init(consumptionStrategy: .partition(topic: topic, partition: partition, offset: offset))
        }

        /// A consumption strategy based on consumer group membership.
        /// The consumer joins a consumer group identified by a group ID and consumes from multiple topics.
        ///
        /// - Parameters:
        ///     - id: The ID of the consumer group to join.
        ///     - topics: An array of topic names to consume from.
        public static func group(id groupID: String, topics: [String]) -> ConsumptionStrategy {
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
