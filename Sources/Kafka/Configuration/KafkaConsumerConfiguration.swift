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

import Crdkafka
import struct Foundation.UUID

public struct KafkaConsumerConfiguration {
    // MARK: - Kafka-specific Config properties

    /// The time between two consecutive polls.
    /// Effectively controls the rate at which incoming events and messages are consumed.
    /// Default: `.milliseconds(100)`
    public var pollInterval: Duration = .milliseconds(100)
    
    public var listenForRebalance: Bool = false
    
    public var enablePartitionEof: Bool = false

    /// A struct representing different back pressure strategies for consuming messages in ``KafkaConsumer``.
    public struct BackPressureStrategy: Sendable, Hashable {
        enum _BackPressureStrategy: Sendable, Hashable {
            case watermark(low: Int, high: Int)
        }

        let _internal: _BackPressureStrategy

        private init(backPressureStrategy: _BackPressureStrategy) {
            self._internal = backPressureStrategy
        }

        /// A back pressure strategy based on high and low watermarks.
        ///
        /// The consumer maintains a buffer size between a low watermark and a high watermark
        /// to control the flow of incoming messages.
        ///
        /// - Parameter low: The lower threshold for the buffer size (low watermark).
        /// - Parameter high: The upper threshold for the buffer size (high watermark).
        public static func watermark(low: Int, high: Int) -> BackPressureStrategy {
            return .init(backPressureStrategy: .watermark(low: low, high: high))
        }
    }

    /// The backpressure strategy to be used for message consumption.
    /// See ``KafkaConsumerConfiguration/BackPressureStrategy-swift.struct`` for more information.
    public var backPressureStrategy: BackPressureStrategy = .watermark(
        low: 10,
        high: 50
    )

    /// A struct representing the different Kafka message consumption strategies.
    public struct ConsumptionStrategy: Sendable, Hashable {
        public struct TopicPartition: Sendable, Hashable {
            public let partition: KafkaPartition
            public let topic: String
            public let offset: KafkaOffset

            public init(partition: KafkaPartition, topic: String, offset: KafkaOffset) {
                self.partition = partition
                self.topic = topic
                self.offset = offset
            }
        }

        enum _ConsumptionStrategy: Sendable, Hashable {
            case partitions(groupID: String?, [TopicPartition])
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
        ///     - offset: The offset to start consuming from. Defaults to the end of the Kafka partition queue (meaning wait for the next produced message).
        public static func partitions(
            groupID: String? = nil,
            partitions: [TopicPartition]
        ) -> ConsumptionStrategy {
            return .init(consumptionStrategy: .partitions(groupID: groupID, partitions))
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

    /// The strategy used for consuming messages.
    /// See ``KafkaConsumerConfiguration/ConsumptionStrategy-swift.struct-swift.struct`` for more information.
    public var consumptionStrategy: ConsumptionStrategy

    // MARK: - Consumer-specific Config Properties

    /// Client group session options.
    public struct SessionOptions: Sendable, Hashable {
        /// Client group session and failure detection timeout.
        /// The consumer sends periodic heartbeats (``KafkaConsumerConfiguration/heartbeatInterval``) to indicate its liveness to the broker.
        /// If no hearts are received by the broker for a group member within the session timeout, the broker will remove the consumer from the group and trigger a rebalance.
        /// (Lowest granularity is milliseconds)
        /// Default: `.milliseconds(45000)`
        public var timeout: Duration = .milliseconds(45000) {
            didSet {
                precondition(
                    timeout.canBeRepresentedAsMilliseconds,
                    "Lowest granularity is milliseconds"
                )
            }
        }

        public init() {}
    }

    /// Client group session options.
    public var session: SessionOptions = .init()

    /// Group session keepalive heartbeat interval.
    /// (Lowest granularity is milliseconds)
    /// Default: `.milliseconds(3000)`
    public var heartbeatInterval: Duration = .milliseconds(3000) {
        didSet {
            precondition(
                heartbeatInterval.canBeRepresentedAsMilliseconds,
                "Lowest granularity is milliseconds"
            )
        }
    }

    /// Maximum allowed time between calls to consume messages. If this interval is exceeded the consumer is considered failed and the group will rebalance to reassign the partitions to another consumer group member.
    ///
    /// - Warning: Offset commits may be not possible at this point.
    ///
    /// The interval is checked two times per second. See KIP-62 for more information.
    ///
    /// (Lowest granularity is milliseconds)
    /// Default: `.milliseconds(300_000)`
    public var maximumPollInterval: Duration = .milliseconds(300_000) {
        didSet {
            precondition(
                maximumPollInterval.canBeRepresentedAsMilliseconds,
                "Lowest granularity is milliseconds"
            )
        }
    }

    /// Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets.
    /// Default: `true`
    public var isAutoCommitEnabled: Bool = true

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

    /// Action to take when there is no initial offset in the offset store or the desired offset is out of range. See ``KafkaConfiguration/AutoOffsetReset`` for more information.
    /// Default: `.largest`
    public var autoOffsetReset: AutoOffsetReset = .largest

    /// Allow automatic topic creation on the broker when subscribing to or assigning non-existent topics.
    /// The broker must also be configured with ``KafkaConsumerConfiguration/isAutoCreateTopicsEnabled`` = `true` for this configuration to take effect.
    /// Default: `false`
    public var isAutoCreateTopicsEnabled: Bool = false

    // MARK: - Common Client Config Properties

    /// Client identifier.
    /// Default: `"rdkafka"`
    public var identifier: String = "rdkafka"

    /// Initial list of brokers.
    /// Default: `[]`
    public var bootstrapBrokerAddresses: [KafkaConfiguration.BrokerAddress] = []

    /// Message options.
    public var message: KafkaConfiguration.MessageOptions = .init()

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hiccups.
    /// Default: `100_000_000`
    public var maximumReceiveMessageBytes: Int = 100_000_000

    /// Maximum number of in-flight requests per broker connection.
    /// This is a generic property applied to all broker communication, however, it is primarily relevant to produce requests.
    /// In particular, note that other mechanisms limit the number of outstanding consumer fetch requests per broker to one.
    /// Default: `1_000_000`
    public var maximumInFlightRequestsPerConnection: Int = 1_000_000

    /// Metadata cache max age.
    /// (Lowest granularity is milliseconds)
    /// Default: `.milliseconds(900_000)`
    public var maximumMetadataAge: Duration = .milliseconds(900_000) {
        didSet {
            precondition(
                maximumMetadataAge.canBeRepresentedAsMilliseconds,
                "Lowest granularity is milliseconds"
            )
        }
    }

    /// Topic metadata options.
    public var topicMetadata: KafkaConfiguration.TopicMetadataOptions = .init()

    /// Topic denylist.
    /// Default: `[]`
    public var topicDenylist: [String] = []

    /// Debug options.
    /// Default: `[]`
    public var debugOptions: [KafkaConfiguration.DebugOption] = []

    /// Socket options.
    public var socket: KafkaConfiguration.SocketOptions = .init()

    /// Broker options.
    public var broker: KafkaConfiguration.BrokerOptions = .init()

    /// Reconnect options.
    public var reconnect: KafkaConfiguration.ReconnectOptions = .init()

    /// Interval for librdkafka statistics reports
    public var statisticsInterval: KafkaConfiguration.KeyRefreshAttempts = .disable

    /// Security protocol to use (plaintext, ssl, sasl_plaintext, sasl_ssl).
    /// Default: `.plaintext`
    public var securityProtocol: KafkaConfiguration.SecurityProtocol = .plaintext

    public var isolationLevel: String?
    
    public var groupInstanceId: String?
    
    public var compression: String?
    
    public var rebalanceStrategy: String?
    
    public init(
        consumptionStrategy: ConsumptionStrategy,
        bootstrapBrokerAddresses: [KafkaConfiguration.BrokerAddress]
    ) {
        self.consumptionStrategy = consumptionStrategy
        self.bootstrapBrokerAddresses = bootstrapBrokerAddresses
    }
}

// MARK: - KafkaConsumerConfiguration + Dictionary

extension KafkaConsumerConfiguration {
    internal var dictionary: [String: String] {
        var resultDict: [String: String] = [:]

        switch self.consumptionStrategy._internal {
        case .partitions(let groupID, _):
            if let groupID = groupID {
                resultDict["group.id"] = groupID
            } else {
                // Although an assignment is not related to a consumer group,
                // librdkafka requires us to set a `group.id`.
                // This is a known issue:
                // https://github.com/edenhill/librdkafka/issues/3261
                resultDict["group.id"] = UUID().uuidString
            }
        case .group(groupID: let groupID, topics: _):
            resultDict["group.id"] = groupID
        }

        resultDict["session.timeout.ms"] = String(session.timeout.inMilliseconds)
        resultDict["heartbeat.interval.ms"] = String(heartbeatInterval.inMilliseconds)
        resultDict["max.poll.interval.ms"] = String(maximumPollInterval.inMilliseconds)
        resultDict["enable.auto.commit"] = String(isAutoCommitEnabled)
        resultDict["auto.offset.reset"] = autoOffsetReset.description
        resultDict["allow.auto.create.topics"] = String(isAutoCreateTopicsEnabled)

        resultDict["client.id"] = identifier
        resultDict["bootstrap.servers"] = bootstrapBrokerAddresses.map(\.description).joined(separator: ",")
        resultDict["message.max.bytes"] = String(message.maximumBytes)
        resultDict["message.copy.max.bytes"] = String(message.maximumBytesToCopy)
        resultDict["receive.message.max.bytes"] = String(maximumReceiveMessageBytes)
        resultDict["max.in.flight.requests.per.connection"] = String(maximumInFlightRequestsPerConnection)
        resultDict["metadata.max.age.ms"] = String(maximumMetadataAge.inMilliseconds)
        resultDict["topic.metadata.refresh.interval.ms"] = String(topicMetadata.refreshInterval.rawValue)
        resultDict["topic.metadata.refresh.fast.interval.ms"] = String(topicMetadata.refreshFastInterval.inMilliseconds)
        resultDict["topic.metadata.refresh.sparse"] = String(topicMetadata.isSparseRefreshingEnabled)
        resultDict["topic.metadata.propagation.max.ms"] = String(topicMetadata.maximumPropagation.inMilliseconds)
        resultDict["topic.blacklist"] = topicDenylist.joined(separator: ",")
        if !debugOptions.isEmpty {
            resultDict["debug"] = debugOptions.map(\.description).joined(separator: ",")
        }
        resultDict["socket.timeout.ms"] = String(socket.timeout.inMilliseconds)
        resultDict["socket.send.buffer.bytes"] = String(socket.sendBufferBytes.rawValue)
        resultDict["socket.receive.buffer.bytes"] = String(socket.receiveBufferBytes.rawValue)
        resultDict["socket.keepalive.enable"] = String(socket.isKeepaliveEnabled)
        resultDict["socket.nagle.disable"] = String(socket.isNagleDisabled)
        resultDict["socket.max.fails"] = String(socket.maximumFailures.rawValue)
        resultDict["socket.connection.setup.timeout.ms"] = String(socket.connectionSetupTimeout.inMilliseconds)
        resultDict["broker.address.ttl"] = String(broker.addressTimeToLive.inMilliseconds)
        resultDict["broker.address.family"] = broker.addressFamily.description
        resultDict["reconnect.backoff.ms"] = String(reconnect.backoff.rawValue)
        resultDict["reconnect.backoff.max.ms"] = String(reconnect.maximumBackoff.inMilliseconds)
        resultDict["queued.max.messages.kbytes"] = String(8 * 1024) // XX MB per partition // TODO: remove
        
        resultDict["statistics.interval.ms"] = String(statisticsInterval.rawValue)
        
        if let isolationLevel {
            resultDict["isolation.level"] = isolationLevel
        }
        
        if let groupInstanceId {
            resultDict["group.instance.id"] = groupInstanceId
        }
        
        if let compression {
            resultDict["compression.codec"] = compression
        }
        
        if let rebalanceStrategy {
            resultDict["partition.assignment.strategy"] = rebalanceStrategy
        }

        // Merge with SecurityProtocol configuration dictionary
        resultDict.merge(securityProtocol.dictionary) { _, _ in
            fatalError("securityProtocol and \(#file) should not have duplicate keys")
        }

        return resultDict
    }
}

// MARK: - KafkaConsumerConfiguration + Sendable

extension KafkaConsumerConfiguration: Sendable {}
