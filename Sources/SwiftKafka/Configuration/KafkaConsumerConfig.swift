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

public struct KafkaConsumerConfig: Hashable, Equatable {
    // MARK: - SwiftKafka-specific Config properties

    /// The backpressure strategy to be used for message consumption.
    public var backPressureStrategy: KafkaSharedConfiguration.BackPressureStrategy = .watermark(
        low: 10,
        high: 50
    )

    // This backs the consumptionStrategy computed property.
    private var _consumptionStrategy: KafkaSharedConfiguration.ConsumptionStrategy

    /// The strategy used for consuming messages.
    /// See ``KafkaSharedConfiguration/ConsumptionStrategy`` for more information.
    public var consumptionStrategy: KafkaSharedConfiguration.ConsumptionStrategy {
        get { self._consumptionStrategy }
        set {
            self._consumptionStrategy = newValue

            // We do not expose the group.id option to the user
            // but rather set it ourselves as part of our much safer
            // consumptionStrategy option.
            switch newValue._internal {
            case .partition:
                // Although an assignment is not related to a consumer group,
                // librdkafka requires us to set a `group.id`.
                // This is a known issue:
                // https://github.com/edenhill/librdkafka/issues/3261
                self.dictionary["group.id"] = UUID().uuidString
            case .group(groupID: let groupID, topics: _):
                self.dictionary["group.id"] = groupID
            }
        }
    }

    // MARK: - librdkafka Config properties

    var dictionary: [String: String] = [:]

    // MARK: - Consumer-specific Config Properties

    /// Client group session and failure detection timeout. The consumer sends periodic heartbeats (heartbeat.interval.ms) to indicate its liveness to the broker. If no hearts are received by the broker for a group member within the session timeout, the broker will remove the consumer from the group and trigger a rebalance. The allowed range is configured with the broker configuration properties group.min.session.timeout.ms and group.max.session.timeout.ms. Also see max.poll.interval.ms.
    public var sessionTimeoutMs: UInt {
        get { self.dictionary.getUInt("session.timeout.ms") ?? 45000 }
        set { self.dictionary["session.timeout.ms"] = String(newValue) }
    }

    /// Group session keepalive heartbeat interval.
    public var heartbeatIntervalMs: UInt {
        get { self.dictionary.getUInt("heartbeat.interval.ms") ?? 3000 }
        set { self.dictionary["heartbeat.interval.ms"] = String(newValue) }
    }

    /// Maximum allowed time between calls to consume messages. If this interval is exceeded the consumer is considered failed and the group will rebalance in order to reassign the partitions to another consumer group member. Warning: Offset commits may be not possible at this point. Note: It is recommended to set enable.auto.offset.store=false for long-time processing applications and then explicitly store offsets (using offsets_store()) after message processing, to make sure offsets are not auto-committed prior to processing has finished. The interval is checked two times per second. See KIP-62 for more information.
    public var maxPollInvervalMs: UInt {
        get { self.dictionary.getUInt("max.poll.interval.ms") ?? 300_000 }
        set { self.dictionary["max.poll.interval.ms"] = String(newValue) }
    }

    /// Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets.
    public var enableAutoCommit: Bool {
        get { self.dictionary.getBool("enable.auto.commit") ?? true }
        set { self.dictionary["enable.auto.commit"] = String(newValue) }
    }

    /// The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. (0 = disable).
    public var autoCommitIntervalMs: UInt {
        get { self.dictionary.getUInt("auto.commit.interval.ms") ?? 5000 }
        set { self.dictionary["auto.commit.interval.ms"] = String(newValue) }
    }

    /// Action to take when there is no initial offset in offset store or the desired offset is out of range. See ``KafkaSharedConfiguration/AutoOffsetReset`` for more information.
    public var autoOffsetReset: KafkaSharedConfiguration.AutoOffsetReset {
        get { self.getAutoOffsetReset() ?? .largest }
        set { self.dictionary["auto.offset.reset"] = newValue.description }
    }

    /// Automatically store offset of last message provided to application. The offset store is an in-memory store of the next offset to (auto-)commit for each partition.
    public var enableAutoOffsetStore: Bool {
        get { self.dictionary.getBool("enable.auto.offset.store") ?? true }
        set { self.dictionary["enable.auto.offset.store"] = String(newValue) }
    }

    /// Allow automatic topic creation on the broker when subscribing to or assigning non-existent topics. The broker must also be configured with auto.create.topics.enable=true for this configuration to take effect. Note: the default value (true) for the producer is different from the default value (false) for the consumer. Further, the consumer default value is different from the Java consumer (true), and this property is not supported by the Java producer. Requires broker version >= 0.11.0.0, for older broker versions only the broker configuration applies.
    public var allowAutoCreateTopics: Bool {
        get { self.dictionary.getBool("allow.auto.create.topics") ?? false }
        set { self.dictionary["allow.auto.create.topics"] = String(newValue) }
    }

    // MARK: - Common Client Config Properties

    /// Client identifier.
    public var clientID: String {
        get { self.dictionary["client.id"] ?? "rdkafka" }
        set { self.dictionary["client.id"] = newValue }
    }

    /// Initial list of brokers as a CSV list of broker host or host:port.
    public var bootstrapServers: [String] {
        get { self.dictionary["bootstrap.servers"]?.components(separatedBy: ",") ?? [] }
        set { self.dictionary["bootstrap.servers"] = newValue.joined(separator: ",") }
    }

    /// Maximum Kafka protocol request message size. Due to differing framing overhead between protocol versions the producer is unable to reliably enforce a strict max message limit at produce time and may exceed the maximum size by one message in protocol ProduceRequests, the broker will enforce the the topic's max.message.bytes limit (see Apache Kafka documentation).
    public var messageMaxBytes: UInt {
        get { self.dictionary.getUInt("message.max.bytes") ?? 1_000_000 }
        set { self.dictionary["message.max.bytes"] = String(newValue) }
    }

    /// Maximum size for message to be copied to buffer. Messages larger than this will be passed by reference (zero-copy) at the expense of larger iovecs.
    public var messageCopyMaxBytes: UInt {
        get { self.dictionary.getUInt("message.copy.max.bytes") ?? 65535 }
        set { self.dictionary["message.copy.max.bytes"] = String(newValue) }
    }

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hickups. This value must be at least fetch.max.bytes + 512 to allow for protocol overhead; the value is adjusted automatically unless the configuration property is explicitly set.
    public var receiveMessageMaxBytes: UInt {
        get { self.dictionary.getUInt("receive.message.max.bytes") ?? 100_000_000 }
        set { self.dictionary["receive.message.max.bytes"] = String(newValue) }
    }

    /// Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one.
    public var maxInFlightRequestsPerConnection: UInt {
        get { self.dictionary.getUInt("max.in.flight.requests.per.connection") ?? 1_000_000 }
        set { self.dictionary["max.in.flight.requests.per.connection"] = String(newValue) }
    }

    /// Metadata cache max age.
    public var metadataMaxAgeMs: UInt {
        get { self.dictionary.getUInt("metadata.max.age.ms") ?? 900_000 }
        set { self.dictionary["metadata.max.age.ms"] = String(newValue) }
    }

    /// Period of time in milliseconds at which topic and broker metadata is refreshed in order to proactively discover any new brokers, topics, partitions or partition leader changes. Use -1 to disable the intervalled refresh (not recommended). If there are no locally referenced topics (no topic objects created, no messages produced, no subscription or no assignment) then only the broker list will be refreshed every interval but no more often than every 10s.
    public var topicMetadataRefreshIntervalMs: Int {
        get { self.dictionary.getInt("topic.metadata.refresh.interval.ms") ?? 300_000 }
        set { self.dictionary["topic.metadata.refresh.interval.ms"] = String(newValue) }
    }

    /// When a topic loses its leader a new metadata request will be enqueued with this initial interval, exponentially increasing until the topic metadata has been refreshed. This is used to recover quickly from transitioning leader brokers.
    public var topicMetadataRefreshFastIntervalMs: UInt {
        get { self.dictionary.getUInt("topic.metadata.refresh.fast.interval.ms") ?? 250 }
        set { self.dictionary["topic.metadata.refresh.fast.interval.ms"] = String(newValue) }
    }

    /// Sparse metadata requests (consumes less network bandwidth).
    public var topicMetadataRefreshSparse: Bool {
        get { self.dictionary.getBool("topic.metadata.refresh.sparse") ?? true }
        set { self.dictionary["topic.metadata.refresh.sparse"] = newValue.description }
    }

    /// Apache Kafka topic creation is asynchronous and it takes some time for a new topic to propagate throughout the cluster to all brokers. If a client requests topic metadata after manual topic creation but before the topic has been fully propagated to the broker the client is requesting metadata from, the topic will seem to be non-existent and the client will mark the topic as such, failing queued produced messages with ERR__UNKNOWN_TOPIC. This setting delays marking a topic as non-existent until the configured propagation max time has passed. The maximum propagation time is calculated from the time the topic is first referenced in the client, e.g., on `send()`.
    public var topicMetadataPropagationMaxMs: UInt {
        get { self.dictionary.getUInt("topic.metadata.propagation.max.ms") ?? 30000 }
        set { self.dictionary["topic.metadata.propagation.max.ms"] = String(newValue) }
    }

    /// Topic denylist, a comma-separated list of regular expressions for matching topic names that should be ignored in broker metadata information as if the topics did not exist.
    public var topicDenylist: [String] {
        get { self.dictionary["topic.blacklist"]?.components(separatedBy: ",") ?? [] }
        set { self.dictionary["topic.blacklist"] = newValue.joined(separator: ",") }
    }

    /// A comma-separated list of debug contexts to enable. Detailed Producer debugging: broker,topic,msg. Consumer: consumer,cgrp,topic,fetch.
    public var debug: [KafkaSharedConfiguration.DebugOption] {
        get { self.getDebugOptions() }
        set {
            if !newValue.isEmpty {
                self.dictionary["debug"] = newValue.map(\.description).joined(separator: ",")
            }
        }
    }

    /// Default timeout for network requests. Producer: ProduceRequests will use the lesser value of socket.timeout.ms and remaining message.timeout.ms for the first message in the batch. Consumer: FetchRequests will use fetch.wait.max.ms + socket.timeout.ms.
    public var socketTimeoutMs: UInt {
        get { self.dictionary.getUInt("socket.timeout.ms") ?? 60000 }
        set { self.dictionary["socket.timeout.ms"] = String(newValue) }
    }

    /// Broker socket send buffer size. System default is used if 0.
    public var socketSendBufferBytes: UInt {
        get { self.dictionary.getUInt("socket.send.buffer.bytes") ?? 0 }
        set { self.dictionary["socket.send.buffer.bytes"] = String(newValue) }
    }

    /// Broker socket receive buffer size. System default is used if 0.
    public var socketReceiveBufferBytes: UInt {
        get { self.dictionary.getUInt("socket.receive.buffer.bytes") ?? 0 }
        set { self.dictionary["socket.receive.buffer.bytes"] = String(newValue) }
    }

    /// Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets.
    public var socketKeepaliveEnable: Bool {
        get { self.dictionary.getBool("socket.keepalive.enable") ?? false }
        set { self.dictionary["socket.keepalive.enable"] = String(newValue) }
    }

    /// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
    public var socketNagleDisable: Bool {
        get { self.dictionary.getBool("socket.nagle.disable") ?? false }
        set { self.dictionary["socket.nagle.disable"] = String(newValue) }
    }

    /// Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. WARNING: It is highly recommended to leave this setting at its default value of 1 to avoid the client and broker to become desynchronized in case of request timeouts. NOTE: The connection is automatically re-established.
    public var socketMaxFails: UInt {
        get { self.dictionary.getUInt("socket.max.fails") ?? 1 }
        set { self.dictionary["socket.max.fails"] = String(newValue) }
    }

    /// Maximum time allowed for broker connection setup (TCP connection setup as well SSL and SASL handshake). If the connection to the broker is not fully functional after this the connection will be closed and retried.
    public var socketConnectionSetupTimeoutMs: UInt {
        get { self.dictionary.getUInt("socket.connection.setup.timeout.ms") ?? 30000 }
        set { self.dictionary["socket.connection.setup.timeout.ms"] = String(newValue) }
    }

    /// How long to cache the broker address resolving results (milliseconds).
    public var brokerAddressTTL: UInt {
        get { self.dictionary.getUInt("broker.address.ttl") ?? 1000 }
        set { self.dictionary["broker.address.ttl"] = String(newValue) }
    }

    /// Allowed broker ``KafkaSharedConfiguration/IPAddressFamily``.
    public var brokerAddressFamily: KafkaSharedConfiguration.IPAddressFamily {
        get { self.getIPAddressFamily() ?? .any }
        set { self.dictionary["broker.address.family"] = newValue.description }
    }

    /// The initial time to wait before reconnecting to a broker after the connection has been closed. The time is increased exponentially until reconnect.backoff.max.ms is reached. -25% to +50% jitter is applied to each reconnect backoff. A value of 0 disables the backoff and reconnects immediately.
    public var reconnectBackoffMs: UInt {
        get { self.dictionary.getUInt("reconnect.backoff.ms") ?? 100 }
        set { self.dictionary["reconnect.backoff.ms"] = String(newValue) }
    }

    /// The maximum time to wait before reconnecting to a broker after the connection has been closed.
    public var reconnectBackoffMaxMs: UInt {
        get { self.dictionary.getUInt("reconnect.backoff.max.ms") ?? 10000 }
        set { self.dictionary["reconnect.backoff.max.ms"] = String(newValue) }
    }

    /// ``KafkaSharedConfiguration/SecurityProtocol`` used to communicate with brokers.
    public var securityProtocol: KafkaSharedConfiguration.SecurityProtocol {
        get { self.getSecurityProtocol() ?? .plaintext }
        set { self.dictionary["security.protocol"] = newValue.description }
    }

    /// Path to client's private key (PEM) used for authentication.
    public var sslKeyLocation: String {
        get { self.dictionary["ssl.key.location"] ?? "" }
        set { self.dictionary["ssl.key.location"] = newValue }
    }

    /// Private key passphrase (for use with ssl.key.location).
    public var sslKeyPassword: String {
        get { self.dictionary["ssl.key.password"] ?? "" }
        set { self.dictionary["ssl.key.password"] = newValue }
    }

    /// Path to client's public key (PEM) used for authentication.
    public var sslCertificateLocation: String {
        get { self.dictionary["ssl.certificate.location"] ?? "" }
        set { self.dictionary["ssl.certificate.location"] = newValue }
    }

    /// File or directory path to CA certificate(s) for verifying the broker's key. Defaults: On Windows the system's CA certificates are automatically looked up in the Windows Root certificate store. On Mac OSX this configuration defaults to probe. It is recommended to install openssl using Homebrew, to provide CA certificates. On Linux install the distribution's ca-certificates package. If OpenSSL is statically linked or ssl.ca.location is set to probe a list of standard paths will be probed and the first one found will be used as the default CA certificate location path. If OpenSSL is dynamically linked the OpenSSL library's default path will be used (see OPENSSLDIR in openssl version -a).
    public var sslCALocation: String {
        get { self.dictionary["ssl.ca.location"] ?? "" }
        set { self.dictionary["ssl.ca.location"] = newValue }
    }

    /// Path to CRL for verifying broker's certificate validity.
    public var sslCRLLocation: String {
        get { self.dictionary["ssl.crl.location"] ?? "" }
        set { self.dictionary["ssl.crl.location"] = newValue }
    }

    /// Path to client's keystore (PKCS#12) used for authentication.
    public var sslKeystoreLocation: String {
        get { self.dictionary["ssl.keystore.location"] ?? "" }
        set { self.dictionary["ssl.keystore.location"] = newValue }
    }

    /// Client's keystore (PKCS#12) password.
    public var sslKeystorePassword: String {
        get { self.dictionary["ssl.keystore.password"] ?? "" }
        set { self.dictionary["ssl.keystore.password"] = newValue }
    }

    /// SASL mechanism to use for authentication.
    public var saslMechanism: KafkaSharedConfiguration.SASLMechanism? {
        get { self.getSASLMechanism() }
        set {
            if let newValue {
                self.dictionary["sasl.mechanism"] = newValue.description
            }
        }
    }

    /// SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms.
    public var saslUsername: String? {
        get { self.dictionary["sasl.username"] }
        set {
            if let newValue {
                self.dictionary["sasl.username"] = newValue
            }
        }
    }

    /// SASL password for use with the PLAIN and SASL-SCRAM-.. mechanisms.
    public var saslPassword: String? {
        get { self.dictionary["sasl.password"] }
        set {
            if let newValue {
                self.dictionary["sasl.password"] = newValue
            }
        }
    }

    public init(
        consumptionStrategy: KafkaSharedConfiguration.ConsumptionStrategy,
        backPressureStrategy: KafkaSharedConfiguration.BackPressureStrategy = .watermark(low: 10, high: 50),
        sessionTimeoutMs: UInt = 45000,
        heartbeatIntervalMs: UInt = 3000,
        maxPollInvervalMs: UInt = 300_000,
        enableAutoCommit: Bool = true,
        autoCommitIntervalMs: UInt = 5000,
        enableAutoOffsetStore: Bool = true,
        autoOffsetReset: KafkaSharedConfiguration.AutoOffsetReset = .largest,
        allowAutoCreateTopics: Bool = false,
        clientID: String = "rdkafka",
        bootstrapServers: [String] = [],
        messageMaxBytes: UInt = 1_000_000,
        messageCopyMaxBytes: UInt = 65535,
        receiveMessageMaxBytes: UInt = 100_000_000,
        maxInFlightRequestsPerConnection: UInt = 1_000_000,
        metadataMaxAgeMs: UInt = 900_000,
        topicMetadataRefreshIntervalMs: Int = 300_000,
        topicMetadataRefreshFastIntervalMs: UInt = 250,
        topicMetadataRefreshSparse: Bool = true,
        topicMetadataPropagationMaxMs: UInt = 30000,
        topicDenylist: [String] = [],
        debug: [KafkaSharedConfiguration.DebugOption] = [],
        socketTimeoutMs: UInt = 60000,
        socketSendBufferBytes: UInt = 0,
        socketReceiveBufferBytes: UInt = 0,
        socketKeepaliveEnable: Bool = false,
        socketNagleDisable: Bool = false,
        socketMaxFails: UInt = 1,
        socketConnectionSetupTimeoutMs: UInt = 30000,
        brokerAddressTTL: UInt = 1000,
        brokerAddressFamily: KafkaSharedConfiguration.IPAddressFamily = .any,
        reconnectBackoffMs: UInt = 100,
        reconnectBackoffMaxMs: UInt = 10000,
        securityProtocol: KafkaSharedConfiguration.SecurityProtocol = .plaintext,
        sslKeyLocation: String = "",
        sslKeyPassword: String = "",
        sslCertificateLocation: String = "",
        sslCALocation: String = "",
        sslCRLLocation: String = "",
        sslKeystoreLocation: String = "",
        sslKeystorePassword: String = "",
        saslMechanism: KafkaSharedConfiguration.SASLMechanism? = nil,
        saslUsername: String? = nil,
        saslPassword: String? = nil
    ) {
        self._consumptionStrategy = consumptionStrategy
        self.consumptionStrategy = consumptionStrategy // used to invoke set { } method
        self.backPressureStrategy = backPressureStrategy

        self.sessionTimeoutMs = sessionTimeoutMs
        self.heartbeatIntervalMs = heartbeatIntervalMs
        self.maxPollInvervalMs = maxPollInvervalMs
        self.enableAutoCommit = enableAutoCommit
        self.autoCommitIntervalMs = autoCommitIntervalMs
        self.enableAutoOffsetStore = enableAutoOffsetStore
        self.autoOffsetReset = autoOffsetReset
        self.allowAutoCreateTopics = allowAutoCreateTopics

        self.clientID = clientID
        self.bootstrapServers = bootstrapServers
        self.messageMaxBytes = messageMaxBytes
        self.messageCopyMaxBytes = messageCopyMaxBytes
        self.receiveMessageMaxBytes = receiveMessageMaxBytes
        self.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection
        self.metadataMaxAgeMs = metadataMaxAgeMs
        self.topicMetadataRefreshIntervalMs = topicMetadataRefreshIntervalMs
        self.topicMetadataRefreshFastIntervalMs = topicMetadataRefreshFastIntervalMs
        self.topicMetadataRefreshSparse = topicMetadataRefreshSparse
        self.topicMetadataPropagationMaxMs = topicMetadataPropagationMaxMs
        self.topicDenylist = topicDenylist
        self.debug = debug
        self.socketTimeoutMs = socketTimeoutMs
        self.socketSendBufferBytes = socketSendBufferBytes
        self.socketReceiveBufferBytes = socketReceiveBufferBytes
        self.socketKeepaliveEnable = socketKeepaliveEnable
        self.socketNagleDisable = socketNagleDisable
        self.socketMaxFails = socketMaxFails
        self.socketConnectionSetupTimeoutMs = socketConnectionSetupTimeoutMs
        self.brokerAddressTTL = brokerAddressTTL
        self.brokerAddressFamily = brokerAddressFamily
        self.reconnectBackoffMs = reconnectBackoffMs
        self.reconnectBackoffMaxMs = reconnectBackoffMaxMs
        self.securityProtocol = securityProtocol
        self.sslKeyLocation = sslKeyLocation
        self.sslKeyPassword = sslKeyPassword
        self.sslCertificateLocation = sslCertificateLocation
        self.sslCALocation = sslCALocation
        self.sslCRLLocation = sslCRLLocation
        self.sslKeystoreLocation = sslKeystoreLocation
        self.sslKeystorePassword = sslKeystorePassword
        self.saslMechanism = saslMechanism
        self.saslUsername = saslUsername
        self.saslPassword = saslPassword
    }

    // MARK: - Helpers

    func getDebugOptions() -> [KafkaSharedConfiguration.DebugOption] {
        guard let options = dictionary["debug"] else {
            return []
        }
        return options.components(separatedBy: ",")
            .map { KafkaSharedConfiguration.DebugOption(description: $0) }
    }

    func getIPAddressFamily() -> KafkaSharedConfiguration.IPAddressFamily? {
        guard let value = dictionary["broker.address.family"] else {
            return nil
        }
        return KafkaSharedConfiguration.IPAddressFamily(description: value)
    }

    func getSecurityProtocol() -> KafkaSharedConfiguration.SecurityProtocol? {
        guard let value = dictionary["security.protocol"] else {
            return nil
        }
        return KafkaSharedConfiguration.SecurityProtocol(description: value)
    }

    func getSASLMechanism() -> KafkaSharedConfiguration.SASLMechanism? {
        guard let value = dictionary["sasl.mechanism"] else {
            return nil
        }
        return KafkaSharedConfiguration.SASLMechanism(description: value)
    }

    func getAutoOffsetReset() -> KafkaSharedConfiguration.AutoOffsetReset? {
        guard let value = dictionary["auto.offset.reset"] else {
            return nil
        }
        return KafkaSharedConfiguration.AutoOffsetReset(description: value)
    }
}

// MARK: - KafkaSharedConfiguration + Consumer Additions

extension KafkaSharedConfiguration {
    /// A struct representing different back pressure strategies for consuming messages in ``KafkaConsumer``.
    public struct BackPressureStrategy: Hashable, Equatable {
        enum _BackPressureStrategy: Hashable, Equatable {
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

    /// A struct representing the different Kafka message consumption strategies.
    public struct ConsumptionStrategy: Hashable, Equatable {
        enum _ConsumptionStrategy: Hashable, Equatable {
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
    public struct AutoOffsetReset: Hashable, Equatable, CustomStringConvertible {
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
