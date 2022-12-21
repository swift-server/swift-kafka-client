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

// TODO: support PEMs?
// TODO: support OAuth?
// TODO: DocC: take from lirbdkafka official documentation
// TODO: Topic config -> see KafkaConfig in SwiftKafka
// TODO: test that values get set accordingly
// TODO: remove old config tests
// TODO: only use official kafka dictionary rather than all dictionary from librdkafka (for all configs)
public struct ConsumerConfig: Hashable, Equatable, StringDictionaryRepresentable {
    // TODO: some dictionary missing, check with franz
    // TODO: how to handle assignment / subscription?
    var dictionary: [String: String] = [:]

    // MARK: - Consumer-specific Config Properties

    public var groupID: String {
        get { self.dictionary["group.id"] ?? "" }
        set { self.dictionary["group.id"] = String(newValue) }
    }

    public var sessionTimeoutMs: UInt {
        get { self.dictionary.getUInt("session.timeout.ms") ?? 45000 }
        set { self.dictionary["session.timeout.ms"] = String(newValue) }
    }

    public var heartbeatIntervalMs: UInt {
        get { self.dictionary.getUInt("heartbeat.interval.ms") ?? 3000 }
        set { self.dictionary["heartbeat.interval.ms"] = String(newValue) }
    }

    public var maxPollInvervalMs: UInt {
        get { self.dictionary.getUInt("max.poll.interval.ms") ?? 300_000 }
        set { self.dictionary["max.poll.interval.ms"] = String(newValue) }
    }

    public var enableAutoCommit: Bool {
        get { self.dictionary.getBool("enable.auto.commit") ?? true }
        set { self.dictionary["enable.auto.commit"] = String(newValue) }
    }

    public var autoCommitIntervalMs: UInt {
        get { self.dictionary.getUInt("auto.commit.interval.ms") ?? 5000 }
        set { self.dictionary["auto.commit.interval.ms"] = String(newValue) }
    }

    public var autoOffsetReset: ConfigEnums.AutoOffsetReset {
        get { self.getAutoOffsetReset() ?? .largest }
        set { self.dictionary["auto.offset.reset"] = newValue.description }
    }

    public var enableAutoOffsetStore: Bool {
        get { self.dictionary.getBool("enable.auto.offset.store") ?? true }
        set { self.dictionary["enable.auto.offset.store"] = String(newValue) }
    }

    public var enablePartitionEOF: Bool {
        get { self.dictionary.getBool("enable.partition.eof") ?? false }
        set { self.dictionary["enable.partition.eof"] = String(newValue) }
    }

    public var allowAutoCreateTopics: Bool {
        get { self.dictionary.getBool("allow.auto.create.topics") ?? false }
        set { self.dictionary["allow.auto.create.topics"] = String(newValue) }
    }

    // MARK: - Common Client Config Properties

    public var clientID: String {
        get { self.dictionary["client.id"] ?? "rdkafka" }
        set { self.dictionary["client.id"] = newValue }
    }

    public var bootstrapServers: [String] {
        get { self.dictionary["bootstrap.servers"]?.components(separatedBy: ",") ?? [] }
        set { self.dictionary["bootstrap.servers"] = newValue.joined(separator: ",") }
    }

    public var messageMaxBytes: UInt {
        get { self.dictionary.getUInt("message.max.bytes") ?? 1_000_000 }
        set { self.dictionary["message.max.bytes"] = String(newValue) }
    }

    public var messageCopyMaxBytes: UInt {
        get { self.dictionary.getUInt("message.copy.max.bytes") ?? 65535 }
        set { self.dictionary["message.copy.max.bytes"] = String(newValue) }
    }

    public var receiveMessageMaxBytes: UInt {
        get { self.dictionary.getUInt("receive.message.max.bytes") ?? 100_000_000 }
        set { self.dictionary["receive.message.max.bytes"] = String(newValue) }
    }

    public var maxInFlightRequestsPerConnection: UInt {
        get { self.dictionary.getUInt("max.in.flight.requests.per.connection") ?? 1_000_000 }
        set { self.dictionary["max.in.flight.requests.per.connection"] = String(newValue) }
    }

    public var metadataMaxAgeMs: UInt {
        get { self.dictionary.getUInt("metadata.max.age.ms") ?? 900_000 }
        set { self.dictionary["metadata.max.age.ms"] = String(newValue) }
    }

    public var topicMetadataRefreshIntervalMs: Int {
        get { self.dictionary.getInt("topic.metadata.refresh.interval.ms") ?? 300_000 }
        set { self.dictionary["topic.metadata.refresh.interval.ms"] = String(newValue) }
    }

    public var topicMetadataRefreshFastIntervalMs: UInt {
        get { self.dictionary.getUInt("topic.metadata.refresh.fast.interval.ms") ?? 250 }
        set { self.dictionary["topic.metadata.refresh.fast.interval.ms"] = String(newValue) }
    }

    public var topicMetadataRefreshSparse: Bool {
        get { self.dictionary.getBool("topic.metadata.refresh.sparse") ?? true }
        set { self.dictionary["topic.metadata.refresh.sparse"] = newValue.description }
    }

    public var topicMetadataPropagationMaxMs: UInt {
        get { self.dictionary.getUInt("topic.metadata.propagation.max.ms") ?? 30000 }
        set { self.dictionary["topic.metadata.propagation.max.ms"] = String(newValue) }
    }

    public var topicDenylist: [String] {
        get { self.dictionary["topic.blacklist"]?.components(separatedBy: ",") ?? [] }
        set { self.dictionary["topic.blacklist"] = newValue.joined(separator: ",") }
    }

    public var debug: [ConfigEnums.DebugOption] {
        get { self.getDebugOptions() }
        set {
            if !newValue.isEmpty {
                self.dictionary["debug"] = newValue.map(\.description).joined(separator: ",")
            }
        }
    }

    public var socketTimeoutMs: UInt {
        get { self.dictionary.getUInt("socket.timeout.ms") ?? 60000 }
        set { self.dictionary["socket.timeout.ms"] = String(newValue) }
    }

    public var socketSendBufferBytes: UInt {
        get { self.dictionary.getUInt("socket.send.buffer.bytes") ?? 0 }
        set { self.dictionary["socket.send.buffer.bytes"] = String(newValue) }
    }

    public var socketReceiveBufferBytes: UInt {
        get { self.dictionary.getUInt("socket.receive.buffer.bytes") ?? 0 }
        set { self.dictionary["socket.receive.buffer.bytes"] = String(newValue) }
    }

    public var socketKeepaliveEnable: Bool {
        get { self.dictionary.getBool("socket.keepalive.enable") ?? false }
        set { self.dictionary["socket.keepalive.enable"] = String(newValue) }
    }

    public var socketNagleDisable: Bool {
        get { self.dictionary.getBool("socket.nagle.disable") ?? false }
        set { self.dictionary["socket.nagle.disable"] = String(newValue) }
    }

    public var socketMaxFails: UInt {
        get { self.dictionary.getUInt("socket.max.fails") ?? 1 }
        set { self.dictionary["socket.max.fails"] = String(newValue) }
    }

    public var socketConnectionSetupTimeoutMs: UInt {
        get { self.dictionary.getUInt("socket.connection.setup.timeout.ms") ?? 30000 }
        set { self.dictionary["socket.connection.setup.timeout.ms"] = String(newValue) }
    }

    public var brokerAddressTTL: UInt {
        get { self.dictionary.getUInt("broker.address.ttl") ?? 1000 }
        set { self.dictionary["broker.address.ttl"] = String(newValue) }
    }

    public var brokerAddressFamily: ConfigEnums.IPAddressFamily {
        get { self.getIPAddressFamily() ?? .any }
        set { self.dictionary["broker.address.family"] = newValue.description }
    }

    public var reconnectBackoffMs: UInt {
        get { self.dictionary.getUInt("reconnect.backoff.ms") ?? 100 }
        set { self.dictionary["reconnect.backoff.ms"] = String(newValue) }
    }

    public var reconnectBackoffMaxMs: UInt {
        get { self.dictionary.getUInt("reconnect.backoff.max.ms") ?? 10000 }
        set { self.dictionary["reconnect.backoff.max.ms"] = String(newValue) }
    }

    public var securityProtocol: ConfigEnums.SecurityProtocol {
        get { self.getSecurityProtocol() ?? .plaintext }
        set { self.dictionary["security.protocol"] = newValue.description }
    }

    public var sslKeyLocation: String {
        get { self.dictionary["ssl.key.location"] ?? "" }
        set { self.dictionary["ssl.key.location"] = newValue }
    }

    public var sslKeyPassword: String {
        get { self.dictionary["ssl.key.password"] ?? "" }
        set { self.dictionary["ssl.key.password"] = newValue }
    }

    public var sslCertificateLocation: String {
        get { self.dictionary["ssl.certificate.location"] ?? "" }
        set { self.dictionary["ssl.certificate.location"] = newValue }
    }

    public var sslCALocation: String {
        get { self.dictionary["ssl.ca.location"] ?? "" }
        set { self.dictionary["ssl.ca.location"] = newValue }
    }

    public var sslCRLLocation: String {
        get { self.dictionary["ssl.crl.location"] ?? "" }
        set { self.dictionary["ssl.crl.location"] = newValue }
    }

    public var sslKeystoreLocation: String {
        get { self.dictionary["ssl.keystore.location"] ?? "" }
        set { self.dictionary["ssl.keystore.location"] = newValue }
    }

    public var sslKeystorePassword: String {
        get { self.dictionary["ssl.keystore.password"] ?? "" }
        set { self.dictionary["ssl.keystore.password"] = newValue }
    }

    public var saslMechanism: ConfigEnums.SASLMechanism? {
        get { self.getSASLMechanism() }
        set {
            if let newValue {
                self.dictionary["sasl.mechanism"] = newValue.description
            }
        }
    }

    public var saslUsername: String? {
        get { self.dictionary["sasl.username"] }
        set {
            if let newValue {
                self.dictionary["sasl.username"] = newValue
            }
        }
    }

    public var saslPassword: String? {
        get { self.dictionary["sasl.password"] }
        set {
            if let newValue {
                self.dictionary["sasl.password"] = newValue
            }
        }
    }

    public init(
        groupID: String = "",
        sessionTimeoutMs: UInt = 45000,
        heartbeatIntervalMs: UInt = 3000,
        maxPollInvervalMs: UInt = 300_000,
        enableAutoCommit: Bool = true,
        autoCommitIntervalMs: UInt = 5000,
        enableAutoOffsetStore: Bool = true,
        autoOffsetReset: ConfigEnums.AutoOffsetReset = .largest,
        enablePartitionEOF: Bool = false,
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
        debug: [ConfigEnums.DebugOption] = [],
        socketTimeoutMs: UInt = 60000,
        socketSendBufferBytes: UInt = 0,
        socketReceiveBufferBytes: UInt = 0,
        socketKeepaliveEnable: Bool = false,
        socketNagleDisable: Bool = false,
        socketMaxFails: UInt = 1,
        socketConnectionSetupTimeoutMs: UInt = 30000,
        brokerAddressTTL: UInt = 1000,
        brokerAddressFamily: ConfigEnums.IPAddressFamily = .any,
        reconnectBackoffMs: UInt = 100,
        reconnectBackoffMaxMs: UInt = 10000,
        securityProtocol: ConfigEnums.SecurityProtocol = .plaintext,
        sslKeyLocation: String = "",
        sslKeyPassword: String = "",
        sslCertificateLocation: String = "",
        sslCALocation: String = "",
        sslCRLLocation: String = "",
        sslKeystoreLocation: String = "",
        sslKeystorePassword: String = "",
        saslMechanism: ConfigEnums.SASLMechanism? = nil,
        saslUsername: String? = nil,
        saslPassword: String? = nil
    ) {
        self.groupID = groupID
        self.sessionTimeoutMs = sessionTimeoutMs
        self.heartbeatIntervalMs = heartbeatIntervalMs
        self.maxPollInvervalMs = maxPollInvervalMs
        self.enableAutoCommit = enableAutoCommit
        self.autoCommitIntervalMs = autoCommitIntervalMs
        self.enableAutoOffsetStore = enableAutoOffsetStore
        self.autoOffsetReset = autoOffsetReset
        self.enablePartitionEOF = enablePartitionEOF
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

    // TODO: docc
    func getDebugOptions() -> [ConfigEnums.DebugOption] {
        guard let options = dictionary["debug"] else {
            return []
        }
        return options.components(separatedBy: ",")
            .map { ConfigEnums.DebugOption(description: $0) }
    }

    func getIPAddressFamily() -> ConfigEnums.IPAddressFamily? {
        guard let value = dictionary["broker.address.family"] else {
            return nil
        }
        return ConfigEnums.IPAddressFamily(description: value)
    }

    func getSecurityProtocol() -> ConfigEnums.SecurityProtocol? {
        guard let value = dictionary["security.protocol"] else {
            return nil
        }
        return ConfigEnums.SecurityProtocol(description: value)
    }

    func getSASLMechanism() -> ConfigEnums.SASLMechanism? {
        guard let value = dictionary["sasl.mechanism"] else {
            return nil
        }
        return ConfigEnums.SASLMechanism(description: value)
    }

    func getAutoOffsetReset() -> ConfigEnums.AutoOffsetReset? {
        guard let value = dictionary["auto.offset.reset"] else {
            return nil
        }
        return ConfigEnums.AutoOffsetReset(description: value)
    }
}

// MARK: - ConfigEnums + AutoOffsetReset

extension ConfigEnums {
    // TODO: docc
    public struct AutoOffsetReset: Hashable, Equatable, CustomStringConvertible {
        public let description: String

        public static let smallest = AutoOffsetReset(description: "smallest")
        public static let earliest = AutoOffsetReset(description: "earliest")
        public static let beginning = AutoOffsetReset(description: "beginning")
        public static let largest = AutoOffsetReset(description: "largest")
        public static let latest = AutoOffsetReset(description: "latest")
        public static let end = AutoOffsetReset(description: "end")
        public static let error = AutoOffsetReset(description: "error")
    }
}
