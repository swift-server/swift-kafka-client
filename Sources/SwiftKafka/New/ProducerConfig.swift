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

public struct ProducerConfig: Hashable, Equatable {
    // TODO: some properties missing, check with franz
    var properties: [String: String] = [:]

    // MARK: - Producer-specific Config Properties

    public var transactionalID: String { // TODO: Use optional or empty string for "no value"?
        get { self.getString("transactional.id") ?? "" }
        set { self.properties["transactional.id"] = newValue }
    }

    public var transactionTimeoutMs: UInt {
        get { self.getUInt("transaction.timeout.ms") ?? 60000 }
        set { self.properties["transaction.timeout.ms"] = String(newValue) }
    }

    public var enableIdempotence: Bool {
        get { self.getBool("enable.idempotence") ?? false }
        set { self.properties["enable.idempotence"] = String(newValue) }
    }

    public var queueBufferingMaxMessages: UInt {
        get { self.getUInt("queue.buffering.max.messages") ?? 100_000 }
        set { self.properties["queue.buffering.max.messages"] = String(newValue) }
    }

    public var queueBufferingMaxKBytes: UInt {
        get { self.getUInt("queue.buffering.max.kbytes") ?? 1_048_576 }
        set { self.properties["queue.buffering.max.kbytes"] = String(newValue) }
    }

    public var queueBufferingMaxMs: UInt {
        get { self.getUInt("queue.buffering.max.ms") ?? 5 }
        set { self.properties["queue.buffering.max.ms"] = String(newValue) }
    }

    public var messageSendMaxRetries: UInt {
        get { self.getUInt("message.send.max.retries") ?? 2_147_483_647 }
        set { self.properties["message.send.max.retries"] = String(newValue) }
    }

    // MARK: - Common Client Config Properties

    public var clientID: String {
        get { self.getString("client.id") ?? "rdkafka" }
        set { self.properties["client.id"] = newValue }
    }

    public var bootstrapServers: [String] {
        get { self.getString("bootstrap.servers")?.components(separatedBy: ",") ?? [] }
        set { self.properties["bootstrap.servers"] = newValue.joined(separator: ",") }
    }

    public var messageMaxBytes: UInt {
        get { self.getUInt("message.max.bytes") ?? 1_000_000 }
        set { self.properties["message.max.bytes"] = String(newValue) }
    }

    public var messageCopyMaxBytes: UInt {
        get { self.getUInt("message.copy.max.bytes") ?? 65535 }
        set { self.properties["message.copy.max.bytes"] = String(newValue) }
    }

    public var receiveMessageMaxBytes: UInt {
        get { self.getUInt("receive.message.max.bytes") ?? 100_000_000 }
        set { self.properties["receive.message.max.bytes"] = String(newValue) }
    }

    public var maxInFlightRequestsPerConnection: UInt {
        get { self.getUInt("max.in.flight.requests.per.connection") ?? 1_000_000 }
        set { self.properties["max.in.flight.requests.per.connection"] = String(newValue) }
    }

    public var metadataMaxAgeMs: UInt {
        get { self.getUInt("metadata.max.age.ms") ?? 900_000 }
        set { self.properties["metadata.max.age.ms"] = String(newValue) }
    }

    public var topicMetadataRefreshIntervalMs: Int {
        get { self.getInt("topic.metadata.refresh.interval.ms") ?? 300_000 }
        set { self.properties["topic.metadata.refresh.interval.ms"] = String(newValue) }
    }

    public var topicMetadataRefreshFastIntervalMs: UInt {
        get { self.getUInt("topic.metadata.refresh.fast.interval.ms") ?? 250 }
        set { self.properties["topic.metadata.refresh.fast.interval.ms"] = String(newValue) }
    }

    public var topicMetadataRefreshSparse: Bool {
        get { self.getBool("topic.metadata.refresh.sparse") ?? true }
        set { self.properties["topic.metadata.refresh.sparse"] = newValue.description }
    }

    public var topicMetadataPropagationMaxMs: UInt {
        get { self.getUInt("topic.metadata.propagation.max.ms") ?? 30000 }
        set { self.properties["topic.metadata.propagation.max.ms"] = String(newValue) }
    }

    public var topicDenylist: [String] {
        get { self.getString("topic.blacklist")?.components(separatedBy: ",") ?? [] }
        set { self.properties["topic.blacklist"] = newValue.joined(separator: ",") }
    }

    public var debug: [ConfigEnums.DebugOption] {
        get { self.getDebugOptions() }
        set {
            if !newValue.isEmpty {
                self.properties["debug"] = newValue.map(\.description).joined(separator: ",")
            }
        }
    }

    public var socketTimeoutMs: UInt {
        get { self.getUInt("socket.timeout.ms") ?? 60000 }
        set { self.properties["socket.timeout.ms"] = String(newValue) }
    }

    public var socketSendBufferBytes: UInt {
        get { self.getUInt("socket.send.buffer.bytes") ?? 0 }
        set { self.properties["socket.send.buffer.bytes"] = String(newValue) }
    }

    public var socketReceiveBufferBytes: UInt {
        get { self.getUInt("socket.receive.buffer.bytes") ?? 0 }
        set { self.properties["socket.receive.buffer.bytes"] = String(newValue) }
    }

    public var socketKeepaliveEnable: Bool {
        get { self.getBool("socket.keepalive.enable") ?? false }
        set { self.properties["socket.keepalive.enable"] = String(newValue) }
    }

    public var socketNagleDisable: Bool {
        get { self.getBool("socket.nagle.disable") ?? false }
        set { self.properties["socket.nagle.disable"] = String(newValue) }
    }

    public var socketMaxFails: UInt {
        get { self.getUInt("socket.max.fails") ?? 1 }
        set { self.properties["socket.max.fails"] = String(newValue) }
    }

    public var socketConnectionSetupTimeoutMs: UInt {
        get { self.getUInt("socket.connection.setup.timeout.ms") ?? 30000 }
        set { self.properties["socket.connection.setup.timeout.ms"] = String(newValue) }
    }

    public var brokerAddressTTL: UInt {
        get { self.getUInt("broker.address.ttl") ?? 1000 }
        set { self.properties["broker.address.ttl"] = String(newValue) }
    }

    public var brokerAddressFamily: ConfigEnums.IPAddressFamily {
        get { self.getIPAddressFamily() ?? .any }
        set { self.properties["broker.address.family"] = newValue.description }
    }

    public var reconnectBackoffMs: UInt {
        get { self.getUInt("reconnect.backoff.ms") ?? 100 }
        set { self.properties["reconnect.backoff.ms"] = String(newValue) }
    }

    public var reconnectBackoffMaxMs: UInt {
        get { self.getUInt("reconnect.backoff.max.ms") ?? 10000 }
        set { self.properties["reconnect.backoff.max.ms"] = String(newValue) }
    }

    public var securityProtocol: ConfigEnums.SecurityProtocol {
        get { self.getSecurityProtocol() ?? .plaintext }
        set { self.properties["security.protocol"] = newValue.description }
    }

    public var sslKeyLocation: String {
        get { self.getString("ssl.key.location") ?? "" }
        set { self.properties["ssl.key.location"] = newValue }
    }

    public var sslKeyPassword: String {
        get { self.getString("ssl.key.password") ?? "" }
        set { self.properties["ssl.key.password"] = newValue }
    }

    public var sslCertificateLocation: String {
        get { self.getString("ssl.certificate.location") ?? "" }
        set { self.properties["ssl.certificate.location"] = newValue }
    }

    public var sslCALocation: String {
        get { self.getString("ssl.ca.location") ?? "" }
        set { self.properties["ssl.ca.location"] = newValue }
    }

    public var sslCRLLocation: String {
        get { self.getString("ssl.crl.location") ?? "" }
        set { self.properties["ssl.crl.location"] = newValue }
    }

    public var sslKeystoreLocation: String {
        get { self.getString("ssl.keystore.location") ?? "" }
        set { self.properties["ssl.keystore.location"] = newValue }
    }

    public var sslKeystorePassword: String {
        get { self.getString("ssl.keystore.password") ?? "" }
        set { self.properties["ssl.keystore.password"] = newValue }
    }

    public var saslMechanism: ConfigEnums.SASLMechanism? {
        get { self.getSASLMechanism() }
        set {
            if let newValue {
                self.properties["sasl.mechanism"] = newValue.description
            }
        }
    }

    public var saslUsername: String? {
        get { self.getString("sasl.username") }
        set {
            if let newValue {
                self.properties["sasl.username"] = newValue
            }
        }
    }

    public var saslPassword: String? {
        get { self.getString("sasl.password") }
        set {
            if let newValue {
                self.properties["sasl.password"] = newValue
            }
        }
    }

    public init(
        transactionalID: String = "",
        transactionalTimeoutMs: UInt = 60000,
        enableIdempotence: Bool = false,
        queueBufferingMaxMessages: UInt = 100_000,
        queueBufferingMaxKBytes: UInt = 1_048_576,
        queueBufferingMaxMs: UInt = 5,
        messageSendMaxRetries: UInt = 2_147_483_647,
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
        self.transactionalID = transactionalID
        self.transactionTimeoutMs = transactionalTimeoutMs
        self.enableIdempotence = enableIdempotence
        self.queueBufferingMaxMessages = queueBufferingMaxMessages
        self.queueBufferingMaxKBytes = queueBufferingMaxKBytes
        self.queueBufferingMaxMs = queueBufferingMaxMs
        self.messageSendMaxRetries = messageSendMaxRetries
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
    func getString(_ key: String) -> String? {
        self.properties[key]
    }

    func getInt(_ key: String) -> Int? {
        guard let value = properties[key] else {
            return nil
        }
        return Int(value)
    }

    func getUInt(_ key: String) -> UInt? {
        guard let value = properties[key] else {
            return nil
        }
        return UInt(value)
    }

    func getBool(_ key: String) -> Bool? {
        guard let value = properties[key] else {
            return nil
        }
        return Bool(value)
    }

    func getDebugOptions() -> [ConfigEnums.DebugOption] {
        guard let options = properties["debug"] else {
            return []
        }
        return options.components(separatedBy: ",")
            .map { ConfigEnums.DebugOption(description: $0) }
    }

    func getIPAddressFamily() -> ConfigEnums.IPAddressFamily? {
        guard let value = properties["broker.address.family"] else {
            return nil
        }
        return ConfigEnums.IPAddressFamily(description: value)
    }

    func getSecurityProtocol() -> ConfigEnums.SecurityProtocol? {
        guard let value = properties["security.protocol"] else {
            return nil
        }
        return ConfigEnums.SecurityProtocol(description: value)
    }

    func getSASLMechanism() -> ConfigEnums.SASLMechanism? {
        guard let value = properties["sasl.mechanism"] else {
            return nil
        }
        return ConfigEnums.SASLMechanism(description: value)
    }
}
