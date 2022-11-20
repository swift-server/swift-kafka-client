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

// TODO: dict add manually by key (unsafe api)?
// TODO: support PEMs?
// TODO: support OAuth?
// TODO: DocC: inherit documentation?
// TODO: DocC: take from lirbdkafka official documentation
// TODO: Topic config -> see KafkaConfig in SwiftKafka
// TODO: create empty init for substructs to disable free initializer?
// TODO: test that values get set accordingly
// TODO: remove old config tests
// TODO: magic nums for default values?
public protocol ClientConfig: Hashable, Equatable {
    // TODO: docc
    var properties: [String: String] { set get }
}

extension ClientConfig {
    // MARK: - Properties of every ClientConfig

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

    // TODO: move to Consumer
    func getAutoOffsetReset() -> ConfigEnums.AutoOffsetReset? {
        guard let value = properties["auto.offset.reset"] else {
            return nil
        }
        return ConfigEnums.AutoOffsetReset(description: value)
    }
}

