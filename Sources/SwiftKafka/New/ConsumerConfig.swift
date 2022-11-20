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

public struct ConsumerConfig: ClientConfig {
    // TODO: some properties missing, check with franz
    // TODO: how to handle assignment / subscription?
    public var properties: [String: String] = [:]

    public var groupID: String {
        get { self.getString("group.id") ?? "" }
        set { self.properties["group.id"] = String(newValue) }
    }

    public var sessionTimeoutMs: UInt {
        get { self.getUInt("session.timeout.ms") ?? 45000 }
        set { self.properties["session.timeout.ms"] = String(newValue) }
    }

    public var heartbeatIntervalMs: UInt {
        get { self.getUInt("heartbeat.interval.ms") ?? 3000 }
        set { self.properties["heartbeat.interval.ms"] = String(newValue) }
    }

    public var maxPollInvervalMs: UInt {
        get { self.getUInt("max.poll.interval.ms") ?? 300_000 }
        set { self.properties["max.poll.interval.ms"] = String(newValue) }
    }

    public var enableAutoCommit: Bool {
        get { self.getBool("enable.auto.commit") ?? true }
        set { self.properties["enable.auto.commit"] = String(newValue) }
    }

    public var autoCommitIntervalMs: UInt {
        get { self.getUInt("auto.commit.interval.ms") ?? 5000 }
        set { self.properties["auto.commit.interval.ms"] = String(newValue) }
    }

    public var autoOffsetReset: ConfigEnums.AutoOffsetReset {
        get { self.getAutoOffsetReset() ?? .largest }
        set { self.properties["auto.offset.reset"] = newValue.description }
    }

    public var enableAutoOffsetStore: Bool {
        get { self.getBool("enable.auto.offset.store") ?? true }
        set { self.properties["enable.auto.offset.store"] = String(newValue) }
    }

    public var enablePartitionEOF: Bool {
        get { self.getBool("enable.partition.eof") ?? false }
        set { self.properties["enable.partition.eof"] = String(newValue) }
    }

    public var allowAutoCreateTopics: Bool {
        get { self.getBool("allow.auto.create.topics") ?? false }
        set { self.properties["allow.auto.create.topics"] = String(newValue) }
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
}
