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

public struct ProducerConfig: ClientConfig {
    // TODO: some properties missing, check with franz
    public var properties: [String: String] = [:]

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
        debug: [DebugOption] = [],
        socketTimeoutMs: UInt = 60000,
        socketSendBufferBytes: UInt = 0,
        socketReceiveBufferBytes: UInt = 0,
        socketKeepaliveEnable: Bool = false,
        socketNagleDisable: Bool = false,
        socketMaxFails: UInt = 1,
        socketConnectionSetupTimeoutMs: UInt = 30000,
        brokerAddressTTL: UInt = 1000,
        brokerAddressFamily: IPAddressFamily = .any,
        reconnectBackoffMs: UInt = 100,
        reconnectBackoffMaxMs: UInt = 10000,
        securityProtocol: SecurityProtocol = .plaintext,
        sslKeyLocation: String = "",
        sslKeyPassword: String = "",
        sslCertificateLocation: String = "",
        sslCALocation: String = "",
        sslCRLLocation: String = "",
        sslKeystoreLocation: String = "",
        sslKeystorePassword: String = "",
        saslMechanism: SASLMechanism? = nil,
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
}
