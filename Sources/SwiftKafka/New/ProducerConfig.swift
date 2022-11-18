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
    public var transactionalID: String = "" // TODO: Use empty string or nil for "no value"?
    public var transactionalTimeoutMs: UInt = 60000
    public var enableIdempotence: Bool = false
    public var queueBufferingMaxMessages: UInt = 100000
    public var queueBufferingMaxKBytes: UInt = 1048576
    public var queueBufferingMaxMs: UInt = 5
    public var messageSendMaxRetries: UInt = 2147483647

    // MARK: - ClientConfig

    public var clientID: String = "rdkafka"

    public var bootstrapServers: [String] = []

    public var messageMaxBytes: UInt = 1000000
    public var messageCopyMaxBytes: UInt = 65535

    public var recieveMessageMaxBytes: UInt = 100000000
    public var maxInFlightRequestsPerConnection: UInt = 1000000
    public var metadataMaxAgeMs: UInt = 900000

    public var topicMetadataRefreshIntervalMs: Int = 300000
    public var topicMetadataRefreshFastIntervalMs: UInt = 250
    public var topicMetadataRefreshSparse: Bool = true
    public var topicMetadataPropagationMaxMs: UInt = 30000
    public var topicDenylist: [String] = []

    public var debug: [DebugOption] = []

    public var socketTimeoutMs: UInt = 60000
    public var socketSendBufferBytes: UInt = 0
    public var socketReceiveBufferBytes: UInt = 0
    public var socketKeepaliveEnable: Bool = false
    public var socketNagleDisable: Bool = false
    public var socketMaxFails: UInt = 1
    public var socketConnectionSetupTimeoutMs: UInt = 30000

    public var brokerAddressTTL: UInt = 1000
    public var brokerAddressFamily: IPAddressFamily = .any

    public var reconnectBackoffMs: UInt = 100
    public var reconnectBackoffMaxMs: UInt = 10000

    public var allowAutoCreateTopics: Bool = true

    public var securityProtocol: SecurityProtocol = .plaintext

    public var sslKeyLocation: String = ""
    public var sslKeyPassword: String = ""
    public var sslCertificateLocation: String = ""
    public var sslCALocation: String = ""
    public var sslCRLLocation: String = ""
    public var sslKeystoreLocation: String = ""
    public var sslKeystorePassword: String = ""

    public var saslMechanism: SASLMechanism = .gssapi
    public var saslUsername: String = ""
    public var saslPassword: String = ""
}
