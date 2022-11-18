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

// TODO: getter for internal dict representation
// TODO: Array to comma separated string
// TODO: default values
// TODO: dict add manually by key (unsafe api)?
// TODO: support PEMs?
// TODO: support OAuth?
// TODO: List continues at group.id
// TODO: DocC: inherit documentation?
// TODO: DocC: take from lirbdkafka official documentation
// TODO: Topic config -> see KafkaConfig in SwiftKafka
// TODO: make IPAddressFamily etc. part of some ConfigProperty like type to avoid cluttering docc
protocol ClientConfig: Hashable, Equatable {
    var clientID: String { get set }

    var bootstrapServers: [String] { get set }

    var messageMaxBytes: UInt { get set }
    var messageCopyMaxBytes: UInt { get set }

    var recieveMessageMaxBytes: UInt { get set }
    var maxInFlightRequestsPerConnection: UInt { get set }
    var metadataMaxAgeMs: UInt { get set }

    var topicMetadataRefreshIntervalMs: Int { get set }
    var topicMetadataRefreshFastIntervalMs: UInt { get set }
    var topicMetadataRefreshSparse: Bool { get set }
    var topicMetadataPropagationMaxMs: UInt { get set } // TODO: needed?
    var topicDenylist: [String] { get set } // TODO: is blacklist actually + property needed?

    var debug: [DebugOption] { get set }

    var socketTimeoutMs: UInt { get set }
    var socketSendBufferBytes: UInt { get set }
    var socketReceiveBufferBytes: UInt { get set }
    var socketKeepaliveEnable: Bool { get set }
    var socketNagleDisable: Bool { get set }
    var socketMaxFails: UInt { get set }
    var socketConnectionSetupTimeoutMs: UInt { get set } // TODO: needed? if not also delete in implementing structs

    var brokerAddressTTL: UInt { get set } // TODO: needed?
    var brokerAddressFamily: IPAddressFamily { get set }

    var reconnectBackoffMs: UInt { get set }
    var reconnectBackoffMaxMs: UInt { get set }

    var allowAutoCreateTopics: Bool { get set } // TODO: needed?

    var securityProtocol: SecurityProtocol { get set }

    var sslKeyLocation: String { get set }
    var sslKeyPassword: String { get set }
    var sslCertificateLocation: String { get set }
    var sslCALocation: String { get set }
    var sslCRLLocation: String { get set }
    var sslKeystoreLocation: String { get set }
    var sslKeystorePassword: String { get set }

    var saslMechanism: SASLMechanism { get set }
    var saslUsername: String { get set }
    var saslPassword: String { get set }
}

// MARK: - Auxiliary Types

public struct DebugOption: Hashable, Equatable, CustomStringConvertible {
    public let description: String

    public static let generic = DebugOption(description: "generic")
    public static let broker = DebugOption(description: "broker")
    public static let topic = DebugOption(description: "topic")
    public static let metadata = DebugOption(description: "metadata")
    public static let feature = DebugOption(description: "feature")
    public static let queue = DebugOption(description: "queue")
    public static let msg = DebugOption(description: "msg")
    public static let `protocol` = DebugOption(description: "protocol")
    public static let cgrp = DebugOption(description: "cgrp")
    public static let security = DebugOption(description: "security")
    public static let fetch = DebugOption(description: "fetch")
    public static let interceptor = DebugOption(description: "interceptor")
    public static let plugin = DebugOption(description: "plugin")
    public static let consumer = DebugOption(description: "consumer")
    public static let admin = DebugOption(description: "admin")
    public static let eos = DebugOption(description: "eos")
    public static let all = DebugOption(description: "all")
}

public struct IPAddressFamily: Hashable, Equatable, CustomStringConvertible {
    public let description: String

    public static let any = IPAddressFamily(description: "any")
    public static let v4 = IPAddressFamily(description: "v4")
    public static let v6 = IPAddressFamily(description: "v6")
}

public struct SecurityProtocol: Hashable, Equatable, CustomStringConvertible {
    public let description: String

    public static let plaintext = SecurityProtocol(description: "plaintext")
    public static let ssl = SecurityProtocol(description: "ssl")
    public static let saslPlaintext = SecurityProtocol(description: "sasl_plaintext")
    public static let saslSSL = SecurityProtocol(description: "sasl_ssl")
}

public struct SASLMechanism: Hashable, Equatable, CustomStringConvertible {
    public let description: String

    public static let gssapi = SASLMechanism(description: "GSSAPI")
    public static let plain = SASLMechanism(description: "PLAIN")
    public static let scramSHA256 = SASLMechanism(description: "SCRAM-SHA-256")
    public static let scramSHA512 = SASLMechanism(description: "SCRAM-SHA-512")
    public static let oauthbearer = SASLMechanism(description: "OAUTHBEARER")
}
