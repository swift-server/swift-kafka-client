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

/// Collection of types used in the configuration structs this library provides.
public enum KafkaConfiguration {
    /// Message options.
    public struct MessageOptions: Sendable, Hashable {
        /// Maximum Kafka protocol request message size.
        public var maxBytes: Int = 1_000_000

        /// Maximum topic message copy size passed to the application.
        public var copyMaxBytes: Int = 65535
    }

    /// Topic metadata options.
    public struct TopicMetadataOptions: Sendable, Hashable {
        /// Topic metadata refresh interval in milliseconds.
        public var refreshIntervalMilliseconds: Int = 300_000

        /// Topic metadata refresh fast interval in milliseconds.
        public var refreshFastIntervalMilliseconds: Int = 250

        /// Sparse topic metadata refresh (librdkafka 0.11.5 and later).
        public var refreshSparse: Bool = true

        /// Topic metadata propagation max interval in milliseconds.
        public var propagationMaxMilliseconds: Int = 30000
    }

    /// Socket options.
    public struct SocketOptions: Sendable, Hashable {
        /// Timeout for network requests.
        public var timeoutMilliseconds: Int = 60000

        /// Send buffer size.
        public var sendBufferBytes: Int = 0

        /// Receive buffer size.
        public var receiveBufferBytes: Int = 0

        /// Enable TCP keep-alives.
        public var keepaliveEnable: Bool = false

        /// Disable Nagle's algorithm.
        public var nagleDisable: Bool = false

        /// Maximum number of connection setup failures.
        public var maxFails: Int = 1

        /// Timeout for broker address acquisition.
        public var connectionSetupTimeoutMilliseconds: Int = 30000
    }

    /// Broker options.
    public struct BrokerOptions: Sendable, Hashable {
        /// Broker address initial TTL.
        public var addressTTL: Int = 1000

        /// Broker address family (any, v4, or v6).
        public var addressFamily: KafkaConfiguration.IPAddressFamily = .any
    }

    /// Reconnect options.
    public struct ReconnectOptions: Sendable, Hashable {
        /// Initial connection reconnect backoff in milliseconds.
        public var backoffMilliseconds: Int = 100

        /// Maximum connection reconnect backoff in milliseconds.
        public var backoffMaxMilliseconds: Int = 10000
    }

    /// SSL options.
    public struct SSLOptions: Sendable, Hashable {
        /// Path to client's private key (PEM) used for authentication.
        public var keyLocation: String = ""

        /// Private key's password.
        public var keyPassword: String = ""

        /// Path to client's public key (PEM) used for authentication.
        public var certificateLocation: String = ""

        /// Path to trusted CA certificate file for verifying the broker's certificate.
        public var CALocation: String = ""

        /// Path to CRL for verifying broker's certificate validity.
        public var CRLLocation: String = ""

        /// Path to client's keystore (PKCS#12) used for authentication.
        public var keystoreLocation: String = ""

        /// Client's keystore (PKCS#12) password.
        public var keystorePassword: String = ""
    }

    /// SASL options.
    public struct SASLOptions: Sendable, Hashable {
        /// SASL mechanism to use for authentication.
        public var mechanism: KafkaConfiguration.SASLMechanism?

        /// SASL username for authentication.
        public var username: String?

        /// SASL password for authentication.
        public var password: String?
    }

    // MARK: - Enum-like Option types

    /// Available debug contexts to enable.
    public struct DebugOption: Sendable, Hashable, CustomStringConvertible {
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

    /// Available IP address families.
    public struct IPAddressFamily: Sendable, Hashable, CustomStringConvertible {
        public let description: String

        /// Use any IP address family.
        public static let any = IPAddressFamily(description: "any")
        /// Use the IPv4 address family.
        public static let v4 = IPAddressFamily(description: "v4")
        /// Use the IPv6 address family.
        public static let v6 = IPAddressFamily(description: "v6")
    }

    /// Protocol used to communicate with brokers.
    public struct SecurityProtocol: Sendable, Hashable, CustomStringConvertible {
        public let description: String

        /// Send messages as plaintext (no security protocol used).
        public static let plaintext = SecurityProtocol(description: "plaintext")
        /// Use the Secure Sockets Layer (SSL) protocol.
        public static let ssl = SecurityProtocol(description: "ssl")
        /// Use the Simple Authentication and Security Layer (SASL).
        public static let saslPlaintext = SecurityProtocol(description: "sasl_plaintext")
        /// Use the Simple Authentication and Security Layer (SASL) with SSL.
        public static let saslSSL = SecurityProtocol(description: "sasl_ssl")
    }

    /// Available SASL mechanisms that can be used for authentication.
    public struct SASLMechanism: Sendable, Hashable, CustomStringConvertible {
        public let description: String

        /// Use the GSSAPI mechanism.
        public static let gssapi = SASLMechanism(description: "GSSAPI")
        /// Use the PLAIN mechanism.
        public static let plain = SASLMechanism(description: "PLAIN")
        /// Use the SCRAM-SHA-256 mechanism.
        public static let scramSHA256 = SASLMechanism(description: "SCRAM-SHA-256")
        /// Use the SCRAM-SHA-512 mechanism.
        public static let scramSHA512 = SASLMechanism(description: "SCRAM-SHA-512")
        /// Use the OAUTHBEARER mechanism.
        public static let oauthbearer = SASLMechanism(description: "OAUTHBEARER")
    }
}
