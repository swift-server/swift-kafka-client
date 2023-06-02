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

/// Collection of `enum` types used in the configuration structs this library provides.
public enum ConfigEnums {
    /// Available debug contexts to enable.
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

    /// Available IP address families.
    public struct IPAddressFamily: Hashable, Equatable, CustomStringConvertible {
        public let description: String

        /// Use any IP address family.
        public static let any = IPAddressFamily(description: "any")
        /// Use the IPv4 address family.
        public static let v4 = IPAddressFamily(description: "v4")
        /// Use the IPv6 address family.
        public static let v6 = IPAddressFamily(description: "v6")
    }

    /// Protocol used to communicate with brokers.
    public struct SecurityProtocol: Hashable, Equatable, CustomStringConvertible {
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
    public struct SASLMechanism: Hashable, Equatable, CustomStringConvertible {
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
