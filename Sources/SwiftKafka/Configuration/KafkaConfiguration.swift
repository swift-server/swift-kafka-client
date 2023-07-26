//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-gsoc project authors
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
    /// A Kafka Broker to connect to.
    public struct Broker: Sendable, Hashable, CustomStringConvertible {
        /// The host component of the broker to connect to.
        public var host: String

        /// The port to connect to.
        public var port: Int

        public var description: String {
            "\(self.host):\(self.port)"
        }

        public init(host: String, port: Int) {
            self.host = host
            self.port = port
        }
    }

    /// Message options.
    public struct MessageOptions: Sendable, Hashable {
        /// Maximum Kafka protocol request message size. Due to differing framing overhead between protocol versions the producer is unable to reliably enforce a strict max message limit at produce time and may exceed the maximum size by one message in protocol ProduceRequests, the broker will enforce the the topic's max.message.bytes limit (see Apache Kafka documentation).
        /// Default: `1_000_000`
        public var maxBytes: Int = 1_000_000

        /// Maximum size for message to be copied to buffer. Messages larger than this will be passed by reference (zero-copy) at the expense of larger iovecs.
        /// Default: `65535`
        public var copyMaxBytes: Int = 65535

        public init(
            maxBytes: Int = 1_000_000,
            copyMaxBytes: Int = 65535
        ) {
            self.maxBytes = maxBytes
            self.copyMaxBytes = copyMaxBytes
        }
    }

    /// Topic metadata options.
    public struct TopicMetadataOptions: Sendable, Hashable {
        /// Period of time in milliseconds at which topic and broker metadata is refreshed in order to proactively discover any new brokers, topics, partitions or partition leader changes. Use -1 to disable the intervalled refresh (not recommended). If there are no locally referenced topics (no topic objects created, no messages produced, no subscription or no assignment) then only the broker list will be refreshed every interval but no more often than every 10s.
        /// Default: `300_000`
        public var refreshIntervalMilliseconds: Int = 300_000

        /// When a topic loses its leader a new metadata request will be enqueued with this initial interval, exponentially increasing until the topic metadata has been refreshed. This is used to recover quickly from transitioning leader brokers.
        /// Default: `250`
        public var refreshFastIntervalMilliseconds: Int = 250

        /// Sparse metadata requests (consumes less network bandwidth).
        /// Default: `true`
        public var refreshSparse: Bool = true

        /// Apache Kafka topic creation is asynchronous and it takes some time for a new topic to propagate throughout the cluster to all brokers. If a client requests topic metadata after manual topic creation but before the topic has been fully propagated to the broker the client is requesting metadata from, the topic will seem to be non-existent and the client will mark the topic as such, failing queued produced messages with ERR__UNKNOWN_TOPIC. This setting delays marking a topic as non-existent until the configured propagation max time has passed. The maximum propagation time is calculated from the time the topic is first referenced in the client, e.g., on `send()`.
        /// Default: `30000`
        public var propagationMaxMilliseconds: Int = 30000

        public init(
            refreshIntervalMilliseconds: Int = 300_000,
            refreshFastIntervalMilliseconds: Int = 250,
            refreshSparse: Bool = true,
            propagationMaxMilliseconds: Int = 30000
        ) {
            self.refreshIntervalMilliseconds = refreshIntervalMilliseconds
            self.refreshFastIntervalMilliseconds = refreshFastIntervalMilliseconds
            self.refreshSparse = refreshSparse
            self.propagationMaxMilliseconds = propagationMaxMilliseconds
        }
    }

    /// Socket options.
    public struct SocketOptions: Sendable, Hashable {
        /// Default timeout for network requests. Producer: ProduceRequests will use the lesser value of socket.timeout.ms and remaining message.timeout.ms for the first message in the batch. Consumer: FetchRequests will use fetch.wait.max.ms + socket.timeout.ms.
        /// Default: `60000`
        public var timeoutMilliseconds: Int = 60000

        /// Broker socket send buffer size. System default is used if 0.
        /// Default: `0`
        public var sendBufferBytes: Int = 0

        /// Broker socket receive buffer size. System default is used if 0.
        /// Default: `0`
        public var receiveBufferBytes: Int = 0

        /// Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets.
        /// Default: `false`
        public var keepaliveEnable: Bool = false

        /// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
        /// Default: `false`
        public var nagleDisable: Bool = false

        /// Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. WARNING: It is highly recommended to leave this setting at its default value of 1 to avoid the client and broker to become desynchronized in case of request timeouts. NOTE: The connection is automatically re-established.
        /// Default: `1`
        public var maxFails: Int = 1

        /// Maximum time allowed for broker connection setup (TCP connection setup as well SSL and SASL handshake). If the connection to the broker is not fully functional after this the connection will be closed and retried.
        /// Default: `30000`
        public var connectionSetupTimeoutMilliseconds: Int = 30000

        public init(
            timeoutMilliseconds: Int = 60000,
            sendBufferBytes: Int = 0,
            receiveBufferBytes: Int = 0,
            keepaliveEnable: Bool = false,
            nagleDisable: Bool = false,
            maxFails: Int = 1,
            connectionSetupTimeoutMilliseconds: Int = 30000
        ) {
            self.timeoutMilliseconds = timeoutMilliseconds
            self.sendBufferBytes = sendBufferBytes
            self.receiveBufferBytes = receiveBufferBytes
            self.keepaliveEnable = keepaliveEnable
            self.nagleDisable = nagleDisable
            self.maxFails = maxFails
            self.connectionSetupTimeoutMilliseconds = connectionSetupTimeoutMilliseconds
        }
    }

    /// Broker options.
    public struct BrokerOptions: Sendable, Hashable {
        /// How long to cache the broker address resolving results (milliseconds).
        /// Default: `1000`
        public var addressTTL: Int = 1000

        /// Allowed broker ``KafkaConfiguration/IPAddressFamily``.
        /// Default: `.any`
        public var addressFamily: KafkaConfiguration.IPAddressFamily = .any

        public init(
            addressTTL: Int = 1000,
            addressFamily: KafkaConfiguration.IPAddressFamily = .any
        ) {
            self.addressTTL = addressTTL
            self.addressFamily = addressFamily
        }
    }

    /// Reconnect options.
    public struct ReconnectOptions: Sendable, Hashable {
        /// The initial time to wait before reconnecting to a broker after the connection has been closed. The time is increased exponentially until reconnect.backoff.max.ms is reached. -25% to +50% jitter is applied to each reconnect backoff. A value of 0 disables the backoff and reconnects immediately.
        /// Default: `100`
        public var backoffMilliseconds: Int = 100

        /// The maximum time to wait before reconnecting to a broker after the connection has been closed.
        /// Default: `10000`
        public var backoffMaxMilliseconds: Int = 10000

        public init(
            backoffMilliseconds: Int = 100,
            backoffMaxMilliseconds: Int = 10000
        ) {
            self.backoffMilliseconds = backoffMilliseconds
            self.backoffMaxMilliseconds = backoffMaxMilliseconds
        }
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
}

extension Duration {
    // Calculated total milliseconds
    internal var totalMilliseconds: Int64 {
        self.components.seconds * 1000 + self.components.attoseconds / 1_000_000_000_000_000
    }
}
