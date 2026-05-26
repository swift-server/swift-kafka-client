//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// A namespace of types used in the library's configuration structs.
public enum KafkaConfiguration {
    /// The address of a Kafka broker.
    public struct BrokerAddress: Sendable, Hashable, CustomStringConvertible {
        /// The host component of the broker address.
        public var host: String

        /// The port to connect to.
        public var port: Int

        /// A textual representation of the broker address in `host:port` form.
        public var description: String {
            "\(self.host):\(self.port)"
        }

        /// Creates a new broker address from a host and port.
        ///
        /// - Parameters:
        ///   - host: The host component of the broker address.
        ///   - port: The port to connect to.
        public init(
            host: String,
            port: Int
        ) {
            self.host = host
            self.port = port
        }
    }

    /// Options that govern Kafka message size and copy behavior.
    public struct MessageOptions: Sendable, Hashable {
        /// Maximum Kafka protocol request message size.
        ///
        /// Due to differing framing overhead between protocol versions, the producer can't reliably enforce a strict max message limit at produce time and may exceed the maximum size by one message in protocol ProduceRequests.
        /// The broker enforces the topic's `max.message.bytes` limit [(see Apache Kafka documentation)](https://kafka.apache.org/documentation/#brokerconfigs_message.max.bytes).
        ///
        /// Default: `1_000_000`
        public var maximumBytes: Int = 1_000_000

        /// Maximum size for a message to be copied to buffer.
        ///
        /// Messages larger than this are passed by reference (zero-copy) at the expense of larger iovecs.
        ///
        /// Default: `65535`
        public var maximumBytesToCopy: Int = 65535

        /// Creates a new set of message options with default values.
        public init() {}
    }

    /// Options that control how the client refreshes topic and broker metadata.
    public struct TopicMetadataOptions: Sendable, Hashable {
        /// Period of time at which topic and broker metadata is refreshed to proactively discover any new brokers, topics, partitions, or partition leader changes.
        public struct RefreshInterval: Sendable, Hashable {
            internal let rawValue: Int

            private init(rawValue: Int) {
                self.rawValue = rawValue
            }

            /// Creates a refresh interval for the given duration.
            ///
            /// - Note: The lowest granularity is milliseconds.
            public static func interval(_ value: Duration) -> RefreshInterval {
                precondition(
                    value.canBeRepresentedAsMilliseconds,
                    "Lowest granularity is milliseconds"
                )
                return .init(rawValue: Int(value.inMilliseconds))
            }

            /// Disable the intervalled refresh (not recommended).
            public static let disabled: RefreshInterval = .init(rawValue: -1)
        }

        /// Period of time at which topic and broker metadata is refreshed to proactively discover any new brokers, topics, partitions, or partition leader changes.
        ///
        /// If there are no locally referenced topics (no topic objects created, no messages produced, no subscription, or no assignment) then only the broker list is refreshed every interval but no more often than every 10s.
        ///
        /// Default: `.interval(.milliseconds(300_000))`
        public var refreshInterval: RefreshInterval = .interval(.milliseconds(300_000))

        /// When a topic loses its leader, the client enqueues a new metadata request with this initial interval, exponentially increasing until the topic metadata has been refreshed.
        ///
        /// This setting helps recover quickly from transitioning leader brokers.
        ///
        /// Default: `.milliseconds(250)`
        public var refreshFastInterval: Duration = .milliseconds(250) {
            didSet {
                precondition(
                    self.refreshFastInterval.canBeRepresentedAsMilliseconds,
                    "Lowest granularity is milliseconds"
                )
            }
        }

        /// Sparse metadata requests (consumes less network bandwidth).
        ///
        /// Default: `true`
        public var isSparseRefreshingEnabled: Bool = true

        /// Apache Kafka topic creation is asynchronous and it takes some time for a new topic to propagate throughout the cluster to all brokers.
        ///
        /// If a client requests topic metadata after manual topic creation but before the topic has been fully propagated to the broker the client is requesting metadata from, the topic seems nonexistent and the client marks the topic as such, failing queued produced messages with ERR__UNKNOWN_TOPIC. This setting delays marking a topic as nonexistent until the configured propagation max time has passed. The maximum propagation time is calculated from the time the topic is first referenced in the client, for example, on `send()`.
        ///
        /// Default: `.milliseconds(30000)`
        public var maximumPropagation: Duration = .milliseconds(30000) {
            didSet {
                precondition(
                    self.maximumPropagation.canBeRepresentedAsMilliseconds,
                    "Lowest granularity is milliseconds"
                )
            }
        }

        /// Creates a new set of topic metadata options with default values.
        public init() {}
    }

    /// Options that configure socket-level networking behavior.
    public struct SocketOptions: Sendable, Hashable {
        /// Default timeout for network requests.
        ///
        /// Producer: ProduceRequests use the lesser value of ``KafkaConfiguration/SocketOptions/timeout``
        /// and remaining ``KafkaTopicConfiguration/messageTimeout`` for the first message in the batch.
        ///
        /// Default: `.milliseconds(60000)`
        public var timeout: Duration = .milliseconds(60000) {
            didSet {
                precondition(
                    self.timeout.canBeRepresentedAsMilliseconds,
                    "Lowest granularity is milliseconds"
                )
            }
        }

        /// Broker socket send/receive buffer size.
        public struct BufferSize: Sendable, Hashable {
            internal let rawValue: Int

            private init(rawValue: Int) {
                self.rawValue = rawValue
            }

            /// Creates a buffer size of the given number of bytes.
            public static func value(_ value: Int) -> BufferSize {
                .init(rawValue: value)
            }

            /// System default for send/receive buffer size.
            public static let systemDefault: BufferSize = .init(rawValue: 0)
        }

        /// Broker socket send buffer size.
        ///
        /// Default: `.systemDefault`
        public var sendBufferBytes: BufferSize = .systemDefault

        /// Broker socket receive buffer size.
        ///
        /// Default: `.systemDefault`
        public var receiveBufferBytes: BufferSize = .systemDefault

        /// Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets.
        ///
        /// Default: `false`
        public var isKeepaliveEnabled: Bool = false

        /// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
        ///
        /// Default: `false`
        public var isNagleDisabled: Bool = false

        /// Disconnects from the broker after this number of send failures (for example, timed-out requests).
        public struct MaximumFailures: Sendable, Hashable {
            internal let rawValue: Int

            private init(rawValue: Int) {
                self.rawValue = rawValue
            }

            /// Creates a maximum-failures threshold from the given number of send failures.
            public static func failures(_ value: Int) -> MaximumFailures {
                .init(rawValue: value)
            }

            /// Disable disconnecting from the broker on a number of send failures.
            public static let disabled: MaximumFailures = .init(rawValue: 0)
        }

        /// Disconnects from the broker after this number of send failures (for example, timed-out requests).
        ///
        /// - Warning: It is highly recommended to leave this setting at its default value of 1 to avoid the client and broker becoming desynchronized in case of request timeouts.
        /// - Note: The connection is automatically re-established.
        ///
        /// Default: `.failures(1)`
        public var maximumFailures: MaximumFailures = .failures(1)

        /// Maximum time allowed for broker connection setup (TCP connection setup as well SSL and SASL handshake).
        ///
        /// If the connection to the broker is not fully functional after this, the client closes and retries the connection.
        ///
        /// Default: `.milliseconds(30000)`
        public var connectionSetupTimeout: Duration = .milliseconds(30000)

        /// Creates a new set of socket options with default values.
        public init() {}
    }

    /// Options that configure broker connection behavior.
    public struct BrokerOptions: Sendable, Hashable {
        /// How long to cache the broker address resolving results.
        ///
        /// (Lowest granularity is milliseconds)
        ///
        /// Default: `.milliseconds(1000)`
        public var addressTimeToLive: Duration = .milliseconds(1000) {
            didSet {
                precondition(
                    self.addressTimeToLive.canBeRepresentedAsMilliseconds,
                    "Lowest granularity is milliseconds"
                )
            }
        }

        /// Allowed broker ``KafkaConfiguration/IPAddressFamily``.
        ///
        /// Default: `.any`
        public var addressFamily: IPAddressFamily = .any

        /// Creates a new set of broker options with default values.
        public init() {}
    }

    /// Options that control reconnection backoff after a broker connection drops.
    public struct ReconnectOptions: Sendable, Hashable {
        /// The initial time to wait before reconnecting to a broker after the connection has been closed.
        public struct Backoff: Sendable, Hashable {
            internal let rawValue: UInt

            private init(rawValue: UInt) {
                self.rawValue = rawValue
            }

            /// Creates a reconnect backoff for the given duration.
            ///
            /// - Note: The lowest granularity is milliseconds.
            public static func backoff(_ value: Duration) -> Backoff {
                precondition(
                    value.canBeRepresentedAsMilliseconds,
                    "Lowest granularity is milliseconds"
                )
                return .init(rawValue: value.inMilliseconds)
            }

            /// Disable the backoff and reconnect immediately.
            public static let disabled: Backoff = .init(rawValue: 0)
        }

        /// The initial time to wait before reconnecting to a broker after the connection has been closed.
        ///
        /// The time is increased exponentially until ``KafkaConfiguration/ReconnectOptions/maximumBackoff`` is reached.
        /// -25% to +50% jitter is applied to each reconnect backoff.
        ///
        /// Default: `.backoff(.milliseconds(100))`
        public var backoff: Backoff = .backoff(.milliseconds(100))

        /// The maximum time to wait before reconnecting to a broker after the connection has been closed.
        ///
        /// Default: `.milliseconds(10000)`
        public var maximumBackoff: Duration = .milliseconds(10000) {
            didSet {
                precondition(
                    self.maximumBackoff.canBeRepresentedAsMilliseconds,
                    "Lowest granularity is milliseconds"
                )
            }
        }

        /// Creates a new set of reconnect options with default values.
        public init() {}
    }

    // MARK: - Enum-like Option types

    /// Available debug contexts to enable.
    public struct DebugOption: Sendable, Hashable, CustomStringConvertible {
        /// A textual representation of the debug context, suitable for librdkafka's `debug` setting.
        public let description: String

        /// Enables debug logging for generic, non-categorized client events.
        public static let generic = DebugOption(description: "generic")
        /// Enables debug logging for broker connection and communication events.
        public static let broker = DebugOption(description: "broker")
        /// Enables debug logging for topic-level operations and state changes.
        public static let topic = DebugOption(description: "topic")
        /// Enables debug logging for cluster metadata requests and updates.
        public static let metadata = DebugOption(description: "metadata")
        /// Enables debug logging for protocol feature negotiation with brokers.
        public static let feature = DebugOption(description: "feature")
        /// Enables debug logging for internal message queue operations.
        public static let queue = DebugOption(description: "queue")
        /// Enables debug logging for individual message production and delivery.
        public static let msg = DebugOption(description: "msg")
        /// Enables debug logging for the Kafka wire protocol exchanges.
        public static let `protocol` = DebugOption(description: "protocol")
        /// Enables debug logging for consumer group coordination and rebalances.
        public static let cgrp = DebugOption(description: "cgrp")
        /// Enables debug logging for security, authentication, and TLS handshake events.
        public static let security = DebugOption(description: "security")
        /// Enables debug logging for consumer fetch requests and responses.
        public static let fetch = DebugOption(description: "fetch")
        /// Enables debug logging for client interceptor invocations.
        public static let interceptor = DebugOption(description: "interceptor")
        /// Enables debug logging for plugin loading and lifecycle events.
        public static let plugin = DebugOption(description: "plugin")
        /// Enables debug logging for high-level consumer operations.
        public static let consumer = DebugOption(description: "consumer")
        /// Enables debug logging for administrative API requests.
        public static let admin = DebugOption(description: "admin")
        /// Enables debug logging for exactly-once semantics, including the idempotent and transactional producer.
        public static let eos = DebugOption(description: "eos")
        /// Enables debug logging for every available context.
        public static let all = DebugOption(description: "all")
    }

    /// Available IP address families.
    public struct IPAddressFamily: Sendable, Hashable, CustomStringConvertible {
        /// A textual representation of the IP address family.
        public let description: String

        /// Use any IP address family.
        public static let any = IPAddressFamily(description: "any")
        /// Use the IPv4 address family.
        public static let v4 = IPAddressFamily(description: "v4")
        /// Use the IPv6 address family.
        public static let v6 = IPAddressFamily(description: "v6")
    }
}
