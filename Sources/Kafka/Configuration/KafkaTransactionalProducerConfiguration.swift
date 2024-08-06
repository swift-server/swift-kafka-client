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

public struct KafkaTransactionalProducerConfiguration {
    // MARK: - Kafka-specific Config properties

    /// If the ``isAutoCreateTopicsEnabled`` option is set to `true`,
    /// the broker will automatically generate topics when producing data to non-existent topics.
    /// The configuration specified in this ``KafkaTopicConfiguration`` will be applied to the newly created topic.
    /// Default: See default values of ``KafkaTopicConfiguration``
    public var topicConfiguration: KafkaTopicConfiguration = .init()

    /// The time between two consecutive polls.
    /// Effectively controls the rate at which incoming events are consumed.
    /// Default: `.milliseconds(100)`
    public var pollInterval: Duration = .milliseconds(100)

    /// Maximum timeout for flushing outstanding produce requests when the ``KafkaProducer`` is shutting down.
    /// Default: `10000`
    public var flushTimeoutMilliseconds: Int = 10000 {
        didSet {
            precondition(
                0...Int(Int32.max) ~= self.flushTimeoutMilliseconds,
                "Flush timeout outside of valid range \(0...Int32.max)"
            )
        }
    }

    // MARK: - Producer-specific Config Properties

    /// When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order.
    /// The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled:
    /// ``KafkaProducerConfiguration/maximumInFlightRequestsPerConnection`` = `5` (must be less than or equal to 5),
    /// ``KafkaProducerConfiguration/maximumMessageSendRetries`` = `UInt32.max` (must be greater than 0),
    /// ``KafkaTopicConfiguration/requiredAcknowledgements`` = ``KafkaTopicConfiguration/RequiredAcknowledgments/all``,
    /// queuing strategy = FIFO.
    /// Producer instantiation will fail if the user-supplied configuration is incompatible.
    /// Default: `false`
    public var isIdempotenceEnabled: Bool = false

    /// Producer queue options.
    public struct QueueConfiguration: Sendable, Hashable {
        /// Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions.
        public struct MessageLimit: Sendable, Hashable {
            internal let rawValue: Int

            private init(rawValue: Int) {
                self.rawValue = rawValue
            }

            public static func maximumLimit(_ value: Int) -> MessageLimit {
                return .init(rawValue: value)
            }

            /// No limit for the maximum number of messages allowed on the producer queue.
            public static let unlimited: MessageLimit = .init(rawValue: 0)
        }

        /// Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions.
        /// Default: `.maximumLimit(100_000)`
        public var messageLimit: MessageLimit = .maximumLimit(100_000)

        /// Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions.
        /// This property has higher priority than ``KafkaConfiguration/QueueOptions/messageLimit``.
        /// Default: `1_048_576 * 1024`
        public var maximumMessageBytes: Int = 1_048_576 * 1024

        /// How long wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers.
        /// A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
        /// (Lowest granularity is milliseconds)
        /// Default: `.milliseconds(5)`
        public var maximumMessageQueueTime: Duration = .milliseconds(5) {
            didSet {
                precondition(
                    self.maximumMessageQueueTime.canBeRepresentedAsMilliseconds,
                    "Lowest granularity is milliseconds"
                )
            }
        }

        public init() {}
    }

    /// Producer queue options.
    public var queue: QueueConfiguration = .init()

    /// How many times to retry sending a failing Message.
    ///
    /// - Note: retrying may cause reordering unless ``KafkaProducerConfiguration/isIdempotenceEnabled`` is set to `true`.
    /// Default: `2_147_483_647`
    public var maximumMessageSendRetries: Int = 2_147_483_647

    /// Allow automatic topic creation on the broker when producing to non-existent topics.
    /// The broker must also be configured with ``isAutoCreateTopicsEnabled`` = `true` for this configuration to take effect.
    /// Default: `true`
    public var isAutoCreateTopicsEnabled: Bool = true

    // MARK: - Common Client Config Properties

    /// Client identifier.
    /// Default: `"rdkafka"`
    public var identifier: String = "rdkafka"

    /// Initial list of brokers.
    /// Default: `[]`
    public var bootstrapBrokerAddresses: [KafkaConfiguration.BrokerAddress] = []

    /// Message options.
    public var message: KafkaConfiguration.MessageOptions = .init()

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hiccups.
    /// Default: `100_000_000`
    public var maximumReceiveMessageBytes: Int = 100_000_000

    /// Maximum number of in-flight requests per broker connection.
    /// This is a generic property applied to all broker communication, however, it is primarily relevant to produce requests.
    /// In particular, note that other mechanisms limit the number of outstanding consumer fetch requests per broker to one.
    /// Default: `5`
    public var maximumInFlightRequestsPerConnection: Int = 5 {
        didSet {
            precondition(
                self.maximumInFlightRequestsPerConnection <= 5,
                "Max in flight requests is 5 for TransactionalProducer"
            )
        }
    }

    /// Metadata cache max age.
    /// (Lowest granularity is milliseconds)
    /// Default: `.milliseconds(900_000)`
    public var maximumMetadataAge: Duration = .milliseconds(900_000) {
        didSet {
            precondition(
                self.maximumMetadataAge.canBeRepresentedAsMilliseconds,
                "Lowest granularity is milliseconds"
            )
        }
    }

    /// Topic metadata options.
    public var topicMetadata: KafkaConfiguration.TopicMetadataOptions = .init()

    /// Topic denylist.
    /// Default: `[]`
    public var topicDenylist: [String] = []

    /// Debug options.
    /// Default: `[]`
    public var debugOptions: [KafkaConfiguration.DebugOption] = []

    /// Socket options.
    public var socket: KafkaConfiguration.SocketOptions = .init()

    /// Broker options.
    public var broker: KafkaConfiguration.BrokerOptions = .init()

    /// Reconnect options.
    public var reconnect: KafkaConfiguration.ReconnectOptions = .init()
    
    /// Options for librdkafka metrics updates
    public var metrics: KafkaConfiguration.ProducerMetrics = .init()

    /// Security protocol to use (plaintext, ssl, sasl_plaintext, sasl_ssl).
    /// Default: `.plaintext`
    public var securityProtocol: KafkaConfiguration.SecurityProtocol = .plaintext

    // TODO: add Docc
    var transactionalId: String
    var transactionsTimeout: Duration = .seconds(60) {
        didSet {
            precondition(
                self.maximumMetadataAge.canBeRepresentedAsMilliseconds,
                "Lowest granularity is milliseconds"
            )
        }
    }
    
    public var compression: String?
    
    public init(
        transactionalId: String,
        bootstrapBrokerAddresses: [KafkaConfiguration.BrokerAddress]
    ) {
        self.transactionalId = transactionalId
        self.bootstrapBrokerAddresses = bootstrapBrokerAddresses
    }
}

// MARK: - KafkaProducerConfiguration + Dictionary

extension KafkaTransactionalProducerConfiguration {
    internal var dictionary: [String: String] {
        var resultDict: [String: String] = [:]
        
        resultDict["transactional.id"] = self.transactionalId
        resultDict["transaction.timeout.ms"] = String(self.transactionsTimeout.totalMilliseconds)
        resultDict["enable.idempotence"] = "true"

        resultDict["queue.buffering.max.messages"] = String(self.queue.messageLimit.rawValue)
        resultDict["queue.buffering.max.kbytes"] = String(self.queue.maximumMessageBytes / 1024)
        resultDict["queue.buffering.max.ms"] = String(self.queue.maximumMessageQueueTime.inMilliseconds)
        resultDict["message.send.max.retries"] = String(self.maximumMessageSendRetries)
        resultDict["allow.auto.create.topics"] = String(self.isAutoCreateTopicsEnabled)

        resultDict["client.id"] = self.identifier
        resultDict["bootstrap.servers"] = self.bootstrapBrokerAddresses.map(\.description).joined(separator: ",")
        resultDict["message.max.bytes"] = String(self.message.maximumBytes)
        resultDict["message.copy.max.bytes"] = String(self.message.maximumBytesToCopy)
        resultDict["receive.message.max.bytes"] = String(self.maximumReceiveMessageBytes)
        resultDict["max.in.flight.requests.per.connection"] = String(self.maximumInFlightRequestsPerConnection)
        resultDict["metadata.max.age.ms"] = String(self.maximumMetadataAge.inMilliseconds)
        resultDict["topic.metadata.refresh.interval.ms"] = String(self.topicMetadata.refreshInterval.rawValue)
        resultDict["topic.metadata.refresh.fast.interval.ms"] = String(self.topicMetadata.refreshFastInterval.inMilliseconds)
        resultDict["topic.metadata.refresh.sparse"] = String(self.topicMetadata.isSparseRefreshingEnabled)
        resultDict["topic.metadata.propagation.max.ms"] = String(self.topicMetadata.maximumPropagation.inMilliseconds)
        resultDict["topic.blacklist"] = self.topicDenylist.joined(separator: ",")
        if !self.debugOptions.isEmpty {
            resultDict["debug"] = self.debugOptions.map(\.description).joined(separator: ",")
        }
        resultDict["socket.timeout.ms"] = String(self.socket.timeout.inMilliseconds)
        resultDict["socket.send.buffer.bytes"] = String(self.socket.sendBufferBytes.rawValue)
        resultDict["socket.receive.buffer.bytes"] = String(self.socket.receiveBufferBytes.rawValue)
        resultDict["socket.keepalive.enable"] = String(self.socket.isKeepaliveEnabled)
        resultDict["socket.nagle.disable"] = String(self.socket.isNagleDisabled)
        resultDict["socket.max.fails"] = String(self.socket.maximumFailures.rawValue)
        resultDict["socket.connection.setup.timeout.ms"] = String(self.socket.connectionSetupTimeout.inMilliseconds)
        resultDict["broker.address.ttl"] = String(self.broker.addressTimeToLive.inMilliseconds)
        resultDict["broker.address.family"] = self.broker.addressFamily.description
        resultDict["reconnect.backoff.ms"] = String(self.reconnect.backoff.rawValue)
        resultDict["reconnect.backoff.max.ms"] = String(self.reconnect.maximumBackoff.inMilliseconds)

        // Merge with SecurityProtocol configuration dictionary
        resultDict.merge(self.securityProtocol.dictionary) { _, _ in
            fatalError("securityProtocol and \(#file) should not have duplicate keys")
        }
        
        if let compression {
            resultDict["compression.codec"] = compression
        }

        if self.metrics.enabled,
           let updateInterval = self.metrics.updateInterval {
            resultDict["statistics.interval.ms"] = String(updateInterval.inMilliseconds)
        }

        return resultDict
    }
}

// MARK: - KafkaProducerConfiguration + Sendable

extension KafkaTransactionalProducerConfiguration: Sendable {}
