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

public struct KafkaProducerConfiguration {
    // MARK: - SwiftKafka-specific Config properties

    /// The time between two consecutive polls.
    /// Effectively controls the rate at which incoming events are consumed.
    /// Default: `.milliseconds(100)`
    public var pollInterval: Duration = .milliseconds(100)

    /// Interval for librdkafka statistics reports
    /// 0ms - disabled
    /// >= 1ms - statistics provided every specified interval
    public var statisticsInterval: Duration = .zero {
        didSet {
            precondition(
                self.statisticsInterval.totalMilliseconds > 0 || self.statisticsInterval == .zero /*self.statisticsInterval.canBeRepresentedAsMilliseconds*/,
                "Lowest granularity is milliseconds"
            )
        }
    }

    /// Maximum timeout for flushing outstanding produce requests when the ``KakfaProducer`` is shutting down.
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

    /// When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: max.in.flight.requests.per.connection=5 (must be less than or equal to 5), retries=INT32_MAX (must be greater than 0), acks=all, queuing.strategy=fifo. Producer instantation will fail if user-supplied configuration is incompatible.
    /// Default: `false`
    public var enableIdempotence: Bool = false

    /// Producer queue options.
    public var queue: KafkaConfiguration.QueueOptions = .init()

    /// How many times to retry sending a failing Message. Note: retrying may cause reordering unless enable.idempotence is set to true.
    /// Default: `2_147_483_647`
    public var messageSendMaxRetries: Int = 2_147_483_647

    /// Allow automatic topic creation on the broker when producing to non-existent topics.
    /// The broker must also be configured with auto.create.topics.enable=true for this configuration to take effect.
    /// Default: `true`
    public var allowAutoCreateTopics: Bool = true

    // MARK: - Common Client Config Properties

    /// Client identifier.
    /// Default: `"rdkafka"`
    public var clientID: String = "rdkafka"

    /// Initial list of brokers.
    /// Default: `[]`
    public var bootstrapServers: [KafkaConfiguration.Broker] = []

    /// Message options.
    public var message: KafkaConfiguration.MessageOptions = .init()

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hickups. This value must be at least fetch.max.bytes + 512 to allow for protocol overhead; the value is adjusted automatically unless the configuration property is explicitly set.
    /// Default: `100_000_000`
    public var receiveMessageMaxBytes: Int = 100_000_000

    /// Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one.
    /// Default: `1_000_000`
    public var maxInFlightRequestsPerConnection: Int = 1_000_000

    /// Metadata cache max age.
    /// Default: `900_000`
    public var metadataMaxAgeMilliseconds: Int = 900_000

    /// Topic metadata options.
    public var topicMetadata: KafkaConfiguration.TopicMetadataOptions = .init()

    /// Topic denylist.
    /// Default: `[]`
    public var topicDenylist: [String] = []

    /// Debug options.
    /// Default: `[]`
    public var debug: [KafkaConfiguration.DebugOption] = []

    /// Socket options.
    public var socket: KafkaConfiguration.SocketOptions = .init()

    /// Broker options.
    public var broker: KafkaConfiguration.BrokerOptions = .init()

    /// Reconnect options.
    public var reconnect: KafkaConfiguration.ReconnectOptions = .init()

    /// Security protocol to use (plaintext, ssl, sasl_plaintext, sasl_ssl).
    /// Default: `.plaintext`
    public var securityProtocol: KafkaConfiguration.SecurityProtocol = .plaintext

    public init() {}
}

// MARK: - KafkaProducerConfiguration + Dictionary

extension KafkaProducerConfiguration {
    internal var dictionary: [String: String] {
        var resultDict: [String: String] = [:]

        resultDict["statistics.interval.ms"] = String(self.statisticsInterval.totalMilliseconds)
        resultDict["enable.idempotence"] = String(self.enableIdempotence)
        resultDict["queue.buffering.max.messages"] = String(self.queue.bufferingMaxMessages)
        resultDict["queue.buffering.max.kbytes"] = String(self.queue.bufferingMaxKBytes)
        resultDict["queue.buffering.max.ms"] = String(self.queue.bufferingMaxMilliseconds)
        resultDict["message.send.max.retries"] = String(self.messageSendMaxRetries)
        resultDict["allow.auto.create.topics"] = String(self.allowAutoCreateTopics)

        resultDict["client.id"] = self.clientID
        resultDict["bootstrap.servers"] = self.bootstrapServers.map(\.description).joined(separator: ",")
        resultDict["message.max.bytes"] = String(self.message.maxBytes)
        resultDict["message.copy.max.bytes"] = String(self.message.copyMaxBytes)
        resultDict["receive.message.max.bytes"] = String(self.receiveMessageMaxBytes)
        resultDict["max.in.flight.requests.per.connection"] = String(self.maxInFlightRequestsPerConnection)
        resultDict["metadata.max.age.ms"] = String(self.metadataMaxAgeMilliseconds)
        resultDict["topic.metadata.refresh.interval.ms"] = String(self.topicMetadata.refreshIntervalMilliseconds)
        resultDict["topic.metadata.refresh.fast.interval.ms"] = String(self.topicMetadata.refreshFastIntervalMilliseconds)
        resultDict["topic.metadata.refresh.sparse"] = String(self.topicMetadata.refreshSparse)
        resultDict["topic.metadata.propagation.max.ms"] = String(self.topicMetadata.propagationMaxMilliseconds)
        resultDict["topic.blacklist"] = self.topicDenylist.joined(separator: ",")
        if !self.debug.isEmpty {
            resultDict["debug"] = self.debug.map(\.description).joined(separator: ",")
        }
        resultDict["socket.timeout.ms"] = String(self.socket.timeoutMilliseconds)
        resultDict["socket.send.buffer.bytes"] = String(self.socket.sendBufferBytes)
        resultDict["socket.receive.buffer.bytes"] = String(self.socket.receiveBufferBytes)
        resultDict["socket.keepalive.enable"] = String(self.socket.keepaliveEnable)
        resultDict["socket.nagle.disable"] = String(self.socket.nagleDisable)
        resultDict["socket.max.fails"] = String(self.socket.maxFails)
        resultDict["socket.connection.setup.timeout.ms"] = String(self.socket.connectionSetupTimeoutMilliseconds)
        resultDict["broker.address.ttl"] = String(self.broker.addressTTL)
        resultDict["broker.address.family"] = self.broker.addressFamily.description
        resultDict["reconnect.backoff.ms"] = String(self.reconnect.backoffMilliseconds)
        resultDict["reconnect.backoff.max.ms"] = String(self.reconnect.backoffMaxMilliseconds)

        // Merge with SecurityProtocol configuration dictionary
        resultDict.merge(self.securityProtocol.dictionary) { _, _ in
            fatalError("securityProtocol and \(#file) should not have duplicate keys")
        }

        return resultDict
    }
}

// MARK: - KafkaProducerConfiguration + Hashable

extension KafkaProducerConfiguration: Hashable {}

// MARK: - KafkaProducerConfiguration + Sendable

extension KafkaProducerConfiguration: Sendable {}

// MARK: - KafkaConfiguration + Producer Additions

extension KafkaConfiguration {
    /// Producer queue options.
    public struct QueueOptions: Sendable, Hashable {
        /// Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions. A value of 0 disables this limit.
        /// Default: `100_000`
        public var bufferingMaxMessages: Int = 100_000

        /// Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions. This property has higher priority than queue.buffering.max.messages.
        /// Default: `1_048_576`
        public var bufferingMaxKBytes: Int = 1_048_576

        /// Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
        /// Default: `5`
        public var bufferingMaxMilliseconds: Int = 5

        public init(
            bufferingMaxMessages: Int = 100_000,
            bufferingMaxKBytes: Int = 1_048_576,
            bufferingMaxMilliseconds: Int = 5
        ) {
            self.bufferingMaxMessages = bufferingMaxMessages
            self.bufferingMaxKBytes = bufferingMaxKBytes
            self.bufferingMaxMilliseconds = bufferingMaxMilliseconds
        }
    }
}
