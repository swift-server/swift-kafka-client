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
    /// Effectively controls the rate at which incoming events and acknowledgements are consumed.
    public var pollInterval: Duration = .milliseconds(100)

    // MARK: - Producer-specific Config Properties

    /// Enables the transactional producer. The transactional.id is used to identify the same transactional producer instance across process restarts. It allows the producer to guarantee that transactions corresponding to earlier instances of the same producer have been finalized prior to starting any new transactions, and that any zombie instances are fenced off. If no transactional.id is provided, then the producer is limited to idempotent delivery (if enable.idempotence is set). Requires broker version >= 0.11.0.
    public var transactionalID: String?

    /// The maximum amount of time in milliseconds that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction. If this value is larger than the transaction.max.timeout.ms setting in the broker, the init_transactions() call will fail with ERR_INVALID_TRANSACTION_TIMEOUT. The transaction timeout automatically adjusts message.timeout.ms and socket.timeout.ms, unless explicitly configured in which case they must not exceed the transaction timeout (socket.timeout.ms must be at least 100ms lower than transaction.timeout.ms). This is also the default timeout value if no timeout (-1) is supplied to the transactional API methods.
    public var transactionTimeoutMilliseconds: Int = 60000

    /// When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: max.in.flight.requests.per.connection=5 (must be less than or equal to 5), retries=INT32_MAX (must be greater than 0), acks=all, queuing.strategy=fifo. Producer instantation will fail if user-supplied configuration is incompatible.
    public var enableIdempotence: Bool = false

    /// Producer queue options.
    public var queue: KafkaConfiguration.QueueOptions = .init()

    /// How many times to retry sending a failing Message. Note: retrying may cause reordering unless enable.idempotence is set to true.
    public var messageSendMaxRetries: Int = 2_147_483_647

    /// Allow automatic topic creation on the broker when producing to non-existent topics.
    /// The broker must also be configured with auto.create.topics.enable=true for this configuration to take effect.
    /// Default value: `true`
    public var allowAutoCreateTopics: Bool = true

    // MARK: - Common Client Config Properties

    /// Client identifier string.
    public var clientID: String = "rdkafka"

    /// Bootstrap broker(s) (host[:port]) for initial connection.
    public var bootstrapServers: [String] = []

    /// Message options.
    public var message: KafkaConfiguration.MessageOptions = .init()

    /// Maximum receive message size for network requests.
    public var receiveMessageMaxBytes: Int = 100_000_000

    /// Maximum number of in-flight requests per broker connection.
    public var maxInFlightRequestsPerConnection: Int = 1_000_000

    /// Maximum time, in milliseconds, that broker metadata can be cached.
    public var metadataMaxAgeMilliseconds: Int = 900_000

    /// Topic metadata options.
    public var topicMetadata: KafkaConfiguration.TopicMetadataOptions = .init()

    /// Topic denylist.
    public var topicDenylist: [String] = []

    /// Debug options.
    public var debug: [KafkaConfiguration.DebugOption] = []

    /// Socket options.
    public var socket: KafkaConfiguration.SocketOptions = .init()

    /// Broker options.
    public var broker: KafkaConfiguration.BrokerOptions = .init()

    /// Reconnect options.
    public var reconnect: KafkaConfiguration.ReconnectOptions = .init()

    /// Security protocol to use (plaintext, ssl, sasl_plaintext, sasl_ssl).
    public var securityProtocol: KafkaConfiguration.SecurityProtocol = .plaintext

    /// SSL options.
    public var ssl: KafkaConfiguration.SSLOptions = .init()

    /// SASL options.
    public var sasl: KafkaConfiguration.SASLOptions = .init()

    public init() {}
}

// MARK: - KafkaProducerConfiguration + Dictionary

extension KafkaProducerConfiguration {
    internal var dictionary: [String: String] {
        var resultDict: [String: String] = [:]

        resultDict["transactional.id"] = self.transactionalID
        resultDict["transaction.timeout.ms"] = String(self.transactionTimeoutMilliseconds)
        resultDict["enable.idempotence"] = String(self.enableIdempotence)
        resultDict["queue.buffering.max.messages"] = String(self.queue.bufferingMaxMessages)
        resultDict["queue.buffering.max.kbytes"] = String(self.queue.bufferingMaxKBytes)
        resultDict["queue.buffering.max.ms"] = String(self.queue.bufferingMaxMilliseconds)
        resultDict["message.send.max.retries"] = String(self.messageSendMaxRetries)
        resultDict["allow.auto.create.topics"] = String(self.allowAutoCreateTopics)

        resultDict["client.id"] = self.clientID
        resultDict["bootstrap.servers"] = self.bootstrapServers.joined(separator: ",")
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
        resultDict["security.protocol"] = self.securityProtocol.description
        resultDict["ssl.key.location"] = self.ssl.keyLocation
        resultDict["ssl.key.password"] = self.ssl.keyPassword
        resultDict["ssl.certificate.location"] = self.ssl.certificateLocation
        resultDict["ssl.ca.location"] = self.ssl.CALocation
        resultDict["ssl.crl.location"] = self.ssl.CRLLocation
        resultDict["ssl.keystore.location"] = self.ssl.keystoreLocation
        resultDict["ssl.keystore.password"] = self.ssl.keystorePassword
        if let saslMechnism = sasl.mechanism {
            resultDict["sasl.mechanism"] = saslMechnism.description
        }
        if let saslUsername = sasl.username {
            resultDict["sasl.username"] = saslUsername
        }
        if let saslPassword = sasl.password {
            resultDict["sasl.password"] = saslPassword
        }

        return resultDict
    }
}

// MARK: - KafkaProducerConfiguration + Hashable

extension KafkaProducerConfiguration: Hashable {}

// MARK: - KafkaProducerConfiguration + Sendable

extension KafkaProducerConfiguration: Sendable {}

// MARK: - KafkaSharedConfiguration + Producer Additions

extension KafkaConfiguration {
    /// Producer queue options.
    public struct QueueOptions: Sendable, Hashable {
        /// Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions. A value of 0 disables this limit.
        public var bufferingMaxMessages: Int = 100_000

        /// Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions. This property has higher priority than queue.buffering.max.messages.
        public var bufferingMaxKBytes: Int = 1_048_576

        /// Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
        public var bufferingMaxMilliseconds: Int = 5
    }
}
