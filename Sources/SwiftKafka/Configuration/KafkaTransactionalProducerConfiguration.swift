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

// FIXME: should we really duplicate `KafkaProducerConfiguration`
// FIXME: after public api updated?
public struct KafkaTransactionalProducerConfiguration {
    // MARK: - SwiftKafka-specific Config properties

    /// The time between two consecutive polls.
    /// Effectively controls the rate at which incoming events are consumed.
    /// Default: `.milliseconds(100)`
    public var pollInterval: Duration = .milliseconds(100)

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
    internal let enableIdempotence: Bool = true

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
    public var maxInFlightRequestsPerConnection: Int = 5 {
        didSet {
            precondition(
                0...5 ~= self.maxInFlightRequestsPerConnection,
                "Transactional producer can have no more than 5 in flight requests"
            )
        }
    }

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

    // TODO: add Docc
    var transactionalId: String
    var transactionsTimeout: Duration = .seconds(60) // equal to socket TODO: add didSet

    public init(transactionalId: String) {
        self.transactionalId = transactionalId
    }
}

// MARK: - KafkaProducerConfiguration + Hashable

extension KafkaTransactionalProducerConfiguration: Hashable {}

// MARK: - KafkaProducerConfiguration + Sendable

extension KafkaTransactionalProducerConfiguration: Sendable {}

extension KafkaTransactionalProducerConfiguration: KafkaProducerSharedProperties {
    internal var dictionary: [String: String] {
        var resultDict: [String: String] = sharedPropsDictionary
        resultDict["transactional.id"] = self.transactionalId
        resultDict["transaction.timeout.ms"] = String(self.transactionsTimeout.totalMilliseconds)
        return resultDict
    }
}
