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

internal protocol KafkaProducerSharedProperties: Sendable, Hashable {
    // MARK: - SwiftKafka-specific Config properties

    /// The time between two consecutive polls.
    /// Effectively controls the rate at which incoming events are consumed.
    /// Default: `.milliseconds(100)`
    var pollInterval: Duration { get }

    /// Maximum timeout for flushing outstanding produce requests when the ``KakfaProducer`` is shutting down.
    /// Default: `10000`
    var flushTimeoutMilliseconds: Int { get }

    // MARK: - Producer-specific Config Properties

    /// When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: max.in.flight.requests.per.connection=5 (must be less than or equal to 5), retries=INT32_MAX (must be greater than 0), acks=all, queuing.strategy=fifo. Producer instantation will fail if user-supplied configuration is incompatible.
    /// Default: `false`
    var enableIdempotence: Bool { get }

    /// Producer queue options.
    var queue: KafkaConfiguration.QueueOptions { get }

    /// How many times to retry sending a failing Message. Note: retrying may cause reordering unless enable.idempotence is set to true.
    /// Default: `2_147_483_647`
    var messageSendMaxRetries: Int { get }

    /// Allow automatic topic creation on the broker when producing to non-existent topics.
    /// The broker must also be configured with auto.create.topics.enable=true for this configuration to take effect.
    /// Default: `true`
    var allowAutoCreateTopics: Bool { get }

    // MARK: - Common Client Config Properties

    /// Client identifier.
    /// Default: `"rdkafka"`
    var clientID: String { get }

    /// Initial list of brokers.
    /// Default: `[]`
    var bootstrapServers: [KafkaConfiguration.Broker] { get }

    /// Message options.
    var message: KafkaConfiguration.MessageOptions { get }

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hickups. This value must be at least fetch.max.bytes + 512 to allow for protocol overhead; the value is adjusted automatically unless the configuration property is explicitly set.
    /// Default: `100_000_000`
    var receiveMessageMaxBytes: Int { get }

    /// Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one.
    /// Default: `1_000_000`
    var maxInFlightRequestsPerConnection: Int { get }

    /// Metadata cache max age.
    /// Default: `900_000`
    var metadataMaxAgeMilliseconds: Int { get }

    /// Topic metadata options.
    var topicMetadata: KafkaConfiguration.TopicMetadataOptions { get }

    /// Topic denylist.
    /// Default: `[]`
    var topicDenylist: [String] { get }

    /// Debug options.
    /// Default: `[]`
    var debug: [KafkaConfiguration.DebugOption] { get }

    /// Socket options.
    var socket: KafkaConfiguration.SocketOptions { get }

    /// Broker options.
    var broker: KafkaConfiguration.BrokerOptions { get }

    /// Reconnect options.
    var reconnect: KafkaConfiguration.ReconnectOptions { get }

    /// Security protocol to use (plaintext, ssl, sasl_plaintext, sasl_ssl).
    /// Default: `.plaintext`
    var securityProtocol: KafkaConfiguration.SecurityProtocol { get }

    var dictionary: [String: String] { get }
}

extension KafkaProducerSharedProperties {
    internal var sharedPropsDictionary: [String: String] {
        var resultDict: [String: String] = [:]

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
