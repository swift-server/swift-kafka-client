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

public struct KafkaProducerConfiguration: Hashable, Equatable {
    var dictionary: [String: String] = [:]

    // MARK: - Producer-specific Config Properties

    /// Enables the transactional producer. The transactional.id is used to identify the same transactional producer instance across process restarts. It allows the producer to guarantee that transactions corresponding to earlier instances of the same producer have been finalized prior to starting any new transactions, and that any zombie instances are fenced off. If no transactional.id is provided, then the producer is limited to idempotent delivery (if enable.idempotence is set). Requires broker version >= 0.11.0.
    public var transactionalID: String { // TODO: Use optional or empty string for "no value"?
        get { self.dictionary["transactional.id"] ?? "" }
        set { self.dictionary["transactional.id"] = newValue }
    }

    /// The maximum amount of time in milliseconds that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction. If this value is larger than the transaction.max.timeout.ms setting in the broker, the init_transactions() call will fail with ERR_INVALID_TRANSACTION_TIMEOUT. The transaction timeout automatically adjusts message.timeout.ms and socket.timeout.ms, unless explicitly configured in which case they must not exceed the transaction timeout (socket.timeout.ms must be at least 100ms lower than transaction.timeout.ms). This is also the default timeout value if no timeout (-1) is supplied to the transactional API methods.
    public var transactionTimeoutMs: UInt {
        get { self.dictionary.getUInt("transaction.timeout.ms") ?? 60000 }
        set { self.dictionary["transaction.timeout.ms"] = String(newValue) }
    }

    /// When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: max.in.flight.requests.per.connection=5 (must be less than or equal to 5), retries=INT32_MAX (must be greater than 0), acks=all, queuing.strategy=fifo. Producer instantation will fail if user-supplied configuration is incompatible.
    public var enableIdempotence: Bool {
        get { self.dictionary.getBool("enable.idempotence") ?? false }
        set { self.dictionary["enable.idempotence"] = String(newValue) }
    }

    /// Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions. A value of 0 disables this limit.
    public var queueBufferingMaxMessages: UInt {
        get { self.dictionary.getUInt("queue.buffering.max.messages") ?? 100_000 }
        set { self.dictionary["queue.buffering.max.messages"] = String(newValue) }
    }

    /// Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions. This property has higher priority than queue.buffering.max.messages.
    public var queueBufferingMaxKBytes: UInt {
        get { self.dictionary.getUInt("queue.buffering.max.kbytes") ?? 1_048_576 }
        set { self.dictionary["queue.buffering.max.kbytes"] = String(newValue) }
    }

    /// Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
    public var queueBufferingMaxMs: UInt {
        get { self.dictionary.getUInt("queue.buffering.max.ms") ?? 5 }
        set { self.dictionary["queue.buffering.max.ms"] = String(newValue) }
    }

    /// How many times to retry sending a failing Message. Note: retrying may cause reordering unless enable.idempotence is set to true.
    public var messageSendMaxRetries: UInt {
        get { self.dictionary.getUInt("message.send.max.retries") ?? 2_147_483_647 }
        set { self.dictionary["message.send.max.retries"] = String(newValue) }
    }

    /// Allow automatic topic creation on the broker when producing to non-existent topics.
    /// The broker must also be configured with auto.create.topics.enable=true for this configuration to take effect.
    /// Default value: `true`
    public var allowAutoCreateTopics: Bool {
        get { self.dictionary.getBool("allow.auto.create.topics") ?? true }
        set { self.dictionary["allow.auto.create.topics"] = String(newValue) }
    }

    // MARK: - Common Client Config Properties

    /// Client identifier.
    public var clientID: String {
        get { self.dictionary["client.id"] ?? "rdkafka" }
        set { self.dictionary["client.id"] = newValue }
    }

    /// Initial list of brokers as a CSV list of broker host or host:port.
    public var bootstrapServers: [String] {
        get { self.dictionary["bootstrap.servers"]?.components(separatedBy: ",") ?? [] }
        set { self.dictionary["bootstrap.servers"] = newValue.joined(separator: ",") }
    }

    /// Maximum Kafka protocol request message size. Due to differing framing overhead between protocol versions the producer is unable to reliably enforce a strict max message limit at produce time and may exceed the maximum size by one message in protocol ProduceRequests, the broker will enforce the the topic's max.message.bytes limit (see Apache Kafka documentation).
    public var messageMaxBytes: UInt {
        get { self.dictionary.getUInt("message.max.bytes") ?? 1_000_000 }
        set { self.dictionary["message.max.bytes"] = String(newValue) }
    }

    /// Maximum size for message to be copied to buffer. Messages larger than this will be passed by reference (zero-copy) at the expense of larger iovecs.
    public var messageCopyMaxBytes: UInt {
        get { self.dictionary.getUInt("message.copy.max.bytes") ?? 65535 }
        set { self.dictionary["message.copy.max.bytes"] = String(newValue) }
    }

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hickups. This value must be at least fetch.max.bytes + 512 to allow for protocol overhead; the value is adjusted automatically unless the configuration property is explicitly set.
    public var receiveMessageMaxBytes: UInt {
        get { self.dictionary.getUInt("receive.message.max.bytes") ?? 100_000_000 }
        set { self.dictionary["receive.message.max.bytes"] = String(newValue) }
    }

    /// Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one.
    public var maxInFlightRequestsPerConnection: UInt {
        get { self.dictionary.getUInt("max.in.flight.requests.per.connection") ?? 1_000_000 }
        set { self.dictionary["max.in.flight.requests.per.connection"] = String(newValue) }
    }

    /// Metadata cache max age.
    public var metadataMaxAgeMs: UInt {
        get { self.dictionary.getUInt("metadata.max.age.ms") ?? 900_000 }
        set { self.dictionary["metadata.max.age.ms"] = String(newValue) }
    }

    /// Period of time in milliseconds at which topic and broker metadata is refreshed in order to proactively discover any new brokers, topics, partitions or partition leader changes. Use -1 to disable the intervalled refresh (not recommended). If there are no locally referenced topics (no topic objects created, no messages produced, no subscription or no assignment) then only the broker list will be refreshed every interval but no more often than every 10s.
    public var topicMetadataRefreshIntervalMs: Int {
        get { self.dictionary.getInt("topic.metadata.refresh.interval.ms") ?? 300_000 }
        set { self.dictionary["topic.metadata.refresh.interval.ms"] = String(newValue) }
    }

    /// When a topic loses its leader a new metadata request will be enqueued with this initial interval, exponentially increasing until the topic metadata has been refreshed. This is used to recover quickly from transitioning leader brokers.
    public var topicMetadataRefreshFastIntervalMs: UInt {
        get { self.dictionary.getUInt("topic.metadata.refresh.fast.interval.ms") ?? 250 }
        set { self.dictionary["topic.metadata.refresh.fast.interval.ms"] = String(newValue) }
    }

    /// Sparse metadata requests (consumes less network bandwidth).
    public var topicMetadataRefreshSparse: Bool {
        get { self.dictionary.getBool("topic.metadata.refresh.sparse") ?? true }
        set { self.dictionary["topic.metadata.refresh.sparse"] = newValue.description }
    }

    /// Apache Kafka topic creation is asynchronous and it takes some time for a new topic to propagate throughout the cluster to all brokers. If a client requests topic metadata after manual topic creation but before the topic has been fully propagated to the broker the client is requesting metadata from, the topic will seem to be non-existent and the client will mark the topic as such, failing queued produced messages with ERR__UNKNOWN_TOPIC. This setting delays marking a topic as non-existent until the configured propagation max time has passed. The maximum propagation time is calculated from the time the topic is first referenced in the client, e.g., on `send()`.
    public var topicMetadataPropagationMaxMs: UInt {
        get { self.dictionary.getUInt("topic.metadata.propagation.max.ms") ?? 30000 }
        set { self.dictionary["topic.metadata.propagation.max.ms"] = String(newValue) }
    }

    /// Topic denylist, a comma-separated list of regular expressions for matching topic names that should be ignored in broker metadata information as if the topics did not exist.
    public var topicDenylist: [String] {
        get { self.dictionary["topic.blacklist"]?.components(separatedBy: ",") ?? [] }
        set { self.dictionary["topic.blacklist"] = newValue.joined(separator: ",") }
    }

    /// A comma-separated list of debug contexts to enable. Detailed Producer debugging: broker,topic,msg. Consumer: consumer,cgrp,topic,fetch.
    public var debug: [KafkaSharedConfiguration.DebugOption] {
        get { self.getDebugOptions() }
        set {
            if !newValue.isEmpty {
                self.dictionary["debug"] = newValue.map(\.description).joined(separator: ",")
            }
        }
    }

    /// Default timeout for network requests. Producer: ProduceRequests will use the lesser value of socket.timeout.ms and remaining message.timeout.ms for the first message in the batch. Consumer: FetchRequests will use fetch.wait.max.ms + socket.timeout.ms.
    public var socketTimeoutMs: UInt {
        get { self.dictionary.getUInt("socket.timeout.ms") ?? 60000 }
        set { self.dictionary["socket.timeout.ms"] = String(newValue) }
    }

    /// Broker socket send buffer size. System default is used if 0.
    public var socketSendBufferBytes: UInt {
        get { self.dictionary.getUInt("socket.send.buffer.bytes") ?? 0 }
        set { self.dictionary["socket.send.buffer.bytes"] = String(newValue) }
    }

    /// Broker socket receive buffer size. System default is used if 0.
    public var socketReceiveBufferBytes: UInt {
        get { self.dictionary.getUInt("socket.receive.buffer.bytes") ?? 0 }
        set { self.dictionary["socket.receive.buffer.bytes"] = String(newValue) }
    }

    /// Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets.
    public var socketKeepaliveEnable: Bool {
        get { self.dictionary.getBool("socket.keepalive.enable") ?? false }
        set { self.dictionary["socket.keepalive.enable"] = String(newValue) }
    }

    /// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
    public var socketNagleDisable: Bool {
        get { self.dictionary.getBool("socket.nagle.disable") ?? false }
        set { self.dictionary["socket.nagle.disable"] = String(newValue) }
    }

    /// Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. WARNING: It is highly recommended to leave this setting at its default value of 1 to avoid the client and broker to become desynchronized in case of request timeouts. NOTE: The connection is automatically re-established.
    public var socketMaxFails: UInt {
        get { self.dictionary.getUInt("socket.max.fails") ?? 1 }
        set { self.dictionary["socket.max.fails"] = String(newValue) }
    }

    /// Maximum time allowed for broker connection setup (TCP connection setup as well SSL and SASL handshake). If the connection to the broker is not fully functional after this the connection will be closed and retried.
    public var socketConnectionSetupTimeoutMs: UInt {
        get { self.dictionary.getUInt("socket.connection.setup.timeout.ms") ?? 30000 }
        set { self.dictionary["socket.connection.setup.timeout.ms"] = String(newValue) }
    }

    /// How long to cache the broker address resolving results (milliseconds).
    public var brokerAddressTTL: UInt {
        get { self.dictionary.getUInt("broker.address.ttl") ?? 1000 }
        set { self.dictionary["broker.address.ttl"] = String(newValue) }
    }

    /// Allowed broker ``KafkaSharedConfiguration/IPAddressFamily``.
    public var brokerAddressFamily: KafkaSharedConfiguration.IPAddressFamily {
        get { self.getIPAddressFamily() ?? .any }
        set { self.dictionary["broker.address.family"] = newValue.description }
    }

    /// The initial time to wait before reconnecting to a broker after the connection has been closed. The time is increased exponentially until reconnect.backoff.max.ms is reached. -25% to +50% jitter is applied to each reconnect backoff. A value of 0 disables the backoff and reconnects immediately.
    public var reconnectBackoffMs: UInt {
        get { self.dictionary.getUInt("reconnect.backoff.ms") ?? 100 }
        set { self.dictionary["reconnect.backoff.ms"] = String(newValue) }
    }

    /// The maximum time to wait before reconnecting to a broker after the connection has been closed.
    public var reconnectBackoffMaxMs: UInt {
        get { self.dictionary.getUInt("reconnect.backoff.max.ms") ?? 10000 }
        set { self.dictionary["reconnect.backoff.max.ms"] = String(newValue) }
    }

    /// ``KafkaSharedConfiguration/SecurityProtocol`` used to communicate with brokers.
    public var securityProtocol: KafkaSharedConfiguration.SecurityProtocol {
        get { self.getSecurityProtocol() ?? .plaintext }
        set { self.dictionary["security.protocol"] = newValue.description }
    }

    /// Path to client's private key (PEM) used for authentication.
    public var sslKeyLocation: String {
        get { self.dictionary["ssl.key.location"] ?? "" }
        set { self.dictionary["ssl.key.location"] = newValue }
    }

    /// Private key passphrase (for use with ssl.key.location).
    public var sslKeyPassword: String {
        get { self.dictionary["ssl.key.password"] ?? "" }
        set { self.dictionary["ssl.key.password"] = newValue }
    }

    /// Path to client's public key (PEM) used for authentication.
    public var sslCertificateLocation: String {
        get { self.dictionary["ssl.certificate.location"] ?? "" }
        set { self.dictionary["ssl.certificate.location"] = newValue }
    }

    /// File or directory path to CA certificate(s) for verifying the broker's key. Defaults: On Windows the system's CA certificates are automatically looked up in the Windows Root certificate store. On Mac OSX this configuration defaults to probe. It is recommended to install openssl using Homebrew, to provide CA certificates. On Linux install the distribution's ca-certificates package. If OpenSSL is statically linked or ssl.ca.location is set to probe a list of standard paths will be probed and the first one found will be used as the default CA certificate location path. If OpenSSL is dynamically linked the OpenSSL library's default path will be used (see OPENSSLDIR in openssl version -a).
    public var sslCALocation: String {
        get { self.dictionary["ssl.ca.location"] ?? "" }
        set { self.dictionary["ssl.ca.location"] = newValue }
    }

    /// Path to CRL for verifying broker's certificate validity.
    public var sslCRLLocation: String {
        get { self.dictionary["ssl.crl.location"] ?? "" }
        set { self.dictionary["ssl.crl.location"] = newValue }
    }

    /// Path to client's keystore (PKCS#12) used for authentication.
    public var sslKeystoreLocation: String {
        get { self.dictionary["ssl.keystore.location"] ?? "" }
        set { self.dictionary["ssl.keystore.location"] = newValue }
    }

    /// Client's keystore (PKCS#12) password.
    public var sslKeystorePassword: String {
        get { self.dictionary["ssl.keystore.password"] ?? "" }
        set { self.dictionary["ssl.keystore.password"] = newValue }
    }

    /// SASL mechanism to use for authentication.
    public var saslMechanism: KafkaSharedConfiguration.SASLMechanism? {
        get { self.getSASLMechanism() }
        set {
            if let newValue {
                self.dictionary["sasl.mechanism"] = newValue.description
            }
        }
    }

    /// SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms.
    public var saslUsername: String? {
        get { self.dictionary["sasl.username"] }
        set {
            if let newValue {
                self.dictionary["sasl.username"] = newValue
            }
        }
    }

    /// SASL password for use with the PLAIN and SASL-SCRAM-.. mechanisms.
    public var saslPassword: String? {
        get { self.dictionary["sasl.password"] }
        set {
            if let newValue {
                self.dictionary["sasl.password"] = newValue
            }
        }
    }

    public init(
        transactionalID: String = "",
        transactionalTimeoutMs: UInt = 60000,
        enableIdempotence: Bool = false,
        queueBufferingMaxMessages: UInt = 100_000,
        queueBufferingMaxKBytes: UInt = 1_048_576,
        queueBufferingMaxMs: UInt = 5,
        messageSendMaxRetries: UInt = 2_147_483_647,
        allowAutoCreateTopics: Bool = true,
        clientID: String = "rdkafka",
        bootstrapServers: [String] = [],
        messageMaxBytes: UInt = 1_000_000,
        messageCopyMaxBytes: UInt = 65535,
        receiveMessageMaxBytes: UInt = 100_000_000,
        maxInFlightRequestsPerConnection: UInt = 1_000_000,
        metadataMaxAgeMs: UInt = 900_000,
        topicMetadataRefreshIntervalMs: Int = 300_000,
        topicMetadataRefreshFastIntervalMs: UInt = 250,
        topicMetadataRefreshSparse: Bool = true,
        topicMetadataPropagationMaxMs: UInt = 30000,
        topicDenylist: [String] = [],
        debug: [KafkaSharedConfiguration.DebugOption] = [],
        socketTimeoutMs: UInt = 60000,
        socketSendBufferBytes: UInt = 0,
        socketReceiveBufferBytes: UInt = 0,
        socketKeepaliveEnable: Bool = false,
        socketNagleDisable: Bool = false,
        socketMaxFails: UInt = 1,
        socketConnectionSetupTimeoutMs: UInt = 30000,
        brokerAddressTTL: UInt = 1000,
        brokerAddressFamily: KafkaSharedConfiguration.IPAddressFamily = .any,
        reconnectBackoffMs: UInt = 100,
        reconnectBackoffMaxMs: UInt = 10000,
        securityProtocol: KafkaSharedConfiguration.SecurityProtocol = .plaintext,
        sslKeyLocation: String = "",
        sslKeyPassword: String = "",
        sslCertificateLocation: String = "",
        sslCALocation: String = "",
        sslCRLLocation: String = "",
        sslKeystoreLocation: String = "",
        sslKeystorePassword: String = "",
        saslMechanism: KafkaSharedConfiguration.SASLMechanism? = nil,
        saslUsername: String? = nil,
        saslPassword: String? = nil
    ) {
        self.transactionalID = transactionalID
        self.transactionTimeoutMs = transactionalTimeoutMs
        self.enableIdempotence = enableIdempotence
        self.queueBufferingMaxMessages = queueBufferingMaxMessages
        self.queueBufferingMaxKBytes = queueBufferingMaxKBytes
        self.queueBufferingMaxMs = queueBufferingMaxMs
        self.messageSendMaxRetries = messageSendMaxRetries
        self.allowAutoCreateTopics = allowAutoCreateTopics
        self.clientID = clientID
        self.bootstrapServers = bootstrapServers
        self.messageMaxBytes = messageMaxBytes
        self.messageCopyMaxBytes = messageCopyMaxBytes
        self.receiveMessageMaxBytes = receiveMessageMaxBytes
        self.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection
        self.metadataMaxAgeMs = metadataMaxAgeMs
        self.topicMetadataRefreshIntervalMs = topicMetadataRefreshIntervalMs
        self.topicMetadataRefreshFastIntervalMs = topicMetadataRefreshFastIntervalMs
        self.topicMetadataRefreshSparse = topicMetadataRefreshSparse
        self.topicMetadataPropagationMaxMs = topicMetadataPropagationMaxMs
        self.topicDenylist = topicDenylist
        self.debug = debug
        self.socketTimeoutMs = socketTimeoutMs
        self.socketSendBufferBytes = socketSendBufferBytes
        self.socketReceiveBufferBytes = socketReceiveBufferBytes
        self.socketKeepaliveEnable = socketKeepaliveEnable
        self.socketNagleDisable = socketNagleDisable
        self.socketMaxFails = socketMaxFails
        self.socketConnectionSetupTimeoutMs = socketConnectionSetupTimeoutMs
        self.brokerAddressTTL = brokerAddressTTL
        self.brokerAddressFamily = brokerAddressFamily
        self.reconnectBackoffMs = reconnectBackoffMs
        self.reconnectBackoffMaxMs = reconnectBackoffMaxMs
        self.securityProtocol = securityProtocol
        self.sslKeyLocation = sslKeyLocation
        self.sslKeyPassword = sslKeyPassword
        self.sslCertificateLocation = sslCertificateLocation
        self.sslCALocation = sslCALocation
        self.sslCRLLocation = sslCRLLocation
        self.sslKeystoreLocation = sslKeystoreLocation
        self.sslKeystorePassword = sslKeystorePassword
        self.saslMechanism = saslMechanism
        self.saslUsername = saslUsername
        self.saslPassword = saslPassword
    }

    // MARK: - Helpers

    func getDebugOptions() -> [KafkaSharedConfiguration.DebugOption] {
        guard let options = dictionary["debug"] else {
            return []
        }
        return options.components(separatedBy: ",")
            .map { KafkaSharedConfiguration.DebugOption(description: $0) }
    }

    func getIPAddressFamily() -> KafkaSharedConfiguration.IPAddressFamily? {
        guard let value = dictionary["broker.address.family"] else {
            return nil
        }
        return KafkaSharedConfiguration.IPAddressFamily(description: value)
    }

    func getSecurityProtocol() -> KafkaSharedConfiguration.SecurityProtocol? {
        guard let value = dictionary["security.protocol"] else {
            return nil
        }
        return KafkaSharedConfiguration.SecurityProtocol(description: value)
    }

    func getSASLMechanism() -> KafkaSharedConfiguration.SASLMechanism? {
        guard let value = dictionary["sasl.mechanism"] else {
            return nil
        }
        return KafkaSharedConfiguration.SASLMechanism(description: value)
    }
}
