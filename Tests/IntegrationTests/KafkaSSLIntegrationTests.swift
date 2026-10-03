//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2026 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@_spi(Internal) import Kafka
import ServiceLifecycle
import Testing

import class Foundation.ProcessInfo
import struct Foundation.UUID

// Secure-listener configuration, supplied by the docker-compose `client` service. The whole
// suite is skipped when KAFKA_SSL_PORT is absent (e.g. a plaintext-only local `swift test`).
private let kafkaHost = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
private let sslPort = ProcessInfo.processInfo.environment["KAFKA_SSL_PORT"]
private let saslSSLPort = ProcessInfo.processInfo.environment["KAFKA_SASL_SSL_PORT"]
private let caLocation = ProcessInfo.processInfo.environment["KAFKA_SSL_CA_LOCATION"]
private let clientCertLocation = ProcessInfo.processInfo.environment["KAFKA_SSL_CLIENT_CERT_LOCATION"]
private let clientKeyLocation = ProcessInfo.processInfo.environment["KAFKA_SSL_CLIENT_KEY_LOCATION"]
private let scramUser = ProcessInfo.processInfo.environment["KAFKA_SCRAM_USER"]
private let scramPassword = ProcessInfo.processInfo.environment["KAFKA_SCRAM_PASSWORD"]
private let authzUser = ProcessInfo.processInfo.environment["KAFKA_AUTHZ_USER"]
private let authzPassword = ProcessInfo.processInfo.environment["KAFKA_AUTHZ_PASSWORD"]
private let authzAllowedTopic = ProcessInfo.processInfo.environment["KAFKA_AUTHZ_ALLOWED_TOPIC"] ?? "authz-allowed"
private let authzDeniedTopic = ProcessInfo.processInfo.environment["KAFKA_AUTHZ_DENIED_TOPIC"] ?? "authz-denied"
private let sslTestsEnabled = sslPort != nil

/// Applies a security protocol plus its TLS/SASL material to a producer or consumer config.
private struct SecurityProfile {
    var port: String
    var securityProtocol: KafkaConfig.SecurityProtocol
    var clientCertLocation: String?
    var clientKeyLocation: String?
    var saslMechanism: String?
    var saslUsername: String?
    var saslPassword: String?

    init(
        port: String,
        securityProtocol: KafkaConfig.SecurityProtocol,
        clientCertLocation: String? = nil,
        clientKeyLocation: String? = nil,
        saslMechanism: String? = nil,
        saslUsername: String? = nil,
        saslPassword: String? = nil
    ) {
        self.port = port
        self.securityProtocol = securityProtocol
        self.clientCertLocation = clientCertLocation
        self.clientKeyLocation = clientKeyLocation
        self.saslMechanism = saslMechanism
        self.saslUsername = saslUsername
        self.saslPassword = saslPassword
    }

    func apply(to config: inout KafkaProducerConfig) {
        config.bootstrapServers = ["\(kafkaHost):\(self.port)"]
        config.brokerAddressFamily = .v4
        config.securityProtocol = self.securityProtocol
        config.sslCaLocation = caLocation  // every secure profile verifies the broker against the test CA
        config.sslCertificateLocation = self.clientCertLocation
        config.sslKeyLocation = self.clientKeyLocation
        config.saslMechanism = self.saslMechanism
        config.saslUsername = self.saslUsername
        config.saslPassword = self.saslPassword
    }

    func apply(to config: inout KafkaConsumerConfig) {
        config.bootstrapServers = ["\(kafkaHost):\(self.port)"]
        config.brokerAddressFamily = .v4
        config.securityProtocol = self.securityProtocol
        config.sslCaLocation = caLocation  // every secure profile verifies the broker against the test CA
        config.sslCertificateLocation = self.clientCertLocation
        config.sslKeyLocation = self.clientKeyLocation
        config.saslMechanism = self.saslMechanism
        config.saslUsername = self.saslUsername
        config.saslPassword = self.saslPassword
    }
}

@Suite(.timeLimit(.minutes(5)), .serialized, .enabled(if: sslTestsEnabled))
struct KafkaSecurityIntegrationTests {
    // MARK: - Transport encryption (TLS handshake)

    @Test func sslTransportProducesAndConsumes() async throws {
        let profile = SecurityProfile(port: sslPort!, securityProtocol: .ssl)
        try await withTestTopic { topic in
            try await Self.produce(profile: profile, topic: topic, count: 5)
            try await Self.consumeAndVerify(profile: profile, topic: topic, expected: 5)
        }
    }

    // MARK: - Authentication (authN)

    @Test func mutualTLSProducesAndConsumes() async throws {
        let profile = SecurityProfile(
            port: sslPort!,
            securityProtocol: .ssl,
            clientCertLocation: clientCertLocation,
            clientKeyLocation: clientKeyLocation
        )
        try await withTestTopic { topic in
            try await Self.produce(profile: profile, topic: topic, count: 5)
            try await Self.consumeAndVerify(profile: profile, topic: topic, expected: 5)
        }
    }

    @Test func saslScramOverTLSProducesAndConsumes() async throws {
        let profile = SecurityProfile(
            port: saslSSLPort!,
            securityProtocol: .sasl_ssl,
            saslMechanism: "SCRAM-SHA-256",
            saslUsername: scramUser,
            saslPassword: scramPassword
        )
        try await withTestTopic { topic in
            try await Self.produce(profile: profile, topic: topic, count: 5)
            try await Self.consumeAndVerify(profile: profile, topic: topic, expected: 5)
        }
    }

    // MARK: - Authorization (authZ)

    @Test func aclAllowsAuthorizedTopic() async throws {
        let profile = SecurityProfile(
            port: saslSSLPort!,
            securityProtocol: .sasl_ssl,
            saslMechanism: "SCRAM-SHA-256",
            saslUsername: authzUser,
            saslPassword: authzPassword
        )
        // `authzuser` has Write access to this topic, so production succeeds.
        try await Self.produce(profile: profile, topic: KafkaTopic(rawValue: authzAllowedTopic), count: 3)
    }

    @Test func aclDeniesUnauthorizedTopic() async throws {
        let profile = SecurityProfile(
            port: saslSSLPort!,
            securityProtocol: .sasl_ssl,
            saslMechanism: "SCRAM-SHA-256",
            saslUsername: authzUser,
            saslPassword: authzPassword
        )
        // `authzuser` has no ACL on this topic; the broker must reject the write.
        try await Self.expectProduceDenied(profile: profile, topic: KafkaTopic(rawValue: authzDeniedTopic))
    }

    // MARK: - Helpers

    private static func produce(profile: SecurityProfile, topic: KafkaTopic, count: UInt) async throws {
        var config = KafkaProducerConfig()
        profile.apply(to: &config)

        let messages = _createTestMessages(topic: topic, count: count)
        let (producer, events) = try KafkaProducer.makeProducer(config: config)
        let serviceGroup = ServiceGroup(
            configuration: ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await serviceGroup.run() }
            group.addTask {
                try await _sendAndAcknowledgeMessages(producer: producer, events: events, messages: messages)
            }
            try await group.next()
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    private static func consumeAndVerify(profile: SecurityProfile, topic: KafkaTopic, expected: UInt) async throws {
        var config = KafkaConsumerConfig()
        config.consumptionStrategy = .group(id: UUID().uuidString, topics: [topic])
        config.autoOffsetReset = .beginning
        profile.apply(to: &config)

        let (consumer, messages, _) = try KafkaConsumer.makeConsumer(config: config)
        let serviceGroup = ServiceGroup(
            configuration: ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await serviceGroup.run() }
            group.addTask {
                var consumed = 0
                for try await _ in messages {
                    consumed += 1
                    if consumed >= Int(expected) {
                        break
                    }
                }
                #expect(consumed == Int(expected))
            }
            try await group.next()
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    /// Sends one message and asserts the broker rejects it with an authorization error, surfaced
    /// as a failed delivery report (or producer error event) rather than an acknowledgement.
    private static func expectProduceDenied(profile: SecurityProfile, topic: KafkaTopic) async throws {
        var config = KafkaProducerConfig()
        profile.apply(to: &config)
        config.messageTimeoutMs = 20000  // fail fast instead of retrying up to the suite time limit

        let message = _createTestMessages(topic: topic, count: 1)[0]
        let (producer, events) = try KafkaProducer.makeProducer(config: config)
        let serviceGroup = ServiceGroup(
            configuration: ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await serviceGroup.run() }
            group.addTask {
                _ = try producer.send(message)
                for await event in events {
                    switch event {
                    case .deliveryReports(let reports):
                        for report in reports {
                            switch report.status {
                            case .failure(let error):
                                #expect(
                                    error.description.lowercased().contains("authorization"),
                                    "Expected an authorization failure, got: \(error.description)"
                                )
                                return
                            case .acknowledged:
                                Issue.record("Write to unauthorized topic was acknowledged; authZ not enforced")
                                return
                            }
                        }
                    case .error(let error):
                        if error.description.lowercased().contains("authorization") {
                            return
                        }
                    }
                }
            }
            try await group.next()
            await serviceGroup.triggerGracefulShutdown()
        }
    }
}
