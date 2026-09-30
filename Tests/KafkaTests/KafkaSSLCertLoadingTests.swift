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

import Foundation
@_spi(Internal) import Kafka
import Logging
import Testing

@testable import Kafka

/// Verifies that the vendored librdkafka can load TLS material through whichever SSL/crypto
/// backend it is linked against (system OpenSSL, or the vendored CNIOBoringSSL used for the
/// static-Linux build).
///
/// librdkafka parses and installs certificates eagerly inside `rd_kafka_new`
/// (`rd_kafka_ssl_ctx_init` → `rd_kafka_ssl_set_certs`), so creating the client is enough to
/// exercise the backend: a successful `makeClient` proves the material parsed, and a malformed
/// input proves the parse path actually runs. No broker connection is involved.
@Suite struct KafkaSSLCertLoadingTests {
    private static let events: [RDKafkaEvent] = [.log, .deliveryReport, .error]

    private func makeSSLConfig() -> KafkaProducerConfig {
        var config = KafkaProducerConfig()
        config.bootstrapServers = ["localhost:9092"]
        config.securityProtocol = .ssl
        return config
    }

    // MARK: - PEM path (PEM_read_bio_X509 / SSL_CTX_use_certificate / SSL_CTX_use_PrivateKey)

    @Test func loadsPEMTrustRootAndClientIdentity() throws {
        var config = self.makeSSLConfig()
        config.sslCaPem = Self.caCertificatePEM
        config.sslCertificatePem = Self.clientCertificatePEM
        config.sslKeyPem = Self.clientPrivateKeyPEM

        // Succeeds only if the backend parsed the CA, the client certificate, and the private key.
        _ = try RDKafkaClient.makeClient(
            type: .producer,
            configDictionary: config.config,
            events: Self.events,
            logger: .kafkaTest
        )
    }

    @Test func rejectsMalformedCertificatePEM() throws {
        var config = self.makeSSLConfig()
        config.sslCaPem = Self.caCertificatePEM
        config.sslCertificatePem = """
            -----BEGIN CERTIFICATE-----
            bm90LWEtdmFsaWQtY2VydGlmaWNhdGU=
            -----END CERTIFICATE-----
            """
        config.sslKeyPem = Self.clientPrivateKeyPEM

        // Proves the certificate parse path runs: a malformed PEM must be rejected.
        #expect(throws: KafkaError.self) {
            _ = try RDKafkaClient.makeClient(
                type: .producer,
                configDictionary: config.config,
                events: Self.events,
                logger: .kafkaTest
            )
        }
    }

    // MARK: - PKCS#12 path (d2i_PKCS12_bio / PKCS12_parse / SSL_CTX_use_cert_and_key)

    @Test func loadsPKCS12Keystore() throws {
        let keystorePath = try Self.writeTemporaryKeystore()
        defer { try? FileManager.default.removeItem(atPath: keystorePath) }

        var config = self.makeSSLConfig()
        config.sslCaPem = Self.caCertificatePEM
        config.sslKeystoreLocation = keystorePath
        config.sslKeystorePassword = Self.keystorePassword

        // Succeeds only if the backend read the PKCS#12 file and installed the identity via
        // SSL_CTX_use_cert_and_key (the function we shim when linking CNIOBoringSSL).
        _ = try RDKafkaClient.makeClient(
            type: .producer,
            configDictionary: config.config,
            events: Self.events,
            logger: .kafkaTest
        )
    }

    @Test func rejectsWrongKeystorePassword() throws {
        let keystorePath = try Self.writeTemporaryKeystore()
        defer { try? FileManager.default.removeItem(atPath: keystorePath) }

        var config = self.makeSSLConfig()
        config.sslCaPem = Self.caCertificatePEM
        config.sslKeystoreLocation = keystorePath
        config.sslKeystorePassword = "wrong-password"

        // Proves the PKCS#12 parse path runs: PKCS12_parse fails the MAC check on a wrong password.
        #expect(throws: KafkaError.self) {
            _ = try RDKafkaClient.makeClient(
                type: .producer,
                configDictionary: config.config,
                events: Self.events,
                logger: .kafkaTest
            )
        }
    }

    // MARK: - Test fixtures

    /// Decodes the embedded PKCS#12 fixture to a temporary file, since librdkafka reads the
    /// keystore from a path (`ssl.keystore.location`) rather than from memory.
    private static func writeTemporaryKeystore() throws -> String {
        let base64 = self.clientKeystoreP12Base64.filter { !$0.isWhitespace }
        guard let data = Data(base64Encoded: base64) else {
            fatalError("Invalid base64 PKCS#12 test fixture")
        }
        let path = NSTemporaryDirectory() + "swift-kafka-ssl-test-\(UInt64.random(in: .min ... .max)).p12"
        try data.write(to: URL(fileURLWithPath: path))
        return path
    }

    // A self-signed CA, a client certificate signed by it, and a matching private key, plus the
    // client identity packaged as a password-protected PKCS#12 keystore. Test-only material.
    private static let keystorePassword = "test-password"

    private static let caCertificatePEM = """
        -----BEGIN CERTIFICATE-----
        MIICuDCCAaACCQCLI1K6w1KIETANBgkqhkiG9w0BAQsFADAeMRwwGgYDVQQDDBNz
        d2lmdC1rYWZrYS10ZXN0LWNhMB4XDTI2MDkyOTIzMzM1MloXDTM2MDkyNjIzMzM1
        MlowHjEcMBoGA1UEAwwTc3dpZnQta2Fma2EtdGVzdC1jYTCCASIwDQYJKoZIhvcN
        AQEBBQADggEPADCCAQoCggEBAMIn9mh91wElgViYwnIPhS27MNKM4a4HlanEaLZU
        s0lgEfbdVL2zLRIur66ZAprdSfzDystDtSMRj4HW15gOBqIBuu9Fp8LtpLdbzGI0
        J4t9fj3qjfLjOOcWDdZU2kdjM148RcclKIvByUxPWZVvDwZ5p+rQFsg09guFIvWN
        WSqKALbITe5zWWSnuElA3f3+9JhqNJTx8NXfpMCVBXPnBFKCj4YaEYAR7vyU/E1b
        uAb6efV8E4XfrVASGfXeqEadODI/3zKzhPGqkmSiyH7w+mr/GFIS+Ye02MU9V8RU
        W2Ok1EruOWvp+X1Dkg1TNn8VzdsjzuXijivxVOE8J2TuSK8CAwEAATANBgkqhkiG
        9w0BAQsFAAOCAQEAMNDfZLqFEx0+dUgQUPSW6y9mdWz4Bw5KtfAGEez8B24Ai4hD
        J81QCxkzZ/PMEUQVB9sAYEhbxLCszXXRwpDXf2jnU3MEaEwL81IJZIcFvsaRlb+r
        SwhCncIUXNAYJjPq09ismtxSqkB75drwNTT5UsVxQCCGz5KoImv7Iyw0FQ7v+0nI
        uyuHMPmMbj0EXteQs2BLyCLSDtOebWdaKByZXrGSiOjWRVb8TjvMQvucRI3ep2qI
        NzLgRAEblC4F9ZYy9bHHJ2KL9xPjkobLp7jhdjN32GyP4MLLyKFAebF0ztPdEgMC
        hk0/uTV7YMo+wwhOh/g7yI7I507LHnyz8OV0SA==
        -----END CERTIFICATE-----
        """

    private static let clientCertificatePEM = """
        -----BEGIN CERTIFICATE-----
        MIICvDCCAaQCCQCv4IXHzsPPTDANBgkqhkiG9w0BAQsFADAeMRwwGgYDVQQDDBNz
        d2lmdC1rYWZrYS10ZXN0LWNhMB4XDTI2MDkyOTIzMzM1MloXDTM2MDkyNjIzMzM1
        MlowIjEgMB4GA1UEAwwXc3dpZnQta2Fma2EtdGVzdC1jbGllbnQwggEiMA0GCSqG
        SIb3DQEBAQUAA4IBDwAwggEKAoIBAQDAXIiNBiWnceGd3E5kjvYJf8hXAKdYE/li
        8OVoCTNBo0nQ6xdN5OySZS3aIzAUT36mQL6WxPoB14yD0LgLj3OrIUURHUifHbNE
        EVSbEUS4ZFSMPvFz7mPP2ZRCuOGrMVq5UdVnmVtboetzExJ3X2eUeo/EHLYsj8MM
        SQhKa4am1AKIPzK9eHWOHrGnB2HToInTlCsftOld8Ij3ZQ6J1cE0DbMe6vwMMUBl
        T7Z7mVpwE1gat8BEdFQD3VMN4GCP+Q5Hm1Ht+8/yscNsqGVj8ZCDeNkx+JPdP9Ut
        rSf2tHNkpkt6rg34DSLeuPyp4a97j322Wlnoyb4J/yEVaeM/pqNNAgMBAAEwDQYJ
        KoZIhvcNAQELBQADggEBAHgVQlxuAFiBcXYCJZ+/nT2JTtp2UFGJv4vR7yByRXL7
        djNj0bqA2qkf92tajXb0CV1/CCuwJkqbRs3MCLFzrYhuzMyQuPXadoC52gNBX6Gd
        H2USjDTM7u+zRBhGYXeAbQ7CWa3r+V6ItmdIqjF+qXYx5SBXeyDI54wgiGVC2UeJ
        S8fKD4yLjrYcslHDhd6gCwpsmK8VyEQsdkMunDLGuMNgvaGWbev0qyWtwIp3xFg0
        F8b1p4m5ohz0r057D4w3B0T3DsXk8S9IAgKdNt8QHLi97Ay38HPW50Sbc0bifSj2
        u9DJhoLyFy1PFTSfsGNwLqV+xS3tD8n6Hwfk4E01CtM=
        -----END CERTIFICATE-----
        """

    private static let clientPrivateKeyPEM = """
        -----BEGIN PRIVATE KEY-----
        MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDAXIiNBiWnceGd
        3E5kjvYJf8hXAKdYE/li8OVoCTNBo0nQ6xdN5OySZS3aIzAUT36mQL6WxPoB14yD
        0LgLj3OrIUURHUifHbNEEVSbEUS4ZFSMPvFz7mPP2ZRCuOGrMVq5UdVnmVtboetz
        ExJ3X2eUeo/EHLYsj8MMSQhKa4am1AKIPzK9eHWOHrGnB2HToInTlCsftOld8Ij3
        ZQ6J1cE0DbMe6vwMMUBlT7Z7mVpwE1gat8BEdFQD3VMN4GCP+Q5Hm1Ht+8/yscNs
        qGVj8ZCDeNkx+JPdP9UtrSf2tHNkpkt6rg34DSLeuPyp4a97j322Wlnoyb4J/yEV
        aeM/pqNNAgMBAAECggEBAJvGU45ge/2ta1e7IICuhPAMWuCZ1uqswjgtfJlTWf69
        jPlyZ9zclfe3l/oepMDN33Ii3DScLxluxIu6alBySuk6eAP0AutX0eQ9ZJZu1Ye9
        N/qavk1zr8CSIk4mChnoTd/lNTxAJIuABHub5khlKixDVEksNxq/EOTR/7TEfrZ5
        caeRI4nDUO7c0CBhS05GgTcVy+B8LfUOfEiskNse1rDeN8umwsqx6IM+wXi7k60m
        2WlUCiIH61JVzQPUerPoOAaTTT+zEefLKIM+QYerU8fAX35ofYSQseSpu1PppyFv
        nAEXqnNYibzO4JZxVd9d6yhQ+UW3/aQnuRtKdelENgECgYEA7MyqXO4ddQtbfskQ
        m3hS40gnqCGukuwdxVxjUBgqDn/vqsVvoQk043VFpQGNFSqyYiUnwxWeynnw0n1m
        kiJsE7L6ri/pLc6+FkvZuR7X/SrMO+Gv+UhFmXgmjZHviNdpsoLEDh/gxjbJJyFn
        yMdrmFGVpaj4nU441MDItIbH89UCgYEAz/VzmFLVgmQiNbx2N4B+lX/3uhE0CN/l
        gwzNS0HSWwfU+vM6TURVQb7/AlBPDAo1odWLuj188xGswZRAFYu2mPS8owiMVenG
        EABIXCFhrpNPRWneRZ97ebwHpSH8bmVNls3VkZS0hK+JMx/j9jaNlFOTiLBCaMJi
        1CZONc33xZkCgYEAsQiCBV/vneWTzcG8LL9UGJ+FJmmn/ieQSlC73tFx/80d/OoG
        frpHVBYEr0vtkfmNy1pD09GCGZX4uSUPXglmC4QvbbJb/ecmrbyfVYr9p0Qr1Ejy
        KsghYI83I3gLQJ7rYo3ozTZ/8TBcSmjrEmwkwIed/W/KDrDARC6s3mo1atECgYAh
        lApkMAvVkAGOF64m7BKEZTNE0f5K6IwQiwKjAi/LDwquspNZYo05nnNilkENOqDK
        TsbO/ZqtTM9OOnfWgsarD+ldkcXHIYYfk0wbxDAXliWYma2QJF41m8zINyKhWK4c
        iYPQi+hR/F306VNPsCNQLeaGjZ9LdweqJb0/JtC5EQKBgQDSFWQ0UFeKVxsAa/yg
        ZS39s8Om5sVaPzs6Y5VhqWA0L1gcStQUkpB9DpohmbW9kqZW3NKfVl92o5KXWH+K
        NrCq99j/w7tbA6jFjPRPBM4HGih4De5PNpVCKHXG1RUJS/ywaFIxIFCulT5vRHda
        YC6jrQ8sA/sUNuT2FGQTTPnujQ==
        -----END PRIVATE KEY-----
        """

    private static let clientKeystoreP12Base64 = """
        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
        """
}
