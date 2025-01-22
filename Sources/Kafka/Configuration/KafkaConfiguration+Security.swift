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

extension KafkaConfiguration {
    // MARK: - TLSConfiguration

    /// Use to configure a TLS connection.
    public struct TLSConfiguration: Sendable, Hashable {
        /// Certificate chain consisting of one leaf certificate and potentially multiple intermediate certificates.
        /// The public key of the leaf certificate will be used for authentication.
        public struct LeafAndIntermediates: Sendable, Hashable {
            internal enum _Key: Sendable, Hashable {
                case file(location: String)
                case pem(String)
            }

            let _internal: _Key

            /// Read certificate chain from a file.
            public static func file(location: String) -> LeafAndIntermediates {
                LeafAndIntermediates(
                    _internal: .file(location: location)
                )
            }

            /// Read X.509 certificate from String.
            public static func pem(_ pem: String) -> LeafAndIntermediates {
                LeafAndIntermediates(
                    _internal: .pem(pem)
                )
            }
        }

        public struct TrustRoots: Sendable, Hashable {
            internal enum _TrustRoots: Sendable, Hashable {
                case probe
                case file(location: String)
                case pem(String)
            }

            let _internal: _TrustRoots

            /// A list of standard paths will be probed and the first one found will be used as the default root certificate location path.
            public static let probe = TrustRoots(_internal: .probe)

            /// File or directory path to root certificate(s) for verifying the broker's key.
            public static func file(location: String) -> TrustRoots {
                TrustRoots(
                    _internal: .file(location: location)
                )
            }

            /// Trust roots certificate String for verifying the broker's key.
            public static func pem(_ pem: String) -> TrustRoots {
                TrustRoots(
                    _internal: .pem(pem)
                )
            }
        }

        /// A TLS private key.
        public struct PrivateKey: Sendable, Hashable {
            public struct Location: Sendable, Hashable {
                internal enum _Location: Sendable, Hashable {
                    case file(location: String)
                    case pem(String)
                }

                let _internal: _Location

                /// A key located in a file at the given `location`.
                public static func file(location: String) -> Location {
                    Location(
                        _internal: .file(location: location)
                    )
                }

                /// A key String (PEM format).
                public static func pem(_ pem: String) -> Location {
                    Location(
                        _internal: .pem(pem)
                    )
                }
            }

            /// The private key itself.
            public var key: Location
            /// The password associated with the private key.
            public var password: String

            public init(location: Location, password: String) {
                self.key = location
                self.password = password
            }
        }

        /// A TLS key store (PKCS#12).
        public struct KeyStore: Sendable, Hashable {
            /// Path to the key store.
            public var location: String
            /// The key store's password.
            public var password: String

            public init(location: String, password: String) {
                self.location = location
                self.password = password
            }
        }

        /// Configuration for the TLS identity of the client.
        public struct ClientIdentity: Sendable, Hashable {
            internal enum _ClientIdentity: Sendable, Hashable {
                case keyPair(
                    privateKey: PrivateKey,
                    certificates: LeafAndIntermediates
                )
                case keyStore(keyStore: KeyStore)
            }

            let _internal: _ClientIdentity

            /// Use TLS client verification with a given private/public key pair.
            ///
            /// - Parameters:
            ///     - privateKey: The client's private key (PEM) used for authentication.
            ///     - certificates: The client's public key (PEM) used for authentication and potentially multiple intermediate certificates.
            public static func keyPair(
                privateKey: PrivateKey,
                certificates: LeafAndIntermediates
            ) -> ClientIdentity {
                .init(
                    _internal: .keyPair(
                        privateKey: privateKey,
                        certificates: certificates
                    )
                )
            }

            /// Use TLS client verification with a given key store.
            ///
            /// - Parameters:
            ///     - keyStore: The client's keystore (PKCS#12) used for authentication.
            public static func keyStore(keyStore: KeyStore) -> ClientIdentity {
                .init(_internal: .keyStore(keyStore: keyStore))
            }
        }

        /// Configuration for the TLS verification of the broker.
        public struct BrokerVerification: Sendable, Hashable {
            internal enum _BrokerVerification: Sendable, Hashable {
                case disabled
                case verify(
                    trustRoots: TrustRoots,
                    certificateRevocationListPath: String?
                )
            }

            let _internal: _BrokerVerification

            /// Do not verify the identity of the broker.
            public static let disabled: BrokerVerification = .init(_internal: .disabled)

            /// Verify the identity of the broker.
            ///
            /// - Parameters:
            ///     - trustRoots: File or directory path to CA certificate(s) for verifying the broker's key.
            ///     - certificateRevocationListPath: Path to CRL for verifying broker's certificate validity.
            public static func verify(
                trustRoots: TrustRoots = .probe,
                certificateRevocationListPath: String? = nil
            ) -> BrokerVerification {
                .init(
                    _internal: .verify(
                        trustRoots: trustRoots,
                        certificateRevocationListPath: certificateRevocationListPath
                    )
                )
            }
        }

        /// Configuration for the TLS verification of the client.
        /// Default: `nil`
        public var clientIdentity: ClientIdentity? = nil

        /// Configuration for the TLS verification of the broker.
        /// Default:  `verify(trustRoots: .probe, certificateRevocationListPath: nil)``
        public var brokerVerification: BrokerVerification = .verify(
            trustRoots: .probe,
            certificateRevocationListPath: nil
        )

        public init() {}

        // MARK: TLSConfiguration + Dictionary

        internal var dictionary: [String: String] {
            var resultDict: [String: String] = [:]

            // Client TLS Verification
            switch self.clientIdentity?._internal {
            case .none:
                break
            case .keyPair(let privateKey, let certificate):
                switch privateKey.key._internal {
                case .file(let location):
                    resultDict["ssl.key.location"] = location
                case .pem(let pem):
                    resultDict["ssl.key.pem"] = pem
                }
                resultDict["ssl.key.password"] = privateKey.password
                switch certificate._internal {
                case .file(let location):
                    resultDict["ssl.key.location"] = location
                    resultDict["ssl.certificate.location"] = location
                case .pem(let pem):
                    resultDict["ssl.certificate.pem"] = pem
                }
            case .keyStore(let keyStore):
                resultDict["ssl.keystore.location"] = keyStore.location
                resultDict["ssl.keystore.password"] = keyStore.password
            }

            // Broker TLS Verification
            switch self.brokerVerification._internal {
            case .disabled:
                resultDict["enable.ssl.certificate.verification"] = String(false)
            case .verify(let trustRoots, let certificateRevocationListPath):
                resultDict["enable.ssl.certificate.verification"] = String(true)
                switch trustRoots._internal {
                case .probe:
                    resultDict["ssl.ca.location"] = "probe"
                case .file(let location):
                    resultDict["ssl.ca.location"] = location
                case .pem(let pem):
                    resultDict["ssl.ca.pem"] = pem
                }
                resultDict["ssl.crl.location"] = certificateRevocationListPath
            }

            return resultDict
        }
    }

    // MARK: - SASLMechanism

    /// Available SASL mechanisms that can be used for authentication.
    public struct SASLMechanism: Sendable, Hashable {
        /// Used to configure Kerberos.
        public struct KerberosConfiguration: Sendable, Hashable {
            /// Kerberos principal name that Kafka runs as, not including `/hostname@REALM`.
            /// Default: `"kafka"`
            public var serviceName: String = "kafka"
            /// This client's Kerberos principal name. (Not supported on Windows, will use the logon user's principal).
            /// Default: `"kafkaclient"`
            public var principal: String = "kafkaclient"
            /// Shell command to refresh or acquire the client's Kerberos ticket.
            /// This command is executed on client creation and every ``KafkaConfiguration/SASLMechanism/KerberosConfiguration/minTimeBeforeRelogin``.
            /// %{config.prop.name} is replaced by corresponding config object value.
            /// Default: `kinit -R -t "%{sasl.kerberos.keytab}" -k %{sasl.kerberos.principal} || kinit -t "%{sasl.kerberos.keytab}" -k %{sasl.kerberos.principal}"`.
            public var kinitCommand: String = """
                kinit -R -t "%{sasl.kerberos.keytab}" -k %{sasl.kerberos.principal} || \
                kinit -t "%{sasl.kerberos.keytab}" -k %{sasl.kerberos.principal}"
                """
            /// Path to Kerberos keytab file.
            /// This configuration property is only used as a variable in ``KafkaConfiguration/SASLMechanism/KerberosConfiguration/kinitCommand``
            /// as  ... -t "%{sasl.kerberos.keytab}".
            public var keytab: String

            /// Minimum time between key refresh attempts.
            public struct KeyRefreshAttempts: Sendable, Hashable {
                internal let rawValue: UInt

                private init(rawValue: UInt) {
                    self.rawValue = rawValue
                }

                /// (Lowest granularity is milliseconds)
                public static func value(_ value: Duration) -> KeyRefreshAttempts {
                    precondition(
                        value.canBeRepresentedAsMilliseconds,
                        "Lowest granularity is milliseconds"
                    )
                    return .init(rawValue: UInt(value.inMilliseconds))
                }

                /// Disable automatic key refresh by setting this property.
                public static let disabled: KeyRefreshAttempts = .init(rawValue: 0)
            }

            /// Minimum time in between key refresh attempts.
            /// Disable automatic key refresh by setting this property to 0.
            /// (Lowest granularity is milliseconds)
            /// Default: `.value(.milliseconds(60000))`
            public var minTimeBeforeRelogin: KeyRefreshAttempts = .value(.milliseconds(60000))

            public init(keytab: String) {
                self.keytab = keytab
            }
        }

        struct OAuthBearerMethod: Sendable, Hashable {
            internal enum _OAuthBearerMethod: Sendable, Hashable {
                case `default`(
                    configuration: String?
                )
                case oidc(
                    configuration: String?,
                    clientID: String,
                    clientSecret: String,
                    tokenEndPointURL: String,
                    scope: String?,
                    extensions: String?
                )
            }

            let _internal: _OAuthBearerMethod

            /// Default OAuthBearer method.
            ///
            /// - Parameters:
            ///     - configuration: SASL/OAUTHBEARER configuration.
            ///     The format is implementation-dependent and must be parsed accordingly.
            ///     The default unsecured token implementation (see https://tools.ietf.org/html/rfc7515#appendix-A.5) recognizes space-separated name=value pairs with valid names including principalClaimName, principal, scopeClaimName, scope, and lifeSeconds.
            ///     The default value for principalClaimName is "sub", the default value for scopeClaimName is "scope", and the default value for lifeSeconds is 3600.
            ///     The scope value is in CSV format with the default value being no/empty scope.
            ///     For example: `principalClaimName=azp principal=admin scopeClaimName=roles scope=role1,role2 lifeSeconds=600`.
            ///     In addition, SASL extensions can be communicated to the broker via `extension_NAME=value`.
            ///     For example: `principal=admin extension_traceId=123`
            static func `default`(configuration: String? = nil) -> OAuthBearerMethod {
                OAuthBearerMethod(_internal: .default(configuration: configuration))
            }

            /// OpenID Connect (OIDC).
            ///
            /// - Parameters:
            ///     - configuration: SASL/OAUTHBEARER configuration.
            ///         The format is implementation-dependent and must be parsed accordingly.
            ///         The default unsecured token implementation (see https://tools.ietf.org/html/rfc7515#appendix-A.5) recognizes space-separated    name=value pairs with valid names including principalClaimName, principal, scopeClaimName, scope, and lifeSeconds.
            ///         The default value for principalClaimName is "sub", the default value for scopeClaimName is "scope", and the default value for lifeSeconds is 3600.
            ///         The scope value is in CSV format with the default value being no/empty scope.
            ///         For example: `principalClaimName=azp principal=admin scopeClaimName=roles scope=role1,role2 lifeSeconds=600`.
            ///         In addition, SASL extensions can be communicated to the broker via `extension_NAME=value`.
            ///         For example: `principal=admin extension_traceId=123`
            ///     - clientID: Public identifier for the application. Must be unique across all clients that the authorization server handles.
            ///     - clientSecret: Client secret only known to the application and the authorization server.
            ///     This should be a sufficiently random string that is not guessable.
            ///     - tokenEndPointURL: OAuth/OIDC issuer token endpoint HTTP(S) URI used to retrieve token.
            ///     - scope: The client uses this to specify the scope of the access request to the broker.
            ///     - extensions: Allow additional information to be provided to the broker.
            ///     Comma-separated list of key=value pairs. E.g., "supportFeatureX=true,organizationId=sales-emea".
            static func oidc(
                configuration: String? = nil,
                clientID: String,
                clientSecret: String,
                tokenEndPointURL: String,
                scope: String? = nil,
                extensions: String? = nil
            ) -> OAuthBearerMethod {
                OAuthBearerMethod(
                    _internal: .oidc(
                        configuration: configuration,
                        clientID: clientID,
                        clientSecret: clientSecret,
                        tokenEndPointURL: tokenEndPointURL,
                        scope: scope,
                        extensions: extensions
                    )
                )
            }
        }

        private enum _SASLMechanism: Sendable, Hashable {
            case gssapi(kerberosConfiguration: KerberosConfiguration)
            case plain(username: String, password: String)
            case scramSHA256(username: String, password: String)
            case scramSHA512(username: String, password: String)
            case oAuthBearer(method: OAuthBearerMethod = .default())
        }

        private let _internal: _SASLMechanism

        /// Use the GSSAPI mechanism.
        public static func gssapi(kerberosConfiguration: KerberosConfiguration) -> SASLMechanism {
            SASLMechanism(
                _internal: .gssapi(kerberosConfiguration: kerberosConfiguration)
            )
        }

        /// Use the PLAIN mechanism.
        public static func plain(username: String, password: String) -> SASLMechanism {
            SASLMechanism(
                _internal: .plain(username: username, password: password)
            )
        }

        /// Use the SCRAM-SHA-256 mechanism.
        public static func scramSHA256(username: String, password: String) -> SASLMechanism {
            SASLMechanism(
                _internal: .scramSHA256(username: username, password: password)
            )
        }

        /// Use the SCRAM-SHA-512 mechanism.
        public static func scramSHA512(username: String, password: String) -> SASLMechanism {
            SASLMechanism(
                _internal: .scramSHA512(username: username, password: password)
            )
        }

        /// Use the OAUTHBEARER mechanism.
        // This is currently disabled since it requires a curl dependency otherwise.
        static func oAuthBearer(method: OAuthBearerMethod) -> SASLMechanism {
            SASLMechanism(
                _internal: .oAuthBearer(method: method)
            )
        }

        // MARK: SASLMechanism + Dictionary

        internal var dictionary: [String: String] {
            var resultDict: [String: String] = [:]

            switch self._internal {
            case .gssapi(let kerberosConfiguration):
                resultDict["sasl.mechanism"] = "GSSAPI"
                resultDict["sasl.kerberos.service.name"] = kerberosConfiguration.serviceName
                resultDict["sasl.kerberos.principal"] = kerberosConfiguration.principal
                resultDict["sasl.kerberos.kinit.cmd"] = kerberosConfiguration.kinitCommand
                resultDict["sasl.kerberos.keytab"] = kerberosConfiguration.keytab
                resultDict["sasl.kerberos.min.time.before.relogin"] = String(
                    kerberosConfiguration.minTimeBeforeRelogin.rawValue
                )
            case .plain(let username, let password):
                resultDict["sasl.mechanism"] = "PLAIN"
                resultDict["sasl.username"] = username
                resultDict["sasl.password"] = password
            case .scramSHA256(let username, let password):
                resultDict["sasl.mechanism"] = "SCRAM-SHA-256"
                resultDict["sasl.username"] = username
                resultDict["sasl.password"] = password
            case .scramSHA512(let username, let password):
                resultDict["sasl.mechanism"] = "SCRAM-SHA-512"
                resultDict["sasl.username"] = username
                resultDict["sasl.password"] = password
            case .oAuthBearer(let method):
                resultDict["sasl.mechanism"] = "OAUTHBEARER"
                switch method._internal {
                case .default(let configuration):
                    resultDict["sasl.oauthbearer.method"] = "default"
                    resultDict["sasl.oauthbearer.config"] = configuration
                case .oidc(
                    let configuration,
                    let clientID,
                    let clientSecret,
                    let tokenEndPointURL,
                    let scope,
                    let extensions
                ):
                    resultDict["sasl.oauthbearer.method"] = "oidc"
                    resultDict["sasl.oauthbearer.config"] = configuration
                    resultDict["sasl.oauthbearer.client.id"] = clientID
                    resultDict["sasl.oauthbearer.client.secret"] = clientSecret
                    resultDict["sasl.oauthbearer.token.endpoint.url"] = tokenEndPointURL
                    resultDict["sasl.oauthbearer.scope"] = scope
                    resultDict["sasl.oauthbearer.extensions"] = extensions
                }
            }

            return resultDict
        }
    }

    // MARK: - SecurityProtocol

    /// Protocol used to communicate with brokers.
    public struct SecurityProtocol: Sendable, Hashable {
        internal enum _SecurityProtocol: Sendable, Hashable {
            case plaintext
            case tls(configuration: TLSConfiguration)
            case saslPlaintext(mechanism: SASLMechanism)
            case saslTLS(saslMechanism: SASLMechanism, tlsConfiguration: TLSConfiguration)
        }

        private let _internal: _SecurityProtocol

        /// Send messages as plaintext (no security protocol used).
        public static let plaintext = SecurityProtocol(
            _internal: .plaintext
        )

        /// Use the Transport Layer Security (TLS) protocol.
        public static func tls(configuration: TLSConfiguration = TLSConfiguration()) -> SecurityProtocol {
            SecurityProtocol(
                _internal: .tls(configuration: configuration)
            )
        }

        /// Use the Simple Authentication and Security Layer (SASL).
        public static func saslPlaintext(mechanism: SASLMechanism) -> SecurityProtocol {
            SecurityProtocol(
                _internal: .saslPlaintext(mechanism: mechanism)
            )
        }

        /// Use the Simple Authentication and Security Layer (SASL) with TLS.
        public static func saslTLS(
            saslMechanism: SASLMechanism,
            tlsConfiguration: TLSConfiguration = TLSConfiguration()
        ) -> SecurityProtocol {
            SecurityProtocol(
                _internal: .saslTLS(saslMechanism: saslMechanism, tlsConfiguration: tlsConfiguration)
            )
        }

        // MARK: SecurityProtocol + Dictionary

        internal var dictionary: [String: String] {
            var resultDict: [String: String] = [:]

            switch self._internal {
            case .plaintext:
                resultDict["security.protocol"] = "plaintext"
            case .tls(let tlsConfig):
                resultDict["security.protocol"] = "ssl"
                // Merge result dict with SASLMechanism config values
                resultDict.merge(tlsConfig.dictionary) { _, _ in
                    fatalError("Tried to override key that was already set!")
                }
            case .saslPlaintext(let saslMechanism):
                resultDict["security.protocol"] = "sasl_plaintext"
                // Merge result dict with SASLMechanism config values
                resultDict.merge(saslMechanism.dictionary) { _, _ in
                    fatalError("Tried to override key that was already set!")
                }
            case .saslTLS(let saslMechanism, let tlsConfig):
                resultDict["security.protocol"] = "sasl_ssl"
                // Merge with other dictionaries
                resultDict.merge(saslMechanism.dictionary) { _, _ in
                    fatalError("Tried to override key that was already set!")
                }
                resultDict.merge(tlsConfig.dictionary) { _, _ in
                    fatalError("Tried to override key that was already set!")
                }
            }

            return resultDict
        }
    }
}
