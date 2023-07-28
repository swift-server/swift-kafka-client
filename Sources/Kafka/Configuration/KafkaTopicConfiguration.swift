//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// Used to configure new topics created by the ``KafkaProducer``.
public struct KafkaTopicConfiguration {
    /// This number of acknowledgments the leader broker must receive from ISR brokers before responding to the request.
    public struct RequiredAcknowledgments: Sendable, Hashable {
        internal let rawValue: Int

        private init(rawValue: Int) {
            self.rawValue = rawValue
        }

        public static func atLeast(_ value: Int) -> RequiredAcknowledgments {
            return .init(rawValue: value)
        }

        /// Broker will block until the message is committed by all in-sync replicas (ISRs).
        public static let all: RequiredAcknowledgments = .init(rawValue: -1)

        /// Broker does not send any response/ack to the client.
        public static let noAcknowledgments: RequiredAcknowledgments = .init(rawValue: 0)
    }

    /// This field indicates the number of acknowledgments the leader broker must receive from ISR brokers before responding to the request.
    /// If there are less than min.insync.replicas (broker configuration) in the ISR set the produce request will fail.
    /// Default: `.all`
    public var requiredAcknowledgements: RequiredAcknowledgments = .all

    /// The ack timeout of the producer request. This value is only enforced by the broker and relies on request.required.acks being != 0.
    /// (Lowest granularity is milliseconds)
    /// Default: `.milliseconds(30000)`
    public var requestTimeout: Duration = .milliseconds(30000) {
        didSet {
            precondition(
                self.requestTimeout.canBeRepresentedAsMilliseconds,
                "Lowest granularity is milliseconds"
            )
        }
    }

    /// Local message timeout.
    public struct MessageTimeout: Sendable, Hashable {
        internal let rawValue: UInt

        private init(rawValue: UInt) {
            self.rawValue = rawValue
        }

        /// (Lowest granularity is milliseconds)
        public static func timeout(_ value: Duration) -> MessageTimeout {
            precondition(
                value.canBeRepresentedAsMilliseconds,
                "Lowest granularity is milliseconds"
            )
            return .init(rawValue: value.inMilliseconds)
        }

        public static let infinite: MessageTimeout = .init(rawValue: 0)
    }

    /// Local message timeout.
    /// This value is only enforced locally and limits the time a produced message waits for successful delivery.
    /// This is the maximum time librdkafka may use to deliver a message (including retries).
    /// Delivery error occurs when either the retry count or the message timeout is exceeded. The message timeout is automatically adjusted to transaction.timeout.ms if transactional.id is configured.
    /// (Lowest granularity is milliseconds)
    /// Default: `.timeout(.milliseconds(300_000))`
    public var messageTimeout: MessageTimeout = .timeout(.milliseconds(300_000))

    /// Partitioner. See ``KafkaSharedConfiguration/Partitioner`` for more information.
    /// Default: `.consistentRandom`
    public var partitioner: KafkaConfiguration.Partitioner = .consistentRandom

    /// Compression-related configuration options.
    public var compression: KafkaConfiguration.Compression = .init()

    public init() {}
}

// MARK: - KafkaTopicConfiguration + Hashable

extension KafkaTopicConfiguration: Hashable {}

// MARK: - KafkaTopicConfiguration + Sendable

extension KafkaTopicConfiguration: Sendable {}

// MARK: - KafkaTopicConfiguration + Dictionary

extension KafkaTopicConfiguration {
    internal var dictionary: [String: String] {
        var resultDict: [String: String] = [:]

        resultDict["acks"] = String(self.requiredAcknowledgements.rawValue)
        resultDict["request.timeout.ms"] = String(self.requestTimeout.inMilliseconds)
        resultDict["message.timeout.ms"] = String(self.messageTimeout.rawValue)
        resultDict["partitioner"] = self.partitioner.description
        resultDict["compression.codec"] = self.compression.codec.description
        resultDict["compression.level"] = String(self.compression.codec.level.rawValue)

        return resultDict
    }
}

// MARK: - KafkaConfiguration + Additions

extension KafkaConfiguration {
    /// Partitioner. Computes the partition that a message is stored in.
    public struct Partitioner: Sendable, Hashable, CustomStringConvertible {
        public let description: String

        /// Random distribution.
        public static let random = Partitioner(description: "random")
        /// CRC32 hash of key (Empty and NULL keys are mapped to a single partition).
        public static let consistent = Partitioner(description: "consistent")
        /// CRC32 hash of key (Empty and NULL keys are randomly partitioned).
        public static let consistentRandom = Partitioner(description: "consistent_random")
        /// Java Producer compatible Murmur2 hash of key (NULL keys are mapped to a single partition).
        public static let murmur2 = Partitioner(description: "murmur2")
        /// Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned. This is functionally equivalent to the default partitioner in the Java Producer).
        public static let murmur2Random = Partitioner(description: "murmur2_random")
        /// FNV-1a hash of key (NULL keys are mapped to a single partition).
        public static let fnv1a = Partitioner(description: "fnv1a")
        /// FNV-1a hash of key (NULL keys are randomly partitioned).
        public static let fnv1aRandom = Partitioner(description: "fnv1a_random")
    }

    /// Compression-related configuration options.
    public struct Compression: Sendable, Hashable {
        /// Compression level parameter for algorithm selected by configuration property compression.codec.
        /// Higher values will result in better compression at the cost of more CPU usage.
        public struct Level: Sendable, Hashable {
            internal let rawValue: Int

            private init(rawValue: Int) {
                self.rawValue = rawValue
            }

            public static func level(_ value: Int) -> Level {
                return .init(rawValue: value)
            }

            /// Codec-dependent default compression level.
            public static let codecDependent: Level = .init(rawValue: -1)
        }

        /// Process to compress and decompress data.
        public struct Codec: Sendable, Hashable, CustomStringConvertible {
            private enum _Codec: Sendable, Hashable, CustomStringConvertible {
                case none
                case gzip(compressionLevel: Level)
                case snappy // only compression level is 0
                case lz4(compressionLevel: Level)
                case zstd(compressionLevel: Level)
                case inherit

                internal var description: String {
                    switch self {
                    case .none:
                        return "none"
                    case .gzip:
                        return "gzip"
                    case .snappy:
                        return "snappy"
                    case .lz4:
                        return "lz4"
                    case .zstd:
                        return "zstd"
                    case .inherit:
                        return "inherit"
                    }
                }

                internal var level: Level {
                    switch self {
                    case .none:
                        return .codecDependent
                    case .gzip(let compressionLevel):
                        return compressionLevel
                    case .snappy:
                        return .codecDependent
                    case .lz4(let compressionLevel):
                        return compressionLevel
                    case .zstd(let compressionLevel):
                        return compressionLevel
                    case .inherit:
                        return .codecDependent
                    }
                }
            }

            private let _internal: _Codec

            public var description: String {
                self._internal.description
            }

            public var level: Level {
                self._internal.level
            }

            /// No compression.
            public static let none = Codec(_internal: .none)

            /// gzip compression.
            ///
            /// Usable compression level range: `0-9`.
            public static func gzip(compressionLevel: Level) -> Codec {
                precondition(
                    0...9 ~= compressionLevel.rawValue || compressionLevel == .codecDependent,
                    "Compression level outside of valid range"
                )
                return Codec(_internal: .gzip(compressionLevel: compressionLevel))
            }

            /// snappy compression.
            public static let snappy = Codec(_internal: .snappy)

            /// lz4 compression.
            ///
            /// Usable compression level range: `0-12`.
            public func lz4(compressionLevel: Level) -> Codec {
                precondition(
                    0...12 ~= compressionLevel.rawValue || compressionLevel == .codecDependent,
                    "Compression level outside of valid range"
                )
                return Codec(_internal: .lz4(compressionLevel: compressionLevel))
            }

            /// zstd compression.
            public func zstd(compressionLevel: Level) -> Codec {
                return Codec(_internal: .zstd(compressionLevel: compressionLevel))
            }

            /// Inherit global compression.codec configuration.
            public static let inherit = Codec(_internal: .inherit)
        }

        /// Compression codec to use for compressing message sets.
        /// Default: `.inherit`
        public var codec: Codec = .inherit
    }
}
