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

/// Used to configure new topics created by the ``KafkaProducer``.
public struct KafkaTopicConfiguration {
    /// This field indicates the number of acknowledgements the leader broker must receive from ISR brokers before responding to the request: 0=Broker does not send any response/ack to client, -1 or all=Broker will block until message is committed by all in sync replicas (ISRs). If there are less than min.insync.replicas (broker configuration) in the ISR set the produce request will fail.
    public var acks: Int = -1

    /// The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on request.required.acks being != 0.
    public var requestTimeoutMilliseconds: Int = 30000

    /// Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite. This is the maximum time librdkafka may use to deliver a message (including retries). Delivery error occurs when either the retry count or the message timeout are exceeded. The message timeout is automatically adjusted to transaction.timeout.ms if transactional.id is configured.
    public var messageTimeoutMilliseconds: Int = 300_000

    /// Paritioner. See ``KafkaSharedConfiguration/Partitioner`` for more information.
    public var partitioner: KafkaConfiguration.Partitioner = .consistentRandom

    /// Compression-related configuratoin options.
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

        resultDict["acks"] = String(self.acks)
        resultDict["request.timeout.ms"] = String(self.requestTimeoutMilliseconds)
        resultDict["message.timeout.ms"] = String(self.messageTimeoutMilliseconds)
        resultDict["partitioner"] = self.partitioner.description
        resultDict["compression.codec"] = self.compression.codec.description
        resultDict["compression.level"] = String(self.compression.level)

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
        /// CRC32 hash of key (Empty and NULL keys are mapped to single partition).
        public static let consistent = Partitioner(description: "consistent")
        /// CRC32 hash of key (Empty and NULL keys are randomly partitioned).
        public static let consistentRandom = Partitioner(description: "consistent_random")
        /// Java Producer compatible Murmur2 hash of key (NULL keys are mapped to single partition).
        public static let murmur2 = Partitioner(description: "murmur2")
        /// Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned. This is functionally equivalent to the default partitioner in the Java Producer).
        public static let murmur2Random = Partitioner(description: "murmur2_random")
        /// FNV-1a hash of key (NULL keys are mapped to single partition).
        public static let fnv1a = Partitioner(description: "fnv1a")
        /// FNV-1a hash of key (NULL keys are randomly partitioned).
        public static let fnv1aRandom = Partitioner(description: "fnv1a_random")
    }

    /// Compression-related configuration options.
    public struct Compression: Sendable, Hashable {
        /// Process to compress and decompress data.
        public struct Codec: Sendable, Hashable, CustomStringConvertible {
            public let description: String

            /// No compression.
            public static let none = Codec(description: "none")
            /// gzip compression.
            public static let gzip = Codec(description: "gzip")
            /// snappy compression.
            public static let snappy = Codec(description: "snappy")
            /// lz4 compression.
            public static let lz4 = Codec(description: "lz4")
            /// zstd compression.
            public static let zstd = Codec(description: "zstd")
            /// Inherit global compression.codec configuration.
            public static let inherit = Codec(description: "inherit")
        }

        /// Compression codec to use for compressing message sets.
        public var codec: Codec = .inherit

        /// Compression level parameter for algorithm selected by configuration property compression.codec. Higher values will result in better compression at the cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip; [0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression level.
        public var level: Int = -1
    }
}
