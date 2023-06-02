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
public struct KafkaTopicConfig: Hashable, Equatable {
    var dictionary: [String: String] = [:]

    /// This field indicates the number of acknowledgements the leader broker must receive from ISR brokers before responding to the request: 0=Broker does not send any response/ack to client, -1 or all=Broker will block until message is committed by all in sync replicas (ISRs). If there are less than min.insync.replicas (broker configuration) in the ISR set the produce request will fail.
    public var acks: Int {
        get { self.dictionary.getInt("acks") ?? -1 }
        set { self.dictionary["acks"] = String(newValue) }
    }

    /// The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on request.required.acks being != 0.
    public var requestTimeoutMs: UInt {
        get { self.dictionary.getUInt("request.timeout.ms") ?? 30000 }
        set { self.dictionary["request.timeout.ms"] = String(newValue) }
    }

    /// Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite. This is the maximum time librdkafka may use to deliver a message (including retries). Delivery error occurs when either the retry count or the message timeout are exceeded. The message timeout is automatically adjusted to transaction.timeout.ms if transactional.id is configured.
    public var messageTimeoutMs: UInt {
        get { self.dictionary.getUInt("message.timeout.ms") ?? 300_000 }
        set { self.dictionary["message.timeout.ms"] = String(newValue) }
    }

    /// Paritioner. See ``ConfigEnums/Partitioner`` for more information.
    public var partitioner: ConfigEnums.Partitioner {
        get { self.getPartitioner() ?? .consistentRandom }
        set { self.dictionary["partitioner"] = newValue.description }
    }

    /// Compression codec to use for compressing message sets.
    public var compressionCodec: ConfigEnums.CompressionCodec {
        get { self.getCompressionCodec() ?? .inherit }
        set { self.dictionary["compression.codec"] = newValue.description }
    }

    /// Compression level parameter for algorithm selected by configuration property compression.codec. Higher values will result in better compression at the cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip; [0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression level.
    public var compressionLevel: Int {
        get { self.dictionary.getInt("compression.level") ?? -1 }
        set { self.dictionary["compression.level"] = String(newValue) }
    }

    public init(
        acks: Int = -1,
        requestTimeoutMs: UInt = 30000,
        messageTimeoutMs: UInt = 300_000,
        partitioner: ConfigEnums.Partitioner = .consistentRandom,
        compressionCodec: ConfigEnums.CompressionCodec = .inherit,
        compressionLevel: Int = -1
    ) {
        self.acks = acks
        self.requestTimeoutMs = requestTimeoutMs
        self.messageTimeoutMs = messageTimeoutMs
        self.partitioner = partitioner
        self.compressionCodec = compressionCodec
        self.compressionLevel = compressionLevel
    }

    // MARK: - Helpers

    func getPartitioner() -> ConfigEnums.Partitioner? {
        guard let value = dictionary["partitioner"] else {
            return nil
        }
        return ConfigEnums.Partitioner(description: value)
    }

    func getCompressionCodec() -> ConfigEnums.CompressionCodec? {
        guard let value = dictionary["compression.codec"] else {
            return nil
        }
        return ConfigEnums.CompressionCodec(description: value)
    }
}

// MARK: - ConfigEnums + Additions

extension ConfigEnums {
    /// Partitioner. Computes the partition that a message is stored in.
    public struct Partitioner: Hashable, Equatable, CustomStringConvertible {
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

    /// Process to compress and decompress data.
    public struct CompressionCodec: Hashable, Equatable, CustomStringConvertible {
        public let description: String

        /// No compression.
        public static let none = CompressionCodec(description: "none")
        /// gzip compression.
        public static let gzip = CompressionCodec(description: "gzip")
        /// snappy compression.
        public static let snappy = CompressionCodec(description: "snappy")
        /// lz4 compression.
        public static let lz4 = CompressionCodec(description: "lz4")
        /// zstd compression.
        public static let zstd = CompressionCodec(description: "zstd")
        /// Inherit global compression.codec configuration.
        public static let inherit = CompressionCodec(description: "inherit")
    }
}
