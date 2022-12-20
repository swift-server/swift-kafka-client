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

public struct TopicConfig: Hashable, Equatable {
    var properties: [String: String] = [:]

    public var acks: Int {
        get { self.getInt("acks") ?? -1 }
        set { self.properties["acks"] = String(newValue) }
    }

    public var requestTimeoutMs: UInt {
        get { self.getUInt("request.timeout.ms") ?? 30000 }
        set { self.properties["request.timeout.ms"] = String(newValue) }
    }

    public var messageTimeoutMs: UInt {
        get { self.getUInt("message.timeout.ms") ?? 300000 }
        set { self.properties["message.timeout.ms"] = String(newValue) }
    }

    public var partitioner: ConfigEnums.Partitioner {
        get { self.getPartitioner() ?? .consistentRandom }
        set { self.properties["partitioner"] = newValue.description }
    }

    public var compressionCodec: ConfigEnums.CompressionCodec {
        get { self.getCompressionCodec() ?? .inherit }
        set { self.properties["compression.codec"] = newValue.description }
    }

    public var compressionLevel: Int {
        get { self.getInt("compression.level") ?? -1 }
        set { self.properties["compression.level"] = String(newValue) }
    }

    public init(
        acks: Int = -1,
        requestTimeoutMs: UInt = 30000,
        messageTimeoutMs: UInt = 300000,
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

    // TODO: docc
    func getInt(_ key: String) -> Int? {
        guard let value = properties[key] else {
            return nil
        }
        return Int(value)
    }

    func getUInt(_ key: String) -> UInt? {
        guard let value = properties[key] else {
            return nil
        }
        return UInt(value)
    }

    func getPartitioner() -> ConfigEnums.Partitioner? {
        guard let value = properties["partitioner"] else {
            return nil
        }
        return ConfigEnums.Partitioner(description: value)
    }

    func getCompressionCodec() -> ConfigEnums.CompressionCodec? {
        guard let value = properties["compression.codec"] else {
            return nil
        }
        return ConfigEnums.CompressionCodec(description: value)
    }
}

// MARK: - ConfigEnums + Additions

extension ConfigEnums {
    // TODO: docc + docc for individual options
    public struct Partitioner: Hashable, Equatable, CustomStringConvertible {
        public let description: String

        public static let random = Partitioner(description: "random")
        public static let consistent = Partitioner(description: "consistent")
        public static let consistentRandom = Partitioner(description: "consistent_random")
        public static let murmur2 = Partitioner(description: "murmur2")
        public static let murmur2Random = Partitioner(description: "murmur2_random")
        public static let fnv1a = Partitioner(description: "fnv1a")
        public static let fnv1aRandom = Partitioner(description: "fnv1a_random")
    }

    // TODO: docc + docc for individual options
    public struct CompressionCodec: Hashable, Equatable, CustomStringConvertible {
        public let description: String

        public static let none = CompressionCodec(description: "none")
        public static let gzip = CompressionCodec(description: "gzip")
        public static let snappy = CompressionCodec(description: "snappy")
        public static let lz4 = CompressionCodec(description: "lz4")
        public static let zstd = CompressionCodec(description: "zstd")
        public static let inherit = CompressionCodec(description: "inherit")
    }
}
