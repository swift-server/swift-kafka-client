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

import NIOCore

/// A structure representing a header for a Kafka message.
/// Headers are key-value pairs that can be attached to Kafka messages to provide additional metadata.
public struct KafkaHeader: Sendable, Hashable {
    /// The key associated with the header.
    public var key: String

    /// The value associated with the header.
    public var value: ByteBuffer? // TODO: KafkaContiguousBytes

    /// Initializes a new Kafka header with the provided key and optional value.
    ///
    /// - Parameters:
    ///   - key: The key associated with the header.
    ///   - value: The optional binary value associated with the header.
    public init(
        key: String,
        value: ByteBuffer? = nil
    ) {
        self.key = key
        self.value = value
    }
}

// TODO: KakfaContiguousBytes
// extension KafkaHeader: Hashable where Value: Hashable {}
//
// extension KafkaHeader: Equatable where Value: Equatable {}
//
// extension KafkaHeader: Sendable where Value: Sendable {}
//
