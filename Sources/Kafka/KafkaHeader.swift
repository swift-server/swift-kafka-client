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

/// A header attached to a Kafka message.
///
/// Headers are key-value pairs that carry additional metadata alongside a Kafka message.
public struct KafkaHeader: Sendable, Hashable {
    /// The key associated with the header.
    public var key: String

    /// The value associated with the header.
    public var value: ByteBuffer?

    /// Creates a new Kafka header with the key and optional value you provide.
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
