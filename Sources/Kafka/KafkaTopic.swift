//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2025 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// The name of a Kafka topic.
public struct KafkaTopic: RawRepresentable, ExpressibleByStringLiteral, CustomStringConvertible, Hashable, Sendable {
    /// The raw string name of the topic.
    public var rawValue: String

    /// A textual representation of the topic name.
    public var description: String { self.rawValue }

    /// Creates a topic name from its raw string value.
    public init(rawValue: String) {
        self.rawValue = rawValue
    }

    /// Creates a topic name from a string literal.
    public init(stringLiteral value: String) {
        self.rawValue = value
    }
}
