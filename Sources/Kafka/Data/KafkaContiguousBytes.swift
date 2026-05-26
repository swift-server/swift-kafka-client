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

/// A general interface for contiguous byte storage used to serialize Kafka messages.
///
/// Conformance to this protocol gives you a way to provide your own byte-container types for the
/// serialization of Kafka messages. It provides a general interface for bytes since the Swift
/// Standard Library currently does not provide such a protocol.
///
/// By conforming your own types to this protocol, you can pass instances of those types
/// directly to ``KafkaProducerMessage`` as key and value.
public protocol KafkaContiguousBytes {
    /// Calls the closure you provide with the contents of the underlying storage.
    ///
    /// - Note: Calling `withUnsafeBytes` multiple times doesn't guarantee that
    ///         the same buffer pointer is passed in every time.
    /// - Warning: Don't store the buffer argument to the body or use it
    ///            outside of the lifetime of the call to the closure.
    func withUnsafeBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R
}
