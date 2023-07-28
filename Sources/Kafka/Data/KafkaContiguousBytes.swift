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

/// Conformance to this protocol gives users a way to provide their own "bag of bytes" types
/// to be used for the serialization of Kafka messages.
/// It provides a general interface for bytes since the Swift Standard Library currently does not
/// provide such a protocol.
///
/// By conforming your own types to this protocol, you will be able to pass instances of said types
/// directly to ``KafkaProducerMessage`` as key and value.
public protocol KafkaContiguousBytes: Sendable, Hashable {
    /// Calls the given closure with the contents of the underlying storage.
    ///
    /// - note: Calling `withUnsafeBytes` multiple times does not guarantee that
    ///         the same buffer pointer will be passed in every time.
    /// - warning: The buffer argument to the body should not be stored or used
    ///            outside of the lifetime of the call to the closure.
    func withUnsafeBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R
}
