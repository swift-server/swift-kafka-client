//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIOCore

extension String: KafkaBuffer {
    /// Yields a buffer pointer containing this `KafkaBuffer`'s readable bytes.
    ///
    /// - warning: Do not escape the pointer from the closure for later use.
    ///
    /// - parameters:
    ///     - body: The closure that will accept the yielded bytes.
    /// - returns: The value returned by `body`.
    public func withUnsafeRawBufferPointer<T>(_ body: (UnsafeRawBufferPointer) throws -> T) rethrows -> T {
        try ByteBuffer(string: self).withUnsafeReadableBytes { UnsafeRawBufferPointer in
            try body(UnsafeRawBufferPointer)
        }
    }
}
