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

extension String: KafkaContiguousBytes {
    public func withUnsafeBytes<R>(_ body: (UnsafeBufferPointer<UInt8>) throws -> R) rethrows -> R {
        if let read = try self.utf8.withContiguousStorageIfAvailable({ unsafePointer in
            // Fast path
            return try body(unsafePointer)
        }) {
            return read
        } else {
            // Slow path
            return try ByteBuffer(string: self).withUnsafeBytes(body)
        }
    }
}
