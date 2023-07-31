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

extension String: KafkaContiguousBytes {
    public func withUnsafeBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        if let read = try self.utf8.withContiguousStorageIfAvailable({ unsafePointer in
            // Fast Path
            let unsafeRawBufferPointer = UnsafeRawBufferPointer(start: unsafePointer.baseAddress, count: self.utf8.count)
            return try body(unsafeRawBufferPointer)
        }) {
            return read
        } else {
            // Slow path
            return try ByteBuffer(string: self).withUnsafeBytes(body)
        }
    }
}
