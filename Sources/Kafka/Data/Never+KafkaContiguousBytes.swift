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

extension Never: KafkaContiguousBytes {
    /// Unreachable conformance; `Never` is uninhabited and this method always traps if invoked.
    public func withUnsafeBytes<R>(_: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        fatalError("This statement should never be reached")
    }
}
