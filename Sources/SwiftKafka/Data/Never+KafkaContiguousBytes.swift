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

extension Never: KafkaContiguousBytes {
    public func withUnsafeBytes<R>(_: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        fatalError("This statement should never be reached")
    }
}
