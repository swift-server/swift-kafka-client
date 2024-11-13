//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2024 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// A wrapper for the `OpaquePointer` used to represent different handles from `librdkafka`.
///
/// This wrapper silences `Sendable` warnings for the pointer introduced in Swift 5.10, and should
/// only be used for handles from `librdkafka` that are known to be thread-safe.
struct SendableOpaquePointer: @unchecked Sendable {
    let pointer: OpaquePointer

    init(_ pointer: OpaquePointer) {
        self.pointer = pointer
    }
}
