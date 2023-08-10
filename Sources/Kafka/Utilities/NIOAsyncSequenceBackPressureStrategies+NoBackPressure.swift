//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIOCore

extension NIOAsyncSequenceProducerBackPressureStrategies {
    /// `NIOAsyncSequenceProducerBackPressureStrategy` that always returns true.
    struct NoBackPressure: NIOAsyncSequenceProducerBackPressureStrategy {
        func didYield(bufferDepth: Int) -> Bool { true }
        func didConsume(bufferDepth: Int) -> Bool { true }
    }
}
