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

extension Duration {
    internal var inMilliseconds: UInt {
        let seconds = Double(components.seconds) * 1000.0
        let attoseconds = Double(components.attoseconds) * 1e-15
        return UInt(seconds + attoseconds)
    }

    internal var canBeRepresentedAsMilliseconds: Bool {
        return self.inMilliseconds > 0
    }
    
    internal static var disabled: Duration { // FIXME: public?
        return .zero
    }
}
