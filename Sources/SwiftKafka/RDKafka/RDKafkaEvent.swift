//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka

/// Swift `enum` wrapping `librdkafka`'s `RD_KAFKA_EVENT_*` types.
/// See `RD_KAFKA_EVENT_*` in rdkafka.h for reference.
internal enum RDKafkaEvent: Int32 {
    case none = 0x0
    case deliveryReport = 0x1
    case fetch = 0x2
    case log = 0x4
    case error = 0x8
    case rebalance = 0x10
    case offsetCommit = 0x20
    case statistics = 0x40
}
