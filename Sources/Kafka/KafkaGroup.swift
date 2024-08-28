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

import Crdkafka

let uuu: Int = 0

public struct KafkaGroup {
    /// Swift `enum` wrapping `librdkafka`'s `RD_KAFKA_CONSUMER_GROUP_STATE_*` values.
    /// See `rd_kafka_consumer_group_state_t` in rdkafka.h for reference.
    public enum State: UInt32 {
        case unknown = 0
        case preparingRebalance = 1
        case completingRebalance = 2
        case stable = 3
        case dead = 4
        case empty = 5
    }

    public let name: String
    public let state: State
    public let isSimple: Bool
}
