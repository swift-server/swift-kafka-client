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

import Benchmark
import Crdkafka
import Kafka

let benchmarks = {
    Benchmark.defaultConfiguration = .init(
        metrics: [.wallClock, .cpuTotal, .allocatedResidentMemory, .contextSwitches, .throughput] + .arc,
        warmupIterations: 0,
        scalingFactor: .one,
        maxDuration: .seconds(5),
        maxIterations: 100,
        thresholds: [
            // Thresholds are wild guess mostly. Have to adjust with time.
            .wallClock: .init(relative: [.p90: 10]),
            .cpuTotal: .init(relative: [.p90: 10]),
            .allocatedResidentMemory: .init(relative: [.p90: 20]),
            .contextSwitches: .init(relative: [.p90: 10]),
            .throughput: .init(relative: [.p90: 10]),
            .objectAllocCount: .init(relative: [.p90: 10]),
            .retainCount: .init(relative: [.p90: 10]),
            .releaseCount: .init(relative: [.p90: 10]),
            .retainReleaseDelta: .init(relative: [.p90: 10]),
        ]
    )

    Benchmark.setup = {}

    Benchmark.teardown = {}
}
