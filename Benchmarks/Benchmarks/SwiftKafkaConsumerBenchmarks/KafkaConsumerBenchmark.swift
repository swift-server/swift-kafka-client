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
import Kafka

let benchmarks = {
    Benchmark.defaultConfiguration = .init(
        metrics: [.wallClock, .cpuTotal, .allocatedResidentMemory, .contextSwitches, .throughput] + .arc,
        warmupIterations: 0,
        scalingFactor: .one,
        maxDuration: .seconds(5),
        maxIterations: 100
    )

    Benchmark.setup = {}

    Benchmark.teardown = {}
}
