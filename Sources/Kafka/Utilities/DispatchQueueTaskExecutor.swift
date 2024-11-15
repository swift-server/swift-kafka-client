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

#if swift(>=6.0)
import Dispatch

final class DispatchQueueTaskExecutor: TaskExecutor {
    let queue: DispatchQueue

    init(_ queue: DispatchQueue) {
        self.queue = queue
    }

    public func enqueue(_ _job: consuming ExecutorJob) {
        let job = UnownedJob(_job)
        queue.async {
            job.runSynchronously(
                on: self.asUnownedTaskExecutor()
            )
        }
    }

    @inlinable
    public func asUnownedTaskExecutor() -> UnownedTaskExecutor {
        UnownedTaskExecutor(ordinary: self)
    }
}
#endif
