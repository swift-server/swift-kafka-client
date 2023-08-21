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

import Logging
import Metrics
import NIOConcurrencyHelpers

extension Logger {
    static var kafkaTest: Logger {
        var logger = Logger(label: "kafka.test")
        logger.logLevel = .info
        return logger
    }
}

// MARK: - Mocks

internal struct LogEvent {
    let level: Logger.Level
    let message: Logger.Message
    let source: String
}

internal struct LogEventRecorder {
    let _recordedEvents = NIOLockedValueBox<[LogEvent]>([])

    var recordedEvents: [LogEvent] {
        self._recordedEvents.withLockedValue { $0 }
    }

    func record(_ event: LogEvent) {
        self._recordedEvents.withLockedValue { $0.append(event) }
    }
}

internal struct MockLogHandler: LogHandler {
    let recorder: LogEventRecorder

    init(recorder: LogEventRecorder) {
        self.recorder = recorder
    }

    func log(
        level: Logger.Level,
        message: Logger.Message,
        metadata: Logger.Metadata?,
        source: String,
        file: String,
        function: String,
        line: UInt
    ) {
        self.recorder.record(LogEvent(level: level, message: message, source: source))
    }

    private var _logLevel: Logger.Level?
    var logLevel: Logger.Level {
        get {
            // get from config unless set
            return self._logLevel ?? .debug
        }
        set {
            self._logLevel = newValue
        }
    }

    private var _metadataSet = false
    private var _metadata = Logger.Metadata() {
        didSet {
            self._metadataSet = true
        }
    }

    public var metadata: Logger.Metadata {
        get {
            return self._metadata
        }
        set {
            self._metadata = newValue
        }
    }

    subscript(metadataKey metadataKey: Logger.Metadata.Key) -> Logger.Metadata.Value? {
        get {
            return self._metadata[metadataKey]
        }
        set {
            self._metadata[metadataKey] = newValue
        }
    }
}

class MockTimerHandler: TimerHandler {
    let expectation: AsyncStream<Int64>
    private let expectationContinuation: AsyncStream<Int64>.Continuation

    init() {
        var expectationContinuation: AsyncStream<Int64>.Continuation!
        self.expectation = AsyncStream<Int64>(bufferingPolicy: .bufferingNewest(1)) { expectationContinuation = $0 }
        self.expectationContinuation = expectationContinuation
    }

    func recordNanoseconds(_ duration: Int64) {
        _ = self.expectationContinuation.yield(duration)
    }
}
