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
import Foundation
import Kafka
import KafkaTestUtils
import Logging

public extension Logger {
    static var testLogger: Logger {
        var logger = Logger(label: "bench_log")
        #if DEBUG
            logger.logLevel = .debug
        #else
            logger.logLevel = .info
        #endif
        return logger
    }
}

public let logger: Logger = .testLogger
public let kafkaHost: String = kafkaHostFromEnv()
public let kafkaPort: Int = kafkaPortFromEnv()
public let numOfMessages: UInt = .init(getFromEnv("MESSAGES_NUMBER") ?? "500000")!


public func benchLog(_ msg: @autoclosure () -> Logger.Message) {
//    Just in case for debug
    #if DEBUG
    logger.debug(msg())
    #endif
}

public func getFromEnv(_ key: String) -> String? {
    ProcessInfo.processInfo.environment[key]
}

public func kafkaHostFromEnv() -> String {
    getFromEnv("KAFKA_HOST") ?? "localhost"
}

public func kafkaPortFromEnv() -> Int {
    .init(getFromEnv("KAFKA_PORT") ?? "9092")!
}

public func bootstrapBrokerAddress() -> KafkaConfiguration.BrokerAddress {
    .init(
        host: kafkaHost,
        port: kafkaPort
    )
}
