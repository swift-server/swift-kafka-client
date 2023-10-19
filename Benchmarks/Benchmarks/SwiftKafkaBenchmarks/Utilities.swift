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

extension Logger {
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

let logger: Logger = .testLogger
let stringSize = 1024
let kafkaHost: String = kafkaHostFromEnv()
let kafkaPort: Int = kafkaPortFromEnv()

func benchLog(_ msg: @autoclosure () -> Logger.Message) {
//    Just in case for debug
    #if DEBUG
    logger.debug(msg())
    #endif
}

//enum RDKafkaClientHolderError: Error {
//    case generic(String)
//}
//
///// ``RDKafkaClientHolder`` is a basic wrapper that automatically destroys kafka handle
//class RDKafkaClientHolder {
//    let kafkaHandle: OpaquePointer
//    
//    enum HandleType {
//        case producer
//        case consumer
//    }
//    
//    init(configDictionary: [String: String], type: HandleType) {
//        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: stringSize)
//        defer { errorChars.deallocate() }
//
//        let config: OpaquePointer = rd_kafka_conf_new()
//        configDictionary.forEach { key, value in
//            let res = rd_kafka_conf_set(
//                config,
//                key,
//                value,
//                errorChars,
//                stringSize
//            )
//            if res != RD_KAFKA_CONF_OK {
//                fatalError("Could not set \(key) with \(value)")
//            }
//        }
//        
//        guard let handle = rd_kafka_new(
//            type == .consumer ? RD_KAFKA_CONSUMER : RD_KAFKA_PRODUCER,
//            config,
//            errorChars,
//            stringSize
//        ) else {
//            fatalError("Could not create client")
//        }
//        self.kafkaHandle = handle
//    }
//    
//    deinit {
//        rd_kafka_poll(self.kafkaHandle, 0)
//        rd_kafka_destroy(self.kafkaHandle)
//    }
//}


func getFromEnv(_ key: String) -> String? {
    ProcessInfo.processInfo.environment[key]
}

func kafkaHostFromEnv() -> String {
    getFromEnv("KAFKA_HOST") ?? "localhost"
}

func kafkaPortFromEnv() -> Int {
    .init(getFromEnv("KAFKA_PORT") ?? "9092")!
}

func bootstrapBrokerAddress() -> KafkaConfiguration.BrokerAddress {
    .init(
        host: kafkaHost,
        port: kafkaPort
    )
}
