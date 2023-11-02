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
import Kafka
import KafkaTestUtils
import Foundation
import NIOCore
import ServiceLifecycle
import Logging
import Benchmark
import SwiftKafkaBenchmarkUtils

private var uniqueTestTopic: String!
private var client: TestRDKafkaClient!

private func withHeaders<T>(_ headers: [KafkaHeader], startHeaderIndex: Int? = nil, _ body: (UnsafePointer<CChar>, UnsafeRawBufferPointer?) throws -> Void, finalizeClosure: () throws -> T) rethrows -> T {
    var index = startHeaderIndex ?? 0
    guard index < headers.count else {
        return try finalizeClosure()
    }
    return try headers[index].key.withCString { keyBuffer in
        if let value = headers[index].value {
            return try value.withUnsafeReadableBytes { valueBuffer in
                try body(keyBuffer, valueBuffer)
                return try withHeaders(headers, startHeaderIndex: index + 1, body, finalizeClosure: finalizeClosure)
            }
        } else {
            try body(keyBuffer, nil)
            return try withHeaders(headers, startHeaderIndex: index + 1, body, finalizeClosure: finalizeClosure)
        }
    }
}

let benchmarks = {
    Benchmark.defaultConfiguration = .init(
        metrics: [.wallClock, .cpuTotal, .allocatedResidentMemory],
        warmupIterations: 0,
        scalingFactor: .one,
        maxDuration: .seconds(5),
        maxIterations: 1
    )
    
    Benchmark.setup = {
        let basicConfig = TestRDKafkaClient._createDummyConfig(bootstrapAddresses: bootstrapBrokerAddress())
        client = try TestRDKafkaClient._makeRDKafkaClient(config: basicConfig)
        
        uniqueTestTopic = try client._createUniqueTopic(timeout: 10 * 1000)
        benchLog("Created topic \(uniqueTestTopic!)")
    }
    
    Benchmark.teardown = {
        try? client._deleteTopic(uniqueTestTopic, timeout: -1)
        client = nil
    }
    
    Benchmark("SwiftKafkaProducer") { benchmark in
        let testMessages = KafkaTestMessages.create(topic: uniqueTestTopic, count: numOfMessages)

        var producerConfig: KafkaProducerConfiguration!
        
        producerConfig = KafkaProducerConfiguration(bootstrapBrokerAddresses: [bootstrapBrokerAddress()])
        producerConfig.broker.addressFamily = .v4

        let (producer, acks) = try KafkaProducer.makeProducerWithEvents(configuration: producerConfig, logger: logger)
        
        
        let serviceGroupConfiguration1 = ServiceGroupConfiguration(services: [producer], gracefulShutdownSignals: [.sigterm, .sigint], logger: logger)
        let serviceGroup1 = ServiceGroup(configuration: serviceGroupConfiguration1)
        
        benchmark.startMeasurement()
        try await withThrowingTaskGroup(of: Void.self) { group in
            benchLog("Start producing \(numOfMessages) messages")
            defer {
                benchLog("Finish producing")
            }
            // Run Task
            group.addTask {
                try await serviceGroup1.run()
            }
            
            group.addTask {
                var receivedDeliveryReportsCtr = 0
                var prevPercent = 0
                
                for await event in acks {
                    switch event {
                    case .deliveryReports(let deliveryReports):
                        receivedDeliveryReportsCtr += deliveryReports.count
                    default:
                        break // Ignore any other events
                    }
                    let curPercent = receivedDeliveryReportsCtr * 100 / testMessages.count
                    if curPercent >= prevPercent + 10 {
                        benchLog("Delivered \(curPercent)% of messages")
                        prevPercent = curPercent
                    }
                    
                    if receivedDeliveryReportsCtr >= testMessages.count {
                        break
                    }
                }
            }
            
            // Producer Task
            group.addTask {
                for message in testMessages {
                    while true { // Note: this is an example of queue full
                        do {
                            try producer.send(message)
                            break
                        } catch let error as KafkaError where error.description.contains("Queue full") {
                            try await Task.sleep(for: .milliseconds(10))
                            continue
                        } catch {
                            benchLog("Caught some error: \(error)")
                            throw error
                        }
                    }
                }
            }
            
            // Wait for Producer Task to complete
            try await group.next()
            try await group.next()
            await serviceGroup1.triggerGracefulShutdown()
        }
        benchmark.stopMeasurement()
    }
    
    Benchmark("SwiftKafkaProducer with headers") { benchmark in
        let testMessages = KafkaTestMessages.create(topic: uniqueTestTopic, headers: KafkaTestMessages.createHeaders(), count: numOfMessages)
        var producerConfig: KafkaProducerConfiguration!
        
        producerConfig = KafkaProducerConfiguration(bootstrapBrokerAddresses: [bootstrapBrokerAddress()])
        producerConfig.broker.addressFamily = .v4

        let (producer, acks) = try KafkaProducer.makeProducerWithEvents(configuration: producerConfig, logger: logger)
        
        
        let serviceGroupConfiguration1 = ServiceGroupConfiguration(services: [producer], gracefulShutdownSignals: [.sigterm, .sigint], logger: logger)
        let serviceGroup1 = ServiceGroup(configuration: serviceGroupConfiguration1)
        
        benchmark.startMeasurement()
        try await withThrowingTaskGroup(of: Void.self) { group in
            benchLog("Start producing \(numOfMessages) messages")
            defer {
                benchLog("Finish producing")
            }
            // Run Task
            group.addTask {
                try await serviceGroup1.run()
            }
            
            group.addTask {
                var receivedDeliveryReportsCtr = 0
                var prevPercent = 0
                
                for await event in acks {
                    switch event {
                    case .deliveryReports(let deliveryReports):
                        receivedDeliveryReportsCtr += deliveryReports.count
                    default:
                        break // Ignore any other events
                    }
                    let curPercent = receivedDeliveryReportsCtr * 100 / testMessages.count
                    if curPercent >= prevPercent + 10 {
                        benchLog("Delivered \(curPercent)% of messages")
                        prevPercent = curPercent
                    }
                    
                    if receivedDeliveryReportsCtr >= testMessages.count {
                        break
                    }
                }
            }
            
            // Producer Task
            group.addTask {
                for message in testMessages {
                    while true { // Note: this is an example of queue full
                        do {
                            try producer.send(message)
                            break
                        } catch let error as KafkaError where error.description.contains("Queue full") {
                            try await Task.sleep(for: .milliseconds(10))
                            continue
                        } catch {
                            benchLog("Caught some error: \(error)")
                            throw error
                        }
                    }
                }
            }
            
            // Wait for Producer Task to complete
            try await group.next()
            try await group.next()
            await serviceGroup1.triggerGracefulShutdown()
        }
        benchmark.stopMeasurement()
    }
    
    
    Benchmark("librdkafka producer")  { benchmark in
        let testMessages = KafkaTestMessages.create(topic: uniqueTestTopic, count: numOfMessages)
        var producerConfig: KafkaProducerConfiguration!

        let uniqueGroupID = UUID().uuidString
        let rdKafkaProducerConfig = TestRDKafkaClient._createDummyConfig(bootstrapAddresses: bootstrapBrokerAddress(), consumer: false)
        
        let producer = try TestRDKafkaClient._makeRDKafkaClient(config: rdKafkaProducerConfig, consumer: false)
        try await producer.withKafkaHandlePointer { kafkaHandle in
            let queue = rd_kafka_queue_get_main(kafkaHandle)
            defer { rd_kafka_queue_destroy(queue) }

            let topicHandle = rd_kafka_topic_new(
                kafkaHandle,
                uniqueTestTopic,
                nil
            )
            defer { rd_kafka_topic_destroy(topicHandle) }
            benchmark.startMeasurement()
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    var messagesSent = 0
                    while messagesSent < numOfMessages {
                        let event = rd_kafka_queue_poll(kafkaHandle, 0)
                        defer { rd_kafka_event_destroy(event) }
                        
                        guard let event else {
                            try await Task.sleep(for: .milliseconds(10))
                            continue
                        }
                        
                        let rdEventType = rd_kafka_event_type(event)
                        if rdEventType == RD_KAFKA_EVENT_DR {
                            messagesSent += rd_kafka_event_message_count(event)
                        }
                    }
                }
                group.addTask {
                    for message in testMessages {
                        while true {
                            let result = message.value.withUnsafeBytes { valueBuffer in
                                message.key!.withUnsafeBytes { keyBuffer in
                                    rd_kafka_produce(
                                        topicHandle,
                                        Int32(message.partition.rawValue),
                                        RD_KAFKA_MSG_F_COPY,
                                        UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress),
                                        valueBuffer.count,
                                        keyBuffer.baseAddress,
                                        keyBuffer.count,
                                        nil
                                    )
                                }
                            }
                            if rd_kafka_resp_err_t(result) != RD_KAFKA_RESP_ERR_NO_ERROR {
                                rd_kafka_flush(kafkaHandle, 10)
                                continue
                            }
                            break
                        }
                    }
                }
                try await group.waitForAll()
            }
            benchmark.stopMeasurement()
        }
    }

    Benchmark("librdkafka producer with headers")  { benchmark in
        let testMessages = KafkaTestMessages.create(topic: uniqueTestTopic, headers: KafkaTestMessages.createHeaders(), count: numOfMessages)
        var producerConfig: KafkaProducerConfiguration!

        let uniqueGroupID = UUID().uuidString
        let rdKafkaProducerConfig = TestRDKafkaClient._createDummyConfig(bootstrapAddresses: bootstrapBrokerAddress(), consumer: false)
        
        let producer = try TestRDKafkaClient._makeRDKafkaClient(config: rdKafkaProducerConfig, consumer: false)
        try await producer.withKafkaHandlePointer { kafkaHandle in
            let queue = rd_kafka_queue_get_main(kafkaHandle)
            defer { rd_kafka_queue_destroy(queue) }

            let topicHandle = rd_kafka_topic_new(
                kafkaHandle,
                uniqueTestTopic,
                nil
            )
            defer { rd_kafka_topic_destroy(topicHandle) }
            benchmark.startMeasurement()
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    var messagesSent = 0
                    while messagesSent < numOfMessages {
                        let event = rd_kafka_queue_poll(kafkaHandle, 0)
                        defer { rd_kafka_event_destroy(event) }
                        
                        guard let event else {
                            try await Task.sleep(for: .milliseconds(10))
                            continue
                        }
                        
                        let rdEventType = rd_kafka_event_type(event)
                        if rdEventType == RD_KAFKA_EVENT_DR {
                            messagesSent += rd_kafka_event_message_count(event)
                        }
                    }
                }
                group.addTask {
                    for message in testMessages {
                        message.value.withUnsafeBytes { valueBuffer in
                            message.key!.withUnsafeBytes { keyBuffer in
                                let sizeWithoutHeaders = 5
                                let size = sizeWithoutHeaders + message.headers.count
                                var arguments = Array(repeating: rd_kafka_vu_t(), count: size)
                                var index = 0

                                arguments[index].vtype = RD_KAFKA_VTYPE_RKT
                                arguments[index].u.rkt = topicHandle
                                index += 1

                                arguments[index].vtype = RD_KAFKA_VTYPE_PARTITION
                                arguments[index].u.i32 = Int32(message.partition.rawValue)
                                index += 1

                                arguments[index].vtype = RD_KAFKA_VTYPE_MSGFLAGS
                                arguments[index].u.i = RD_KAFKA_MSG_F_COPY
                                index += 1

                                arguments[index].vtype = RD_KAFKA_VTYPE_KEY
                                arguments[index].u.mem.ptr = UnsafeMutableRawPointer(mutating: keyBuffer.baseAddress)
                                arguments[index].u.mem.size = keyBuffer.count
                                index += 1

                                arguments[index].vtype = RD_KAFKA_VTYPE_VALUE
                                arguments[index].u.mem.ptr = UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress)
                                arguments[index].u.mem.size = valueBuffer.count
                                index += 1

                                return withHeaders(message.headers) { key, value in
                                    arguments[index].vtype = RD_KAFKA_VTYPE_HEADER

                                    arguments[index].u.header.name = key
                                    arguments[index].u.header.val = value?.baseAddress
                                    arguments[index].u.header.size = value?.count ?? 0

                                    index += 1
                                } finalizeClosure: {
                                    assert(arguments.count == size)
                                    while true {
                                        let result = rd_kafka_produceva(
                                            kafkaHandle,
                                            arguments,
                                            arguments.count
                                        )
                                        if rd_kafka_error_code(result) != RD_KAFKA_RESP_ERR_NO_ERROR {
                                            rd_kafka_flush(kafkaHandle, 10)
                                            continue
                                        }
                                        break
                                    }
                                }
                            }
                        }
                    }
                }
                try await group.waitForAll()
            }
            benchmark.stopMeasurement()
        }
    }
}
