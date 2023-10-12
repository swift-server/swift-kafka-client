import Crdkafka
import Kafka
import Foundation
import NIOCore
import ServiceLifecycle
import Logging
import Benchmark

private let numOfMessages: UInt =  .init(getFromEnv("MESSAGES_NUMBER") ?? "3000000")!

private var uniqueTestTopic: String!
private var client: RDKafkaClientHolder!
private var testMessages: [KafkaProducerMessage<String, String>]!

let benchmarks = {
    Benchmark.defaultConfiguration = .init(
        metrics: [.wallClock, .cpuTotal, .allocatedResidentMemory] + BenchmarkMetric.arc,
        warmupIterations: 0,
        scalingFactor: .one,
        maxDuration: .seconds(5),
        maxIterations: 1
    )
    
    Benchmark.setup = {
        var basicConfigDict: [String: String] = [
            "bootstrap.servers": "\(kafkaHost):\(kafkaPort)",
            "broker.address.family" : "v4"
        ]
        
        client = RDKafkaClientHolder(configDictionary: basicConfigDict, type: .consumer)
        
        uniqueTestTopic = try client._createUniqueTopic(timeout: 10 * 1000)
        benchLog("Created topic \(uniqueTestTopic!)")

        benchLog("Generating \(numOfMessages) messages")
        testMessages = createTestMessages(topic: uniqueTestTopic, count: numOfMessages)
        benchLog("Finish generating \(numOfMessages) messages")
        
        var producerConfig: KafkaProducerConfiguration!
        
        producerConfig = KafkaProducerConfiguration(bootstrapBrokerAddresses: [bootstrapBrokerAddress()])
        producerConfig.broker.addressFamily = .v4

        let (producer, acks) = try KafkaProducer.makeProducerWithEvents(configuration: producerConfig, logger: logger)
        
        
        let serviceGroupConfiguration1 = ServiceGroupConfiguration(services: [producer], gracefulShutdownSignals: [.sigterm, .sigint], logger: logger)
        let serviceGroup1 = ServiceGroup(configuration: serviceGroupConfiguration1)
        
        try await withThrowingTaskGroup(of: Void.self) { group in
            benchLog("Start producing \(numOfMessages) messages")
            defer {
                benchLog("Finish producing")
            }
            // Run Task
            group.addTask {
                try await serviceGroup1.run()
            }
            
            // Producer Task
            group.addTask {
                try await sendAndAcknowledgeMessages(
                    producer: producer,
                    events: acks,
                    messages: testMessages
                )
            }
            
            // Wait for Producer Task to complete
            try await group.next()
            await serviceGroup1.triggerGracefulShutdown()
        }
    }
    
    Benchmark.teardown = {
        try? client._deleteTopic(uniqueTestTopic, timeout: -1)
    }
    
    Benchmark("SwiftKafkaConsumer") { benchmark in
        let uniqueGroupID = UUID().uuidString
        var consumerConfig = KafkaConsumerConfiguration(
            consumptionStrategy: .group(
                id: uniqueGroupID,
                topics: [uniqueTestTopic]
            ),
            bootstrapBrokerAddresses: [bootstrapBrokerAddress()]
        )
        consumerConfig.pollInterval = .milliseconds(1)
        consumerConfig.autoOffsetReset = .beginning
        consumerConfig.broker.addressFamily = .v4
        consumerConfig.pollInterval = .milliseconds(1)
        
        let consumer = try KafkaConsumer(
            configuration: consumerConfig,
            logger: logger
        )
        
        let serviceGroupConfiguration2 = ServiceGroupConfiguration(services: [consumer], gracefulShutdownSignals: [.sigterm, .sigint], logger: logger)
        let serviceGroup2 = ServiceGroup(configuration: serviceGroupConfiguration2)
        
        benchmark.startMeasurement()
        
        try await withThrowingTaskGroup(of: Void.self) { group in
            benchLog("Start consuming")
            defer {
                benchLog("Finish consuming")
            }
            // Run Task
            group.addTask {
                try await serviceGroup2.run()
            }
            
            // Second Consumer Task
            group.addTask {
                var ctr: UInt64 = 0
                var tmpCtr: UInt64 = 0
                let interval: UInt64 = Swift.max(UInt64(numOfMessages / 20), 1)
                let totalStartDate = Date.timeIntervalSinceReferenceDate
                var totalBytes: UInt64 = 0
                
                for try await record in consumer.messages {
                    ctr += 1
                    totalBytes += UInt64(record.value.readableBytes)
                    
                    tmpCtr += 1
                    if tmpCtr >= interval {
                        benchLog("read \(ctr * 100 / UInt64(numOfMessages))%")
                        tmpCtr = 0
                    }
                    if ctr >= numOfMessages {
                        break
                    }
                }
                let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
                let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
                benchLog("All read up to ctr: \(ctr), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec")
            }
            
            // Wait for second Consumer Task to complete
            try await group.next()
            // Shutdown the serviceGroup
            await serviceGroup2.triggerGracefulShutdown()
        }
        
        benchmark.stopMeasurement()
    }
    
    Benchmark("librdkafka")  { benchmark in
        let uniqueGroupID = UUID().uuidString
        let rdKafkaConsumerConfig: [String: String] = [
            "group.id": uniqueGroupID,
            "bootstrap.servers": "\(kafkaHost):\(kafkaPort)",
            "broker.address.family": "v4",
            "auto.offset.reset": "beginning"
        ]
        
        let consumer = RDKafkaClientHolder(configDictionary: rdKafkaConsumerConfig, type: .consumer)
        rd_kafka_poll_set_consumer(consumer.kafkaHandle)
        let subscriptionList = rd_kafka_topic_partition_list_new(1)
        defer {
            rd_kafka_topic_partition_list_destroy(subscriptionList)
        }
        rd_kafka_topic_partition_list_add(
            subscriptionList,
            uniqueTestTopic,
            RD_KAFKA_PARTITION_UA
        )
        rd_kafka_subscribe(consumer.kafkaHandle, subscriptionList)
        rd_kafka_poll(consumer.kafkaHandle, 0)
        
        var ctr: UInt64 = 0
        var tmpCtr: UInt64 = 0
        
        let interval: UInt64 = Swift.max(UInt64(numOfMessages / 20), 1)
        let totalStartDate = Date.timeIntervalSinceReferenceDate
        var totalBytes: UInt64 = 0
        
        benchmark.startMeasurement()

        while ctr < numOfMessages {
            guard let record = rd_kafka_consumer_poll(consumer.kafkaHandle, 0) else {
                try await Task.sleep(for: .milliseconds(1)) // set as defaulat pollInterval for swift-kafka
                continue
            }
            defer {
                rd_kafka_message_destroy(record)
            }
            ctr += 1
            totalBytes += UInt64(record.pointee.len)
            
            tmpCtr += 1
            if tmpCtr >= interval {
                benchLog("read \(ctr * 100 / UInt64(numOfMessages))%")
                tmpCtr = 0
            }
        }
        
        benchmark.stopMeasurement()
        
        rd_kafka_consumer_close(consumer.kafkaHandle)
        
        let timeIntervalTotal = Date.timeIntervalSinceReferenceDate - totalStartDate
        let avgRateMb = Double(totalBytes) / timeIntervalTotal / 1024
        benchLog("All read up to ctr: \(ctr), avgRate: (\(Int(avgRateMb))KB/s), timePassed: \(Int(timeIntervalTotal))sec")
    }
}
