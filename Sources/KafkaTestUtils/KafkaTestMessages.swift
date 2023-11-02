import Kafka
import struct Foundation.Date
import Logging

public struct KafkaTestMessages {
    public static func sendAndAcknowledge(
        producer: KafkaProducer,
        events: KafkaProducerEvents,
        messages: [KafkaProducerMessage<String, String>],
        logger: Logger = .kafkaTest
    ) async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for message in messages {
                    while true { // Note: this is an example of queue full
                        do {
                            try producer.send(message)
                            break
                        } catch let error as KafkaError where error.description.contains("Queue full") {
                            try await Task.sleep(for: .milliseconds(10))
                            continue
                        } catch {
                            logger.error("Caught some error: \(error)")
                            throw error
                        }
                    }
                }
            }

            group.addTask {
                var receivedDeliveryReportsCtr = 0
                var prevPercent = 0
                
                for await event in events {
                    switch event {
                    case .deliveryReports(let deliveryReports):
                        receivedDeliveryReportsCtr += deliveryReports.count
                    default:
                        break // Ignore any other events
                    }
                    let curPercent = receivedDeliveryReportsCtr * 100 / messages.count
                    if curPercent >= prevPercent + 10 {
                        logger.debug("Delivered \(curPercent)% of messages")
                        prevPercent = curPercent
                    }
                    
                    if receivedDeliveryReportsCtr >= messages.count {
                        break
                    }
                }
            }
            try await group.waitForAll()
        }
    }
    
    public static func create(
        topic: String,
        headers: [KafkaHeader] = [],
        count: UInt
    ) -> [KafkaProducerMessage<String, String>] {
        return Array(0..<count).map {
            KafkaProducerMessage(
                topic: topic,
                headers: headers,
                key: "key \($0)",
                value: "Hello, World! \($0) - \(Date().description)"
            )
        }
    }
    
    public static func createHeaders(count: Int = 10) -> [KafkaHeader] {
        return Array(0..<count).map { idx in
            "\(idx.hashValue)".withUnsafeBytes { value in
                .init(key: "\(idx)", value: .init(bytes: value))
            }
        }
    }
}
