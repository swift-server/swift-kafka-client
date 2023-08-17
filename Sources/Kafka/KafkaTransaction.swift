import Logging

public final class KafkaTransaction {
    let client: RDKafkaClient
    let producer: KafkaProducer
//    let logger: Logger
    
    var offsetSend = 0
    var offsetNum = 0
    var msgNum = 0

    init(client: RDKafkaClient, producer: KafkaProducer, logger: Logger) throws {
        self.client = client
        self.producer = producer
//        self.logger = logger

        try client.beginTransaction()
    }

    deinit {}

    public func send(
        offsets: KafkaTopicList,
        forConsumer consumer: KafkaConsumer,
        timeout: Duration = .kafkaUntilEndOfTransactionTimeout,
        attempts: UInt64 = .max
    ) async throws {
        let consumerClient = try consumer.client()
        try await consumerClient.withKafkaHandlePointer {
            offsetNum += offsets.list.count
            offsetSend += 1
//            self.logger.info("Sending offsets \(offsets)")
            try await self.client.send(attempts: attempts, offsets: offsets.list, forConsumerKafkaHandle: $0, timeout: timeout)
        }
    }

    @discardableResult
    public func send<Key, Value>(_ message: KafkaProducerMessage<Key, Value>) throws -> KafkaProducerMessageID {
//        self.logger.info("Sending message \(message)")
        msgNum += 1
        return try self.producer.send(message)
    }

    func commit() async throws {
//        self.logger.info("Committing transaction msgNum: \(msgNum), offsetSend: \(offsetSend), offsetNum: \(offsetNum)")
        try await self.client.commitTransaction(attempts: .max, timeout: .kafkaUntilEndOfTransactionTimeout)
    }

    func abort() async throws {
//        self.logger.info("Aborting transaction")
        try await self.client.abortTransaction(attempts: .max, timeout: .kafkaUntilEndOfTransactionTimeout)
    }
}
