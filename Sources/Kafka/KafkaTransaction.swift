import Logging

public final class KafkaTransaction {
    let client: RDKafkaClient
    let producer: KafkaProducer
    let logger: Logger
    
    var offsetSend = 0
    var offsetNum = 0
    var sendTries = 0
    var msgNum = 0
    var totalBytes = 0
    var firstRetryFound = false

    init(client: RDKafkaClient, producer: KafkaProducer, logger: Logger) throws {
        self.client = client
        self.producer = producer
        self.logger = logger

        try client.beginTransaction()
    }

    deinit {
        self.logger.info("Destructing transaction msgNum: \(msgNum), offsetSend: \(offsetSend), offsetNum: \(offsetNum), totalBytes: \(totalBytes), sendTries: \(sendTries)")
    }

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
        if !firstRetryFound && sendTries == msgNum + 1 {
            firstRetryFound = true
            self.logger.info("retry found on sending message \(msgNum + 1)")
        }
        sendTries += 1
//        self.logger.info("Sending message \(message)")
        let id = try self.producer.send(message)
//        self.logger.info("sent message \(msgNum + 1)")
        totalBytes += message.value.withUnsafeBytes({ $0.count })
        msgNum += 1
        return id
    }
    
    
    public func flush(timeout: Duration) async {
        self.logger.info("Flushing transaction msgNum: \(msgNum), offsetSend: \(offsetSend), offsetNum: \(offsetNum), totalBytes: \(totalBytes), sendTries: \(sendTries)")
        await self.producer.flush(timeout: timeout)
    }


    func commit() async throws {
        self.logger.info("Committing transaction msgNum: \(msgNum), offsetSend: \(offsetSend), offsetNum: \(offsetNum), totalBytes: \(totalBytes), sendTries: \(sendTries)")
        try await self.client.commitTransaction(attempts: .max, timeout: .kafkaUntilEndOfTransactionTimeout)
    }

    func abort() async throws {
        self.logger.info("Aborting transaction msgNum: \(msgNum), offsetSend: \(offsetSend), offsetNum: \(offsetNum), totalBytes: \(totalBytes), sendTries: \(sendTries)")
//        self.logger.info("Aborting transaction")
        try await self.client.abortTransaction(attempts: .max, timeout: .kafkaUntilEndOfTransactionTimeout)
    }
}
