

public final class KafkaTransaction {
    let client: RDKafkaClient
    let producer: KafkaProducer
    let config: KafkaTransactionalProducerConfiguration
    
    init(client: RDKafkaClient, producer: KafkaProducer, config: KafkaTransactionalProducerConfiguration) throws {
        self.client = client
        self.producer = producer
        self.config = config

        try client.beginTransaction()
    }
    
    deinit {
    }

    public func send(
        offsets: RDKafkaTopicPartitionList,
        forConsumer consumer: KafkaConsumer,
        timeout: Duration = .kafkaUntilEndOfTransactionTimeout,
        attempts: UInt64 = .max
    ) async throws {
        let consumerClient = try consumer.client()
        try await consumerClient.withKafkaHandlePointer {
            try await client.send(attempts: attempts, offsets: offsets, forConsumerKafkaHandle: $0, timeout: timeout)
        }
    }
    
    @discardableResult
    public func send<Key, Value>(_ message: KafkaProducerMessage<Key, Value>) throws -> KafkaProducerMessageID {
        try self.producer.send(message)
    }
    
    func commit() async throws {
        try await client.commitTransaction(attempts: .max, timeout: .kafkaUntilEndOfTransactionTimeout)
    }
    
    func abort() async throws {
        try await client.abortTransaction(attempts: .max, timeout: .kafkaUntilEndOfTransactionTimeout)
    }
}
