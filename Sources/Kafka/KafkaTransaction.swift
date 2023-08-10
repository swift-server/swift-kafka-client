

public final class KafkaTransaction {
    let client: RDKafkaClient
    let producer: KafkaProducer

    init(client: RDKafkaClient, producer: KafkaProducer) throws {
        self.client = client
        self.producer = producer

        try client.beginTransaction()
    }

    deinit {}

    public func send(
        offsets: RDKafkaTopicPartitionList,
        forConsumer consumer: KafkaConsumer,
        timeout: Duration = .kafkaUntilEndOfTransactionTimeout,
        attempts: UInt64 = .max
    ) async throws {
        let consumerClient = try consumer.client()
        try await consumerClient.withKafkaHandlePointer {
            try await self.client.send(attempts: attempts, offsets: offsets, forConsumerKafkaHandle: $0, timeout: timeout)
        }
    }

    @discardableResult
    public func send<Key, Value>(_ message: KafkaProducerMessage<Key, Value>) throws -> KafkaProducerMessageID {
        try self.producer.send(message)
    }

    func commit() async throws {
        try await self.client.commitTransaction(attempts: .max, timeout: .kafkaUntilEndOfTransactionTimeout)
    }

    func abort() async throws {
        try await self.client.abortTransaction(attempts: .max, timeout: .kafkaUntilEndOfTransactionTimeout)
    }
}
