import Logging
import ServiceLifecycle

public final class KafkaTransactionalProducer: Service, Sendable {
    let producer: KafkaProducer

    private init(producer: KafkaProducer, config: KafkaTransactionalProducerConfiguration) async throws {
        self.producer = producer
        let client = try producer.client()
        try await client.initTransactions(timeout: config.transactionsTimeout)
    }

    /// Initialize a new ``KafkaTransactionalProducer``.
    ///
    /// This creates a producer without listening for events.
    ///
    /// - Parameter config: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Returns: The newly created ``KafkaProducer``.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public convenience init(
        config: KafkaTransactionalProducerConfiguration,
        logger: Logger
    ) async throws {
        let producer = try KafkaProducer(configuration: config, logger: logger)
        try await self.init(producer: producer, config: config)
    }

    /// Initialize a new ``KafkaTransactionalProducer`` and a ``KafkaProducerEvents`` asynchronous sequence.
    ///
    /// Use the asynchronous sequence to consume events.
    ///
    /// - Important: When the asynchronous sequence is deinited the producer will be shutdown and disallow sending more messages.
    /// Additionally, make sure to consume the asynchronous sequence otherwise the events will be buffered in memory indefinitely.
    ///
    /// - Parameter config: The ``KafkaProducerConfiguration`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfiguration`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Returns: A tuple containing the created ``KafkaProducer`` and the ``KafkaProducerEvents``
    /// `AsyncSequence` used for receiving message events.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public static func makeTransactionalProducerWithEvents(
        config: KafkaTransactionalProducerConfiguration,
        logger: Logger
    ) async throws -> (KafkaTransactionalProducer, KafkaProducerEvents) {
        let (producer, events) = try KafkaProducer.makeProducerWithEvents(
            configuration: config,
            logger: logger
        )

        let transactionalProducer = try await KafkaTransactionalProducer(producer: producer, config: config)

        return (transactionalProducer, events)
    }

    //
    public func withTransaction(_ body: @Sendable (KafkaTransaction) async throws -> Void) async throws {
        let transaction = try KafkaTransaction(
            client: try producer.client(),
            producer: self.producer
        )

        do { // need to think here a little bit how to abort transaction
            try await body(transaction)
            try await transaction.commit()
        } catch { // FIXME: maybe catch AbortTransaction?
            do {
                try await transaction.abort()
            } catch {
                // FIXME: that some inconsistent state
                // should we emit fatalError(..)
                // or propagate error as exception with isFatal flag?
            }
            throw error
        }
    }

    public func run() async throws {
        try await self.producer.run()
    }
}
