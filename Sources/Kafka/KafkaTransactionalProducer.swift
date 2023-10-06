import Logging
import ServiceLifecycle
import Atomics

public final class KafkaTransactionalProducer: Service, Sendable {
    private let producer: KafkaProducer
    private let logger: Logger
    
    private let id: ManagedAtomic<Int> = .init(0)

    private init(producer: KafkaProducer, config: KafkaTransactionalProducerConfiguration, logger: Logger) async throws {
        self.producer = producer
        self.logger = logger
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
        try await self.init(producer: producer, config: config, logger: logger)
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

        let transactionalProducer = try await KafkaTransactionalProducer(producer: producer, config: config, logger: logger)

        return (transactionalProducer, events)
    }

    //
    public func withTransaction(_ body: @Sendable (KafkaTransaction) async throws -> Void) async throws {
        let id = id.loadThenWrappingIncrement(ordering: .relaxed)
        var logger = Logger(label: "Transaction \(id)")
        logger.logLevel = self.logger.logLevel

        logger.debug("Begin txn \(id)")
        defer {
            logger.debug("End txn \(id)")
        }
        let transaction = try KafkaTransaction(
            client: try producer.client(),
            producer: self.producer,
            logger: logger
        )

        do { // need to think here a little bit how to abort transaction
            logger.debug("Fill the transaction \(id)")
            try await body(transaction)
            logger.debug("Committing the transaction \(id)")
            try await transaction.commit()
        } catch let error as KafkaError  where error.code == .transactionAborted {
            logger.debug("Transaction aborted by librdkafa")
            throw error // transaction already aborted
        } catch { // FIXME: maybe catch AbortTransaction?
            logger.debug("Caught error for transaction \(id): \(error), aborting")
            do {
                try await transaction.abort()
                logger.debug("Transaction \(id) aborted")
            } catch {
                logger.debug("Failed to perform abort for transaction \(id): \(error)")
                // FIXME: that some inconsistent state
                // should we emit fatalError(..)
                // or propagate error as exception with isFgsatal flag?
            }
            throw error
        }
    }

    public func run() async throws {
        try await self.producer.run()
    }
}
