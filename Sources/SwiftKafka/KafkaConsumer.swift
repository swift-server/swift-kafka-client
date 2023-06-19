//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka
import Logging
import NIOConcurrencyHelpers

// TODO: move other stuff also to RDKafka
// TODO: remove backpressure option in config

/// `AsyncSequence` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
public struct ConsumerMessagesAsyncSequence: AsyncSequence {
    public typealias Element = Result<KafkaConsumerMessage, KafkaError>
    internal let stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>

    /// `AsynceIteratorProtocol` implementation for handling messages received from the Kafka cluster (``KafkaConsumerMessage``).
    public struct ConsumerMessagesAsyncIterator: AsyncIteratorProtocol {
        class _Internal {
            private let _stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>
            init(stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>) {
                self._stateMachine = stateMachine
            }

            deinit {
                self.shutdownGracefully()
            }

            internal func next() async -> Element? {
                let messageResult: Result<KafkaConsumerMessage, KafkaError>
                while !Task.isCancelled {
                    let nextAction = self._stateMachine.withLockedValue { $0.nextPollAction() }
                    switch nextAction {
                    case .pollForMessage(let client, let logger):
                        do {
                            // TODO: timeout
                            guard let message = try await client.consumerPoll() else { // TODO: pollInterval here
                                continue
                            }
                            messageResult = .success(message)
                        } catch let kafkaError as KafkaError {
                            messageResult = .failure(kafkaError)
                        } catch {
                            logger.error("KafkaConsumer caught error: \(error)")
                            continue
                        }
                        return messageResult
                    case .none:
                        return nil
                    }
                }
                return nil // Returning nil ends the sequence
            }

            private func shutdownGracefully() {
                let action = self._stateMachine.withLockedValue { $0.finish() }
                switch action {
                case .shutdownGracefully(let client, let subscribedTopicsPointer, let logger):
                    KafkaConsumer.shutdownGracefully(
                        client: client,
                        subscribedTopicsPointer: subscribedTopicsPointer,
                        logger: logger
                    )
                case .none:
                    return
                }
            }
        }

        private let _internal: _Internal

        init(stateMachine: NIOLockedValueBox<KafkaConsumer.StateMachine>) {
            self._internal = .init(stateMachine: stateMachine)
        }

        public func next() async -> Element? {
            await self._internal.next()
        }
    }

    public func makeAsyncIterator() -> ConsumerMessagesAsyncIterator {
        return ConsumerMessagesAsyncIterator(stateMachine: self.stateMachine)
    }
}

/// Receive messages from the Kafka cluster.
public final class KafkaConsumer {
    /// The configuration object of the consumer client.
    private var config: KafkaConsumerConfiguration
    /// State of the `KafkaConsumer`.
    private let stateMachine: NIOLockedValueBox<StateMachine>

    /// `AsyncSequence` that returns all ``KafkaConsumerMessage`` objects that the consumer receives.
    public let messages: ConsumerMessagesAsyncSequence

    /// Initialize a new ``KafkaConsumer``.
    /// To listen to incoming messages, please subscribe to a list of topics using ``subscribe(topics:)``
    /// or assign the consumer to a particular topic + partition pair using ``assign(topic:partition:offset:)``.
    /// - Parameter config: The ``KafkaConsumerConfiguration`` for configuring the ``KafkaConsumer``.
    /// - Parameter logger: A logger.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    public init(
        config: KafkaConsumerConfiguration,
        logger: Logger
    ) throws {
        self.config = config

        let client = try RDKafka.createClient(type: .consumer, configDictionary: config.dictionary, logger: logger)

        guard let subscribedTopicsPointer = rd_kafka_topic_partition_list_new(1) else {
            throw KafkaError.client(reason: "Failed to allocate Topic+Partition list.")
        }

        self.stateMachine = NIOLockedValueBox(StateMachine(state: .initializing(client: client, subscribedTopicsPointer: subscribedTopicsPointer, logger: logger)))
        self.messages = ConsumerMessagesAsyncSequence(stateMachine: self.stateMachine)

        // Events that would be triggered by rd_kafka_poll
        // will now be also triggered by rd_kafka_consumer_poll
        let result = client.withKafkaHandlePointer { handle in
            rd_kafka_poll_set_consumer(handle)
        }
        guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.rdKafkaError(wrapping: result)
        }

        switch config.consumptionStrategy._internal {
        case .partition(topic: let topic, partition: let partition, offset: let offset):
            try self.assign(topic: topic, partition: partition, offset: offset)
        case .group(groupID: _, topics: let topics):
            try self.subscribe(topics: topics)
        }
    }

    deinit {
        // This occurs e.g. when for await loop is exited through break
        self.shutdownGracefully()
    }

    /// Subscribe to the given list of `topics`.
    /// The partition assignment happens automatically using `KafkaConsumer`'s consumer group.
    /// - Parameter topics: An array of topic names to subscribe to.
    /// - Throws: A ``KafkaError`` if subscribing to the topic list failed.
    private func subscribe(topics: [String]) throws {
        let action = self.stateMachine.withLockedValue { $0.setUpConnection() }
        switch action {
        case .setUpConnection(let client, let subscribedTopicsPointer):
            for topic in topics {
                rd_kafka_topic_partition_list_add(
                    subscribedTopicsPointer,
                    topic,
                    KafkaPartition.unassigned.rawValue
                )
            }

            let result = client.withKafkaHandlePointer { handle in
                rd_kafka_subscribe(handle, subscribedTopicsPointer)
            }

            guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: result)
            }
        }
    }

    /// Assign the``KafkaConsumer`` to a specific `partition` of a `topic`.
    /// - Parameter topic: Name of the topic that this ``KafkaConsumer`` will read from.
    /// - Parameter partition: Partition that this ``KafkaConsumer`` will read from.
    /// - Parameter offset: The topic offset where reading begins. Defaults to the offset of the last read message.
    /// - Throws: A ``KafkaError`` if the consumer could not be assigned to the topic + partition pair.
    private func assign(
        topic: String,
        partition: KafkaPartition,
        offset: Int
    ) throws {
        let action = self.stateMachine.withLockedValue { $0.setUpConnection() }
        switch action {
        case .setUpConnection(let client, let subscribedTopicsPointer):
            guard let partitionPointer = rd_kafka_topic_partition_list_add(
                subscribedTopicsPointer,
                topic,
                partition.rawValue
            ) else {
                fatalError("rd_kafka_topic_partition_list_add returned invalid pointer")
            }

            partitionPointer.pointee.offset = Int64(offset)

            let result = client.withKafkaHandlePointer { handle in
                rd_kafka_assign(handle, subscribedTopicsPointer)
            }

            guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: result)
            }
        }
    }

    /// Mark `message` in the topic as read and request the next message from the topic.
    /// This method is only used for manual offset management.
    /// - Parameter message: Last received message that shall be marked as read.
    /// - Throws: A ``KafkaError`` if committing failed.
    /// - Warning: This method fails if the `enable.auto.commit` configuration property is set to `true`.
    public func commitSync(_ message: KafkaConsumerMessage) async throws {
        try await withCheckedThrowingContinuation { continuation in
            do {
                try self._commitSync(message) // Blocks until commiting the offset is done
                continuation.resume()
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }

    private func _commitSync(_ message: KafkaConsumerMessage) throws {
        let action = self.stateMachine.withLockedValue { $0.commitSync() }
        switch action {
        case .throwClosedError:
            throw KafkaError.connectionClosed(reason: "Tried to commit message offset on a closed consumer")
        case .commitSync(let client):
            guard self.config.enableAutoCommit == false else {
                throw KafkaError.config(reason: "Committing manually only works if enable.auto.commit is set to false")
            }

            let changesList = rd_kafka_topic_partition_list_new(1)
            defer { rd_kafka_topic_partition_list_destroy(changesList) }
            guard let partitionPointer = rd_kafka_topic_partition_list_add(
                changesList,
                message.topic,
                message.partition.rawValue
            ) else {
                fatalError("rd_kafka_topic_partition_list_add returned invalid pointer")
            }

            // The offset committed is always the offset of the next requested message.
            // Thus, we increase the offset of the current message by one before committing it.
            // See: https://github.com/edenhill/librdkafka/issues/2745#issuecomment-598067945
            partitionPointer.pointee.offset = Int64(message.offset + 1)
            let result = client.withKafkaHandlePointer { handle in
                rd_kafka_commit(
                    handle,
                    changesList,
                    0
                ) // Blocks until commiting the offset is done
            }
            guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.rdKafkaError(wrapping: result)
            }
        }
    }

    /// This function is used to gracefully shut down a Kafka consumer client.
    ///
    /// - Note: Invoking this function is not always needed as the ``KafkaConsumer``
    /// will already shut down when consumption of the ``ConsumerMessagesAsyncSequence`` has ended.
    public func shutdownGracefully() {
        let action = self.stateMachine.withLockedValue { $0.finish() }
        switch action {
        case .shutdownGracefully(let client, let subscribedTopicsPointer, let logger):
            KafkaConsumer.shutdownGracefully(
                client: client,
                subscribedTopicsPointer: subscribedTopicsPointer,
                logger: logger
            )
        case .none:
            return
        }
    }
}

// MARK: - KafkaConsumer + static shutdownGracefully

extension KafkaConsumer {
    /// This function is used to gracefully shut down a Kafka consumer client.
    fileprivate static func shutdownGracefully(
        client: KafkaClient,
        subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>,
        logger: Logger
    ) {
        let result = client.withKafkaHandlePointer { handle in
            rd_kafka_consumer_close(handle)
        }

        rd_kafka_topic_partition_list_destroy(subscribedTopicsPointer)

        guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
            let error = KafkaError.rdKafkaError(wrapping: result)
            logger.error("Closing KafkaConsumer failed: \(error.description)")
            return
        }
    }
}

// MARK: - KafkaConsumer + StateMachine

extension KafkaConsumer {
    /// State machine representing the state of the ``KafkaConsumer``.
    struct StateMachine {
        /// The state of the ``StateMachine``.
        enum State {
            /// We are in the process of initializing the ``KafkaConsumer``,
            /// though ``subscribe()`` / ``assign()`` have not been invoked.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter subscribedTopicsPointer: Pointer to a list of topics + partition pairs.
            /// - Parameter logger: A logger.
            case initializing(
                client: KafkaClient,
                subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>,
                logger: Logger
            )
            /// The ``KafkaConsumer`` is consuming messages.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter subscribedTopicsPointer: Pointer to a list of topics + partition pairs.
            /// - Parameter logger: A logger.
            case consuming(
                client: KafkaClient,
                subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>,
                logger: Logger
            )
            /// The ``KafkaConsumer`` has been closed.
            case finished
        }

        /// The current state of the StateMachine.
        var state: State

        /// Action to be taken when wanting to poll for a new message.
        enum PollAction {
            /// Poll for a new ``KafkaConsumerMessage``.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter logger: A logger.
            case pollForMessage(
                client: KafkaClient,
                logger: Logger
            )
        }

        /// Returns the next action to be taken when wanting to poll.
        /// - Returns: The next action to be taken when wanting to poll, or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        func nextPollAction() -> PollAction? {
            switch self.state {
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before reading messages")
            case .consuming(let client, _, let logger):
                return .pollForMessage(client: client, logger: logger)
            case .finished:
                return nil
            }
        }

        /// Action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        enum SetUpConnectionAction {
            /// Set up the connection through ``subscribe()`` or ``assign()``.
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter subscribedTopicsPointer: Pointer to a list of topics + partition pairs.
            case setUpConnection(client: KafkaClient, subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>)
        }

        /// Get action to be taken when wanting to set up the connection through ``subscribe()`` or ``assign()``.
        /// - Returns: The action to be taken.
        mutating func setUpConnection() -> SetUpConnectionAction {
            switch self.state {
            case .initializing(let client, let subscribedTopicsPointer, let logger):
                self.state = .consuming(
                    client: client,
                    subscribedTopicsPointer: subscribedTopicsPointer,
                    logger: logger
                )
                return .setUpConnection(client: client, subscribedTopicsPointer: subscribedTopicsPointer)
            case .consuming, .finished:
                fatalError("\(#function) should only be invoked upon initialization of KafkaConsumer")
            }
        }

        /// Action to be taken when wanting to do a synchronous commit.
        enum CommitSyncAction {
            /// Do a synchronous commit.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            case commitSync(
                client: KafkaClient
            )
            /// Throw an error. The ``KafkaConsumer`` is closed.
            case throwClosedError
        }

        /// Get action to be taken when wanting to do a synchronous commit.
        /// - Returns: The action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        func commitSync() -> CommitSyncAction {
            switch self.state {
            case .initializing:
                fatalError("Subscribe to consumer group / assign to topic partition pair before committing offsets")
            case .consuming(let client, _, _):
                return .commitSync(client: client)
            case .finished:
                return .throwClosedError
            }
        }

        /// Action to be taken when wanting to do close the consumer.
        enum FinishAction {
            /// Shut down the ``KafkaConsumer``.
            ///
            /// - Parameter client: Client used for handling the connection to the Kafka cluster.
            /// - Parameter subscribedTopicsPointer: Pointer to a list of topics + partition pairs.
            /// - Parameter logger: A logger.
            case shutdownGracefully(
                client: KafkaClient,
                subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>,
                logger: Logger
            )
        }

        /// Get action to be taken when wanting to do close the consumer.
        /// - Returns: The action to be taken,  or `nil` if there is no action to be taken.
        ///
        /// - Important: This function throws a `fatalError` if called while in the `.initializing` state.
        mutating func finish() -> FinishAction? {
            switch self.state {
            case .initializing:
                fatalError("subscribe() / assign() should have been invoked before \(#function)")
            case .consuming(let client, let subscribedTopicsPointer, let logger):
                self.state = .finished
                return .shutdownGracefully(client: client, subscribedTopicsPointer: subscribedTopicsPointer, logger: logger)
            case .finished:
                return nil
            }
        }
    }
}
