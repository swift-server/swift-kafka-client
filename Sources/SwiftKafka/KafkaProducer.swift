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
import NIOCore

/// `NIOAsyncSequenceProducerBackPressureStrategy` that always returns true.
struct NoBackPressure: NIOAsyncSequenceProducerBackPressureStrategy {
    func didYield(bufferDepth: Int) -> Bool { true }
    func didConsume(bufferDepth: Int) -> Bool { true }
}

/// `NIOAsyncSequenceProducerDelegate` that does nothing.
struct NoDelegate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {}
    func didTerminate() {}
}

/// `AsyncSequence` implementation for handling messages acknowledged by the Kafka cluster (``KafkaAcknowledgedMessage``).
public struct KafkaMessageAcknowledgements: AsyncSequence {
    public typealias Element = Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>
    typealias WrappedSequence = NIOAsyncSequenceProducer<Element, NoBackPressure, NoDelegate>
    let wrappedSequence: WrappedSequence

    /// `AsynceIteratorProtocol` implementation for handling messages acknowledged by the Kafka cluster (``KafkaAcknowledgedMessage``).
    public struct AcknowledgedMessagesAsyncIterator: AsyncIteratorProtocol {
        let wrappedIterator: NIOAsyncSequenceProducer<Element, NoBackPressure, NoDelegate>.AsyncIterator

        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    public func makeAsyncIterator() -> AcknowledgedMessagesAsyncIterator {
        return AcknowledgedMessagesAsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

/// Send messages to the Kafka cluster.
/// Please make sure to explicitly call ``shutdownGracefully(timeout:)`` when the ``KafkaProducer`` is not used anymore.
/// - Note: When messages get published to a non-existent topic, a new topic is created using the ``KafkaTopicConfig``
/// configuration object (only works if server has `auto.create.topics.enable` property set).
public actor KafkaProducer {
    /// States that the ``KafkaProducer`` can have.
    private enum State {
        /// The ``KafkaProducer`` has started and is ready to use.
        case started
        /// ``KafkaProducer/shutdownGracefully()`` has been invoked and the ``KafkaProducer``
        /// is in the process of receiving all outstanding acknowlegements and shutting down.
        case shuttingDown
        /// The ``KafkaProducer`` has been shut down and cannot be used anymore.
        case shutDown
    }

    /// State of the ``KafkaProducer``.
    private var state: State

    /// Counter that is used to assign each message a unique ID.
    /// Every time a new message is sent to the Kafka cluster, the counter is increased by one.
    private var messageIDCounter: UInt = 0
    /// The ``TopicConfig`` used for newly created topics.
    private let topicConfig: KafkaTopicConfig
    /// A logger.
    private let logger: Logger
    /// Dictionary containing all topic names with their respective `rd_kafka_topic_t` pointer.
    private var topicHandles: [String: OpaquePointer]

    /// Used for handling the connection to the Kafka cluster.
    private let client: KafkaClient

    // Private initializer, use factory methods to create KafkaProducer
    /// Initialize a new ``KafkaProducer``.
    /// - Parameter config: The ``KafkaProducerConfig`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfig`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    private init(
        client: KafkaClient,
        topicConfig: KafkaTopicConfig,
        logger: Logger
    ) async throws {
        self.client = client
        self.topicConfig = topicConfig
        self.topicHandles = [:]
        self.logger = logger
        self.state = .started
    }

    /// Initialize a new ``KafkaProducer`` that ignores incoming message acknowledgements.
    /// - Parameter config: The ``KafkaProducerConfig`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfig`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Returns: The newly created ``KafkaProducer``.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public static func newProducer(
        config: KafkaProducerConfig = KafkaProducerConfig(),
        topicConfig: KafkaTopicConfig = KafkaTopicConfig(),
        logger: Logger
    ) async throws -> KafkaProducer {
        let client = try RDKafka.createClient(
            type: .producer,
            configDictionary: config.dictionary,
            // Having no callback will discard any incoming acknowlegement messages
            // Ref: rdkafka_broker.c:rd_kafka_dr_msgq
            callback: nil,
            logger: logger
        )

        let producer = try await KafkaProducer(
            client: client,
            topicConfig: topicConfig,
            logger: logger
        )

        return producer
    }

    /// Initialize a new ``KafkaProducer`` alongside a ``KafkaMessageAcknowledgements`` `AsyncSequence` that can be used
    /// to receive message acknowlegements.
    /// - Parameter config: The ``KafkaProducerConfig`` for configuring the ``KafkaProducer``.
    /// - Parameter topicConfig: The ``KafkaTopicConfig`` used for newly created topics.
    /// - Parameter logger: A logger.
    /// - Returns: A tuple containing the created ``KafkaProducer`` and the ``KafkaMessageAcknowledgements``
    /// `AsyncSequence` used for receiving message acknowledgements.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public static func newProducerWithAcknowledgements(
        config: KafkaProducerConfig = KafkaProducerConfig(),
        topicConfig: KafkaTopicConfig = KafkaTopicConfig(),
        logger: Logger
    ) async throws -> (KafkaProducer, KafkaMessageAcknowledgements) {
        // (NIOAsyncSequenceProducer.makeSequence Documentation Excerpt)
        // This method returns a struct containing a NIOAsyncSequenceProducer.Source and a NIOAsyncSequenceProducer.
        // The source MUST be held by the caller and used to signal new elements or finish.
        // The sequence MUST be passed to the actual consumer and MUST NOT be held by the caller.
        // This is due to the fact that deiniting the sequence is used as part of a trigger to
        // terminate the underlying source.
        let sourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            elementType: Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>.self,
            backPressureStrategy: NoBackPressure(),
            delegate: NoDelegate()
        )
        let source = sourceAndSequence.source

        let client = try RDKafka.createClient(
            type: .producer,
            configDictionary: config.dictionary,
            callback: { [logger, source] messageResult in
                guard let messageResult else {
                    logger.error("Could not resolve acknowledged message")
                    return
                }

                _ = source.yield(messageResult) // Ignore YieldResult
            },
            logger: logger
        )

        let producer = try await KafkaProducer(
            client: client,
            topicConfig: topicConfig,
            logger: logger
        )

        let acknowlegementsSequence = KafkaMessageAcknowledgements(wrappedSequence: sourceAndSequence.sequence)
        return (producer, acknowlegementsSequence)
    }

    /// Method to shutdown the ``KafkaProducer``.
    ///
    /// This method flushes any buffered messages and waits until a callback is received for all of them.
    /// Afterwards, it shuts down the connection to Kafka and cleans any remaining state up.
    /// - Parameter timeout: Maximum amount of milliseconds this method waits for any outstanding messages to be sent.
    public func shutdownGracefully(timeout: Int32 = 10000) async {
        switch self.state {
        case .started:
            self.state = .shuttingDown
            await self._shutDownGracefully(timeout: timeout)
        case .shuttingDown, .shutDown:
            return
        }
    }

    private func _shutDownGracefully(timeout: Int32) async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            // Wait `timeout` seconds for outstanding messages to be sent and callbacks to be called
            self.client.withKafkaHandlePointer { handle in
                rd_kafka_flush(handle, timeout)
                continuation.resume()
            }
        }

        for (_, topicHandle) in self.topicHandles {
            rd_kafka_topic_destroy(topicHandle)
        }

        self.state = .shutDown
    }

    /// Start polling Kafka for acknowledged messages.
    ///
    /// - Parameter pollInterval: The desired time interval between two consecutive polls.
    /// - Returns: An awaitable task representing the execution of the poll loop.
    public func run(pollInterval: Duration = .milliseconds(100)) async throws {
        // TODO(felix): make pollInterval part of config -> easier to adapt to Service protocol (service-lifecycle)
        while self.state == .started {
            self.client.withKafkaHandlePointer { handle in
                rd_kafka_poll(handle, 0)
            }
            try await Task.sleep(for: pollInterval)
        }
    }

    /// Send messages to the Kafka cluster asynchronously, aka "fire and forget".
    /// This function is non-blocking.
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    /// - Returns: Unique message identifier matching the `id` property of the corresponding ``KafkaAcknowledgedMessage``
    /// - Throws: A ``KafkaError`` if sending the message failed.
    @discardableResult
    public func sendAsync(_ message: KafkaProducerMessage) throws -> UInt {
        switch self.state {
        case .started:
            return try self._sendAsync(message)
        case .shuttingDown, .shutDown:
            throw KafkaError.connectionClosed(reason: "Tried to produce a message with a closed producer")
        }
    }

    private func _sendAsync(_ message: KafkaProducerMessage) throws -> UInt {
        let topicHandle = try self.createTopicHandleIfNeeded(topic: message.topic)

        let keyBytes: [UInt8]?
        if var key = message.key {
            keyBytes = key.readBytes(length: key.readableBytes)
        } else {
            keyBytes = nil
        }

        self.messageIDCounter += 1

        let responseCode = message.value.withUnsafeReadableBytes { valueBuffer in

            // Pass message over to librdkafka where it will be queued and sent to the Kafka Cluster.
            // Returns 0 on success, error code otherwise.
            return rd_kafka_produce(
                topicHandle,
                message.partition.rawValue,
                RD_KAFKA_MSG_F_COPY,
                UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress),
                valueBuffer.count,
                keyBytes,
                keyBytes?.count ?? 0,
                UnsafeMutableRawPointer(bitPattern: self.messageIDCounter)
            )
        }

        guard responseCode == 0 else {
            throw KafkaError.rdKafkaError(wrapping: rd_kafka_last_error())
        }

        return self.messageIDCounter
    }

    /// Check `topicHandles` for a handle matching the topic name and create a new handle if needed.
    /// - Parameter topic: The name of the topic that is addressed.
    private func createTopicHandleIfNeeded(topic: String) throws -> OpaquePointer? {
        if let handle = self.topicHandles[topic] {
            return handle
        } else {
            let newHandle = try self.client.withKafkaHandlePointer { handle in
                let rdTopicConf = try RDKafkaTopicConfig.createFrom(topicConfig: self.topicConfig)
                return rd_kafka_topic_new(
                    handle,
                    topic,
                    rdTopicConf
                )
                // rd_kafka_topic_new deallocates topic config object
            }
            if newHandle != nil {
                self.topicHandles[topic] = newHandle
            }
            return newHandle
        }
    }
}
