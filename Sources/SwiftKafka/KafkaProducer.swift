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

/// `NIOAsyncSequenceProducerBackPressureStrategy` that does nothing.
struct NoDelegate: NIOAsyncSequenceProducerDelegate {
    func produceMore() {}
    func didTerminate() {}
}

/// `AsyncSequence` implementation for handling messages acknowledged by the Kafka cluster (``KafkaAcknowledgedMessage``).
public struct AcknowledgedMessagesAsyncSequence: AsyncSequence {
    public typealias Element = Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>
    let wrappedSequence: NIOAsyncSequenceProducer<Element, NoBackPressure, NoDelegate>

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
/// Please make sure to explicitly call ``shutdownGracefully()`` when the `KafkaProducer` is not used anymore.
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
    /// The configuration object of the producer client.
    private var config: KafkaConfig
    /// The ``KafkaTopicConfig`` used for newly created topics.
    private let topicConfig: KafkaTopicConfig
    /// A logger.
    private let logger: Logger
    /// Dictionary containing all topic names with their respective `rd_kafka_topic_t` pointer.
    private var topicHandles: [String: OpaquePointer]

    /// `NIOAsyncSequenceProducer.Source` used for publishing values to the ``acknowledgements`` sequence.
    private typealias Acknowledgement = Result<KafkaAcknowledgedMessage, KafkaAcknowledgedMessageError>
    private let acknowledgementsSource: NIOAsyncSequenceProducer<
        Acknowledgement,
        NoBackPressure,
        NoDelegate
    >.Source
    /// `AsyncSequence` that returns all ``KafkaProducerMessage`` objects that have been
    /// acknowledged by the Kafka cluster.
    public nonisolated let acknowledgements: AcknowledgedMessagesAsyncSequence

    // Implicitly unwrapped optionals because we must pass self to our config before initializing client etc.
    /// Unmanaged reference to self with an unbalanced retain.
    private var referenceToSelf: UnsafeMutableRawPointer!
    /// Used for handling the connection to the Kafka cluster.
    private var client: KafkaClient!
    /// Refrence to `librdkafka`'s `rd_kafka_t` instance.
    private var kafkaHandle: OpaquePointer!
    /// Task that polls the Kafka cluster for updates periodically.
    private var pollTask: Task<Void, Never>!

    /// Initialize a new `KafkaProducer`.
    /// - Parameter config: The ``KafkaConfig`` for configuring the `KafkaProducer`.
    /// - Parameter topicConfig: The ``KafkaTopicConfig`` used for newly created topics.
    /// - Parameter logger: A logger.
    public init(
        config: KafkaConfig = KafkaConfig(),
        topicConfig: KafkaTopicConfig = KafkaTopicConfig(),
        logger: Logger
    ) async throws {
        self.config = config
        self.topicConfig = topicConfig
        self.logger = logger
        self.topicHandles = [:]
        self.state = .started

        // (NIOAsyncSequenceProducer.makeSequence Documentation Excerpt)
        // This method returns a struct containing a NIOAsyncSequenceProducer.Source and a NIOAsyncSequenceProducer.
        // The source MUST be held by the caller and used to signal new elements or finish.
        // The sequence MUST be passed to the actual consumer and MUST NOT be held by the caller.
        // This is due to the fact that deiniting the sequence is used as part of a trigger to
        // terminate the underlying source.
        let acknowledgementsSourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            of: Acknowledgement.self,
            backPressureStrategy: NoBackPressure(),
            delegate: NoDelegate()
        )
        self.acknowledgementsSource = acknowledgementsSourceAndSequence.source
        self.acknowledgements = AcknowledgedMessagesAsyncSequence(
            wrappedSequence: acknowledgementsSourceAndSequence.sequence
        )

        // Create a new reference to self that is then passed
        // to the KafkaConfig as an opaque pointer.
        // To avoid memory leaks, this reference is evantually
        // removed in the shutdownGracefully method.
        self.referenceToSelf = Unmanaged.passRetained(self).toOpaque()

        self.config.setDeliveryReportCallback(callback: self.deliveryReportCallback)
        self.config.setOpaque(opaque: self.referenceToSelf)

        self.client = try KafkaClient(type: .producer, config: self.config, logger: self.logger)
        self.kafkaHandle = self.client.kafkaHandle

        // Poll Kafka every millisecond
        self.pollTask = Task { [kafkaHandle] in
            while !Task.isCancelled {
                rd_kafka_poll(kafkaHandle, 0)
                try? await Task.sleep(nanoseconds: 1_000_000)
            }
        }
    }

    /// Method to shutdown the ``KafkaProducer``.
    ///
    /// This method flushes any buffered messages and waits until a callback is received for all of them.
    /// Afterwards, it shuts down the connection to Kafka and cleans any remaining state up.
    public func shutdownGracefully() throws {
        switch self.state {
        case .started:
            self.state = .shuttingDown
            self._shutDownGracefully()
        case .shuttingDown, .shutDown:
            return
        }
    }

    private func _shutDownGracefully() {
        // Wait 10 seconds for outstanding messages to be sent and callbacks to be called
        rd_kafka_flush(self.kafkaHandle, 10000)

        for (_, topicHandle) in self.topicHandles {
            rd_kafka_topic_destroy(topicHandle)
        }
        self.pollTask.cancel()

        // Decrease reference count of self by one.
        // This is necessary because we created an additional reference to self in our initializer.
        _ = Unmanaged<KafkaProducer>.fromOpaque(self.referenceToSelf).takeRetainedValue()

        self.state = .shutDown
    }

    /// Send messages to the Kafka cluster asynchronously, aka "fire and forget".
    /// This function is non-blocking.
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    /// - Returns: Unique message identifier matching the `id` property of the corresponding ``KafkaAcknowledgedMessage``
    @discardableResult
    public func sendAsync(message: KafkaProducerMessage) throws -> UInt {
        switch self.state {
        case .started:
            return try self._sendAsync(message: message)
        case .shuttingDown, .shutDown:
            throw KafkaError(description: "Trying to invoke method on producer that has been shut down.")
        }
    }

    private func _sendAsync(message: KafkaProducerMessage) throws -> UInt {
        let topicHandle = self.createTopicHandleIfNeeded(topic: message.topic)

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
                UnsafeMutableRawPointer(bitPattern: messageIDCounter)
            )
        }

        guard responseCode == 0 else {
            throw KafkaError(rawValue: responseCode)
        }

        return self.messageIDCounter
    }

    // Closure that is executed when a message has been acknowledged by Kafka
    private let deliveryReportCallback: (
        @convention(c) (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, UnsafeMutableRawPointer?) -> Void
    ) = { _, messagePointer, producerPointer in
        guard let producerPointer = producerPointer else {
            fatalError("Could not resolve reference to KafkaProducer instance")
        }
        // Get a reference to the KafkaProducer without decrementing its reference count
        let producer = Unmanaged<KafkaProducer>.fromOpaque(producerPointer).takeUnretainedValue()

        guard let messagePointer = messagePointer else {
            producer.logger.error("Could not resolve acknowledged message")
            return
        }

        let messageID = UInt(bitPattern: messagePointer.pointee._private)

        guard messagePointer.pointee.err.rawValue == 0 else {
            let error = KafkaAcknowledgedMessageError(
                rawValue: messagePointer.pointee.err.rawValue,
                description: "TODO: implement in separate error issue",
                messageID: messageID
            )
            _ = producer.acknowledgementsSource.yield(.failure(error))

            return
        }

        do {
            let message = try KafkaAcknowledgedMessage(messagePointer: messagePointer, id: messageID)
            _ = producer.acknowledgementsSource.yield(.success(message))
        } catch {
            guard let error = error as? KafkaAcknowledgedMessageError else {
                fatalError("Caught error that is not of type \(KafkaAcknowledgedMessageError.self)")
            }
            _ = producer.acknowledgementsSource.yield(.failure(error))
        }

        // The messagePointer is automatically destroyed by librdkafka
        // For safety reasons, we only use it inside of this closure
    }

    /// Check `topicHandles` for a handle matching the topic name and create a new handle if needed.
    /// - Parameter topic: The name of the topic that is addressed.
    private func createTopicHandleIfNeeded(topic: String) -> OpaquePointer? {
        if let handle = self.topicHandles[topic] {
            return handle
        } else {
            let newHandle = rd_kafka_topic_new(
                self.kafkaHandle,
                topic,
                self.topicConfig.createDuplicatePointer() // Duplicate because rd_kafka_topic_new deallocates config object
            )
            if newHandle != nil {
                self.topicHandles[topic] = newHandle
            }
            return newHandle
        }
    }
}
