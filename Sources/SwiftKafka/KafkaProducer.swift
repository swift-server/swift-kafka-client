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
public struct NoBackPressure: NIOAsyncSequenceProducerBackPressureStrategy {
    public func didYield(bufferDepth: Int) -> Bool { true }
    public func didConsume(bufferDepth: Int) -> Bool { true }
}

/// `NIOAsyncSequenceProducerBackPressureStrategy` that does nothing.
public struct NoDelegate: NIOAsyncSequenceProducerDelegate {
    public func produceMore() {}
    public func didTerminate() {}
}

/// `AsyncSequence` implementation for handling messages acknowledged by the Kafka cluster (``KafkaAckedMessage``).
public struct AcknowledgedMessagesAsyncSequence: AsyncSequence {
    public typealias Element = KafkaAckedMessage
    public typealias AsyncIterator = NIOAsyncSequenceProducer<KafkaAckedMessage, NoBackPressure, NoDelegate>.AsyncIterator

    let _internal = NIOAsyncSequenceProducer.makeSequence(
        of: Element.self,
        backPressureStrategy: NoBackPressure(),
        delegate: NoDelegate()
    )

    func produceMessage(_ message: KafkaAckedMessage) {
        _ = self._internal.source.yield(message)
    }

    public func makeAsyncIterator() -> AsyncIterator {
        self._internal.sequence.makeAsyncIterator()
    }
}

/// Send messages to the Kafka cluster.
/// Please make sure to explicitly call ``shutdownGracefully()`` when the `KafkaProducer` is not used anymore.
/// - Note: When messages get published to a non-existent topic, a new topic is created using the ``KafkaTopicConfig``
/// configuration object (only works if server has `auto.create.topics.enable` property set).
public actor KafkaProducer {
    /// `AsyncSequence` that returns all ``KafkaProducerMessage`` objects that have been
    /// acknowledged by the Kafka cluster.
    public nonisolated let acknowledgements = AcknowledgedMessagesAsyncSequence()

    private var messageIDCounter: UInt = 0
    private var config: KafkaConfig
    private let topicConfig: KafkaTopicConfig
    private let logger: Logger
    private var topicHandles: [String: OpaquePointer]

    // Implicitly unwrapped optional because we must pass self to our config before initializing client etc.
    private var referenceToSelf: UnsafeMutableRawPointer!
    private var client: KafkaClient!
    private var kafkaHandle: OpaquePointer!
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

        self.referenceToSelf = Unmanaged.passRetained(self).toOpaque()
        self.config.setDeliveryReportCallback(callback: self.deliveryReportCallback)
        self.config.setCallbackOpaque(opaque: self.referenceToSelf)

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

    /// Kafka producer must be closed **explicitly**.
    public func shutdownGracefully() {
        // Wait 10 seconds for outstanding messages to be sent and callbacks to be called
        rd_kafka_flush(self.kafkaHandle, 10000)

        for (_, topicHandle) in self.topicHandles {
            rd_kafka_topic_destroy(topicHandle)
        }
        self.pollTask.cancel()

        _ = Unmanaged<KafkaProducer>.fromOpaque(self.referenceToSelf).takeRetainedValue()
    }

    /// Send messages to the Kafka cluster asynchronously, aka "fire and forget".
    /// This function is non-blocking.
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    public func sendAsync(message: KafkaProducerMessage) throws {
        let topicHandle = self.createTopicHandleIfNeeded(topic: message.topic)

        let keyBytes: [UInt8]?
        if let key = message.key {
            keyBytes = key.getBytes(at: 0, length: key.readableBytes)
        } else {
            keyBytes = nil
        }

        self.messageIDCounter += 1

        let responseCode = message.value.withUnsafeReadableBytes { valueBuffer in

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
    }

    // Closure that is executed when a message has been acknowledged by Kafka
    private let deliveryReportCallback: (
        @convention(c) (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, UnsafeMutableRawPointer?) -> Void
    ) = { _, messagePointer, producerPointer in

        guard let messagePointer = messagePointer else {
            return
        }

        guard let producerPointer = producerPointer else {
            return
        }
        let producer = Unmanaged<KafkaProducer>.fromOpaque(producerPointer).takeUnretainedValue()

        let messageID = messagePointer.pointee._private
        guard let message = KafkaAckedMessage(messagePointer: messagePointer, id: UInt(bitPattern: messageID)) else {
            Task {
                producer.logger.error("Sending message failed due to an unknown error")
            }
            return
        }

        // TODO: is this thread-safe???
        producer.acknowledgements.produceMessage(message)
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
