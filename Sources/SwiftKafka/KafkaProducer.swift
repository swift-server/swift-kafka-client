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
import struct Foundation.UUID
import Logging

/// Send messages to the Kafka cluster.
/// - Note: When messages get published to a non-existent topic, a new topic is created using the ``KafkaTopicConfig``
/// configuration object (only works if server has `auto.create.topics.enable` property set).
public actor KafkaProducer {
    /// Class that lets us wrap both the `messageID` and the `producer` into a single pointer
    private final class CallbackOpaquePointer {
        let messageID: UUID // TODO: to avoid Foundation, have a counter in KafkaProducer that serves as ID
        let producer: KafkaProducer

        init(_ producer: KafkaProducer) {
            self.messageID = UUID()
            self.producer = producer
        }
    }

    private var config: KafkaConfig
    private let client: KafkaClient
    private let kafkaHandle: OpaquePointer
    private let topicConfig: KafkaTopicConfig
    private var topicHandles: [String: OpaquePointer]
    private var unacknowledgedMessages: [UUID: KafkaProducerMessage]
    private var receivedMessages: [KafkaProducerMessage]
    private var callbackOpaquePointers: [UnsafeMutableRawPointer]
    private let pollTask: Task<Void, Never>

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
        self.config.setDeliveryReportCallback(callback: deliveryReportCallback)

        self.topicConfig = topicConfig
        self.client = try KafkaClient(type: .producer, config: self.config, logger: logger)
        self.kafkaHandle = client.kafkaHandle
        self.topicHandles = [:]
        self.unacknowledgedMessages = [:]
        self.receivedMessages = []
        self.callbackOpaquePointers = []

        // Poll Kafka every millisecond
        self.pollTask = Task { [kafkaHandle] in
            while !Task.isCancelled {
                rd_kafka_poll(kafkaHandle, 0)
                try? await Task.sleep(nanoseconds: 1_000_000)
            }
        }
    }

    /// Kafka producer must be closed **explicitly**.
    public func close() {
        // Wait 10 seconds for outstanding messages to be sent and callbacks to be called
        rd_kafka_flush(kafkaHandle, 10000)

        for (_, topicHandle) in self.topicHandles {
            rd_kafka_topic_destroy(topicHandle)
        }

        callbackOpaquePointers.forEach {
            _ = Unmanaged<CallbackOpaquePointer>.fromOpaque($0).takeRetainedValue()
        }

        self.pollTask.cancel()
    }

    /// Send messages to the Kafka cluster asynchronously, aka "fire and forget".
    /// This function is non-blocking.
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    public func sendAsync(message: KafkaProducerMessage) throws {
        let topicHandle = createTopicHandleIfNeeded(topic: message.topic)

        let keyBytes: [UInt8]?
        if let key = message.key {
            keyBytes = key.withUnsafeBytes { Array($0) }
        } else {
            keyBytes = nil
        }

        let callbackOpaque = CallbackOpaquePointer(self)
        let pointer = UnsafeMutableRawPointer(Unmanaged.passRetained(callbackOpaque).toOpaque())
        callbackOpaquePointers.append(pointer)

        let responseCode = message.value.withUnsafeBytes { valueBuffer in

            return rd_kafka_produce(
                topicHandle,
                message.partition.rawValue,
                RD_KAFKA_MSG_F_COPY,
                UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress),
                valueBuffer.count,
                keyBytes,
                keyBytes?.count ?? 0,
                pointer
            )
        }

        guard responseCode == 0 else {
            throw KafkaError(rawValue: responseCode)
        }

        unacknowledgedMessages[callbackOpaque.messageID] = message
    }

    func markMessageAsAcknowledged(_ messageID: UUID) {
        guard let message = unacknowledgedMessages[messageID] else {
            return
        }

        self.unacknowledgedMessages[messageID] = nil
        self.receivedMessages.append(message)
    }

    /// `AsyncStream` that returns all ``KafkaProducerMessage`` objects that have been
    /// acknowledged by the Kafka cluster.
    public nonisolated var acknowledgements: AsyncStream<KafkaProducerMessage> {
        AsyncStream { continuation in
            Task {
                while !Task.isCancelled {
                    // TODO: Sleep here to slow down rate?
                    if let firstElement = await receivedMessagesPopFirst() {
                        continuation.yield(firstElement)
                    }
                }
            }
        }
    }

    private func receivedMessagesPopFirst() -> KafkaProducerMessage? {
        guard let first = receivedMessages.first else {
            return nil
        }
        receivedMessages.removeFirst()
        return first
    }

    // Closure that is executed when a message has been acknowledged by Kafka
    private let deliveryReportCallback: (
        @convention(c) (OpaquePointer?, UnsafePointer<rd_kafka_message_t>?, UnsafeMutableRawPointer?) -> Void
    ) = { _, message, _ in
        guard let pointer = message?.pointee._private else {
            return
        }

        let callbackOpaque = Unmanaged<CallbackOpaquePointer>.fromOpaque(pointer).takeUnretainedValue()
        let messageID = callbackOpaque.messageID
        let producer = callbackOpaque.producer

        Task {
            await producer.deallocateCallbackOpaquePointer(pointer)
            await producer.markMessageAsAcknowledged(messageID)
        }
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
                topicConfig.createDuplicatePointer() // Duplicate because rd_kafka_topic_new deallocates config object
            )
            if newHandle != nil {
                self.topicHandles[topic] = newHandle
            }
            return newHandle
        }
    }

    private func deallocateCallbackOpaquePointer(_ pointer: UnsafeMutableRawPointer) {
        _ = Unmanaged<CallbackOpaquePointer>.fromOpaque(pointer).takeRetainedValue()
        callbackOpaquePointers.removeAll(where: { $0 == pointer })
    }
}
