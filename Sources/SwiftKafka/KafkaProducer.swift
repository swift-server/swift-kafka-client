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

/// Send messages to the Kafka cluster.
/// - Note: When messages get published to a non-existent topic, a new topic is created using the ``KafkaTopicConfig``
/// configuration object (only works if server has `auto.create.topics.enable` property set).
public struct KafkaProducer {
    private final class _Internal {
        let client: KafkaClient
        let topicHandles: KafkaTopicHandles
        let pollTask: Task<Void, Never>

        init(client: KafkaClient) {
            self.client = client
            self.topicHandles = KafkaTopicHandles(kafkaHandle: self.client.kafkaHandle)
            self.pollTask = Task {
                while !Task.isCancelled {
                    rd_kafka_poll(client.kafkaHandle, 0)
                    try? await Task.sleep(nanoseconds: 1_000_000)
                }
            }
        }

        deinit {
            let kafkaHandle = client.kafkaHandle

            // Wait 10 seconds for outstanding messages to be sent and callbacks to be called
            rd_kafka_flush(kafkaHandle, 10000)

            Task {
                await KafkaProducerCallbacks.shared.deallocateCallbacks(for: kafkaHandle)
            }

            self.pollTask.cancel()
        }
    }

    let config: KafkaConfig
    private let client: KafkaClient
    private let topicConfig: KafkaTopicConfig

    private var _internal: _Internal

    /// Initialize a new `KafkaProducer`.
    /// - Parameter config: The ``KafkaConfig`` for configuring the `KafkaProducer`.
    /// - Parameter topicConfig: The ``KafkaTopicConfig`` used for newly created topics.
    /// - Parameter logger: A logger.
    public init(
        config: KafkaConfig = KafkaConfig(),
        topicConfig: KafkaTopicConfig = KafkaTopicConfig(),
        logger: Logger
    ) throws {
        self.config = config
        self.topicConfig = topicConfig
        self.client = try KafkaClient(type: .producer, config: config, logger: logger)
        self._internal = .init(client: self.client)
    }

    /// Send messages to the Kafka cluster asynchronously, aka "fire and forget".
    /// This function is non-blocking.
    /// - Parameter message: The ``KafkaProducerMessage`` that is sent to the KafkaCluster.
    /// - Parameter completionHandler: Closure that will be executed once the Kafka cluster has received the message.
    public func sendAsync(
        message: KafkaProducerMessage,
        completionHandler: ((Result<KafkaConsumerMessage, KafkaError>) -> Void)? = nil
    ) {
        Task {
            let topicHandle = await self._internal.topicHandles.getTopicHandle(
                topic: message.topic,
                topicConfig: self.topicConfig
            )

            let keyBytes: [UInt8]?
            if let key = message.key {
                keyBytes = key.withUnsafeBytes { Array($0) }
            } else {
                keyBytes = nil
            }

            let idPointer = UnsafeMutableRawPointer.allocate(byteCount: 1, alignment: 0)

            let responseCode = message.value.withUnsafeBytes { valueBuffer in

                return rd_kafka_produce(
                    topicHandle,
                    message.partition.rawValue,
                    RD_KAFKA_MSG_F_COPY,
                    UnsafeMutableRawPointer(mutating: valueBuffer.baseAddress),
                    valueBuffer.count,
                    keyBytes,
                    keyBytes?.count ?? 0,
                    idPointer
                )
            }

            guard responseCode == 0 else {
                completionHandler?(.failure(KafkaError(rawValue: responseCode)))
                return
            }

            if let completionHandler = completionHandler {
                let kafkaHandle = self.client.kafkaHandle
                await KafkaProducerCallbacks.shared.set(
                    for: kafkaHandle,
                    messageID: idPointer,
                    callback: completionHandler
                )
            }
        }
    }
}
