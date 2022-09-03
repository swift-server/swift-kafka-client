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

public final class KafkaConsumer {

    private var state: State

    private enum State {
        case started
        case closed
    }

    // TODO: backpressure??
    // TODO: is access to the var's thread-safe?
    private var config: KafkaConfig
    private let logger: Logger
    private let client: KafkaClient

    private let subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>

    public init(
        config: KafkaConfig = KafkaConfig(),
        logger: Logger
    ) throws {
        self.config = config
        self.logger = logger
        self.client = try KafkaClient(type: .consumer, config: self.config, logger: self.logger)

        self.subscribedTopicsPointer = rd_kafka_topic_partition_list_new(1)

        // Events that would be triggered by rd_kafka_poll
        // will now be also triggered by rd_kafka_consumer_poll
        let result = self.client.withKafkaHandlePointer { handle in
            rd_kafka_poll_set_consumer(handle)
        }
        guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError(rawValue: result.rawValue)
        }

        self.state = .started
    }

    deinit {
        switch self.state {
        case .started:
            self.client.withKafkaHandlePointer { handle in
                rd_kafka_consumer_close(handle)
            }
            rd_kafka_topic_partition_list_destroy(subscribedTopicsPointer)
        case .closed:
            return
        }
    }

    public func subscribe(topics: [String]) throws {
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }

        for topic in topics {
            rd_kafka_topic_partition_list_add(
                subscribedTopicsPointer,
                topic,
                KafkaPartition.unassigned.rawValue
            )
        }

        let result = self.client.withKafkaHandlePointer { handle in
            rd_kafka_subscribe(handle, subscribedTopicsPointer)
        }

        guard result.rawValue == 0 else {
            throw KafkaError(rawValue: result.rawValue)
        }
    }

    // TODO: docc timeout is in ms
    // TODO: clock API
    func poll(timeout: Int32 = 100) async throws -> KafkaConsumerMessage? {
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }

        return try await withCheckedThrowingContinuation { continuation in
            guard let messagePointer = self.client.withKafkaHandlePointer({ handle in
                rd_kafka_consumer_poll(handle, timeout)
            }) else {
                // No error, there might be no more messages
                continuation.resume(returning: nil)
                return
            }

            defer {
                // Destroy message otherwise poll() will block forever
                rd_kafka_message_destroy(messagePointer)
            }

            // Reached the end of the topic+partition queue on the broker
            if messagePointer.pointee.err == RD_KAFKA_RESP_ERR__PARTITION_EOF {
                continuation.resume(returning: nil)
                return
            }

            do {
                let message = try KafkaConsumerMessage(messagePointer: messagePointer)
                continuation.resume(returning: message)
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }

    public func close() throws {
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }

        self.client.withKafkaHandlePointer { handle in
            rd_kafka_consumer_close(handle)
        }
        rd_kafka_topic_partition_list_destroy(subscribedTopicsPointer)
        self.state = .closed
    }

    // TODO: assign method (subscription without balancing - no group id?)
    // TODO: commitSync (manual offset management)?
}
