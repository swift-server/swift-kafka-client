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
import struct Foundation.UUID // TODO: can we avoid this Foundation import??
import Logging
import NIOCore

public struct ConsumerMessagesAsyncSequence: AsyncSequence {
    public typealias Element = Result<KafkaConsumerMessage, Error> // TODO: replace with something like KafkaConsumerError
    let wrappedSequence: NIOAsyncSequenceProducer<Element, NoBackPressure, NoDelegate>

    public struct ConsumerMessagesAsyncIterator: AsyncIteratorProtocol {
        let wrappedIterator: NIOAsyncSequenceProducer<Element, NoBackPressure, NoDelegate>.AsyncIterator

        public mutating func next() async -> Element? {
            await self.wrappedIterator.next()
        }
    }

    public func makeAsyncIterator() -> ConsumerMessagesAsyncIterator {
        return ConsumerMessagesAsyncIterator(wrappedIterator: self.wrappedSequence.makeAsyncIterator())
    }
}

public final class KafkaConsumer {
    private var state: State

    private enum State {
        case started
        case closed
    }

    // TODO: backpressure??
    // TODO: do we want to allow users to subscribe / assign to more topics during runtime?
    // TODO: function that returns all partitions for topic -> use rd_kafka_metadata, dedicated MetaData class
    // TODO: is access to the var's thread-safe?
    private var config: KafkaConfig
    private let logger: Logger
    private let client: KafkaClient
    private let subscribedTopicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>
    private var pollTask: Task<Void, Never>?

    private typealias Element = Result<KafkaConsumerMessage, Error> // TODO: replace with a more specific Error type
    private let messagesSource: NIOAsyncSequenceProducer<
        Element,
        NoBackPressure,
        NoDelegate
    >.Source
    public let messages: ConsumerMessagesAsyncSequence

    private init(
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

        let messagesSourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            of: Element.self,
            backPressureStrategy: NoBackPressure(),
            delegate: NoDelegate()
        )
        self.messagesSource = messagesSourceAndSequence.source
        self.messages = ConsumerMessagesAsyncSequence(
            wrappedSequence: messagesSourceAndSequence.sequence
        )

        self.state = .started

        // TODO: will messagesSourceAndSequence be captured -> it shouldn't!
        self.pollTask = Task { [weak self] in
            while !Task.isCancelled {
                do {
                    guard let message = try await self?.poll() else {
                        continue
                    }
                    _ = self?.messagesSource.yield(.success(message))
                } catch {
                    _ = self?.messagesSource.yield(.failure(error))
                }
            }
        }
    }

    // MARK: - Initialize as member of a consumner group

    public convenience init(
        topics: [String],
        groupID: String,
        config: KafkaConfig = KafkaConfig(),
        logger: Logger
    ) throws {
        // TODO: make prettier
        // TODO: use inout parameter here?
        var config = config
        if let configGroupID = config.value(forKey: "group.id") {
            if configGroupID != groupID {
                throw KafkaError(description: "Group ID does not match with group ID found in the configuration")
            }
        } else {
            try config.set(groupID, forKey: "group.id")
        }

        try self.init(
            config: config,
            logger: logger
        )
        try self.subscribe(topics: topics)
    }

    // MARK: - Initialize as assignment to particular topic + partition pair

    // TODO: DocC group.id property will be ignored
    public convenience init(
        topic: String,
        partition: KafkaPartition,
        config: KafkaConfig = KafkaConfig(),
        logger: Logger
    ) throws {
        // Althogh an assignment is not related to a consumer group,
        // librdkafka requires us to set a `group.id`.
        // This is a known issue:
        // https://github.com/edenhill/librdkafka/issues/3261
        var config = config
        try config.set(UUID().uuidString, forKey: "group.id")

        try self.init(
            config: config,
            logger: logger
        )
        try self.assign(topic: topic, partition: partition)
    }

    deinit {
        switch self.state {
        case .started:
            self._close()
        case .closed:
            return
        }
    }

    private func subscribe(topics: [String]) throws {
        // TODO: is this state needed for a method that is only invoked upon init?
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }

        for topic in topics {
            rd_kafka_topic_partition_list_add(
                self.subscribedTopicsPointer,
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

    private func assign(
        topic: String,
        partition: KafkaPartition,
        offset: Int64 = Int64(RD_KAFKA_OFFSET_END)
    ) throws {
        // TODO: is this state needed for a method that is only invoked upon init?
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }

        guard let partitionPointer = rd_kafka_topic_partition_list_add(
            self.subscribedTopicsPointer,
            topic,
            partition.rawValue
        ) else {
            fatalError("rd_kafka_topic_partition_list_add returned invalid pointer")
        }

        partitionPointer.pointee.offset = offset

        let result = self.client.withKafkaHandlePointer { handle in
            rd_kafka_assign(handle, self.subscribedTopicsPointer)
        }

        guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError(rawValue: result.rawValue)
        }
    }

    // TODO: docc timeout is in ms
    // TODO: clock API
    // TODO: ideally: make private
    func poll(timeout: Int32 = 100) async throws -> KafkaConsumerMessage? {
        // TODO: is this state needed here? Are we publishing a result type
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

    // TODO: DocC: Note about enable.auto.commit
    // TODO: docc: https://github.com/segmentio/kafka-go#explicit-commits note about highest offset
    public func commitSync(_ message: KafkaConsumerMessage) async throws {
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }
        try await self._commitSync(message)
    }

    // TODO: commit multiple messages at once -> different topics + test
    private func _commitSync(_ message: KafkaConsumerMessage) async throws {
        guard self.config.value(forKey: "enable.auto.commit") == "false" else {
            throw KafkaError(description: "Committing manually only works if enable.auto.commit is set to false")
        }

        return try await withCheckedThrowingContinuation { continuation in
            let changesList = rd_kafka_topic_partition_list_new(1)
            defer { rd_kafka_topic_partition_list_destroy(changesList) }
            guard let partitionPointer = rd_kafka_topic_partition_list_add(
                changesList,
                message.topic,
                message.partition.rawValue
            ) else {
                fatalError("rd_kafka_topic_partition_list_add returned invalid pointer")
            }
            // TODO: DocC: commit offset is the offset where consumption should resume
            // https://github.com/edenhill/librdkafka/issues/2745#issuecomment-598067945
            partitionPointer.pointee.offset = message.offset + 1
            let result = self.client.withKafkaHandlePointer { handle in
                rd_kafka_commit(
                    handle,
                    changesList,
                    0
                )
            }
            guard result == RD_KAFKA_RESP_ERR_NO_ERROR else {
                continuation.resume(throwing: KafkaError(rawValue: result.rawValue))
                return
            }
            continuation.resume()
        }
    }

    public func close() throws {
        switch self.state {
        case .started:
            break
        case .closed:
            throw KafkaError(description: "Trying to invoke method on consumer that has been closed.")
        }
        self._close()
    }

    private func _close() {
        self.pollTask?.cancel()
        self.client.withKafkaHandlePointer { handle in
            rd_kafka_consumer_close(handle)
        }
        rd_kafka_topic_partition_list_destroy(subscribedTopicsPointer)
        self.state = .closed
    }
}
