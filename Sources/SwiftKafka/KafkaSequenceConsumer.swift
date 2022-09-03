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

import NIOCore
import Logging

// TODO: can we use backpressure here? If not, move NoBackPressure and NoDelegate to a dedicated file
// TODO: backpressure: rd_kafka_query_watermark_offsets

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

public class KafkaSequenceConsumer {

    private typealias Element = Result<KafkaConsumerMessage, Error> // TODO: replace with a more specific Error type

    private let messagesSource: NIOAsyncSequenceProducer<
        Element,
        NoBackPressure,
        NoDelegate
    >.Source
    public let messages: ConsumerMessagesAsyncSequence

    private let consumer: KafkaConsumer
    private var pollTask: Task<Void, Never>?

    public convenience init(
        config: KafkaConfig = KafkaConfig(),
        logger: Logger
    ) throws {
        let consumer = try KafkaConsumer(
            config: config,
            logger: logger
        )
        self.init(consumer: consumer)
    }

    // TODO: better way to make this testable?? -> we need to call poll on consumer from outside
    init (consumer: KafkaConsumer) {
        self.consumer = consumer
        // (NIOAsyncSequenceProducer.makeSequence Documentation Excerpt)
        // This method returns a struct containing a NIOAsyncSequenceProducer.Source and a NIOAsyncSequenceProducer.
        // The source MUST be held by the caller and used to signal new elements or finish.
        // The sequence MUST be passed to the actual consumer and MUST NOT be held by the caller.
        // This is due to the fact that deiniting the sequence is used as part of a trigger to
        // terminate the underlying source.
        let messagesSourceAndSequence = NIOAsyncSequenceProducer.makeSequence(
            of: Element.self,
            backPressureStrategy: NoBackPressure(),
            delegate: NoDelegate()
        )
        self.messagesSource = messagesSourceAndSequence.source
        self.messages = ConsumerMessagesAsyncSequence(
            wrappedSequence: messagesSourceAndSequence.sequence
        )

        // TODO: will messagesSourceAndSequence be captured -> it shouldn't!
        self.pollTask = Task { [weak self] in
            while !Task.isCancelled {
                do {
                    guard let message = try await self?.consumer.poll() else {
                        continue
                    }
                    _ = self?.messagesSource.yield(.success(message))
                } catch {
                    _ = self?.messagesSource.yield(.failure(error))
                }
            }
        }
    }

    deinit {
        self.pollTask?.cancel()
    }

    public func subscribe(topics: [String]) throws {
        try self.consumer.subscribe(topics: topics)
    }

    public func close() throws {
        self.pollTask?.cancel()
        try self.consumer.close()
    }
}
