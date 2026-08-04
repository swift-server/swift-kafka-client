//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Crdkafka

public struct KafkaTopicList {
    let list: RDKafkaTopicPartitionList

    init(from: RDKafkaTopicPartitionList) {
        self.list = from
    }

    public init(size: Int32 = 1) {
        self.list = RDKafkaTopicPartitionList(size: size)
    }

    public func append(topic: TopicPartition) {
        self.list.setOffset(topic: topic.topic, partition: topic.partition, offset: topic.offset)
    }
}

public struct TopicPartition {
    public let topic: String
    public let partition: KafkaPartition
    public let offset: KafkaOffset

    public init(_ topic: String, _ partition: KafkaPartition, _ offset: KafkaOffset) {
        self.topic = topic
        self.partition = partition
        self.offset = offset
    }
}

extension TopicPartition: Sendable {}
extension TopicPartition: Hashable {}

extension KafkaTopicList : Sendable {}
extension KafkaTopicList : Hashable {}

extension KafkaTopicList: CustomStringConvertible {
    public var description: String {
        self.list.description
    }
}

extension KafkaTopicList : Sequence {
    public struct TopicPartitionIterator : IteratorProtocol {
        private let list: RDKafkaTopicPartitionList
        private var idx = 0

        init(list: RDKafkaTopicPartitionList) {
            self.list = list
        }

        mutating public func next() -> TopicPartition? {
            guard let topic = list.getByIdx(idx: idx) else {
                return nil
            }
            idx += 1
            return topic
        }
    }

    public func makeIterator() -> TopicPartitionIterator {
        TopicPartitionIterator(list: self.list)
    }
}

public enum KafkaRebalanceProtocol: Sendable, Hashable {
    case cooperative
    case eager
    case none

    static func convert(from proto: String) -> KafkaRebalanceProtocol{
        switch proto {
        case "COOPERATIVE": return .cooperative
        case "EAGER": return .eager
        default: return .none
        }
    }
}

public enum RebalanceAction : Sendable, Hashable {
    case assign(KafkaRebalanceProtocol, KafkaTopicList)
    case revoke(KafkaRebalanceProtocol, KafkaTopicList)
    case error(KafkaRebalanceProtocol, KafkaTopicList, KafkaError)
}

public struct KafkaFetch: @unchecked Sendable {
    var event: OpaquePointer?

    init(_ event: OpaquePointer) {
        self.event = event
    }

    public mutating func withMessages(
        _ body: (borrowing KafkaConsumerStream.Message) throws -> Void
    ) rethrows {
        defer {
            rd_kafka_event_destroy(event)
            self.event = nil
        }
        while let message = rd_kafka_event_message_next(event) {
            try body(.init(messagePointer: message))
        }
    }
}

/// An enumeration representing events that can be received through the ``KafkaConsumerEvents`` asynchronous sequence.
public enum KafkaConsumerEvent: Sendable {
    case fetch(KafkaFetch)

    /// Rebalance from librdkafka
    case rebalance(RebalanceAction)

    /// Error from librdkafka
    case error(KafkaError)

    /// End of a partition has been reached (see `enable.partition.eof`).
    case partitionEOF(TopicPartition)

    /// Consumer health status.
    case healthStatus(KafkaConsumerHealthStatus)

    /// - Important: Always provide a `default` case when switiching over this `enum`.
    case DO_NOT_SWITCH_OVER_THIS_EXHAUSITVELY

    internal init(_ event: RDKafkaClient.KafkaEvent) {
        switch event {
        case .fetch:
            fatalError("Cannot cast \(event) to KafkaConsumerEvent")
        case .statistics(let statistics):
            self = .healthStatus(statistics.consumerHealthStatus)
        case .rebalance(let action):
            self = .rebalance(action)
        case .error(let error):
            self = .error(error)
        case .partitionEOF(let topicPartition):
            self = .partitionEOF(topicPartition)
        case .deliveryReport:
            fatalError("Cannot cast \(event) to KafkaConsumerEvent")
        }
    }
}
