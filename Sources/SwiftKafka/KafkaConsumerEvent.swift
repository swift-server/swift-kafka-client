#if false
//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//


// TODO: Move new structures to Rebalance:

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

extension KafkaTopicList : Sendable {}
extension KafkaTopicList : Hashable {}

extension KafkaTopicList : CustomDebugStringConvertible {
    public var debugDescription: String {
        list.debugDescription
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
            return TopicPartition(from: topic)
        }
    }
    
    public func makeIterator() -> TopicPartitionIterator {
        TopicPartitionIterator(list: self.list)
    }
}

public typealias KafkaOffset = RDKafkaOffset // Should we wrap it?


public struct TopicPartition: Sendable, Hashable {
    public private(set) var topic: String
    public private(set) var partition: KafkaPartition
    public private(set) var offset: KafkaOffset
    
    init(from topicPartition: RDKafkaTopicPartition) {
        self.topic = topicPartition.topic
        self.partition = topicPartition.partition
        self.offset = KafkaOffset(rawValue: topicPartition.offset)
    }
    
    public init(_ topic: String, _ partition: KafkaPartition, _ offset: KafkaOffset = .beginning) {
        self.topic = topic
        self.partition = partition
        self.offset = offset
    }

    public var name: String {
        "\(topic):\(partition):\(offset)"
    }
}

extension TopicPartition : CustomDebugStringConvertible {
    public var debugDescription: String {
        "\(topic):\(partition):\(offset)"
    }
}
//
//public struct TopicPartition {
//
//
//    let kafkaTopicPartition: RDKafkaTopicPartition
//
//    init(kafkaTopicPartition: RDKafkaTopicPartition) {
//        self.kafkaTopicPartition = kafkaTopicPartition
//    }
//
//    var topic: String {
//        self.kafkaTopicPartition.topic
//    }
//
//    var partition: KafkaPartition {
//        kafkaTopicPartition.partition
//    }
//
//    var offset: Int64 {
//        kafkaTopicPartition.offset
//    }
//}

public enum KafkaRebalanceProtocol: Sendable, Hashable {
    case cooperative
    case eager
    case none
    
    static func convert(from proto: RDKafkaRebalanceProtocol) -> KafkaRebalanceProtocol{
        switch proto {
        case .cooperative: return .cooperative
        case .eager: return .eager
        case .none: return .none
        }
    }
}

public enum RebalanceAction : Sendable, Hashable {
    case assign(KafkaRebalanceProtocol, KafkaTopicList)
    case revoke(KafkaRebalanceProtocol, KafkaTopicList)
    case error(KafkaRebalanceProtocol, KafkaTopicList, KafkaError)
    
    static func convert(from rebalance: RDKafkaRebalanceAction) -> RebalanceAction {
        switch rebalance {
        case .assign(let proto, let list):
            return .assign(.convert(from: proto), .init(from: list))
        case .revoke(let proto, let list):
            return .revoke(.convert(from: proto), .init(from: list))
        case .error(let proto, let list, let err):
            return .error(.convert(from: proto), .init(from: list), err)
        }
    }
}


/// An enumeration representing events that can be received through the ``KafkaConsumerEvents`` asynchronous sequence.
public enum KafkaConsumerEvent: Sendable, Hashable {
    /// Statistics from librdkafka
    case statistics(KafkaStatistics)
    /// Rebalance callback from Kafka
    case rebalance(RebalanceAction)
    /// - Important: Always provide a `default` case when switiching over this `enum`.
    case DO_NOT_SWITCH_OVER_THIS_EXHAUSITVELY

    internal init(_ event: RDKafkaClient.KafkaEvent) {
        switch event {
        case .statistics(let stat):
            self = .statistics(stat)
        case .rebalance(let action):
            self = .rebalance(.convert(from: action))
        case .deliveryReport:
            fatalError("Cannot cast \(event) to KafkaConsumerEvent")
        case .consumerMessages:
            fatalError("Consumer messages should be handled in the KafkaConsumerMessages asynchronous sequence")
        }
    }
}
#endif
