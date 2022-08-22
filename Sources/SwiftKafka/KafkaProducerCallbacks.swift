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

/// Actor used for storing callback functions of sent messages.
@globalActor
actor KafkaProducerCallbacks {
    static let shared = KafkaProducerCallbacks()

    /// 2-dimensional Dictionary containing all callback closures `[kafkaHandle: [messageID: callback]]`.
    private var callbacks = [
        OpaquePointer: [
            UnsafeMutableRawPointer: (Result<KafkaConsumerMessage, KafkaError>) -> Void
        ]
    ]()

    // KafkaProducerCallback is a singleton and therefore not initializable
    private init() {}

    /// Get callback for a given Kafka handle and a message identifier.
    /// - Parameter for: The `kafkaHandle` of the requesting `KafkaProducer`'s client.
    /// - Parameter messageID: The message identifier.
    /// - Returns: An optional callback for the given parameters.
    func get(
        for kafkaHandle: OpaquePointer,
        messageID: UnsafeMutableRawPointer
    ) -> ((Result<KafkaConsumerMessage, KafkaError>) -> Void)? {

        let callback = self.callbacks[kafkaHandle]?[messageID]
        if callback != nil {
            self.callbacks[kafkaHandle]?[messageID] = nil
        }
        return callback
    }

    /// Set callback for a given Kafka handle and a message identifier.
    /// - Parameter for: The `kafkaHandle` of the requesting `KafkaProducer`'s client.
    /// - Parameter messageID: The message identifier.
    /// - Parameter callback: Closure that is called once the message delivery report is received
    func set(
        for kafkaHandle: OpaquePointer,
        messageID: UnsafeMutableRawPointer,
        callback: @escaping (Result<KafkaConsumerMessage, KafkaError>) -> Void
    ) {
        if self.callbacks[kafkaHandle] == nil {
            self.callbacks[kafkaHandle] = [:]
        }

        callbacks[kafkaHandle]?[messageID] = callback
    }

    /// Deallocate all message ID pointers and dictionary entries for a given Kafka handle
    /// - Parameter for: The `kafkaHandle` of the requesting `KafkaProducer`'s client.
    func deallocateCallbacks(for kafkaHandle: OpaquePointer) {
        self.callbacks[kafkaHandle]?.forEach { (messageID, _) in
            messageID.deallocate()
            self.callbacks[kafkaHandle]?[messageID] = nil
        }
    }
}
