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

import Logging
@_spi(Internal) import Kafka
import struct Foundation.UUID

public struct TestRDKafkaClient {
    public static let stringSize = 1024
    
    let client: Kafka.RDKafkaClient
    
    /// creates librdkafka dictionary config
    public static func _createDummyConfig(bootstrapAddresses: KafkaConfiguration.BrokerAddress, addressFamily: KafkaConfiguration.IPAddressFamily = .any, consumer: Bool = true) -> [String: String] {
        if consumer {
            var config = KafkaConsumerConfiguration(consumptionStrategy: .group(id: "[no id]", topics: []), bootstrapBrokerAddresses: [bootstrapAddresses])
            config.broker.addressFamily = addressFamily
            return config.dictionary
        }
        var config = KafkaProducerConfiguration(bootstrapBrokerAddresses: [bootstrapAddresses])
        config.broker.addressFamily = addressFamily
        return config.dictionary
    }
    
    /// creates RDKafkaClient with dictionary config
    public static func _makeRDKafkaClient(config: [String: String], logger: Logger? = nil, consumer: Bool = true) throws -> TestRDKafkaClient {
        let rdKafkaClient = try Kafka.RDKafkaClient.makeClient(type: consumer ? .consumer : .producer, configDictionary: config, events: consumer ? [] : [.deliveryReport], logger: logger ?? .kafkaTest)
        return TestRDKafkaClient(client: rdKafkaClient)
    }
    
    
    @discardableResult
    public func withKafkaHandlePointer<T>(_ body: (OpaquePointer) throws -> T) rethrows -> T {
        try self.client.withKafkaHandlePointer(body)
    }
    
    @discardableResult
    public func withKafkaHandlePointer<T>(_ body: (OpaquePointer) async throws -> T) async rethrows -> T {
        try await self.client.withKafkaHandlePointer(body)
    }
}
