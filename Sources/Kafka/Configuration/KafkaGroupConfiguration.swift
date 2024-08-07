//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

public struct KafkaGroupConfiguration {
    // MARK: - Common Client Config Properties

    /// Initial list of brokers.
    /// Default: `[]`
    public var bootstrapBrokerAddresses: [KafkaConfiguration.BrokerAddress] = []

    public init(bootstrapBrokerAddresses: [KafkaConfiguration.BrokerAddress]) {
        self.bootstrapBrokerAddresses = bootstrapBrokerAddresses
    }
}

// MARK: - KafkaGroupConfiguration + Dictionary

extension KafkaGroupConfiguration {
    internal var dictionary: [String: String] {
        var resultDict: [String: String] = [:]
        resultDict["bootstrap.servers"] = bootstrapBrokerAddresses.map(\.description).joined(separator: ",")
        return resultDict
    }
}

extension KafkaGroupConfiguration: Sendable {}
