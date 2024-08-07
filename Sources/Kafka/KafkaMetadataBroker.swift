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

/// A proxy structure for rd_kafka_metadata_broker_t
public struct KafkaMetadataBroker: Sendable, Hashable {
    public let id: Int32
    public let host: String
    public let port: Int32

    public init(id: Int32, host: String, port: Int32) {
        self.id = id
        self.host = host
        self.port = port
    }

    init(_ rdMetadataBroker: rd_kafka_metadata_broker_t) {
        self.id = rdMetadataBroker.id
        self.host = String(cString: rdMetadataBroker.host)
        self.port = rdMetadataBroker.port
    }
}
