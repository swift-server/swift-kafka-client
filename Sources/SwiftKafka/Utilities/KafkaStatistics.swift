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

import ExtrasJSON

public struct KafkaStatistics: Sendable, Hashable {
    public let jsonString: String

    public var json: KafkaStatisticsJson {
        get throws {
            return try XJSONDecoder().decode(KafkaStatisticsJson.self, from: self.jsonString.utf8)
        }
    }
}
