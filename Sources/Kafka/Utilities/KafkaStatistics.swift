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

import ExtrasJSON

struct KafkaStatistics: Sendable, Hashable {
    let jsonString: String
    
    func fill(_ options: KafkaConfiguration.MetricsOptions) {
        do {
            let json = try XJSONDecoder().decode(KafkaStatisticsJson.self, from: self.jsonString.utf8)
            if let age = options.age,
               let jsonAge = json.age {
                age.recordMicroseconds(jsonAge)
            }
            
            // TODO: other metrics
        } catch {
            fatalError("Statistics json decode error \(error)")
        }
    }
}
