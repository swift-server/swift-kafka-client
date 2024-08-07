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

public struct KafkaGroup {
    public let name: String
}

extension KafkaGroup {
    public static func list(configuration: KafkaGroupConfiguration,
                            group: String? = nil,
                            retries: Int = 5,
                            timeout: Duration = .seconds(5)) throws -> [KafkaGroup] {
        let configDictionary = configuration.dictionary
        let rdConfig = try RDKafkaConfig.createFrom(configDictionary: configDictionary)

        let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: RDKafkaClient.stringSize)
        defer { errorChars.deallocate() }

        guard let kafkaHandle = rd_kafka_new(RD_KAFKA_PRODUCER, rdConfig, errorChars, RDKafkaClient.stringSize) else {
            rd_kafka_conf_destroy(rdConfig)
            let errorString = String(cString: errorChars)
            throw KafkaError.client(reason: errorString)
        }
        defer { rd_kafka_destroy(kafkaHandle) }

        let rdGroup = group?.cString(using: .utf8)
        let timeoutMs = Int32(timeout.inMilliseconds)
        var err = RD_KAFKA_RESP_ERR_NO_ERROR
        var grplist: UnsafePointer<rd_kafka_group_list>? = nil
        var retries = min(retries, 1)
        while true {
            err = rd_kafka_list_groups(kafkaHandle, rdGroup, &grplist, timeoutMs)
            if err == RD_KAFKA_RESP_ERR_NO_ERROR {
                break
            }
            else if (err == RD_KAFKA_RESP_ERR__TRANSPORT) || (err == RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS) {
                retries -= 1
                if retries == 0 {
                    throw KafkaError.rdKafkaError(wrapping: err)
                }
            } else {
                throw KafkaError.rdKafkaError(wrapping: err)
            }
        }

        defer { rd_kafka_group_list_destroy(grplist) }

        if let grplist {
            var groups = [KafkaGroup]()
            for idx in 0..<Int(grplist.pointee.group_cnt) {
                let rdGroupInfo = grplist.pointee.groups[idx]
                let groupName = String(cString: rdGroupInfo.group)
                groups.append(KafkaGroup(name: groupName))
            }
            return groups
        } else {
            return []
        }
    }
}
