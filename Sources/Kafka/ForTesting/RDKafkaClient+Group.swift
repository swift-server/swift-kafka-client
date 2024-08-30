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

import Crdkafka
import Logging

@_spi(Internal)
extension RDKafkaClient {
    /// List client consumer groups in cluster.
    /// Blocks for a maximum of `timeout` milliseconds.
    /// - Parameter timeout: timeout
    /// - Returns: Groups
    /// - Throws: A ``KafkaError`` if kafka call fails.
    public func _listGroups(timeout: Duration) throws -> [KafkaGroup] {
        try withKafkaHandlePointer { kafkaHandle in
            var adminOptions = rd_kafka_AdminOptions_new(kafkaHandle, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS)
            defer { rd_kafka_AdminOptions_destroy(adminOptions) }

            let timeoutMs = Int32(timeout.inMilliseconds)
            if timeoutMs > 0 {
                let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: RDKafkaClient.stringSize)
                defer { errorChars.deallocate() }
                let err = rd_kafka_AdminOptions_set_request_timeout(adminOptions, timeoutMs, errorChars, RDKafkaClient.stringSize)
                if err != RD_KAFKA_RESP_ERR_NO_ERROR {
                    throw KafkaError.rdKafkaError(wrapping: err, errorMessage: String(cString: errorChars))
                }
            }

            let resultQueue = rd_kafka_queue_new(kafkaHandle)
            defer { rd_kafka_queue_destroy(resultQueue) }

            rd_kafka_ListConsumerGroups(kafkaHandle, adminOptions, resultQueue)

            guard let resultEvent = rd_kafka_queue_poll(resultQueue, timeoutMs) else {
                throw KafkaError.rdKafkaError(errorMessage: "rd_kafka_queue_poll() timed out")
            }
            defer { rd_kafka_event_destroy(resultEvent) }

            if rd_kafka_event_error(resultEvent) != RD_KAFKA_RESP_ERR_NO_ERROR {
                let errorMessage = String(cString: rd_kafka_event_error_string(resultEvent))
                throw KafkaError.rdKafkaError(wrapping: rd_kafka_event_error(resultEvent), errorMessage: errorMessage)
            }

            let result = rd_kafka_event_ListConsumerGroups_result(resultEvent)

            var groupsCnt: size_t = 0
            let groups = rd_kafka_ListConsumerGroups_result_valid(result, &groupsCnt)
            guard let groups else {
                return []
            }

            var ret = [KafkaGroup]()
            for idx in 0..<groupsCnt {
                let group = groups[idx]
                let groupId = rd_kafka_ConsumerGroupListing_group_id(group)
                guard let groupId else {
                    throw KafkaError.rdKafkaError(errorMessage: "rd_kafka_ConsumerGroupListing_group_id() unexpectedly returned nil")
                }
                let kafkaGroupState = rd_kafka_ConsumerGroupListing_state(group)
                guard let state = KafkaGroup.State(rawValue: kafkaGroupState.rawValue) else {
                    throw KafkaError.rdKafkaError(errorMessage: "unexpected value \(kafkaGroupState) for rd_kafka_consumer_group_state_t enumeration")
                }
                let isSimple = rd_kafka_ConsumerGroupListing_is_simple_consumer_group(group)
                let kafkaGroup = KafkaGroup(name: String(cString: groupId), state: state, isSimple: isSimple != 0)
                ret.append(kafkaGroup)
            }

            return ret
        }
    }

    /// Delete client consumer groups from the cluster.
    /// Blocks for a maximum of `timeout` milliseconds.
    /// - Parameter groups: groups to delete
    /// - Parameter timeout: timeout
    /// - Returns: groups that could not be deleted with error message
    /// - Throws: A ``KafkaError`` if kafka call fails.
    public func _deleteGroups(_ groups: [String], timeout: Duration) throws -> [(groupId: String, errorMessage: String)] {
        try withKafkaHandlePointer { kafkaHandle in
            let delGroups = UnsafeMutablePointer<OpaquePointer?>.allocate(capacity: groups.count)

            for idx in 0..<groups.count {
                let group = groups[idx]
                delGroups[idx] = rd_kafka_DeleteGroup_new(group.cString(using: .utf8))
            }

            defer {
                rd_kafka_DeleteGroup_destroy_array(delGroups, groups.count)
                delGroups.deallocate()
            }

            var adminOptions = rd_kafka_AdminOptions_new(kafkaHandle, RD_KAFKA_ADMIN_OP_DELETEGROUPS)
            defer { rd_kafka_AdminOptions_destroy(adminOptions) }

            let timeoutMs = Int32(timeout.inMilliseconds)
            if timeoutMs > 0 {
                let errorChars = UnsafeMutablePointer<CChar>.allocate(capacity: RDKafkaClient.stringSize)
                defer { errorChars.deallocate() }
                let err = rd_kafka_AdminOptions_set_request_timeout(adminOptions, timeoutMs, errorChars, RDKafkaClient.stringSize)
                if err != RD_KAFKA_RESP_ERR_NO_ERROR {
                    throw KafkaError.rdKafkaError(wrapping: err, errorMessage: String(cString: errorChars))
                }
            }

            let resultQueue = rd_kafka_queue_new(kafkaHandle)
            defer { rd_kafka_queue_destroy(resultQueue) }

            rd_kafka_DeleteGroups(kafkaHandle, delGroups, groups.count, adminOptions, resultQueue);

            guard let resultEvent = rd_kafka_queue_poll(resultQueue, timeoutMs) else {
                throw KafkaError.rdKafkaError(errorMessage: "rd_kafka_queue_poll() timed out")
            }
            defer { rd_kafka_event_destroy(resultEvent) }

            if rd_kafka_event_error(resultEvent) != RD_KAFKA_RESP_ERR_NO_ERROR {
                let errorMessage = String(cString: rd_kafka_event_error_string(resultEvent))
                throw KafkaError.rdKafkaError(wrapping: rd_kafka_event_error(resultEvent), errorMessage: errorMessage)
            }

            var resultGroupsCnt = 0
            let resultGroups = rd_kafka_DeleteGroups_result_groups(resultEvent, &resultGroupsCnt);
            guard let resultGroups else {
                return []
            }

            var ret = [(String, String)]()
            for idx in 0..<resultGroupsCnt {
                if let groupResultError = rd_kafka_group_result_error(resultGroups[idx]) {
                    let groupName = String(cString: rd_kafka_group_result_name(resultGroups[idx]))
                    let errorString = String(cString: rd_kafka_error_string(groupResultError))
                    ret.append((groupName, errorString))
                }
            }

            return ret
        }
    }
}
