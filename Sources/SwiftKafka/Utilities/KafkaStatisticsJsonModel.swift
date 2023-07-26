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

// This file was generated from JSON Schema using quicktype, do not modify it directly.
// To parse the JSON, add this file to your project and do:
//
//   let statistics = try? newJSONDecoder().decode(KafkaStatisticsJsonModel.self, from: jsonData)

// MARK: - Statistics

public struct KafkaStatisticsJson: Hashable, Codable {
    let name, clientID, type: String?
    let ts, time, age, replyq: Int?
    let msgCnt, msgSize, msgMax, msgSizeMax: Int?
    let simpleCnt, metadataCacheCnt: Int?
    let brokers: [String: Broker]?
    let topics: [String: Topic]?
    let cgrp: Cgrp?
    let tx, txBytes, rx, rxBytes: Int?
    let txmsgs, txmsgBytes, rxmsgs, rxmsgBytes: Int?

    enum CodingKeys: String, CodingKey {
        case name
        case clientID = "client_id"
        case type, ts, time, age, replyq
        case msgCnt = "msg_cnt"
        case msgSize = "msg_size"
        case msgMax = "msg_max"
        case msgSizeMax = "msg_size_max"
        case simpleCnt = "simple_cnt"
        case metadataCacheCnt = "metadata_cache_cnt"
        case brokers, topics, cgrp, tx
        case txBytes = "tx_bytes"
        case rx
        case rxBytes = "rx_bytes"
        case txmsgs
        case txmsgBytes = "txmsg_bytes"
        case rxmsgs
        case rxmsgBytes = "rxmsg_bytes"
    }
}

// MARK: - Broker

public struct Broker: Hashable, Codable {
    let name: String?
    let nodeid: Int?
    let nodename, source, state: String?
    let stateage, outbufCnt, outbufMsgCnt, waitrespCnt: Int?
    let waitrespMsgCnt, tx, txbytes, txerrs: Int?
    let txretries, txidle, reqTimeouts, rx: Int?
    let rxbytes, rxerrs, rxcorriderrs, rxpartial: Int?
    let rxidle, zbufGrow, bufGrow, wakeups: Int?
    let connects, disconnects: Int?
    let intLatency, outbufLatency, rtt, throttle: [String: Int]?
    let req: [String: Int]?
    let toppars: [String: Toppar]?

    enum CodingKeys: String, CodingKey {
        case name, nodeid, nodename, source, state, stateage
        case outbufCnt = "outbuf_cnt"
        case outbufMsgCnt = "outbuf_msg_cnt"
        case waitrespCnt = "waitresp_cnt"
        case waitrespMsgCnt = "waitresp_msg_cnt"
        case tx, txbytes, txerrs, txretries, txidle
        case reqTimeouts = "req_timeouts"
        case rx, rxbytes, rxerrs, rxcorriderrs, rxpartial, rxidle
        case zbufGrow = "zbuf_grow"
        case bufGrow = "buf_grow"
        case wakeups, connects, disconnects
        case intLatency = "int_latency"
        case outbufLatency = "outbuf_latency"
        case rtt, throttle, req, toppars
    }
}

// MARK: - Toppars

struct Toppar: Hashable, Codable {
    let topic: String?
    let partition: Int?

    enum CodingKeys: String, CodingKey {
        case topic, partition
    }
}

// MARK: - Cgrp

struct Cgrp: Hashable, Codable {
    let state: String?
    let stateage: Int?
    let joinState: String?
    let rebalanceAge, rebalanceCnt: Int?
    let rebalanceReason: String?
    let assignmentSize: Int?

    enum CodingKeys: String, CodingKey {
        case state, stateage
        case joinState = "join_state"
        case rebalanceAge = "rebalance_age"
        case rebalanceCnt = "rebalance_cnt"
        case rebalanceReason = "rebalance_reason"
        case assignmentSize = "assignment_size"
    }
}

// MARK: - Topic

struct Topic: Hashable, Codable {
    let topic: String?
    let age, metadataAge: Int?
    let batchsize, batchcnt: [String: Int]?
    let partitions: [String: Partition]?

    enum CodingKeys: String, CodingKey {
        case topic, age
        case metadataAge = "metadata_age"
        case batchsize, batchcnt, partitions
    }
}

// MARK: - Partition

struct Partition: Hashable, Codable {
    let partition, broker, leader: Int?
    let desired, unknown: Bool?
    let msgqCnt, msgqBytes, xmitMsgqCnt, xmitMsgqBytes: Int?
    let fetchqCnt, fetchqSize: Int?
    let fetchState: String?
    let queryOffset, nextOffset, appOffset, storedOffset: Int?
    let commitedOffset, committedOffset, eofOffset, loOffset: Int?
    let hiOffset, lsOffset, consumerLag, consumerLagStored: Int?
    let txmsgs, txbytes, rxmsgs, rxbytes: Int?
    let msgs, rxVerDrops, msgsInflight, nextACKSeq: Int?
    let nextErrSeq, ackedMsgid: Int?

    enum CodingKeys: String, CodingKey {
        case partition, broker, leader, desired, unknown
        case msgqCnt = "msgq_cnt"
        case msgqBytes = "msgq_bytes"
        case xmitMsgqCnt = "xmit_msgq_cnt"
        case xmitMsgqBytes = "xmit_msgq_bytes"
        case fetchqCnt = "fetchq_cnt"
        case fetchqSize = "fetchq_size"
        case fetchState = "fetch_state"
        case queryOffset = "query_offset"
        case nextOffset = "next_offset"
        case appOffset = "app_offset"
        case storedOffset = "stored_offset"
        case commitedOffset = "commited_offset"
        case committedOffset = "committed_offset"
        case eofOffset = "eof_offset"
        case loOffset = "lo_offset"
        case hiOffset = "hi_offset"
        case lsOffset = "ls_offset"
        case consumerLag = "consumer_lag"
        case consumerLagStored = "consumer_lag_stored"
        case txmsgs, txbytes, rxmsgs, rxbytes, msgs
        case rxVerDrops = "rx_ver_drops"
        case msgsInflight = "msgs_inflight"
        case nextACKSeq = "next_ack_seq"
        case nextErrSeq = "next_err_seq"
        case ackedMsgid = "acked_msgid"
    }
}
