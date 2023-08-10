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

// This file was generated from JSON Schema using quicktype, do not modify it directly.
// To parse the JSON, add this file to your project and do:
//
//   let statistics = try? newJSONDecoder().decode(KafkaStatisticsJsonModel.self, from: jsonData)

// MARK: - Statistics

public struct KafkaStatisticsJson: Hashable, Codable {
    public let name, clientID, type: String?
    public let ts, time, age, replyq: Int?
    public let msgCnt, msgSize, msgMax, msgSizeMax: Int?
    public let simpleCnt, metadataCacheCnt: Int?
    public let brokers: [String: Broker]?
    public let topics: [String: Topic]?
    public let cgrp: Cgrp?
    public let tx, txBytes, rx, rxBytes: Int?
    public let txmsgs, txmsgBytes, rxmsgs, rxmsgBytes: Int?

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
    public let name: String?
    public let nodeid: Int?
    public let nodename, source, state: String?
    public let stateage, outbufCnt, outbufMsgCnt, waitrespCnt: Int?
    public let waitrespMsgCnt, tx, txbytes, txerrs: Int?
    public let txretries, txidle, reqTimeouts, rx: Int?
    public let rxbytes, rxerrs, rxcorriderrs, rxpartial: Int?
    public let rxidle, zbufGrow, bufGrow, wakeups: Int?
    public let connects, disconnects: Int?
    public let intLatency, outbufLatency, rtt, throttle: [String: Int]?
    public let req: [String: Int]?
    public let toppars: [String: Toppar]?

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

public struct Toppar: Hashable, Codable {
    public let topic: String?
    public let partition: Int?

    enum CodingKeys: String, CodingKey {
        case topic, partition
    }
}

// MARK: - Cgrp

public struct Cgrp: Hashable, Codable {
    public let state: String?
    public let stateage: Int?
    public let joinState: String?
    public let rebalanceAge, rebalanceCnt: Int?
    public let rebalanceReason: String?
    public let assignmentSize: Int?

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

public struct Topic: Hashable, Codable {
    public let topic: String?
    public let age, metadataAge: Int?
    public let batchsize, batchcnt: [String: Int]?
    public let partitions: [String: Partition]?

    enum CodingKeys: String, CodingKey {
        case topic, age
        case metadataAge = "metadata_age"
        case batchsize, batchcnt, partitions
    }
}

// MARK: - Partition

public struct Partition: Hashable, Codable {
    public let partition, broker, leader: Int?
    public let desired, unknown: Bool?
    public let msgqCnt, msgqBytes, xmitMsgqCnt, xmitMsgqBytes: Int?
    public let fetchqCnt, fetchqSize: Int?
    public let fetchState: String?
    public let queryOffset, nextOffset, appOffset, storedOffset: Int?
    public let commitedOffset, committedOffset, eofOffset, loOffset: Int?
    public let hiOffset, lsOffset, consumerLag, consumerLagStored: Int?
    public let txmsgs, txbytes, rxmsgs, rxbytes: Int?
    public let msgs, rxVerDrops, msgsInflight, nextACKSeq: Int?
    public let nextErrSeq, ackedMsgid: Int?

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
