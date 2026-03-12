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

// MARK: - RDKafkaStatistics

struct RDKafkaStatistics: Hashable, Codable {
    /// Handle instance name.
    let name: String
    /// Client identifier.
    let clientId: String
    /// Instance type (consumer, producer).
    let type: String

    /// Number of operations (callbacks, events, etc.) waiting in queue for application to serve with `poll()`
    let queueOperations: Int
    /// Current number of messages in producer queues.
    let queueMessages: Int
    /// Current total size of messages in producer queues (bytes).
    let queueMessagesSize: Int
    /// Threshold: maximum number of messages allowed in the producer queues.
    let queueMessagesMax: Int
    /// Threshold: maximum total size of messages allowed in the producer queues (bytes).
    let queueMessagesSizeMax: Int

    /// Total number of requests sent to Kafka brokers.
    let requestsSentTotal: Int
    /// Total number of bytes sent to Kafka brokers.
    let bytesSentTotal: Int
    /// Total number of responses received from Kafka brokers.
    let responsesReceivedTotal: Int
    /// Total number of bytes received from Kafka brokers.
    let bytesReceivedTotal: Int
    /// Total number of messages transmitted (produced) to Kafka brokers.
    let messagesSentTotal: Int
    /// Total number of message bytes (including framing) transmitted to Kafka brokers.
    let messageBytesSentTotal: Int
    /// Total number of messages consumed not including ignored messages (due to offset, etc) from Kafka brokers.
    let messagesReceivedTotal: Int
    /// Total number of message bytes (including framing) received from Kafka brokers.
    let messageBytesReceivedTotal: Int

    /// Per-broker statistics, keyed by broker name (e.g. "localhost:9092/0").
    let brokers: [String: BrokerStats]
    /// Per-topic statistics, keyed by topic name.
    let topics: [String: TopicStats]
    /// Consumer group statistics (consumer instances only).
    let consumerGroup: ConsumerGroupStats?

    enum CodingKeys: String, CodingKey {
        case name
        case clientId = "client_id"
        case type
        case queueOperations = "replyq"
        case queueMessages = "msg_cnt"
        case queueMessagesSize = "msg_size"
        case queueMessagesMax = "msg_max"
        case queueMessagesSizeMax = "msg_size_max"
        case requestsSentTotal = "tx"
        case bytesSentTotal = "tx_bytes"
        case responsesReceivedTotal = "rx"
        case bytesReceivedTotal = "rx_bytes"
        case messagesSentTotal = "txmsgs"
        case messageBytesSentTotal = "txmsg_bytes"
        case messagesReceivedTotal = "rxmsgs"
        case messageBytesReceivedTotal = "rxmsg_bytes"
        case brokers
        case topics
        case consumerGroup = "cgrp"
    }
}

extension RDKafkaStatistics {
    /// Rolling window statistics (latency, batch sizes, etc.).
    struct WindowStats: Hashable, Codable {
        let min: Int
        let max: Int
        let avg: Int
        let sum: Int
        let count: Int
        let stddev: Int
        let hdrsize: Int
        let p50: Int
        let p75: Int
        let p90: Int
        let p95: Int
        let p99: Int
        let p99_99: Int
        let outOfRange: Int

        enum CodingKeys: String, CodingKey {
            case min, max, avg, sum
            case count = "cnt"
            case stddev, hdrsize
            case p50, p75, p90, p95, p99
            case p99_99
            case outOfRange = "outofrange"
        }
    }
}

extension RDKafkaStatistics {
    /// Per-broker statistics.
    struct BrokerStats: Hashable, Codable {
        /// Broker hostname, port, and broker id.
        let name: String
        /// Broker id (-1 for bootstraps).
        let nodeId: Int
        /// Broker hostname.
        let nodeName: String
        /// Broker source (learned, configured, internal, logical).
        let source: String
        /// Broker state (INIT, DOWN, CONNECT, AUTH, APIVERSION, AUTH_HANDSHAKE, UP, UPDATE).
        let state: String
        /// Time since last broker state change (microseconds).
        let stateAge: Int

        /// Number of requests awaiting transmission to the broker.
        let outbufRequests: Int
        /// Number of messages awaiting transmission to the broker.
        let outbufMessages: Int
        /// Number of requests in-flight to the broker awaiting response.
        let waitrespRequests: Int
        /// Number of messages in-flight to the broker awaiting response.
        let waitrespMessages: Int

        /// Total number of requests sent.
        let requestsSentTotal: Int
        /// Total number of bytes sent.
        let bytesSentTotal: Int
        /// Total number of transmission errors.
        let transmitErrorsTotal: Int
        /// Total number of request retries.
        let requestRetriesTotal: Int
        /// Microseconds since last socket send (or -1 if no sends yet).
        let transmitIdleTime: Int
        /// Total number of requests timed out.
        let requestTimeoutsTotal: Int
        /// Total number of responses received.
        let responsesReceivedTotal: Int
        /// Total number of bytes received.
        let bytesReceivedTotal: Int
        /// Total number of receive errors.
        let receiveErrorsTotal: Int
        /// Total number of unmatched correlation ids in response.
        let correlationIdErrorsTotal: Int
        /// Total number of partial MessageSets received.
        let partialResponsesTotal: Int

        /// Microseconds since last socket receive (or -1 if no receives yet).
        let receiveIdleTime: Int
        /// Total number of decompression buffer size increases.
        let decompressBufferGrowTotal: Int
        /// Broker thread poll wakeups.
        let wakeupsTotal: Int
        /// Number of connection attempts, including successful and failed.
        let connectsTotal: Int
        /// Number of disconnects (triggered by broker, network, load-balancer, etc.).
        let disconnectsTotal: Int
        /// Internal producer queue latency (microseconds).
        let internalLatency: WindowStats
        /// Request queue latency (microseconds).
        let outbufLatency: WindowStats
        /// Broker latency / round-trip time (microseconds).
        let roundTripTime: WindowStats
        /// Broker throttle time (milliseconds).
        let throttleTime: WindowStats

        enum CodingKeys: String, CodingKey {
            case name
            case nodeId = "nodeid"
            case nodeName = "nodename"
            case source, state
            case stateAge = "stateage"
            case outbufRequests = "outbuf_cnt"
            case outbufMessages = "outbuf_msg_cnt"
            case waitrespRequests = "waitresp_cnt"
            case waitrespMessages = "waitresp_msg_cnt"
            case requestsSentTotal = "tx"
            case bytesSentTotal = "txbytes"
            case transmitErrorsTotal = "txerrs"
            case requestRetriesTotal = "txretries"
            case transmitIdleTime = "txidle"
            case requestTimeoutsTotal = "req_timeouts"
            case responsesReceivedTotal = "rx"
            case bytesReceivedTotal = "rxbytes"
            case receiveErrorsTotal = "rxerrs"
            case correlationIdErrorsTotal = "rxcorriderrs"
            case partialResponsesTotal = "rxpartial"
            case receiveIdleTime = "rxidle"
            case decompressBufferGrowTotal = "zbuf_grow"
            case wakeupsTotal = "wakeups"
            case connectsTotal = "connects"
            case disconnectsTotal = "disconnects"
            case internalLatency = "int_latency"
            case outbufLatency = "outbuf_latency"
            case roundTripTime = "rtt"
            case throttleTime = "throttle"
        }
    }
}

extension RDKafkaStatistics {
    /// Per-topic statistics.
    struct TopicStats: Hashable, Codable {
        /// Topic name.
        let topic: String?
        /// Age of the client's topic object (microseconds).
        let age: Int
        /// Age of metadata from broker for this topic (milliseconds).
        let metadataAge: Int
        /// Batch sizes in bytes.
        let batchBytes: WindowStats?
        /// Batch message counts.
        let batchMessages: WindowStats?
        /// Per-partition statistics, keyed by partition id as string.
        let partitions: [String: PartitionStats]?

        enum CodingKeys: String, CodingKey {
            case topic, age
            case metadataAge = "metadata_age"
            case batchBytes = "batchsize"
            case batchMessages = "batchcnt"
            case partitions
        }
    }
}

extension RDKafkaStatistics {
    /// Per-partition statistics.
    struct PartitionStats: Hashable, Codable {
        /// Partition id.
        let partition: Int
        /// The id of the broker this partition is served by.
        let broker: Int
        /// Current leader broker id.
        let leader: Int
        /// Partition is explicitly desired by the application.
        let desired: Bool
        /// Partition is not seen in topic metadata from broker.
        let unknown: Bool
        /// Number of messages waiting to be produced in first-level queue.
        let queueMessages: Int
        /// Number of bytes in messages waiting to be produced in first-level queue.
        let queueBytes: Int
        /// Number of messages ready to be produced in transmit queue.
        let transmitQueueMessages: Int
        /// Number of bytes ready to be produced in transmit queue.
        let transmitQueueBytes: Int
        /// Number of pre-fetched messages in fetch queue.
        let fetchQueueMessages: Int
        /// Bytes of pre-fetched messages in fetch queue.
        let fetchQueueSize: Int

        /// Consumer fetch state for this partition (e.g. "active", "none").
        let fetchState: String?
        /// Current/Last logical offset query.
        let queryOffset: Int
        /// Next offset to fetch.
        let nextOffset: Int
        /// Offset of last message passed to the application + 1.
        let appOffset: Int
        /// Offset to be committed.
        let storedOffset: Int
        /// Partition leader epoch of stored offset
        let storedLeaderEpoch: Int
        /// Last committed offset.
        let committedOffset: Int
        /// Partition leader epoch of committed offset
        let committedLeaderEpoch: Int
        /// Last PARTITION_EOF signaled offset.
        let eofOffset: Int
        /// Partition's low watermark offset on the broker.
        let lowWatermarkOffset: Int
        /// Partition's high watermark offset on the broker.
        let highWatermarkOffset: Int
        /// Partition's last stable offset on the broker (or same as highWatermarkOffset).
        let lastStableOffset: Int
        /// Difference between highWatermarkOffset and committedOffset (consumer lag).
        let consumerLag: Int
        /// Difference between highWatermarkOffset and storedOffset (consumer lag including not yet committed).
        let consumerLagStored: Int
        /// Last known partition leader epoch, or -1 if unknown.
        let leaderEpoch: Int

        /// Total number of messages transmitted (produced).
        let messagesSentTotal: Int
        /// Total number of bytes transmitted.
        let bytesSentTotal: Int
        /// Total number of messages consumed.
        let messagesReceivedTotal: Int
        /// Total number of bytes received.
        let bytesReceivedTotal: Int
        /// Total number of messages received (consumer) or produced (producer).
        let messagesTotal: Int
        /// Dropped outdated messages.
        let receivedVersionDropsTotal: Int
        /// Current number of messages in-flight to/from broker.
        let messagesInFlight: Int
        /// Next expected acknowledged sequence (idempotent producer).
        let nextAckSequence: Int
        /// Next expected error sequence (idempotent producer).
        let nextErrorSequence: Int
        /// Last acknowledged internal message id.
        let ackedMessageId: Int

        enum CodingKeys: String, CodingKey {
            case partition, broker, leader, desired, unknown
            case queueMessages = "msgq_cnt"
            case queueBytes = "msgq_bytes"
            case transmitQueueMessages = "xmit_msgq_cnt"
            case transmitQueueBytes = "xmit_msgq_bytes"
            case fetchQueueMessages = "fetchq_cnt"
            case fetchQueueSize = "fetchq_size"
            case fetchState = "fetch_state"
            case queryOffset = "query_offset"
            case nextOffset = "next_offset"
            case appOffset = "app_offset"
            case storedOffset = "stored_offset"
            case storedLeaderEpoch = "stored_leader_epoch"
            case committedOffset = "committed_offset"
            case committedLeaderEpoch = "committed_leader_epoch"
            case eofOffset = "eof_offset"
            case lowWatermarkOffset = "lo_offset"
            case highWatermarkOffset = "hi_offset"
            case lastStableOffset = "ls_offset"
            case consumerLag = "consumer_lag"
            case consumerLagStored = "consumer_lag_stored"
            case leaderEpoch = "leader_epoch"
            case messagesSentTotal = "txmsgs"
            case bytesSentTotal = "txbytes"
            case messagesReceivedTotal = "rxmsgs"
            case bytesReceivedTotal = "rxbytes"
            case messagesTotal = "msgs"
            case receivedVersionDropsTotal = "rx_ver_drops"
            case messagesInFlight = "msgs_inflight"
            case nextAckSequence = "next_ack_seq"
            case nextErrorSequence = "next_err_seq"
            case ackedMessageId = "acked_msgid"
        }
    }
}

extension RDKafkaStatistics {
    /// Consumer group statistics.
    struct ConsumerGroupStats: Hashable, Codable {
        /// Local consumer group handler's state.
        let state: String?
        /// Time elapsed since last state change (milliseconds).
        let stateAge: Int
        /// Local consumer group handler's join state.
        let joinState: String?
        /// Time elapsed since last rebalance (assign or revoke) (milliseconds).
        let rebalanceAge: Int
        /// Total number of rebalances (assign or revoke).
        let rebalancesTotal: Int
        /// Reason for the last rebalance.
        let rebalanceReason: String?
        /// Current assignment's partition count.
        let assignmentSize: Int

        enum CodingKeys: String, CodingKey {
            case state
            case stateAge = "stateage"
            case joinState = "join_state"
            case rebalanceAge = "rebalance_age"
            case rebalancesTotal = "rebalance_cnt"
            case rebalanceReason = "rebalance_reason"
            case assignmentSize = "assignment_size"
        }
    }
}
