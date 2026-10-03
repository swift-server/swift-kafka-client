//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2026 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import CoreMetrics
import Foundation
import MetricsTestKit
import Testing

@testable import Kafka

/// Verifies that `RDKafkaStatistics` decodes a representative librdkafka statistics
/// payload, including the nested broker / topic / partition / consumer-group stats.
@Suite
struct RDKafkaStatisticsTests {
    let statisticsJson = """
        {
          "name": "rdkafka#producer-1",
          "client_id": "rdkafka",
          "type": "producer",
          "ts": 5016483227792,
          "time": 1527060869,
          "replyq": 0,
          "msg_cnt": 22710,
          "msg_size": 704010,
          "msg_max": 500000,
          "msg_size_max": 1073741824,
          "simple_cnt": 0,
          "metadata_cache_cnt": 1,
          "brokers": {
            "localhost:9092/2": {
              "name": "localhost:9092/2",
              "nodeid": 2,
              "nodename": "localhost:9092",
              "source": "learned",
              "state": "UP",
              "stateage": 9057234,
              "outbuf_cnt": 0,
              "outbuf_msg_cnt": 0,
              "waitresp_cnt": 0,
              "waitresp_msg_cnt": 0,
              "tx": 320,
              "txbytes": 84283332,
              "txerrs": 0,
              "txretries": 0,
              "req_timeouts": 0,
              "rx": 320,
              "rxbytes": 15708,
              "rxerrs": 0,
              "rxcorriderrs": 0,
              "rxpartial": 0,
              "zbuf_grow": 0,
              "buf_grow": 0,
              "wakeups": 591067,
              "connects": 1,
              "disconnects": 0,
              "txidle": 10000,
              "rxidle": 10000,
              "int_latency": {
                "min": 86,
                "max": 59375,
                "avg": 23726,
                "sum": 5694616664,
                "stddev": 13982,
                "p50": 28031,
                "p75": 36095,
                "p90": 39679,
                "p95": 43263,
                "p99": 48639,
                "p99_99": 59391,
                "outofrange": 0,
                "hdrsize": 11376,
                "cnt": 240012
              },
              "outbuf_latency": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "stddev": 0,
                "p50": 0,
                "p75": 0,
                "p90": 0,
                "p95": 0,
                "p99": 0,
                "p99_99": 0,
                "outofrange": 0,
                "hdrsize": 0,
                "cnt": 0
              },
              "rtt": {
                "min": 1580,
                "max": 3389,
                "avg": 2349,
                "sum": 79868,
                "stddev": 474,
                "p50": 2319,
                "p75": 2543,
                "p90": 3183,
                "p95": 3199,
                "p99": 3391,
                "p99_99": 3391,
                "outofrange": 0,
                "hdrsize": 13424,
                "cnt": 34
              },
              "throttle": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "stddev": 0,
                "p50": 0,
                "p75": 0,
                "p90": 0,
                "p95": 0,
                "p99": 0,
                "p99_99": 0,
                "outofrange": 0,
                "hdrsize": 17520,
                "cnt": 34
              },
              "toppars": {
                "test-1": {
                  "topic": "test",
                  "partition": 1
                }
              }
            },
            "localhost:9093/3": {
              "name": "localhost:9093/3",
              "nodeid": 3,
              "nodename": "localhost:9093",
              "source": "learned",
              "state": "UP",
              "stateage": 9057209,
              "outbuf_cnt": 0,
              "outbuf_msg_cnt": 0,
              "waitresp_cnt": 0,
              "waitresp_msg_cnt": 0,
              "tx": 310,
              "txbytes": 84301122,
              "txerrs": 0,
              "txretries": 0,
              "req_timeouts": 0,
              "rx": 310,
              "rxbytes": 15104,
              "rxerrs": 0,
              "rxcorriderrs": 0,
              "rxpartial": 0,
              "zbuf_grow": 0,
              "buf_grow": 0,
              "wakeups": 607956,
              "connects": 1,
              "disconnects": 0,
              "txidle": 10000,
              "rxidle": 10000,
              "int_latency": {
                "min": 82,
                "max": 58069,
                "avg": 23404,
                "sum": 5617432101,
                "stddev": 14021,
                "p50": 27391,
                "p75": 35839,
                "p90": 39679,
                "p95": 42751,
                "p99": 48639,
                "p99_99": 58111,
                "outofrange": 0,
                "hdrsize": 11376,
                "cnt": 240016
              },
              "outbuf_latency": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "stddev": 0,
                "p50": 0,
                "p75": 0,
                "p90": 0,
                "p95": 0,
                "p99": 0,
                "p99_99": 0,
                "outofrange": 0,
                "hdrsize": 0,
                "cnt": 0
              },
              "rtt": {
                "min": 1704,
                "max": 3572,
                "avg": 2493,
                "sum": 87289,
                "stddev": 559,
                "p50": 2447,
                "p75": 2895,
                "p90": 3375,
                "p95": 3407,
                "p99": 3583,
                "p99_99": 3583,
                "outofrange": 0,
                "hdrsize": 13424,
                "cnt": 35
              },
              "throttle": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "stddev": 0,
                "p50": 0,
                "p75": 0,
                "p90": 0,
                "p95": 0,
                "p99": 0,
                "p99_99": 0,
                "outofrange": 0,
                "hdrsize": 17520,
                "cnt": 35
              },
              "toppars": {
                "test-0": {
                  "topic": "test",
                  "partition": 0
                }
              }
            },
            "localhost:9094/4": {
              "name": "localhost:9094/4",
              "nodeid": 4,
              "nodename": "localhost:9094",
              "source": "learned",
              "state": "UP",
              "stateage": 9057207,
              "outbuf_cnt": 0,
              "outbuf_msg_cnt": 0,
              "waitresp_cnt": 0,
              "waitresp_msg_cnt": 0,
              "tx": 1,
              "txbytes": 25,
              "txerrs": 0,
              "txretries": 0,
              "req_timeouts": 0,
              "rx": 1,
              "rxbytes": 272,
              "rxerrs": 0,
              "rxcorriderrs": 0,
              "rxpartial": 0,
              "zbuf_grow": 0,
              "buf_grow": 0,
              "wakeups": 4,
              "connects": 1,
              "disconnects": 0,
              "txidle": 0,
              "rxidle": 0,
              "int_latency": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "stddev": 0,
                "p50": 0,
                "p75": 0,
                "p90": 0,
                "p95": 0,
                "p99": 0,
                "p99_99": 0,
                "outofrange": 0,
                "hdrsize": 11376,
                "cnt": 0
              },
              "outbuf_latency": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "stddev": 0,
                "p50": 0,
                "p75": 0,
                "p90": 0,
                "p95": 0,
                "p99": 0,
                "p99_99": 0,
                "outofrange": 0,
                "hdrsize": 0,
                "cnt": 0
              },
              "rtt": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "stddev": 0,
                "p50": 0,
                "p75": 0,
                "p90": 0,
                "p95": 0,
                "p99": 0,
                "p99_99": 0,
                "outofrange": 0,
                "hdrsize": 13424,
                "cnt": 0
              },
              "throttle": {
                "min": 0,
                "max": 0,
                "avg": 0,
                "sum": 0,
                "stddev": 0,
                "p50": 0,
                "p75": 0,
                "p90": 0,
                "p95": 0,
                "p99": 0,
                "p99_99": 0,
                "outofrange": 0,
                "hdrsize": 17520,
                "cnt": 0
              },
              "toppars": {}
            }
          },
          "topics": {
            "test": {
              "topic": "test",
              "age": 9060000,
              "metadata_age": 9060,
              "batchsize": {
                "min": 99,
                "max": 391805,
                "avg": 272593,
                "sum": 18808985,
                "stddev": 180408,
                "p50": 393215,
                "p75": 393215,
                "p90": 393215,
                "p95": 393215,
                "p99": 393215,
                "p99_99": 393215,
                "outofrange": 0,
                "hdrsize": 14448,
                "cnt": 69
              },
              "batchcnt": {
                "min": 1,
                "max": 10000,
                "avg": 6956,
                "sum": 480028,
                "stddev": 4608,
                "p50": 10047,
                "p75": 10047,
                "p90": 10047,
                "p95": 10047,
                "p99": 10047,
                "p99_99": 10047,
                "outofrange": 0,
                "hdrsize": 8304,
                "cnt": 69
              },
              "partitions": {
                "0": {
                  "partition": 0,
                  "broker": 3,
                  "leader": 3,
                  "desired": false,
                  "unknown": false,
                  "msgq_cnt": 1,
                  "msgq_bytes": 31,
                  "xmit_msgq_cnt": 0,
                  "xmit_msgq_bytes": 0,
                  "fetchq_cnt": 0,
                  "fetchq_size": 0,
                  "fetch_state": "none",
                  "query_offset": 0,
                  "next_offset": 0,
                  "app_offset": -1001,
                  "stored_offset": -1001,
                  "stored_leader_epoch": -1,
                  "commited_offset": -1001,
                  "committed_offset": -1001,
                  "committed_leader_epoch": -1,
                  "eof_offset": -1001,
                  "lo_offset": -1001,
                  "hi_offset": -1001,
                  "ls_offset": -1001,
                  "consumer_lag": -1,
                  "consumer_lag_stored": -1,
                  "leader_epoch": -1,
                  "txmsgs": 2150617,
                  "txbytes": 66669127,
                  "rxmsgs": 0,
                  "rxbytes": 0,
                  "msgs": 2160510,
                  "rx_ver_drops": 0,
                  "msgs_inflight": 0,
                  "next_ack_seq": 0,
                  "next_err_seq": 0,
                  "acked_msgid": 0
                },
                "1": {
                  "partition": 1,
                  "broker": 2,
                  "leader": 2,
                  "desired": false,
                  "unknown": false,
                  "msgq_cnt": 0,
                  "msgq_bytes": 0,
                  "xmit_msgq_cnt": 0,
                  "xmit_msgq_bytes": 0,
                  "fetchq_cnt": 0,
                  "fetchq_size": 0,
                  "fetch_state": "none",
                  "query_offset": 0,
                  "next_offset": 0,
                  "app_offset": -1001,
                  "stored_offset": -1001,
                  "stored_leader_epoch": -1,
                  "commited_offset": -1001,
                  "committed_offset": -1001,
                  "committed_leader_epoch": -1,
                  "eof_offset": -1001,
                  "lo_offset": -1001,
                  "hi_offset": -1001,
                  "ls_offset": -1001,
                  "consumer_lag": -1,
                  "consumer_lag_stored": -1,
                  "leader_epoch": -1,
                  "txmsgs": 2150136,
                  "txbytes": 66654216,
                  "rxmsgs": 0,
                  "rxbytes": 0,
                  "msgs": 2159735,
                  "rx_ver_drops": 0,
                  "msgs_inflight": 0,
                  "next_ack_seq": 0,
                  "next_err_seq": 0,
                  "acked_msgid": 0
                },
                "-1": {
                  "partition": -1,
                  "broker": -1,
                  "leader": -1,
                  "desired": false,
                  "unknown": false,
                  "msgq_cnt": 0,
                  "msgq_bytes": 0,
                  "xmit_msgq_cnt": 0,
                  "xmit_msgq_bytes": 0,
                  "fetchq_cnt": 0,
                  "fetchq_size": 0,
                  "fetch_state": "none",
                  "query_offset": 0,
                  "next_offset": 0,
                  "app_offset": -1001,
                  "stored_offset": -1001,
                  "stored_leader_epoch": -1,
                  "commited_offset": -1001,
                  "committed_offset": -1001,
                  "committed_leader_epoch": -1,
                  "eof_offset": -1001,
                  "lo_offset": -1001,
                  "hi_offset": -1001,
                  "ls_offset": -1001,
                  "consumer_lag": -1,
                  "consumer_lag_stored": -1,
                  "leader_epoch": -1,
                  "txmsgs": 0,
                  "txbytes": 0,
                  "rxmsgs": 0,
                  "rxbytes": 0,
                  "msgs": 1177,
                  "rx_ver_drops": 0,
                  "msgs_inflight": 0,
                  "next_ack_seq": 0,
                  "next_err_seq": 0,
                  "acked_msgid": 0
                }
              }
            }
          },
          "tx": 631,
          "tx_bytes": 168584479,
          "rx": 631,
          "rx_bytes": 31084,
          "txmsgs": 4300753,
          "txmsg_bytes": 133323343,
          "rxmsgs": 0,
          "rxmsg_bytes": 0,
          "cgrp": {
            "state": "up",
            "stateage": 0,
            "join_state": "steady",
            "rebalance_age": 0,
            "rebalance_cnt": 2,
            "assignment_size": 0
          }
        }
        """.data(using: .utf8)!

    @Test func decodesNestedBrokerTopicPartitionAndConsumerGroupStats() throws {
        let stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: statisticsJson)

        // Top-level counters decode.
        #expect(stats.type == "producer")
        #expect(stats.requestsSentTotal >= 0)
        #expect(stats.topicsInMetadataCache != nil)

        // Per-broker stats, including the latency window objects (rtt/throttle).
        #expect(!stats.brokers.isEmpty)
        let broker = try #require(stats.brokers.values.first)
        #expect(broker.roundTripTime.hdrsize >= 0)
        #expect(broker.throttleTime.hdrsize >= 0)

        // Per-partition stats, including consumer lag, are decoded.
        let partitions = stats.topics.values.compactMap(\.partitions).flatMap(\.values)
        #expect(!partitions.isEmpty)
        // Every partition exposes a (possibly -1) consumer lag value.
        #expect(partitions.allSatisfy { $0.consumerLag >= -1 })

        // Consumer-group stats are present and decode the rebalance count.
        let cgrp = try #require(stats.consumerGroup)
        #expect(cgrp.rebalancesTotal == 2)
    }

    // MARK: - Statistics → metric instrument mapping

    // These drive `updateFromStatistics` directly against the decoded fixture inside a
    // `withMetricsFactory` scope so the lazily-created dimensioned gauges (per-broker latency
    // windows) capture the task-local `TestMetrics`. This covers the emission paths that the
    // broker-backed integration test cannot assert on (its dimensioned gauges are created on the
    // run-loop task, outside the task-local scope).

    @Test func producerStatisticsPopulateInstruments() throws {
        let metrics = TestMetrics()
        let stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: self.statisticsJson)

        withMetricsFactory(metrics) {
            KafkaProducerMetrics(prefix: "kafka", clientID: "test-client").updateFromStatistics(stats)
        }

        // Every instrument carries the client dimension.
        let client = [("client_id", "test-client")]
        // Top-level producer gauges.
        #expect(try metrics.expectGauge("kafka.producer.queue.messages", client).lastValue == 22710)
        #expect(try metrics.expectGauge("kafka.producer.queue.bytes", client).lastValue == 704010)
        // Cumulative librdkafka totals are set on a meter as-is.
        #expect(try metrics.expectMeter("kafka.producer.messages.sent", client).lastValue == 4_300_753)

        // Per-broker latency windows are recorded as avg/p99/max gauges (in milliseconds),
        // dimensioned by client and broker.
        let broker = client + [("broker", "localhost:9092/2")]
        #expect(try metrics.expectGauge("kafka.producer.broker.rtt.avg.ms", broker).lastValue == 2349.0 / 1000)
        #expect(try metrics.expectGauge("kafka.producer.broker.rtt.p99.ms", broker).lastValue == 3391.0 / 1000)
        #expect(try metrics.expectGauge("kafka.producer.broker.rtt.max.ms", broker).lastValue == 3389.0 / 1000)
        // int_latency maps to queue.latency.
        #expect(
            try metrics.expectGauge("kafka.producer.broker.queue.latency.avg.ms", broker).lastValue == 23726.0 / 1000
        )
        // outbuf_latency has cnt == 0 in the fixture, so the window is skipped entirely.
        #expect((try? metrics.expectGauge("kafka.producer.broker.request.latency.avg.ms", broker)) == nil)
    }

    @Test func consumerStatisticsPopulateInstruments() throws {
        let metrics = TestMetrics()
        let stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: self.statisticsJson)

        withMetricsFactory(metrics) {
            KafkaConsumerMetrics(prefix: "kafka", clientID: "test-client").updateFromStatistics(stats)
        }

        let client = [("client_id", "test-client")]
        // queue.operations mirrors `replyq` (0 in the fixture).
        #expect(try metrics.expectGauge("kafka.consumer.queue.operations", client).lastValue == 0)
        // Rebalances mirror the authoritative cgrp counter (rebalance_cnt == 2).
        #expect(try metrics.expectMeter("kafka.consumer.rebalances", client).lastValue == 2)
        // Every partition in the fixture reports consumer_lag == -1 (unknown), which is skipped, so
        // the max-lag gauge settles at 0.
        #expect(try metrics.expectGauge("kafka.consumer.lag.max", client).lastValue == 0)
        // Per-broker round-trip latency is recorded for the consumer too (milliseconds).
        let broker = client + [("broker", "localhost:9092/2")]
        #expect(try metrics.expectGauge("kafka.consumer.broker.rtt.avg.ms", broker).lastValue == 2349.0 / 1000)
    }

    @Test func cumulativeTotalsAreSetPerClientWithoutAccumulating() throws {
        let metrics = TestMetrics()
        let stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: self.statisticsJson)

        withMetricsFactory(metrics) {
            let first = KafkaProducerMetrics(prefix: "kafka", clientID: "producer-a")
            let second = KafkaProducerMetrics(prefix: "kafka", clientID: "producer-b")
            // Two samples of the same cumulative total must not double the value.
            first.updateFromStatistics(stats)
            first.updateFromStatistics(stats)
            second.updateFromStatistics(stats)
        }

        // Each client reports into its own series instead of overwriting the other's.
        let first = try metrics.expectMeter("kafka.producer.messages.sent", [("client_id", "producer-a")])
        let second = try metrics.expectMeter("kafka.producer.messages.sent", [("client_id", "producer-b")])
        #expect(first.lastValue == 4_300_753)
        #expect(second.lastValue == 4_300_753)
    }
}
