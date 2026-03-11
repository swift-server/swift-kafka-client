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

import Logging
import Metrics
import MetricsTestKit
import ServiceLifecycle
import Testing

import struct Foundation.UUID

@testable import Kafka

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

final class KafkaMetricsTests {
    let kafkaHost: String = ProcessInfo.processInfo.environment["KAFKA_HOST"] ?? "localhost"
    let kafkaPort: Int = .init(ProcessInfo.processInfo.environment["KAFKA_PORT"] ?? "9092")!
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

    private var testMetrics: TestMetrics
    private var kafkaMetrics: KafkaMetrics
    private var stats: RDKafkaStatistics

    init() throws {
        testMetrics = TestMetrics()
        kafkaMetrics = KafkaMetrics(prefix: "kafka", factory: testMetrics)
        stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: statisticsJson)
    }

    @Test func topLevelStatistics() throws {
        kafkaMetrics.update(with: stats)

        let metric = try testMetrics.expectGauge("kafka_bytes_sent_total", [("client", "rdkafka")])
        #expect(metric.lastValue == 168584479.0)
    }

    @Test func brokerStatistics() throws {
        kafkaMetrics.update(with: stats)

        let dimensions = [("client", "rdkafka"), ("broker", "localhost:9092/2")]

        let metric = try testMetrics.expectGauge("kafka_broker_requests_sent_total", dimensions)
        #expect(metric.lastValue == 320.0)
    }

    @Test func topicStatistics() throws {
        kafkaMetrics.update(with: stats)

        let dimensions = [("client", "rdkafka"), ("topic", "test")]

        let metric = try testMetrics.expectGauge("kafka_topic_batch_bytes_min", dimensions)
        #expect(metric.lastValue == 99.0)
    }

    @Test func partitionStatistics() throws {
        kafkaMetrics.update(with: stats)

        let dimensions = [("client", "rdkafka"), ("topic", "test"), ("partition", "1")]

        let metric = try testMetrics.expectGauge("kafka_partition_messages_sent_total", dimensions)
        #expect(metric.lastValue == 2150136.0)
    }

    @Test func consumerGroupStatistics() throws {
        kafkaMetrics.update(with: stats)

        let metric = try testMetrics.expectGauge("kafka_cgrp_rebalances_total", [("client", "rdkafka")])
        #expect(metric.lastValue == 2.0)
    }

    @Test func customPrefix() throws {
        let kafkaMetrics = KafkaMetrics(prefix: "myapp_consumer", factory: testMetrics)
        kafkaMetrics.update(with: stats)

        let metric = try testMetrics.expectGauge("myapp_consumer_bytes_sent_total", [("client", "rdkafka")])
        #expect(metric.lastValue == 168584479.0)
    }

    @Test func brokerMetricsDisabled() throws {
        let kafkaMetrics = KafkaMetrics(
            prefix: "kafka",
            enableBrokerMetrics: false,
            factory: testMetrics
        )
        kafkaMetrics.update(with: stats)

        let metric = try testMetrics.expectGauge("kafka_queue_operations", [("client", "rdkafka")])
        #expect(metric.lastValue == 0.0)

        let skippedMetric = [("client", "rdkafka"), ("broker", "localhost:9092/2")]
        #expect(throws: TestMetricsError.self) {
            try self.testMetrics.expectGauge("kafka_broker_requests_sent_total", skippedMetric)
        }
    }

    @Test func topicMetricsDisabled() throws {
        let kafkaMetrics = KafkaMetrics(
            prefix: "kafka",
            enableTopicMetrics: false,
            factory: testMetrics
        )
        kafkaMetrics.update(with: stats)

        let metric = try testMetrics.expectGauge("kafka_queue_operations", [("client", "rdkafka")])
        #expect(metric.lastValue == 0.0)

        #expect(throws: TestMetricsError.self) {
            try self.testMetrics.expectGauge("kafka_topic_metadata_age", [("client", "rdkafka"), ("topic", "test")])
        }

        #expect(throws: TestMetricsError.self) {
            try self.testMetrics.expectGauge(
                "kafka_partition_consumer_lag",
                [("client", "rdkafka"), ("topic", "test"), ("partition", "0")]
            )
        }
    }

    @Test func customWindowStatistics() throws {
        let kafkaMetrics = KafkaMetrics(
            prefix: "kafka",
            windowMetrics: [.p99, .p99_99],
            factory: testMetrics
        )

        let stats = try JSONDecoder().decode(RDKafkaStatistics.self, from: statisticsJson)
        kafkaMetrics.update(with: stats)

        let dimensions = [("client", "rdkafka"), ("broker", "localhost:9092/2")]

        let rttP99 = try testMetrics.expectGauge("kafka_broker_round_trip_time_p99", dimensions)
        #expect(rttP99.lastValue == 3391.0)

        let rttP9999 = try testMetrics.expectGauge("kafka_broker_round_trip_time_p99_99", dimensions)
        #expect(rttP9999.lastValue == 3391.0)

        #expect(throws: TestMetricsError.self) {
            try self.testMetrics.expectGauge("kafka_broker_round_trip_time_min", dimensions)
        }
    }

    @Test func consumerIntegration() async throws {
        var config = KafkaConsumerConfig()
        config.consumptionStrategy = .group(
            id: UUID().uuidString,
            topics: ["this-topic-does-not-exist"]
        )
        config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
        config.clientId = "consumer-1"
        config.brokerAddressFamily = .v4
        config.statisticsIntervalMs = 1000
        config.metrics = kafkaMetrics

        let consumer = try KafkaConsumer(config: config, logger: .kafkaTest)

        let svcGroupConfig = ServiceGroupConfiguration(services: [consumer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: svcGroupConfig)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(1100))

            await serviceGroup.triggerGracefulShutdown()
        }

        let replyq = try testMetrics.expectGauge("kafka_bytes_sent_total", [("client", "consumer-1")])
        #expect(replyq.lastValue! > 0.0)
    }

    @Test func producerIntegration() async throws {
        var config = KafkaProducerConfig()
        config.bootstrapServers = ["\(kafkaHost):\(kafkaPort)"]
        config.clientId = "producer-1"
        config.brokerAddressFamily = .v4
        config.statisticsIntervalMs = 1000
        config.metrics = kafkaMetrics

        let producer = try KafkaProducer(
            config: config,
            logger: .kafkaTest
        )

        let svcGroupConfig = ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        let serviceGroup = ServiceGroup(configuration: svcGroupConfig)

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await serviceGroup.run()
            }

            try await Task.sleep(for: .milliseconds(1100))

            await serviceGroup.triggerGracefulShutdown()
        }

        let replyq = try testMetrics.expectGauge("kafka_bytes_sent_total", [("client", "producer-1")])
        #expect(replyq.lastValue! > 0.0)
    }
}
