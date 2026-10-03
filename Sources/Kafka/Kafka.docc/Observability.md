# Observing Kafka clients

Emit metrics and structured logs from the producer and consumer.

## Overview

The Kafka client integrates with [swift-metrics](https://github.com/apple/swift-metrics) for runtime metrics and [swift-log](https://github.com/apple/swift-log) for structured logging. Metrics are enabled by default and report into whatever backend you bootstrap; logs go to the `Logger` you provide.

## Emit metrics

Metrics are **auto-registered** and **enabled by default**: the client periodically samples its internal librdkafka statistics and delivery/commit events, then records a fixed set of instruments under a prefix you choose. You never assign individual instruments — the client owns them and routes their values to whatever backend you bootstrap through `MetricsSystem`.

Metrics default to ``KafkaMetricsConfig/enabled(prefix:updateInterval:)`` with the `kafka` prefix and a 5-second interval. Change the prefix or interval, or turn metrics off, through the ``KafkaConsumerConfig/metrics`` (or ``KafkaProducerConfig/metrics``) property:

```swift
import Kafka
import Metrics

var config = KafkaConsumerConfig()
config.bootstrapServers = ["localhost:9092"]
config.consumptionStrategy = .group(id: "example-group", topics: ["topic-name"])

// Sample every second under a custom prefix instead of the default ("kafka", 5 seconds).
config.metrics = .enabled(prefix: "orders", updateInterval: .seconds(1))

// Or turn metrics and statistics collection off entirely.
// config.metrics = .disabled

let (consumer, _, _) = try KafkaConsumer.makeConsumer(config: config)
```

While metrics are enabled, the client requests librdkafka statistics at `updateInterval` unless you set `statistics.interval.ms` yourself. When they're ``KafkaMetricsConfig/disabled``, the client skips statistics collection entirely.

Every instrument carries a `client_id` dimension that identifies the client instance, so several producers or consumers in one process report separate series. Its value is the configured `clientId`, or librdkafka's unique instance name (for example, `rdkafka#consumer-1`) when no `clientId` is set. Set `clientId` to get a value that stays stable across restarts.

### Consumer metrics

All instruments are prefixed with the configured prefix (for example, `kafka.consumer.lag`). The dimensions column lists dimensions in addition to `client_id`. Meters report librdkafka's running totals since the client started.

| Metric | Type | Dimensions | Description |
| --- | --- | --- | --- |
| `consumer.lag` | Gauge | `topic`, `partition` | Per-partition consumer lag (broker high watermark − committed offset). |
| `consumer.lag.max` | Gauge | — | Maximum lag across all assigned partitions. |
| `consumer.queue.operations` | Gauge | — | Operations waiting for the application to serve with `poll()`. |
| `consumer.messages.received` | Meter | — | Messages consumed. |
| `consumer.bytes.received` | Meter | — | Message bytes consumed. |
| `consumer.errors.total` | Counter | — | Client error events. |
| `consumer.rebalances` | Meter | — | Consumer group rebalances (librdkafka `rebalance_cnt`). |
| `consumer.commits.total` | Counter | — | Successful manual commits. |
| `consumer.commits.failed` | Counter | — | Failed manual commits. |
| `consumer.commit.duration` | Timer | — | Manual commit latency. |
| `consumer.broker.rtt.{avg,p99,max}.ms` | Gauge | `broker` | Broker round-trip time (milliseconds). |
| `consumer.broker.throttle.{avg,p99,max}.ms` | Gauge | `broker` | Broker throttle time (milliseconds). |

- Note: Commit metrics cover **manual** commits only (``KafkaConsumerConfig/enableAutoCommit`` set to `false`). Auto-commit runs inside librdkafka with no Swift call site to observe.

### Producer metrics

| Metric | Type | Dimensions | Description |
| --- | --- | --- | --- |
| `producer.queue.messages` | Gauge | — | Messages currently in the producer queues. |
| `producer.queue.bytes` | Gauge | — | Total size of messages in the producer queues (bytes). |
| `producer.messages.sent` | Meter | — | Messages produced to brokers. |
| `producer.bytes.sent` | Meter | — | Message bytes produced to brokers. |
| `producer.batch.size.avg` | Gauge | — | Average produce batch size across topics (bytes). |
| `producer.delivery.success.total` | Counter | — | Acknowledged deliveries. |
| `producer.delivery.failure.total` | Counter | — | Failed deliveries. |
| `producer.send.errors.total` | Counter | — | Produce-enqueue failures (message rejected before transmission). |
| `producer.send.duration` | Timer | — | End-to-end latency of an acknowledged `sendAndAwait(_:)` (enqueue → delivery report). |
| `producer.errors.total` | Counter | — | Client error events. |
| `producer.broker.rtt.{avg,p99,max}.ms` | Gauge | `broker` | Broker round-trip time (milliseconds). |
| `producer.broker.throttle.{avg,p99,max}.ms` | Gauge | `broker` | Broker throttle time (milliseconds). |
| `producer.broker.queue.latency.{avg,p99,max}.ms` | Gauge | `broker` | Internal producer queue latency (milliseconds). |
| `producer.broker.request.latency.{avg,p99,max}.ms` | Gauge | `broker` | Request-buffer (outbuf) latency (milliseconds). |

librdkafka pre-aggregates the per-broker latency windows, so the client surfaces the pre-computed `avg`, `p99`, and `max` summary points as discrete gauges rather than feeding raw samples into a `Timer`. All four are normalized to and reported in **milliseconds** (the `.ms` suffix; librdkafka reports `rtt`/`int_latency`/`outbuf_latency` in microseconds and `throttle` in milliseconds). A window with no measurements in the sampling interval (for example, an idle bootstrap broker) is skipped.

## Emit structured logs

Provide a `Logger` when creating a ``KafkaProducer`` or ``KafkaConsumer`` — the consumer and producer both read the task-local logger set with `withLogger(_:_:)`. The client logs lifecycle and operational events through it, and enriches every entry with structured metadata so you can filter and correlate logs across many clients:

| Metadata key | Value |
| --- | --- |
| `kafka.client.id` | the configured `clientId` |
| `kafka.client.type` | `producer` or `consumer` |
| `kafka.group.id` | the consumer group (consumers only) |

```swift
import Kafka
import Logging

let logger = Logger(label: "kafka")

var config = KafkaConsumerConfig()
config.clientId = "orders-consumer"
config.consumptionStrategy = .group(id: "orders", topics: ["orders"])

try withLogger(logger) { _ in
    let (consumer, _, _) = try KafkaConsumer.makeConsumer(config: config)
    // Every log entry from this consumer now carries kafka.client.id, kafka.client.type,
    // and kafka.group.id.
}
```

Set the `Logger`'s log level to control verbosity — the client logs routine progress at `debug` and `trace`, and surfaces problems at `info` and above.

## Topics

### Metrics

- ``KafkaMetricsConfig``
