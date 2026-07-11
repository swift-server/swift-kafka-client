# Observability

Emit metrics and structured logs from the producer and consumer.

## Overview

The Kafka client integrates with [swift-metrics](https://github.com/apple/swift-metrics) for runtime metrics and [swift-log](https://github.com/apple/swift-log) for structured logging. Both are opt-in through the client configuration and the `Logger` you provide.

## Emit metrics

The client periodically samples librdkafka's internal statistics and records them into [swift-metrics](https://github.com/apple/swift-metrics) gauges that you supply. Configure this through the ``KafkaConsumerConfig/metrics`` (or ``KafkaProducerConfig/metrics``) property: set an update interval and assign a `Gauge` to each statistic you want to track.

```swift
import Kafka
import Metrics

var config = KafkaConsumerConfig()
config.bootstrapServers = ["localhost:9092"]
config.consumptionStrategy = .group(id: "example-group", topics: ["topic-name"])

// Sample statistics once per second into swift-metrics gauges.
config.metrics.updateInterval = .seconds(1)
config.metrics.totalKafkaBrokerRequests = Gauge(label: "kafka_consumer_broker_requests")
config.metrics.totalKafkaBrokerMessagesReceived = Gauge(label: "kafka_consumer_messages_received")
config.metrics.queuedOperation = Gauge(label: "kafka_consumer_queued_operations")

let consumer = try KafkaConsumer(config: config, logger: logger)
```

Metrics are emitted only when ``KafkaConfiguration/ConsumerMetrics/updateInterval`` is set **and** at least one gauge is assigned; otherwise the client skips statistics collection entirely. The producer exposes the same pattern through ``KafkaProducerConfig/metrics`` with producer-specific gauges such as ``KafkaConfiguration/ProducerMetrics/queuedProducerMessages`` and ``KafkaConfiguration/ProducerMetrics/totalKafkaBrokerMessagesSent``.

The gauges you assign are ordinary [swift-metrics](https://github.com/apple/swift-metrics) types, so the values reach whatever metrics backend you bootstrap through `MetricsSystem`.

## Structured logging

Pass a `Logger` when creating a ``KafkaProducer`` or ``KafkaConsumer``. The client logs lifecycle and operational events through it, and enriches every entry with structured metadata so you can filter and correlate logs across many clients:

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

let consumer = try KafkaConsumer(config: config, logger: logger)
// Every log entry from this consumer now carries kafka.client.id, kafka.client.type,
// and kafka.group.id.
```

Set the `Logger`'s log level to control verbosity — the client logs routine progress at `debug` and `trace`, and surfaces problems at `info` and above.

## Topics

### Metrics

- ``KafkaConfiguration/ConsumerMetrics``
- ``KafkaConfiguration/ProducerMetrics``
