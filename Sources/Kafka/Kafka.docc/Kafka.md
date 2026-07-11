# ``Kafka``

A Swift client for Apache Kafka built on structured concurrency and Service Lifecycle.

## Overview

Kafka wraps the [`librdkafka`](https://github.com/confluentinc/librdkafka) C library and exposes it through idiomatic Swift APIs. You configure a producer or consumer, run it as part of a `ServiceGroup`, and exchange records with the broker over `async`/`await` and `AsyncSequence`.

Kafka integrates with [swift-log](https://github.com/apple/swift-log) for structured logging, [swift-metrics](https://github.com/apple/swift-metrics) for observability, and [swift-service-lifecycle](https://github.com/swift-server/swift-service-lifecycle) for graceful startup and shutdown. It supports plaintext, TLS, and SASL authentication. Additional features include dynamic subscription management, manual offset control for at-least-once delivery, and the ability to pause and resume partition consumption.

## Topics

### Essentials

- <doc:GettingStarted>
- <doc:ProducingMessages>
- <doc:ConsumingMessages>
- <doc:HandlingErrors>
- <doc:Observability>
- <doc:SecuringConnections>
- <doc:Migrating>

### Producing messages

- ``KafkaProducer``
- ``KafkaProducerMessage``
- ``KafkaDeliveryReport``
- ``KafkaAcknowledgedMessage``
- ``KafkaProducerMessageID``
- ``KafkaProducerEvents``
- ``KafkaProducerEvent``

### Consuming messages

- ``KafkaConsumer``
- ``KafkaConsumerMessages``
- ``KafkaConsumerMessage``
- ``KafkaConsumerEvents``
- ``KafkaConsumerEvent``
- ``KafkaConsumerRebalance``
- ``KafkaTimestampType``

### Configuring clients

- ``KafkaProducerConfig``
- ``KafkaConsumerConfig``
- ``KafkaProducerConfiguration``
- ``KafkaConsumerConfiguration``
- ``KafkaTopicConfiguration``
- ``KafkaConfig``
- ``KafkaConfiguration``

### Identifying topics, partitions, and offsets

- ``KafkaTopicPartition``
- ``KafkaTopicPartitionOffset``
- ``KafkaPartition``
- ``KafkaOffset``
- ``KafkaHeader``

### Handling errors

- ``KafkaError``

### Working with message data

- ``KafkaContiguousBytes``
