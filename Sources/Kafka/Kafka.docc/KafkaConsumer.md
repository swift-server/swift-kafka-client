# ``Kafka/KafkaConsumer``

Consumes messages from a Kafka cluster as part of a service lifecycle.

## Overview

Create a consumer from a ``KafkaConsumerConfig``, run it inside a `ServiceGroup`, and iterate ``KafkaConsumer/messages`` to receive records. The consumer conforms to `Service`, so the surrounding application controls its lifecycle; the `run()` method drives the underlying poll loop until the calling task is canceled or a graceful shutdown is triggered.

By default, the consumer stores and commits offsets automatically as iteration proceeds. For at-least-once delivery, disable automatic offset storage and call ``storeOffset(_:)`` after processing each record. For full control over commit timing, disable auto-commit as well and use ``commit(_:)`` or ``commit()`` explicitly.

For an end-to-end guide including configuration, rebalance handling, and offset patterns, see <doc:ConsumingMessages>.

## Topics

### Creating a consumer

- ``init(config:logger:)``
- ``makeConsumerWithEvents(config:logger:)``

### Consuming messages

- ``messages``
- ``KafkaConsumerMessages``
- ``KafkaConsumerMessage``

### Running the service

- ``run()``
- ``triggerGracefulShutdown()``

### Managing subscriptions

- ``subscribe(topics:)``
- ``unsubscribe()``
- ``subscribedTopics()``

### Storing and committing offsets

- ``storeOffset(_:)``
- ``commit(_:)``
- ``commit()``
- ``scheduleCommit(_:)``
- ``scheduleCommit()``

### Querying position

- ``committed(topicPartitions:timeout:)``
- ``position(topicPartitions:)``
- ``isAssignmentLost``

### Pausing and resuming partitions

- ``pause(topicPartitions:)``
- ``resume(topicPartitions:)``

### Seeking

- ``seek(topicPartitionOffsets:timeout:)``

### Observing events

- ``KafkaConsumerEvents``
- ``KafkaConsumerEvent``
- ``KafkaConsumerRebalance``
