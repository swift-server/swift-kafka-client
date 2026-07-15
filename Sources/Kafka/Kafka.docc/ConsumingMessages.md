# Consuming messages

Receive records from Kafka topics as an asynchronous sequence, control offset commits, and pause or resume partitions.

## Overview

A ``KafkaConsumer`` joins a consumer group and exposes records through a ``KafkaConsumerMessages`` asynchronous sequence. Iterate the sequence with `for try await`, and the consumer integrates naturally with structured concurrency, task cancellation, and graceful shutdown through `ServiceGroup`.

By default, the consumer stores and commits offsets automatically as iteration proceeds. For at-least-once delivery, disable automatic offset storage and store offsets after processing each record. For full control, also turn off the periodic auto-commit and commit explicitly.

### Configure a consumer group

A ``KafkaConsumerConfig`` holds the broker addresses and the consumption strategy. To join a consumer group, set ``KafkaConsumerConfig/consumptionStrategy`` to `.group(id:topics:)`:

```swift
var config = KafkaConsumerConfig()
config.bootstrapServers = ["localhost:9092"]
config.consumptionStrategy = .group(
    id: "example-group",
    topics: ["topic-name"]
)
```

For security options, see <doc:SecuringConnections>.

### Iterate the message sequence

Run the consumer inside a `ServiceGroup` and read records from ``KafkaConsumer/messages``:

```swift
let consumer = try KafkaConsumer(config: config, logger: logger)

let serviceGroup = ServiceGroup(
    services: [consumer],
    gracefulShutdownSignals: [.sigterm],
    logger: logger
)

await withThrowingTaskGroup(of: Void.self) { group in
    group.addTask { try await serviceGroup.run() }

    group.addTask {
        for try await message in consumer.messages {
            print("Received: \(message.topic)/\(message.partition) at offset \(message.offset)")
        }
    }
}
```

Each ``KafkaConsumerMessage`` carries the topic, partition, offset, key, value, headers, and timestamp.

### Achieve at-least-once delivery

For at-least-once semantics, disable automatic offset storage and call ``KafkaConsumer/storeOffset(_:)`` only after processing each record. The consumer's background auto-commit timer then commits the stored offsets:

```swift
var config = KafkaConsumerConfig()
config.bootstrapServers = ["localhost:9092"]
config.consumptionStrategy = .group(
    id: "example-group",
    topics: ["topic-name"]
)
config.enableAutoOffsetStore = false

let consumer = try KafkaConsumer(config: config, logger: logger)

// ... run inside a ServiceGroup as in the previous example.

for try await message in consumer.messages {
    // Process the record.
    try consumer.storeOffset(message)
}
```

If processing fails before `storeOffset(_:)` runs, the consumer redelivers the record on its next start.

### Commit offsets manually

To control exactly when offsets reach the broker, disable auto-commit and call ``KafkaConsumer/commit(_:)`` per record:

```swift
var config = KafkaConsumerConfig()
config.bootstrapServers = ["localhost:9092"]
config.consumptionStrategy = .group(
    id: "example-group",
    topics: ["topic-name"]
)
config.enableAutoCommit = false

let consumer = try KafkaConsumer(config: config, logger: logger)

// ... run inside a ServiceGroup as above.

for try await message in consumer.messages {
    // Process the record.
    try await consumer.commit(message)
}
```

To commit every previously stored offset in one call, use ``KafkaConsumer/commit()``:

```swift
try await consumer.commit()
```

### Manage subscriptions dynamically

Topic subscriptions change at runtime — call ``KafkaConsumer/subscribe(topics:)`` again to update them:

```swift
// Subscribe to additional topics.
try consumer.subscribe(topics: ["topic-a", "topic-b"])

// Query the current subscription.
let topics = try consumer.subscribedTopics()

// Unsubscribe from all topics.
try consumer.unsubscribe()
```

### Pause and resume partitions

Pause specific partitions to apply backpressure or perform maintenance without leaving the consumer group:

```swift
let partition = KafkaTopicPartition(
    topic: "topic-name",
    partition: KafkaPartition(rawValue: 0)
)
try consumer.pause(topicPartitions: [partition])
// ... later
try consumer.resume(topicPartitions: [partition])
```

While a partition is paused, the consumer stops fetching records for it but continues to participate in the group, including heartbeats and rebalances.

### Observe rebalances

When the membership of a consumer group changes — a consumer joins, leaves, or fails — Kafka redistributes the group's partitions across the remaining members. This is a *rebalance*. ``KafkaConsumer`` performs the assign and unassign automatically and surfaces a ``KafkaConsumerRebalance`` notification through the ``KafkaConsumerEvents`` sequence, so you can react — for example, by committing offsets for partitions that are moving away.

Create the consumer with ``KafkaConsumer/makeConsumerWithEvents(config:logger:)`` and iterate the event sequence alongside the messages:

```swift
config.partitionAssignmentStrategy = "cooperative-sticky"

let (consumer, events) = try KafkaConsumer.makeConsumerWithEvents(config: config, logger: logger)

let serviceGroup = ServiceGroup(
    services: [consumer],
    gracefulShutdownSignals: [.sigterm],
    logger: logger
)

await withThrowingTaskGroup(of: Void.self) { group in
    group.addTask { try await serviceGroup.run() }

    // Consume records.
    group.addTask {
        for try await message in consumer.messages {
            // Process the record, then store or commit its offset.
        }
    }

    // Observe rebalances.
    group.addTask {
        for await event in events {
            switch event {
            case .rebalance(let rebalance):
                switch rebalance.kind {
                case .assign:
                    // Partitions were assigned; initialize per-partition state.
                    break
                case .revoke:
                    // Partitions are moving away; commit their offsets first.
                    break
                case .error(let description):
                    logger.warning("Rebalance error", metadata: ["error": "\(description)"])
                }
            case .error(let error):
                logger.error("Kafka client error", metadata: ["error": "\(error)"])
            default:
                break
            }
        }
    }
}
```

Each ``KafkaConsumerRebalance`` reports its ``KafkaConsumerRebalance/kind`` — ``KafkaConsumerRebalance/Kind/assign``, ``KafkaConsumerRebalance/Kind/revoke``, or ``KafkaConsumerRebalance/Kind/error(_:)`` — and the ``KafkaConsumerRebalance/partitions`` involved. Commit offsets on revoke so another consumer resumes from the right position.

Choose an assignment strategy with ``KafkaConsumerConfig/partitionAssignmentStrategy``: `cooperative-sticky` for incremental cooperative rebalancing, or `range` and `roundrobin` for eager rebalancing. ``KafkaConsumer`` adapts to the negotiated protocol, so your handling code is identical either way. Cooperative and eager strategies must not be mixed within a group.

> Important: Consume the events sequence. If you create it but never iterate it, events buffer in memory indefinitely.

## Topics

### Articles

- <doc:GettingStarted>
- <doc:SecuringConnections>
- <doc:HandlingErrors>
- <doc:Observability>

### Reading records

- ``KafkaConsumer/messages``
- ``KafkaConsumerMessages``
- ``KafkaConsumerMessage``

### Managing subscriptions

- ``KafkaConsumer/subscribe(topics:)``
- ``KafkaConsumer/unsubscribe()``
- ``KafkaConsumer/subscribedTopics()``

### Controlling offsets

- ``KafkaConsumer/storeOffset(_:)``
- ``KafkaConsumer/commit(_:)``
- ``KafkaConsumer/commit()``

### Pausing partitions

- ``KafkaConsumer/pause(topicPartitions:)``
- ``KafkaConsumer/resume(topicPartitions:)``

### Observing rebalances and events

- ``KafkaConsumer/makeConsumerWithEvents(config:logger:)``
- ``KafkaConsumerRebalance``
- ``KafkaConsumerEvent``
- ``KafkaConsumerEvents``
