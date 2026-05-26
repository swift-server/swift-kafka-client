# Producing messages

Send records to Kafka topics with awaitable acknowledgments or with high-throughput batched delivery reports.

## Overview

A ``KafkaProducer`` offers two send styles. The awaitable ``KafkaProducer/sendAndAwait(_:)`` returns once the broker confirms delivery, and the fire-and-forget ``KafkaProducer/send(_:)`` enqueues records for transmission and reports outcomes through an `AsyncSequence` of events.

Choose `sendAndAwait(_:)` when you need confirmation per message — for example, in a request handler that returns the resulting partition and offset to its caller. Choose `send(_:)` paired with an events sequence when you need maximum throughput and can process delivery reports in batches.

Either way, the producer conforms to the `Service` protocol and runs inside a `ServiceGroup`, so the surrounding application controls its lifecycle.

### Configure the producer

A ``KafkaProducerConfig`` holds the broker addresses and other producer settings. The minimum requirement is `bootstrapServers`:

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
```

For configurable security options, see <doc:SecuringConnections>.

### Send and await acknowledgment

Construct a ``KafkaProducer``, run it inside a `ServiceGroup`, and call ``KafkaProducer/sendAndAwait(_:)`` from a sibling task. The returned ``KafkaDeliveryReport`` carries a ``KafkaDeliveryReport/status`` value of `.acknowledged(KafkaAcknowledgedMessage)` on success, or `.failure(KafkaError)` if the broker rejects the record.

```swift
let producer = try KafkaProducer(config: config, logger: logger)

let serviceGroup = ServiceGroup(
    services: [producer],
    gracefulShutdownSignals: [.sigterm],
    logger: logger
)

await withThrowingTaskGroup(of: Void.self) { group in
    group.addTask { try await serviceGroup.run() }

    group.addTask {
        let message = KafkaProducerMessage(topic: "topic-name", value: "Hello, World!")
        let report = try await producer.sendAndAwait(message)
        switch report.status {
        case .acknowledged(let ack):
            print("Delivered to partition \(ack.partition) at offset \(ack.offset)")
        case .failure(let error):
            print("Delivery failed: \(error)")
        }
    }
}
```

### Send records with batched delivery reports

For high-throughput pipelines, create the producer alongside its events sequence with ``KafkaProducer/makeProducerWithEvents(config:logger:)``. Call ``KafkaProducer/send(_:)`` to enqueue records, and consume ``KafkaProducerEvent`` values from the events sequence to learn outcomes:

```swift
let (producer, events) = try KafkaProducer.makeProducerWithEvents(
    config: config,
    logger: logger
)

let serviceGroup = ServiceGroup(
    services: [producer],
    gracefulShutdownSignals: [.sigterm],
    logger: logger
)

await withThrowingTaskGroup(of: Void.self) { group in
    group.addTask { try await serviceGroup.run() }

    group.addTask {
        let message = KafkaProducerMessage(topic: "topic-name", value: "Hello, World!")
        try producer.send(message)

        for await event in events {
            switch event {
            case .deliveryReports(let reports):
                for report in reports {
                    // Handle delivery acknowledgment.
                }
            default:
                break
            }
        }
    }
}
```

The events sequence delivers reports in batches as `librdkafka` flushes them, which keeps overhead lower than awaiting each message individually.

### Identify a record before delivery

The synchronous ``KafkaProducer/send(_:)`` returns a ``KafkaProducerMessageID``. Use this identifier to match a later ``KafkaDeliveryReport`` to the originating record; the same identifier appears on the report.

### Handle producer events

Beyond delivery reports, the events sequence emits errors and other broker events. See ``KafkaProducerEvent`` for the full set of cases. To classify errors, read ``KafkaError/isFatal`` and ``KafkaError/isRetriable``: a fatal error means the producer can't recover, and your application needs to shut it down. A retriable error typically resolves on its own as the broker recovers.

## Topics

### Sending records

- ``KafkaProducer/send(_:)``
- ``KafkaProducer/sendAndAwait(_:)``
- ``KafkaProducer/makeProducerWithEvents(config:logger:)``

### Inspecting outcomes

- ``KafkaDeliveryReport``
- ``KafkaAcknowledgedMessage``
- ``KafkaProducerMessageID``
- ``KafkaProducerEvent``
