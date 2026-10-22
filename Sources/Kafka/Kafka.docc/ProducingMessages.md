# Producing messages

Send records to Kafka topics with awaitable acknowledgments or with high-throughput batched delivery reports.

## Overview

A ``KafkaProducer`` offers two send styles. The awaitable ``KafkaProducer/sendAndAwait(_:)`` returns once the broker confirms delivery, and the fire-and-forget ``KafkaProducer/send(_:)`` enqueues records for transmission and reports outcomes through an `AsyncSequence` of events.

Choose `sendAndAwait(_:)` for per-message confirmation — for example, in a request handler that returns the resulting partition and offset to its caller. Choose `send(_:)` paired with an events sequence for maximum throughput when batched delivery report processing is acceptable.

Either way, the producer conforms to the `Service` protocol and runs inside a `ServiceGroup`, so the surrounding application controls its lifecycle.

### Configure the producer

A ``KafkaProducerConfig`` holds the broker addresses and other producer settings. The minimum requirement is `bootstrapServers`:

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
```

For configurable security options, see <doc:SecuringConnections>.

### Send and await acknowledgment

Construct a ``KafkaProducer``, run it inside a `ServiceGroup`, and call ``KafkaProducer/sendAndAwait(_:)`` from a sibling task. The returned ``KafkaProducer/DeliveryReport`` carries a ``KafkaProducer/DeliveryReport/status`` value of `.acknowledged(KafkaProducer/AcknowledgedMessage)` on success, or `.failure(KafkaError)` if the broker rejects the record.

```swift
let logger = Logger(label: "kafka-example")

try await withLogger(logger) { _ in
    let (producer, _) = try KafkaProducer.makeProducer(config: config)

    let serviceGroup = ServiceGroup(
        services: [producer],
        gracefulShutdownSignals: [.sigterm]
    )

    await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask { try await serviceGroup.run() }

        group.addTask {
            let message = KafkaProducer.Message(topic: "topic-name", value: "Hello, World!")
            let report = try await producer.sendAndAwait(message)
            switch report.status {
            case .acknowledged(let ack):
                print("Delivered to partition \(ack.partition) at offset \(ack.offset)")
            case .failure(let error):
                print("Delivery failed: \(error)")
            }
        }
    }
}
```

### Send records with batched delivery reports

For high-throughput pipelines, create the producer alongside its events sequence with ``KafkaProducer/makeProducer(config:)``. Call ``KafkaProducer/send(_:)`` to enqueue records, and consume ``KafkaProducer/Event`` values from the events sequence to learn outcomes:

```swift
let logger = Logger(label: "kafka-example")

try await withLogger(logger) { _ in
    let (producer, events) = try KafkaProducer.makeProducer(config: config)

    let serviceGroup = ServiceGroup(
        services: [producer],
        gracefulShutdownSignals: [.sigterm]
    )

    await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask { try await serviceGroup.run() }

        group.addTask {
            let message = KafkaProducer.Message(topic: "topic-name", value: "Hello, World!")
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
}
```

The events sequence delivers reports in batches as `librdkafka` flushes them, which keeps overhead lower than awaiting each message individually.

### Send bytes you already hold

``KafkaProducer/Message`` stores its key and value as `[UInt8]`, and builds them from a `String` or an array. If your payload lives in another type, such as `Foundation.Data` from a `JSONEncoder`, pass its bytes as a `RawSpan` instead of copying them into an array first:

```swift
let payload: Data = try JSONEncoder().encode(order)
let report = try await producer.sendAndAwait(topic: "orders", value: payload.bytes)
```

``KafkaProducer/send(topic:partition:headers:key:value:)`` and ``KafkaProducer/sendAndAwait(topic:partition:headers:key:value:)`` accept a `RawSpan` for the key and the value. Header values stay `[UInt8]`.

### Identify a record before delivery

The synchronous ``KafkaProducer/send(_:)`` returns a ``KafkaProducer/MessageID``. Use this identifier to match a later ``KafkaProducer/DeliveryReport`` to the originating record; the same identifier appears on the report.

### Handle producer events

Beyond delivery reports, the events sequence emits errors and other broker events. See ``KafkaProducer/Event`` for the full set of cases. To classify errors, read ``KafkaError/isFatal`` and ``KafkaError/isRetriable``: a fatal error means the producer can't recover, and the application needs to shut it down. A retriable error typically resolves on its own as the broker recovers.

## Topics

### Sending records

- ``KafkaProducer/send(_:)``
- ``KafkaProducer/sendAndAwait(_:)``
- ``KafkaProducer/send(topic:partition:headers:key:value:)``
- ``KafkaProducer/sendAndAwait(topic:partition:headers:key:value:)``
- ``KafkaProducer/makeProducer(config:)``

### Inspecting outcomes

- ``KafkaProducer/DeliveryReport``
- ``KafkaProducer/AcknowledgedMessage``
- ``KafkaProducer/MessageID``
- ``KafkaProducer/Event``
