# Getting started

Add the Kafka client to your package, then run a producer or consumer inside a service group.

## Overview

Kafka exposes a producer and a consumer that conform to the `Service` protocol from `swift-service-lifecycle`. Run them inside a `ServiceGroup`, which manages graceful startup and shutdown, and exchange records with the broker through `async`/`await` and `AsyncSequence`.

This article walks you through adding the dependency, sending a single message, and reading messages from a topic.

### Add Kafka as a dependency

Add the package to the dependencies in your `Package.swift`:

```swift
.package(
    url: "https://github.com/swift-server/swift-kafka-client", 
    branch: "main"
)
```

Then add `Kafka` to your target's dependencies:

```swift
.target(
    name: "MyApp",
    dependencies: [
        .product(name: "Kafka", package: "swift-kafka-client"),
    ]
)
```

Finally, import the module in your source code:

```swift
import Kafka
```

### Install system dependencies

The package wraps the native `librdkafka` C library, which depends on OpenSSL.

- On macOS, run `brew install openssl@3`.
- On Linux, run `apt-get install libssl-dev libsasl2-dev`.

### Send a first message

To produce a message and wait for the broker to acknowledge it, configure a ``KafkaProducer``, run it inside a `ServiceGroup`, and call ``KafkaProducer/sendAndAwait(_:)``:

```swift
import Kafka
import Logging
import ServiceLifecycle

let logger = Logger(label: "kafka-example")

var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]

let producer = try KafkaProducer(config: config, logger: logger)

let serviceGroup = ServiceGroup(
    services: [producer],
    gracefulShutdownSignals: [.sigterm],
    logger: logger
)

await withThrowingTaskGroup(of: Void.self) { group in
    group.addTask { try await serviceGroup.run() }

    group.addTask {
        let message = KafkaProducerMessage(
            topic: "topic-name",
            value: "Hello, World!"
        )
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

### Read messages from a topic

To consume messages, create a ``KafkaConsumer`` with a group consumption strategy, run it inside a `ServiceGroup`, and iterate ``KafkaConsumer/messages``:

```swift
var config = KafkaConsumerConfig()
config.bootstrapServers = ["localhost:9092"]
config.consumptionStrategy = .group(
    id: "example-group",
    topics: ["topic-name"]
)

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

### Run a local broker

You can stand up a Kafka broker on `localhost:9092` with Docker:

```shell
docker run -d -p 9092:9092 apache/kafka:3.9.1
```

### Next steps

For more detail on each side of the pipeline, see <doc:ProducingMessages> and <doc:ConsumingMessages>. To configure TLS or SASL authentication, see <doc:SecuringConnections>.
