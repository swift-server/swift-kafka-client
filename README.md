# Swift Kafka Client

The Swift Kafka Client library provides a convenient way to interact with [Apache Kafka](https://kafka.apache.org) by leveraging [Swift's new concurrency features](https://docs.swift.org/swift-book/LanguageGuide/Concurrency.html). This package wraps the native [`librdkafka`](https://github.com/confluentinc/librdkafka) library.

## Features

- Async/await producer with awaitable delivery acknowledgments (`sendAndAwait`)
- High-throughput fire-and-forget producer with batched delivery reports
- `AsyncSequence`-based consumer with automatic rebalancing
- At-least-once delivery semantics via manual offset storage
- Consumer group support with dynamic subscription management
- Pause/resume partition consumption
- Typed error handling with retriable and fatal error classification
- SASL and TLS authentication
- Integration with [swift-service-lifecycle](https://github.com/swift-server/swift-service-lifecycle), [swift-log](https://github.com/apple/swift-log), and [swift-metrics](https://github.com/apple/swift-metrics)
- Full librdkafka configuration exposed as typed Swift properties

## Adding Kafka as a dependency

To use the `Kafka` library in a SwiftPM project,
add the following line to the dependencies in your `Package.swift` file:

```swift
.package(url: "https://github.com/swift-server/swift-kafka-client", branch: "main")
```

Include `"Kafka"` as a dependency for your executable target:

```swift
.target(name: "<target>", dependencies: [
    .product(name: "Kafka", package: "swift-kafka-client"),
]),
```

Finally, add `import Kafka` to your source code.

## Usage

Run `Kafka` within a [`Swift Service Lifecycle`](https://github.com/swift-server/swift-service-lifecycle)
[`ServiceGroup`](https://swiftpackageindex.com/swift-server/swift-service-lifecycle/main/documentation/servicelifecycle/servicegroup) for proper startup and shutdown handling.
Both the `KafkaProducer` and the `KafkaConsumer` implement the [`Service`](https://swiftpackageindex.com/swift-server/swift-service-lifecycle/main/documentation/servicelifecycle/service) protocol.

### Producer API

The `sendAndAwait(_:)` method produces a message and asynchronously awaits broker acknowledgment, confirming the partition and offset where your message landed without blocking any threads:

```swift
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

For high-throughput pipelines that need to maximize send rate, use the fire-and-forget `send(_:)` method and process delivery reports in batches through the events sequence:

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]

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
                    // Handle delivery acknowledgment
                }
            default:
                break
            }
        }
    }
}
```

### Consumer API

The consumer delivers messages as an `AsyncSequence`. Iterate them with a standard `for try await` loop that integrates naturally with Swift concurrency, structured tasks, and cancellation:

#### Consumer groups

```swift
var config = KafkaConsumerConfig()
config.bootstrapServers = ["localhost:9092"]
config.consumptionStrategy = .group(id: "example-group", topics: ["topic-name"])

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

#### At-least-once processing

For at-least-once delivery semantics, disable automatic offset storage and manually store offsets after processing:

```swift
var config = KafkaConsumerConfig()
config.bootstrapServers = ["localhost:9092"]
config.consumptionStrategy = .group(id: "example-group", topics: ["topic-name"])
config.enableAutoOffsetStore = false

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
            // Process message...
            try consumer.storeOffset(message)
            // The background auto-commit timer commits the offset automatically.
        }
    }
}
```

#### Manual commits

To control exactly when offsets are committed to the broker:

```swift
var config = KafkaConsumerConfig()
config.bootstrapServers = ["localhost:9092"]
config.consumptionStrategy = .group(id: "example-group", topics: ["topic-name"])
config.enableAutoCommit = false

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
            // Process message...
            try await consumer.commit(message)
        }
    }
}
```

To commit all previously stored offsets at once:

```swift
try await consumer.commit()
```

#### Dynamic subscription management

Change topics at runtime:

```swift
// Subscribe to additional topics
try consumer.subscribe(topics: ["topic-a", "topic-b"])

// Query current subscription
let topics = try consumer.subscribedTopics()

// Unsubscribe from all topics
try consumer.unsubscribe()
```

#### Pause and resume

Pause and resume partition consumption temporarily, useful for applying backpressure or performing maintenance without leaving the consumer group:

```swift
let partition = KafkaTopicPartition(topic: "topic-name", partition: KafkaPartition(rawValue: 0))
try consumer.pause(topicPartitions: [partition])
// ... later
try consumer.resume(topicPartitions: [partition])
```

### Security mechanisms

Configure both the `KafkaProducer` and the `KafkaConsumer` to use different security mechanisms via the `securityProtocol` property.

#### Plaintext

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
config.securityProtocol = .plaintext
```

#### TLS (SSL)

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
config.securityProtocol = .ssl
```

#### SASL + plaintext

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
config.securityProtocol = .sasl_plaintext
config.saslMechanisms = "PLAIN"
config.saslUsername = "user"
config.saslPassword = "password"
```

#### SASL + TLS

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
config.securityProtocol = .sasl_ssl
config.saslMechanisms = "SCRAM-SHA-256"
config.saslUsername = "user"
config.saslPassword = "password"
```

### Error handling

The events sequence surfaces errors from librdkafka with typed error codes:

```swift
let (consumer, events) = try KafkaConsumer.makeConsumerWithEvents(config: config, logger: logger)

for await event in events {
    switch event {
    case .error(let error):
        if error.isFatal {
            // The client is irrecoverable — initiate shutdown.
        } else if error.isRetriable {
            // Transient error — likely resolves on its own.
        }
        print("Error: \(error)")
    default:
        break
    }
}
```

## librdkafka

The Package depends on [the `librdkafka` library](https://github.com/confluentinc/librdkafka), which is included as a git submodule.
Its source files are excluded in `Package.swift`.

### Dependencies

- **macOS**: `brew install openssl@3`
- **Linux**: `apt-get install libssl-dev libsasl2-dev`

## Development setup

### Running tests locally

Integration tests require a running Kafka broker. Start one with Docker:

```shell
docker run -d -p 9092:9092 apache/kafka:3.9.1

# After starting the container run the tests
swift test
```

### Running tests in Docker

The package provides a Docker environment that automatically starts a local Kafka server and runs the tests:

```shell
docker compose -f docker/docker-compose.yaml run client swift test
```

Alternatively, use a `Makefile` target:
```shell
make docker-test
```

Specify the Swift compiler version using the `SWIFT_VERSION` environment variable:
```shell
SWIFT_VERSION=6.2 make docker-test
```