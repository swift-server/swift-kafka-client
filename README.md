# 🚧WIP🚧: SwiftKafka

SwiftKafka is a Swift Package in development that provides a convenient way to communicate with [Apache Kafka](https://kafka.apache.org) servers. The main goal was to create an API that leverages [Swift's new concurrency features](https://docs.swift.org/swift-book/LanguageGuide/Concurrency.html). Under the hood, this package uses the [`librdkafka`](github.com/edenhill/librdkafka) C library.

## Getting Started

### Docker

We provide a Docker environment for this package. This will automatically start a local Kafka server and run the package tests.

```bash
docker-compose -f docker/docker-compose.yaml run swift-kafka-gsoc
```

## Overview

### Producer API

The `sendAsync(_:)` method of `KafkaProducer` returns a message-id that can later be used to identify the corresponding acknowledgement. Acknowledgements are received through the `acknowledgements` [`AsyncSequence`](https://developer.apple.com/documentation/swift/asyncsequence). Each acknowledgement indicates that producing a message was successful or returns an error.

```swift
var config = KafkaConfig()
try config.set("localhost:9092", forKey: "bootstrap.servers")

let producer = try await KafkaProducer(
    config: config,
    logger: .kafkaTest // Your logger here
)

let messageID = try await producer.sendAsync(
    KafkaProducerMessage(
        topic: "topic-name",
        value: "Hello, World!"
    )
)

for await acknowledgement in producer.acknowledgements {
    // Check if acknowledgement belongs to the sent message
}

// Required
await producer.shutdownGracefully()
```

### Consumer API

After initializing the `KafkaConsumer` with a topic-partition pair to read from, messages can be consumed using the `messages` [`AsyncSequence`](https://developer.apple.com/documentation/swift/asyncsequence).

```swift
var config = KafkaConfig()
try config.set("localhost:9092", forKey: "bootstrap.servers")

let consumer = try KafkaConsumer(
    topic: "topic-name",
    partition: KafkaPartition(rawValue: 0),
    config: config,
    logger: .kafkaTest // Your logger here
)

for await messageResult in consumer.messages {
    switch messageResult {
    case .success(let message):
        // Do something with message
    case .failure(let error):
        // Handle error
    }
}
```

#### Consumer Groups

SwiftKafka also allows users to subscribe to an array of topics as part of a consumer group.

```swift
var config = KafkaConfig()
try config.set("localhost:9092", forKey: "bootstrap.servers")

let consumer = try KafkaConsumer(
    topics: ["topic-name"],
    groupID: "example-group",
    config: config,
    logger: .kafkaTest // Your logger here
)

for await messageResult in consumer.messages {
    switch messageResult {
    case .success(let message):
        // Do something with message
    case .failure(let error):
        // Handle error
    }
}
```

#### Manual commits

By default, the `KafkaConsumer` automatically commits message offsets after receiving the corresponding message. However, we allow users to disable this setting and commit message offsets manually.

```swift
var config = KafkaConfig()
try config.set("localhost:9092", forKey: "bootstrap.servers")
try config.set("false", forKey: "enable.auto.commit")

let consumer = try KafkaConsumer(
    topics: ["topic-name"],
    groupID: "example-group",
    config: config,
    logger: .kafkaTest // Your logger here
)

for await messageResult in consumer.messages {
    switch messageResult {
    case .success(let message):
        // Do something with message
        try await consumer.commitSync(message)
    case .failure(let error):
        // Handle error
    }
}
```
