# 🚧WIP🚧: SwiftKafka

SwiftKafka is a Swift Package in development that provides a convenient way to communicate with [Apache Kafka](https://kafka.apache.org) servers. The main goal was to create an API that leverages [Swift's new concurrency features](https://docs.swift.org/swift-book/LanguageGuide/Concurrency.html). Under the hood, this package uses the [`librdkafka`](https://github.com/confluentinc/librdkafka) C library.

## Adding SwiftKafka as a Dependency

To use the `SwiftKafka` library in a SwiftPM project,
add the following line to the dependencies in your `Package.swift` file:

```swift
.package(url: "https://github.com/swift-server/swift-kafka-gsoc", branch: "main")
```

Include `"SwiftKafka"` as a dependency for your executable target:

```swift
.target(name: "<target>", dependencies: [
    .product(name: "SwiftKafka", package: "swift-kafka-gsoc"),
]),
```

Finally, add `import SwiftKafka` to your source code.

## Usage

### Producer API

The `send(_:)` method of `KafkaProducer` returns a message-id that can later be used to identify the corresponding acknowledgement. Acknowledgements are received through the `acknowledgements` [`AsyncSequence`](https://developer.apple.com/documentation/swift/asyncsequence). Each acknowledgement indicates that producing a message was successful or returns an error.

```swift
let config = KafkaProducerConfiguration(bootstrapServers: ["localhost:9092"])

let (producer, acknowledgements) = try KafkaProducer.makeProducerWithAcknowledgements(
    config: config,
    logger: .kafkaTest // Your logger here
)

await withThrowingTaskGroup(of: Void.self) { group in

    // Run Task
    group.addTask {
        try await producer.run()
    }

    // Task receiving acknowledgements
    group.addTask {
        let messageID = try await producer.send(
            KafkaProducerMessage(
                topic: "topic-name",
                value: "Hello, World!"
            )
        )

        for await acknowledgement in acknowledgements {
            // Check if acknowledgement belongs to the sent message
        }

        // Required
        await producer.shutdownGracefully()
    }
}
```

### Consumer API

After initializing the `KafkaConsumer` with a topic-partition pair to read from, messages can be consumed using the `messages` [`AsyncSequence`](https://developer.apple.com/documentation/swift/asyncsequence).

```swift
let config = KafkaConsumerConfiguration(
    consumptionStrategy: .partition(
        topic: "topic-name",
        partition: KafkaPartition(rawValue: 0)
    ),
    bootstrapServers: ["localhost:9092"]
)

let consumer = try KafkaConsumer(
    config: config,
    logger: .kafkaTest // Your logger here
)

await withThrowingTaskGroup(of: Void.self) { group in

    // Run Task
    group.addTask {
        try await consumer.run()
    }

    // Task receiving messages
    group.addTask {
        for await message in consumer.messages {
            // Do something with message
        }
    }
}
```

#### Consumer Groups

SwiftKafka also allows users to subscribe to an array of topics as part of a consumer group.

```swift
let config = KafkaConsumerConfiguration(
    consumptionStrategy: .group(groupID: "example-group", topics: ["topic-name"]),
    bootstrapServers: ["localhost:9092"]
)

let consumer = try KafkaConsumer(
    config: config,
    logger: .kafkaTest // Your logger here
)

await withThrowingTaskGroup(of: Void.self) { group in

    // Run Task
    group.addTask {
        try await consumer.run()
    }

    // Task receiving messages
    group.addTask {
        for await message in consumer.messages {
            // Do something with message
        }
    }
}
```

#### Manual commits

By default, the `KafkaConsumer` automatically commits message offsets after receiving the corresponding message. However, we allow users to disable this setting and commit message offsets manually.

```swift
let config = KafkaConsumerConfiguration(
    consumptionStrategy: .group(groupID: "example-group", topics: ["topic-name"]),
    enableAutoCommit: false,
    bootstrapServers: ["localhost:9092"]
)

let consumer = try KafkaConsumer(
    config: config,
    logger: .kafkaTest // Your logger here
)

await withThrowingTaskGroup(of: Void.self) { group in

    // Run Task
    group.addTask {
        try await consumer.run()
    }

    // Task receiving messages
    group.addTask {
        for await message in consumer.messages {
            // Do something with message
            // ...
            try await consumer.commitSync(message)
        }
    }
}
```

## librdkafka

The Package depends on [the librdkafka library](https://github.com/confluentinc/librdkafka), which is included as a git submodule.
It has source files that are excluded in `Package.swift`.

## Development Setup

We provide a Docker environment for this package. This will automatically start a local Kafka server and run the package tests.

```bash
docker-compose -f docker/docker-compose.yaml run test
```
