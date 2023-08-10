# Swift Kafka Client

The Swift Kafka Client library provides a convenient way to interact with [Apache Kafka](https://kafka.apache.org) by leveraging [Swift's new concurrency features](https://docs.swift.org/swift-book/LanguageGuide/Concurrency.html). This package wraps the native [`librdkafka`](https://github.com/confluentinc/librdkafka) library.

## Adding Kafka as a Dependency

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

`Kafka` should be used within a [`Swift Service Lifecycle`](https://github.com/swift-server/swift-service-lifecycle)
[`ServiceGroup`](https://swiftpackageindex.com/swift-server/swift-service-lifecycle/main/documentation/servicelifecycle/servicegroup) for proper startup and shutdown handling.
Both the `KafkaProducer` and the `KafkaConsumer` implement the [`Service`](https://swiftpackageindex.com/swift-server/swift-service-lifecycle/main/documentation/servicelifecycle/service) protocol.

### Producer API

The `send(_:)` method of `KafkaProducer` returns a message-id that can later be used to identify the corresponding acknowledgement. Acknowledgements are received through the `events` [`AsyncSequence`](https://developer.apple.com/documentation/swift/asyncsequence). Each acknowledgement indicates that producing a message was successful or returns an error.

```swift
let brokerAddress = KafkaConfiguration.BrokerAddress(host: "localhost", port: 9092)
let configuration = KafkaProducerConfiguration(bootstrapBrokerAddresses: [brokerAddress])

let (producer, events) = try KafkaProducer.makeProducerWithEvents(
    configuration: configuration,
    logger: logger
)

await withThrowingTaskGroup(of: Void.self) { group in

    // Run Task
    group.addTask {
        let serviceGroup = ServiceGroup(
            services: [producer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: logger
        )
        try await serviceGroup.run()
    }

    // Task sending message and receiving events
    group.addTask {
        let messageID = try producer.send(
            KafkaProducerMessage(
                topic: "topic-name",
                value: "Hello, World!"
            )
        )

        for await event in events {
            switch event {
            case .deliveryReports(let deliveryReports):
                // Check what messages the delivery reports belong to
            default:
                break // Ignore any other events
            }
        }
    }
}
```

### Consumer API

After initializing the `KafkaConsumer` with a topic-partition pair to read from, messages can be consumed using the `messages` [`AsyncSequence`](https://developer.apple.com/documentation/swift/asyncsequence).

```swift
let brokerAddress = KafkaConfiguration.BrokerAddress(host: "localhost", port: 9092)
let configuration = KafkaConsumerConfiguration(
    consumptionStrategy: .partition(
        KafkaPartition(rawValue: 0),
        topic: "topic-name"
    ),
    bootstrapBrokerAddresses: [brokerAddress]
)

let consumer = try KafkaConsumer(
    configuration: configuration,
    logger: logger
)

await withThrowingTaskGroup(of: Void.self) { group in

    // Run Task
    group.addTask {
        let serviceGroup = ServiceGroup(
            services: [consumer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: logger
        )
        try await serviceGroup.run()
    }

    // Task receiving messages
    group.addTask {
        for try await message in consumer.messages {
            // Do something with message
        }
    }
}
```

#### Consumer Groups

Kafka also allows users to subscribe to an array of topics as part of a consumer group.

```swift
let brokerAddress = KafkaConfiguration.BrokerAddress(host: "localhost", port: 9092)
let configuration = KafkaConsumerConfiguration(
    consumptionStrategy: .group(id: "example-group", topics: ["topic-name"]),
    bootstrapBrokerAddresses: [brokerAddress]
)

let consumer = try KafkaConsumer(
    configuration: configuration,
    logger: logger
)

await withThrowingTaskGroup(of: Void.self) { group in

    // Run Task
    group.addTask {
        let serviceGroup = ServiceGroup(
            services: [consumer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: logger
        )
        try await serviceGroup.run()
    }

    // Task receiving messages
    group.addTask {
        for try await message in consumer.messages {
            // Do something with message
        }
    }
}
```

#### Manual commits

By default, the `KafkaConsumer` automatically commits message offsets after receiving the corresponding message. However, we allow users to disable this setting and commit message offsets manually.

```swift
let brokerAddress = KafkaConfiguration.BrokerAddress(host: "localhost", port: 9092)
var configuration = KafkaConsumerConfiguration(
    consumptionStrategy: .group(id: "example-group", topics: ["topic-name"]),
    bootstrapBrokerAddresses: [brokerAddress]
)
configuration.isAutoCommitEnabled = false

let consumer = try KafkaConsumer(
    configuration: configuration,
    logger: logger
)

await withThrowingTaskGroup(of: Void.self) { group in

    // Run Task
    group.addTask {
        let serviceGroup = ServiceGroup(
            services: [consumer],
            configuration: ServiceGroupConfiguration(gracefulShutdownSignals: []),
            logger: logger
        )
        try await serviceGroup.run()
    }

    // Task receiving messages
    group.addTask {
        for try await message in consumer.messages {
            // Do something with message
            // ...
            try await consumer.commitSync(message)
        }
    }
}
```

### Security Mechanisms

Both the `KafkaProducer` and the `KafkaConsumer` can be configured to use different security mechanisms.

#### Plaintext

```swift
var configuration = KafkaProducerConfiguration(bootstrapBrokerAddresses: [])
configuration.securityProtocol = .plaintext
```

#### TLS

```swift
let leafCert = KafkaConfiguration.TLSConfiguration.LeafAndIntermediates.pem("YOUR_LEAF_CERTIFICATE")
let rootCert = KafkaConfiguration.TLSConfiguration.TrustRoots.pem("YOUR_ROOT_CERTIFICATE")

let privateKey = KafkaConfiguration.TLSConfiguration.PrivateKey(
    location: .file(location: "KEY_FILE"),
    password: ""
)

var tlsConfig = KafkaConfiguration.TLSConfiguration()
tlsConfig.clientIdentity = .none
tlsConfig.brokerVerification = .verify(crlLocation: nil)

var configuration = KafkaProducerConfiguration(bootstrapBrokerAddresses: [])
configuration.securityProtocol = .tls(configuration: tlsConfig)
```

#### SASL

```swift
let kerberosConfiguration = KafkaConfiguration.SASLMechanism.KerberosConfiguration(
    keytab: "KEYTAB_FILE"
)

var config = KafkaProducerConfiguration(bootstrapBrokerAddresses: [])
config.securityProtocol = .saslPlaintext(
    mechanism: .gssapi(kerberosConfiguration: kerberosConfiguration)
)
```

#### SASL + TLS

```swift
let leafCert = KafkaConfiguration.TLSConfiguration.LeafAndIntermediates.pem("YOUR_LEAF_CERTIFICATE")
let rootCert = KafkaConfiguration.TLSConfiguration.TrustRoots.pem("YOUR_ROOT_CERTIFICATE")

let privateKey = KafkaConfiguration.TLSConfiguration.PrivateKey(
    location: .file(location: "KEY_FILE"),
    password: ""
)

var tlsConfig = KafkaConfiguration.TLSConfiguration()
tlsConfig.clientIdentity = .none
tlsConfig.brokerVerification = .verify(crlLocation: nil)

let saslMechanism = KafkaConfiguration.SASLMechanism.scramSHA256(
    username: "USERNAME",
    password: "PASSWORD"
)

var config = KafkaProducerConfiguration(bootstrapBrokerAddresses: [])
config.securityProtocol = .saslTLS(
    saslMechanism: saslMechanism,
    tlsConfiguaration: tlsConfig
)
```

## librdkafka

The Package depends on [the librdkafka library](https://github.com/confluentinc/librdkafka), which is included as a git submodule.
It has source files that are excluded in `Package.swift`.

## Development Setup

We provide a Docker environment for this package. This will automatically start a local Kafka server and run the package tests.

```bash
docker-compose -f docker/docker-compose.yaml run test
```
