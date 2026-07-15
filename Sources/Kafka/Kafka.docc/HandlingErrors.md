# Handling errors

Find where Kafka errors surface and classify them to decide whether to retry, recreate the client, or fail.

## Overview

Both the producer and the consumer report failures as ``KafkaError`` values. A ``KafkaError`` carries a ``KafkaError/code`` describing the kind of failure and, when the error originates from librdkafka, a ``KafkaError/rdKafkaCode`` with the specific underlying code. The ``KafkaError/isFatal`` and ``KafkaError/isRetriable`` flags tell you how to respond.

## Where errors surface

A ``KafkaError`` reaches your code in three ways:

- **Thrown** from throwing calls such as ``KafkaProducer/send(_:)``, ``KafkaProducer/sendAndAwait(_:)``, ``KafkaConsumer/subscribe(topics:)``, and ``KafkaConsumer/commit(_:)``.
- **On an event sequence**, as ``KafkaConsumerEvent/error(_:)`` or ``KafkaProducerEvent/error(_:)`` — for example, a broker disconnection or an authentication failure.
- **In a delivery report**, when a ``KafkaDeliveryReport``'s status is `failure` because a message could not be delivered.

## Classify an error

Use ``KafkaError/isFatal`` and ``KafkaError/isRetriable`` to decide how to respond, and inspect ``KafkaError/rdKafkaCode`` for finer-grained handling:

```swift
func handle(_ error: KafkaError, logger: Logger) {
    // A fatal error means the client instance can no longer be used:
    // shut it down and create a new one.
    if error.isFatal {
        logger.critical("Fatal Kafka error, recreate the client", metadata: ["error": "\(error)"])
        return
    }

    // A retriable error may succeed if the operation is attempted again.
    if error.isRetriable {
        logger.warning("Retriable Kafka error, retrying", metadata: ["error": "\(error)"])
        return
    }

    // For finer-grained handling, inspect the underlying librdkafka code.
    switch error.rdKafkaCode {
    case .allBrokersDown, .transport:
        logger.warning("Lost connectivity to the cluster", metadata: ["error": "\(error)"])
    case .authentication, .ssl:
        logger.error("Authentication or TLS failure", metadata: ["error": "\(error)"])
    default:
        logger.error("Kafka error", metadata: ["kind": "\(error.code)", "error": "\(error)"])
    }
}
```

``KafkaError/rdKafkaCode`` is `nil` for errors that don't originate from librdkafka — such as configuration or lifecycle errors — in which case ``KafkaError/isFatal`` and ``KafkaError/isRetriable`` are both `false`.

## Topics

### Articles

- <doc:ProducingMessages>
- <doc:ConsumingMessages>
- <doc:Observability>

### Errors

- ``KafkaError``
- ``KafkaError/ErrorCode``
- ``KafkaError/RDKafkaCode``

### Error events

- ``KafkaConsumerEvent``
- ``KafkaProducerEvent``
- ``KafkaDeliveryReport``
