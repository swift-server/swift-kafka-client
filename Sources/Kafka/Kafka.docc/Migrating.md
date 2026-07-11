# Migrating to the current API

Move from the deprecated configuration types and methods to their current replacements.

## Overview

Earlier versions of the client configured a ``KafkaConsumer`` or ``KafkaProducer`` with the ``KafkaConsumerConfiguration`` and ``KafkaProducerConfiguration`` types. These are deprecated in favor of ``KafkaConsumerConfig`` and ``KafkaProducerConfig``, which expose librdkafka's full configuration surface as typed Swift properties.

The deprecated types still compile, but the compiler emits a warning pointing to the replacement. Adopt the current API to silence those warnings and to reach configuration options the older types don't expose.

## Configuration types

Replace each `Configuration` type with the corresponding `Config` type:

| Deprecated | Current |
| --- | --- |
| ``KafkaConsumerConfiguration`` | ``KafkaConsumerConfig`` |
| ``KafkaProducerConfiguration`` | ``KafkaProducerConfig`` |

## Initializers

The client initializers now take a `config:` parameter of the `Config` type in place of the `configuration:` parameter of the `Configuration` type:

```swift
// Deprecated
let consumer = try KafkaConsumer(configuration: consumerConfiguration, logger: logger)

// Current
var config = KafkaConsumerConfig()
config.bootstrapServers = ["localhost:9092"]
config.consumptionStrategy = .group(id: "example-group", topics: ["topic-name"])
let consumer = try KafkaConsumer(config: config, logger: logger)
```

For the event-carrying variants, use ``KafkaConsumer/makeConsumerWithEvents(config:logger:)`` and ``KafkaProducer/makeProducerWithEvents(config:logger:)``, which likewise take the `Config` type.

## Renamed methods

``KafkaConsumer/commitSync(_:)`` was renamed to ``KafkaConsumer/commit(_:)``. The behavior is unchanged.

## Topics

### Current configuration types

- ``KafkaConsumerConfig``
- ``KafkaProducerConfig``

### Deprecated types

- ``KafkaConsumerConfiguration``
- ``KafkaProducerConfiguration``
