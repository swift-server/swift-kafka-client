# ``Kafka/KafkaProducer``

Sends messages to a Kafka cluster as part of a service lifecycle.

## Overview

Create a producer from a ``KafkaProducerConfig``, run it inside a `ServiceGroup`, and call ``send(_:)`` or ``sendAndAwait(_:)`` to publish records. The producer conforms to `Service`, so the surrounding application controls its lifecycle; the `run()` method drives the internal poll loop, dispatches delivery reports, and flushes outstanding messages on graceful shutdown.

Two send styles are available. Use ``sendAndAwait(_:)`` when the caller needs the delivery outcome inline — the method suspends until the broker acknowledges (or rejects) the message and returns a ``KafkaDeliveryReport``. Use ``send(_:)`` paired with ``makeProducerWithEvents(config:logger:)`` for maximum throughput, processing delivery reports asynchronously through the events sequence.

When messages are published to a nonexistent topic, a new topic is created using the default topic configuration (based on ``KafkaProducerConfig`` topic-level properties).

For an end-to-end guide including configuration, delivery patterns, and event handling, see <doc:ProducingMessages>.

## Topics

### Creating a producer

- ``init(config:logger:)``
- ``makeProducerWithEvents(config:logger:)``

### Sending messages

- ``send(_:)``
- ``sendAndAwait(_:)``
- ``KafkaProducerMessage``
- ``KafkaProducerMessageID``

### Running the service

- ``run()``
- ``triggerGracefulShutdown()``

### Observing outcomes

- ``KafkaProducerEvents``
- ``KafkaProducerEvent``
- ``KafkaDeliveryReport``
- ``KafkaAcknowledgedMessage``
