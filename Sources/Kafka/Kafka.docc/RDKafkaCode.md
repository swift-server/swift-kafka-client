# ``Kafka/KafkaError/RDKafkaCode``

An error code reported by the underlying C library that backs the client.

## Overview

When a ``KafkaError`` originates from the underlying C library, its ``KafkaError/rdKafkaCode`` carries one of these codes. Match against the static constants to handle specific failures without importing the C module — for example, telling a connectivity problem (``allBrokersDown``, ``transport``) apart from an authentication problem (``authentication``, ``ssl``).

Each code's ``description`` includes the underlying name and numeric value, which is useful to surface in logs.

## Topics

### Connectivity

- ``allBrokersDown``
- ``transport``
- ``timedOut``

### Authentication and security

- ``authentication``
- ``ssl``

### Producing and consuming

- ``messageTimedOut``
- ``queueFull``
- ``maxPollExceeded``

### Topics and partitions

- ``unknownTopic``
- ``unknownPartition``

### Other

- ``fatal``
- ``invalidArgument``
- ``noError``
