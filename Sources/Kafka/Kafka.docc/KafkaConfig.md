# ``Kafka/KafkaConfig``

A namespace of typed option values for Kafka client configuration properties.

## Overview

Nested types provide type-safe wrappers for the string values librdkafka accepts for enum-valued options such as ``KafkaConfig/SecurityProtocol``, ``KafkaConfig/CompressionCodec``, and ``KafkaConfig/GroupProtocol``. Assign these values to properties on ``KafkaConsumerConfig`` and ``KafkaProducerConfig``.
