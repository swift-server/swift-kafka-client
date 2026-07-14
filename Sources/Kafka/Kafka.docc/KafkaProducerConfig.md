# ``Kafka/KafkaProducerConfig``

Configures a Kafka producer.

## Overview

Provide broker addresses through ``KafkaProducerConfig/bootstrapServers``, then pass the configuration to a ``KafkaProducer`` initializer. Additional properties mirror librdkafka's producer configuration surface; consult individual property documentation for defaults and semantics.
