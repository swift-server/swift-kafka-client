# ``Kafka/KafkaConsumerConfig``

Configures a ``KafkaConsumer``.

## Overview

Provide broker addresses through ``KafkaConsumerConfig/bootstrapServers`` and a consumption strategy through ``KafkaConsumerConfig/consumptionStrategy``, then pass the configuration to a ``KafkaConsumer`` initializer. Additional properties mirror librdkafka's consumer configuration surface; consult individual property documentation for defaults and semantics.
