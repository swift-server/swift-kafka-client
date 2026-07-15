# ``Kafka/KafkaConsumerConfig``

Configures a Kafka consumer.

## Overview

Provide broker addresses through ``KafkaConsumerConfig/bootstrapServers`` and a consumption strategy through ``KafkaConsumerConfig/consumptionStrategy``, then pass the configuration to a ``KafkaConsumer`` initializer. Additional properties expose the consumer's configuration options; consult individual property documentation for defaults and semantics.

For security options, see <doc:SecuringConnections>. For an end-to-end example, see <doc:ConsumingMessages>.
