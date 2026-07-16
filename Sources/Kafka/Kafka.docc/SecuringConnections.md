# Securing connections

Configure TLS or SASL authentication for producers and consumers connecting to a broker.

## Overview

Both ``KafkaProducer`` and ``KafkaConsumer`` accept the same security settings on their configuration types. Select the wire protocol with `securityProtocol`, and provide SASL credentials when the chosen protocol requires them.

The following examples configure a ``KafkaProducerConfig``. The same property names apply to ``KafkaConsumerConfig``.

### Use plaintext for local development

Plaintext sends data without encryption, and is appropriate only for local development against a broker on a trusted network:

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
config.securityProtocol = .plaintext
```

### Encrypt with TLS

To encrypt the connection without authenticating, use TLS:

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
config.securityProtocol = .ssl
```

### Authenticate with SASL over plaintext

For environments that authenticate clients with SASL but don't require encryption, set the protocol to `.sasl_plaintext` and provide a mechanism with credentials:

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
config.securityProtocol = .sasl_plaintext
config.saslMechanisms = "PLAIN"
config.saslUsername = "user"
config.saslPassword = "password"
```

### Authenticate with SASL over TLS

For production, combine SASL authentication with TLS encryption:

```swift
var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
config.securityProtocol = .sasl_ssl
config.saslMechanisms = "SCRAM-SHA-256"
config.saslUsername = "user"
config.saslPassword = "password"
```

`SCRAM-SHA-256` and `SCRAM-SHA-512` keep the password from traveling in the clear during the handshake; `PLAIN` does not, so reserve `PLAIN` for transports already protected by TLS.

## Topics

### Configuration

- ``KafkaProducerConfig``
- ``KafkaConsumerConfig``
- ``KafkaConfig``
