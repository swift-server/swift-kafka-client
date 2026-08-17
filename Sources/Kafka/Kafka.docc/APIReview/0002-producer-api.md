# Swift-kafka-client-0002: Producer API

## Overview

- **Proposal:** Swift-kafka-client-0002
- **Implementation PR:** [swift-server/swift-kafka-client#250](https://github.com/swift-server/swift-kafka-client/pull/250) — tracks this proposal's implementation
- **Note:** This PR exists to collect API review feedback. Once the design is finalized and landed in #250, this PR will be closed rather than merged.

## Introduction

`KafkaProducer` publishes records to a Kafka cluster. Like the consumer, it conforms to `Service` and runs inside a `ServiceGroup`.

## Motivation

Two send styles cover the two dominant producer patterns:

- **Awaitable send** — caller needs delivery outcome inline (e.g., a request handler returning the resulting partition/offset).
- **Fire-and-forget send with events sequence** — high-throughput pipelines that process delivery reports asynchronously.

This proposal freezes the 1.0 Beta shape of the producer surface.

## Proposed API

```swift
/// Sends messages to a Kafka cluster as part of a service lifecycle.
public final class KafkaProducer: Service, Sendable {

    // MARK: Creation

    /// Creates a producer and its paired events sequence.
    ///
    /// Every producer receives an events sequence for delivery reports (from `send(_:)`)
    /// and error observation. Callers who only use `sendAndAwait(_:)` may discard the
    /// returned `events` value.
    ///
    /// - Parameter config: The ``KafkaProducerConfig`` for configuring the producer.
    /// - Returns: A named tuple containing the created ``KafkaProducer`` and its ``KafkaProducer/Events`` `AsyncSequence`.
    /// - Throws: A ``KafkaError`` if initializing the producer failed.
    public static func makeProducer(
        config: KafkaProducerConfig
    ) throws -> (producer: KafkaProducer, events: Events)

    /// Creates a producer, runs it for the duration of the closure, and shuts it down on return.
    ///
    /// Spawns the producer's `run()` loop internally and cancels it when `body` returns or
    /// throws.
    ///
    /// - Parameters:
    ///   - config: The ``KafkaProducerConfig`` for configuring the producer.
    ///   - body: A closure that receives the producer and its events sequence.
    /// - Returns: The value returned by `body`.
    /// - Throws: A ``KafkaError`` if initialization failed, or any error thrown by `body`.
    public static func withProducer<Result: ~Copyable>(
        config: KafkaProducerConfig,
        _ body: (KafkaProducer, Events) async throws -> Result
    ) async throws -> Result

    // MARK: Service conformance

    /// Starts the producer's event loop.
    ///
    /// This method must be called for the producer to start sending records and polling for events.
    /// It returns once the producer is closed or the task is cancelled.
    public func run() async throws

    // MARK: Sending
    //
    // Key/value byte-input generic constraint is covered by proposal 0006.

    /// Enqueues a record for delivery.
    ///
    /// Returns immediately with an identifier that will appear on the eventual ``KafkaProducer/DeliveryReport``.
    /// Outcomes are reported through the ``KafkaProducer/Events`` sequence.
    ///
    /// - Parameter message: The message to send.
    /// - Returns: A unique identifier for the message.
    /// - Throws: A ``KafkaError`` if enqueuing the message failed or the producer is closed.
    public func send<Key: KafkaContiguousBytes, Value: KafkaContiguousBytes>(
        _ message: Message<Key, Value>
    ) throws -> MessageID

    /// Sends a record and awaits its delivery report.
    ///
    /// - Parameter message: The message to send.
    /// - Returns: A ``KafkaProducer/DeliveryReport`` indicating the outcome of the delivery.
    /// - Throws: A ``KafkaError`` if sending the message failed or the producer is closed.
    public func sendAndAwait<Key: KafkaContiguousBytes, Value: KafkaContiguousBytes>(
        _ message: Message<Key, Value>
    ) async throws -> DeliveryReport
}
```

### Event sequence and outcome types

```swift
extension KafkaProducer {
    /// An `AsyncSequence` of delivery reports (from `send(_:)`) and error events for the producer.
    public struct Events: Sendable, AsyncSequence {
        public typealias Element = Event
        public struct AsyncIterator: AsyncIteratorProtocol {
            public mutating func next() async -> Element?
        }
        public func makeAsyncIterator() -> AsyncIterator
    }

    /// An event observed on the producer's events sequence.
    @nonexhaustive
    public enum Event: Sendable, Hashable {
        /// Delivery reports for messages sent via `send(_:)`.
        case deliveryReports([DeliveryReport])
        /// A non-fatal error occurred.
        case error(KafkaError)
    }

    /// A record to be sent to a Kafka cluster.
    public struct Message<Key: KafkaContiguousBytes, Value: KafkaContiguousBytes> {
        /// The topic to send this message to.
        public var topic: KafkaTopic
        /// The partition to send this message to.
        public var partition: KafkaPartition
        /// The message key, if any.
        public var key: Key?
        /// The message value.
        public var value: Value
        /// The message headers.
        public var headers: [KafkaHeader]

        /// Creates a new producer message.
        public init(
            topic: KafkaTopic,
            partition: KafkaPartition = .unassigned,
            key: Key? = nil,
            value: Value,
            headers: [KafkaHeader] = []
        )
    }

    /// A unique identifier for a message enqueued via `send(_:)`.
    public struct MessageID: Sendable, Hashable { }

    /// The outcome of a single message delivery.
    public struct DeliveryReport: Sendable, Hashable {
        /// Whether the message was acknowledged or failed to deliver.
        public enum Status: Sendable, Hashable {
            /// The message was successfully delivered.
            case acknowledged(AcknowledgedMessage)
            /// The message failed to deliver.
            case failure(KafkaError)
        }
        /// The delivery outcome.
        public var status: Status { get }
        /// The identifier returned by the corresponding `send(_:)` call.
        public var messageID: MessageID { get }
    }

    /// A message that was successfully delivered to a Kafka cluster.
    public struct AcknowledgedMessage: Sendable {
        /// The topic the message was delivered to.
        public var topic: KafkaTopic { get }
        /// The partition the message was delivered to.
        public var partition: KafkaPartition { get }
        /// The offset assigned to the message.
        public var offset: KafkaOffset { get }
    }
}
```

`KafkaTopic`, along with the supporting value types it uses (`KafkaHeader`, `KafkaPartition`, `KafkaOffset`), is shared with the consumer proposal (proposal 0001) — the same underlying types, defined there and not redefined here.

### Delivery-report routing

Each delivery lands on exactly one channel:

- Reports for `send(_:)` calls → emitted on `KafkaProducer.Event.deliveryReports([...])`.
- Reports for `sendAndAwait(_:)` calls → resolved on the awaiting continuation only; not re-emitted on the events sequence.
- Non-delivery events (broker disconnection, authentication failure, transport errors) → `KafkaProducer.Event.error(_:)` regardless.
