# Swift-kafka-client-0001: Consumer API

## Overview

- **Proposal:** Swift-kafka-client-0001
- **Implementation PR:** [swift-server/swift-kafka-client#250](https://github.com/swift-server/swift-kafka-client/pull/250) — tracks this proposal's implementation
- **Note:** This PR exists to collect API review feedback. Once the design is finalized and landed in #250, this PR will be closed rather than merged.

## Introduction

`KafkaConsumer` is the entry point for reading records from a Kafka cluster. It integrates with `swift-service-lifecycle` (`Service`) and exposes records through an `AsyncSequence`.

## Motivation

Kafka's consumer semantics cover four distinct axes:

- Offset persistence — automatic (auto-store + auto-commit), manual per-message, or batched.
- Subscription vs assignment — dynamic group membership vs pinned partitions.
- Backpressure — pause/resume without leaving the group.
- Rebalance observation — react to partition assignment changes.

This proposal freezes the 1.0 Beta shape of the consumer surface.

## Proposed API

```swift
/// Consumes messages from a Kafka cluster as part of a service lifecycle.
public final class KafkaConsumer: Sendable, Service {

    // MARK: Creation

    /// Creates a consumer and its paired message and events sequences.
    ///
    /// Every consumer receives an events sequence for rebalance and error observation.
    /// Callers who don't need it discard the returned `events` value.
    ///
    /// - Parameter config: The ``KafkaConsumerConfig`` for configuring the consumer.
    /// - Returns: A named tuple containing the created ``KafkaConsumer`` and its ``KafkaConsumer/Messages`` and ``KafkaConsumer/Events`` `AsyncSequence`s.
    /// - Throws: A ``KafkaError`` if the initialization failed.
    public static func makeConsumer(
        config: KafkaConsumerConfig
    ) throws -> (consumer: KafkaConsumer, messages: Messages, events: Events)

    /// Creates a consumer, runs it for the duration of the closure, and shuts it down on return.
    ///
    /// Spawns the consumer's `run()` loop internally and cancels it when `body` returns or
    /// throws.
    ///
    /// - Parameters:
    ///   - config: The ``KafkaConsumerConfig`` for configuring the consumer.
    ///   - body: A closure that receives the consumer and its message and events sequences.
    /// - Returns: The value returned by `body`.
    /// - Throws: A ``KafkaError`` if initialization failed, or any error thrown by `body`.
    public static func withConsumer<Result: ~Copyable>(
        config: KafkaConsumerConfig,
        _ body: (KafkaConsumer, Messages, Events) async throws -> Result
    ) async throws -> Result

    // MARK: Service conformance

    /// Starts the consumer's event loop.
    ///
    /// This method must be called for the consumer to start receiving records and events.
    /// It returns once the consumer is closed or the task is cancelled.
    public func run() async throws

    // MARK: Subscription management

    /// Subscribes to the given topics.
    ///
    /// - Parameter topics: The list of topics to subscribe to.
    /// - Throws: A ``KafkaError`` if subscribing failed or the consumer is closed.
    public func subscribe(topics: [KafkaTopic]) throws

    /// Unsubscribes from all currently subscribed topics.
    ///
    /// - Throws: A ``KafkaError`` if unsubscribing failed or the consumer is closed.
    public func unsubscribe() throws

    /// The topics the consumer is currently subscribed to.
    public var subscribedTopics: [KafkaTopic] { get throws }

    // MARK: Pause / resume

    /// Pauses consumption for the given topic-partitions.
    ///
    /// - Parameter topicPartitions: The topic-partitions to pause.
    /// - Throws: A ``KafkaError`` if pausing failed or the consumer is closed.
    public func pause(topicPartitions: [KafkaTopicPartition]) throws

    /// Resumes consumption for the given topic-partitions.
    ///
    /// - Parameter topicPartitions: The topic-partitions to resume.
    /// - Throws: A ``KafkaError`` if resuming failed or the consumer is closed.
    public func resume(topicPartitions: [KafkaTopicPartition]) throws

    // MARK: Offset management

    /// Records an offset in librdkafka's internal store for later commit.
    ///
    /// Requires `enableAutoOffsetStore == false`.
    ///
    /// - Parameter message: The message whose offset should be stored.
    /// - Throws: A ``KafkaError`` if storing the offset failed or the consumer is closed.
    public func storeOffset(_ message: Message) throws

    /// Commits the offset of one message directly. Bypasses the offset store.
    ///
    /// - Parameter message: The message whose offset should be committed.
    /// - Throws: A ``KafkaError`` if committing failed or the consumer is closed.
    public func commit(_ message: Message) async throws

    /// Commits every offset currently in the local offset store.
    ///
    /// - Throws: A ``KafkaError`` if committing failed or the consumer is closed.
    public func commitStoredOffsets() async throws

    // MARK: Position query

    /// Retrieves the last-committed offsets from the broker for the given topic-partitions.
    ///
    /// - Parameters:
    ///   - topicPartitions: The topic-partitions to query.
    ///   - timeout: The maximum time to wait for the broker to respond.
    /// - Returns: A list of topic-partition offsets.
    /// - Throws: A ``KafkaError`` if querying failed or the consumer is closed.
    public func committed(
        topicPartitions: [KafkaTopicPartition],
        timeout: Duration
    ) async throws -> [KafkaTopicPartitionOffset]

    /// Retrieves the current positions (next offset to be fetched) for the given topic-partitions.
    ///
    /// - Parameter topicPartitions: The topic-partitions to query.
    /// - Returns: A list of topic-partition offsets.
    /// - Throws: A ``KafkaError`` if querying failed or the consumer is closed.
    public func position(
        topicPartitions: [KafkaTopicPartition]
    ) throws -> [KafkaTopicPartitionOffset]

    /// A boolean indicating whether the consumer's assignment has been lost.
    public var isAssignmentLost: Bool { get }

    // MARK: Seeking

    /// Seeks to a specific offset for each of the given topic-partitions.
    ///
    /// - Parameters:
    ///   - topicPartitionOffsets: The offsets to seek to.
    ///   - timeout: The maximum time to wait for the seek to complete.
    /// - Throws: A ``KafkaError`` if seeking failed or the consumer is closed.
    public func seek(
        topicPartitionOffsets: [KafkaTopicPartitionOffset],
        timeout: Duration
    ) async throws
}
```

### Message and event sequences

```swift
extension KafkaConsumer {
    /// An `AsyncSequence` of records consumed from the Kafka cluster.
    public struct Messages: Sendable, AsyncSequence {
        public typealias Element = Message
        public struct AsyncIterator: AsyncIteratorProtocol {
            public mutating func next() async throws -> Element?
        }
        public func makeAsyncIterator() -> AsyncIterator
    }

    /// An `AsyncSequence` of rebalance and error events for the consumer.
    public struct Events: Sendable, AsyncSequence {
        public typealias Element = Event
        public struct AsyncIterator: AsyncIteratorProtocol {
            public mutating func next() async -> Element?
        }
        public func makeAsyncIterator() -> AsyncIterator
    }

    /// An event observed on the consumer's events sequence.
    @nonexhaustive
    public enum Event: Sendable, Hashable {
        /// The consumer's partition assignment changed.
        case rebalance(Rebalance)
        /// A non-fatal error occurred.
        case error(KafkaError)
    }

    /// A change in the consumer's partition assignment.
    public struct Rebalance: Sendable, Hashable {
        /// The kind of rebalance that occurred.
        public enum Kind: Sendable, Hashable {
            /// Partitions were assigned to this consumer.
            case assign
            /// Partitions were revoked from this consumer.
            case revoke
            /// The rebalance failed with the given reason.
            case error(String)
        }
        /// Whether partitions were assigned, revoked, or failed to rebalance.
        public var kind: Kind { get }
        /// The topic-partitions affected by this rebalance.
        public var partitions: [KafkaTopicPartition] { get }
    }

    /// A single record consumed from a Kafka cluster.
    public struct Message: Sendable {
        /// The topic this message was consumed from.
        public var topic: KafkaTopic { get }
        /// The partition this message was consumed from.
        public var partition: KafkaPartition { get }
        /// This message's offset within its partition.
        public var offset: KafkaOffset { get }
        /// The message key, if any.
        public var key: [UInt8]? { get }
        /// The message value.
        public var value: [UInt8] { get }
        /// The message headers.
        public var headers: [KafkaHeader] { get }
        /// The timestamp of the message in milliseconds since epoch, or `nil` if not available.
        public var timestamp: Int64? { get }
        /// The type of timestamp on this message.
        public var timestampType: KafkaTimestampType { get }
    }
}
```

### Topic type

Shared with the producer proposal — matches the existing ``KafkaPartition``/``KafkaOffset`` pattern of wrapping the underlying primitive instead of passing a raw `String`.

```swift
/// The name of a Kafka topic.
public struct KafkaTopic: RawRepresentable, ExpressibleByStringLiteral, CustomStringConvertible, Hashable, Sendable {
    /// The raw string name of the topic.
    public var rawValue: String

    /// A textual representation of the topic name.
    public var description: String { get }

    /// Creates a topic name from its raw string value.
    public init(rawValue: String)

    /// Creates a topic name from a string literal.
    public init(stringLiteral value: String)
}
```

### Timestamp type

Wraps the underlying librdkafka timestamp-type value, exposing named constants instead of raw integers — same pattern as ``KafkaPartition``/``KafkaOffset``.

```swift
/// The type of timestamp on a Kafka message.
public struct KafkaTimestampType: Hashable, Sendable, CustomStringConvertible {
    /// The raw value corresponding to the librdkafka timestamp type.
    public let rawValue: Int32

    /// Creates a timestamp type from its raw librdkafka value.
    ///
    /// Internal: the library constructs these from librdkafka. Callers only read
    /// `rawValue` or compare against the static constants below.
    init(rawValue: Int32)

    /// Timestamp not available.
    public static let notAvailable: KafkaTimestampType
    /// Timestamp set by the producer (message creation time).
    public static let createTime: KafkaTimestampType
    /// Timestamp set by the broker (log append time).
    public static let logAppendTime: KafkaTimestampType

    /// A textual representation of the timestamp type.
    public var description: String { get }
}
```

### Supporting value types

Shared with the producer proposal. Each wraps an underlying primitive — matching the ``KafkaPartition``/``KafkaOffset`` pattern — so call sites use named types instead of bare `String`/`Int` values.

```swift
/// A header attached to a Kafka message.
public struct KafkaHeader: Sendable, Hashable {
    /// The key associated with the header.
    public var key: String
    /// The value associated with the header.
    public var value: [UInt8]?

    /// Creates a new Kafka header with the key and optional value you provide.
    public init(key: String, value: [UInt8]? = nil)
}

/// The identifier of a partition within a topic.
public struct KafkaPartition: RawRepresentable, CustomStringConvertible, Hashable, Sendable {
    /// The raw integer identifier of the partition.
    public var rawValue: Int
    /// A textual representation of the partition identifier.
    public var description: String { get }

    /// Creates a partition identifier from its raw integer value.
    public init(rawValue: Int)

    /// A sentinel value that defers partition assignment to the topic's partitioner function.
    public static let unassigned: KafkaPartition
}

/// An offset within a Kafka partition.
public struct KafkaOffset: RawRepresentable, CustomStringConvertible, Hashable, Sendable {
    /// The raw integer value of the offset.
    public var rawValue: Int
    /// A textual representation of the offset.
    public var description: String { get }

    /// Creates a Kafka offset from its raw integer value.
    public init(rawValue: Int)

    /// Start consuming from the beginning of the partition (the oldest message).
    public static let beginning: KafkaOffset
    /// Start consuming from the end of the partition (wait for the next produced message).
    public static let end: KafkaOffset
    /// Start consuming from the offset retrieved from the offset store.
    public static let storedOffset: KafkaOffset
    /// Start consuming with the latest `count` messages of the partition.
    public static func tail(_ count: Int) -> KafkaOffset
}

/// A topic paired with one of its partitions.
public struct KafkaTopicPartition: Sendable, Hashable {
    /// The name of the Kafka topic.
    public var topic: KafkaTopic
    /// The partition within the topic.
    public var partition: KafkaPartition

    /// Creates a topic-partition pair from a topic name and partition.
    public init(topic: KafkaTopic, partition: KafkaPartition)
}

/// A topic-partition paired with an offset.
public struct KafkaTopicPartitionOffset: Sendable, Hashable {
    /// The topic and partition.
    public var topicPartition: KafkaTopicPartition
    /// The offset for this topic-partition, or `nil` if no committed offset or position exists.
    public var offset: KafkaOffset?

    /// The name of the Kafka topic. A convenience accessor for ``KafkaTopicPartition/topic``.
    public var topic: KafkaTopic { get }
    /// The partition within the topic. A convenience accessor for ``KafkaTopicPartition/partition``.
    public var partition: KafkaPartition { get }

    /// Creates a topic-partition-offset triple from a topic name, partition, and offset.
    public init(topic: KafkaTopic, partition: KafkaPartition, offset: KafkaOffset?)
    /// Creates a topic-partition-offset triple from an existing topic-partition pair and an offset.
    public init(topicPartition: KafkaTopicPartition, offset: KafkaOffset?)
}
```
