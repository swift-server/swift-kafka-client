//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2026 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

// ============================================================================
// This file is a PROPOSAL, not an implementation.
//
// It shows the intended public API surface of `Kafka` for the 1.0 Beta release.
// Bodies are `fatalError(...)` stubs. The file is not part of any build target.
//
// Line-comment on any signature to raise concerns. Once we agree on the shape,
// follow-up PRs adopt each change in Sources/Kafka/. See README.md alongside.
//
// Legend:
//   // OPEN Qn: <description>   — an unresolved decision, matches README.md
//   // SETTLED (#PR):           — already agreed in the referenced PR
// ============================================================================

import Logging
import ServiceLifecycle

// MARK: - Consumer

/// Consumes messages from a Kafka cluster as part of a service lifecycle.
public final class KafkaConsumer: Sendable, Service {

    // MARK: Creation

    /// Creates a consumer.
    ///
    /// The consumer joins the group specified by ``KafkaConsumerConfig/consumptionStrategy``
    /// and receives messages through ``messages`` once the containing `ServiceGroup` runs it.
    public convenience init(config: KafkaConsumerConfig, logger: Logger) throws { fatalError() }

    /// Creates a consumer paired with an event sequence.
    ///
    /// The events sequence surfaces rebalance notifications and errors. If you create the
    /// sequence you **must** iterate it; otherwise events buffer indefinitely.
    /// See also OPEN Q9 on bounded backpressure (PR #237).
    public static func makeConsumerWithEvents(
        config: KafkaConsumerConfig,
        logger: Logger
    ) throws -> (KafkaConsumer, KafkaConsumerEvents) { fatalError() }

    // MARK: Service conformance

    /// Drives the underlying poll loop. Runs until the calling task is canceled
    /// or graceful shutdown is triggered.
    public func run() async throws { fatalError() }

    /// Requests a graceful shutdown of the consumer.
    public func triggerGracefulShutdown() { fatalError() }

    // MARK: Message consumption

    /// Asynchronous sequence of records received from the cluster.
    public var messages: KafkaConsumerMessages { fatalError() }

    // MARK: Subscription management

    public func subscribe(topics: [String]) throws { fatalError() }
    public func unsubscribe() throws { fatalError() }
    public func subscribedTopics() throws -> [String] { fatalError() }

    // MARK: Pause / resume

    public func pause(topicPartitions: [KafkaTopicPartition]) throws { fatalError() }
    public func resume(topicPartitions: [KafkaTopicPartition]) throws { fatalError() }

    // MARK: Offsets — store and commit
    // ------------------------------------------------------------------------
    // Design notes from earlier discussion:
    //   - `storeOffset(_:)` writes to librdkafka's internal offset store.
    //     Requires `enableAutoOffsetStore == false`. Doesn't touch the broker.
    //   - `commit(_:)` commits a single message's offset directly, bypassing
    //     the store. Requires `enableAutoCommit == false`.
    //   - `commitStoredOffsets()` (renamed from `commit()`, SETTLED in #250)
    //     flushes whatever is currently in the store to the broker.
    // ------------------------------------------------------------------------

    /// Records an offset in librdkafka's internal offset store for later commit.
    // OPEN Q4: Keep name `storeOffset` (matches librdkafka + Rust + Python)
    //           or rename to `markProcessed` for Swift-native readability?
    public func storeOffset(_ message: KafkaConsumerMessage) throws { fatalError() }

    /// Commits the offset of the given message. Awaits the broker response.
    public func commit(_ message: KafkaConsumerMessage) async throws { fatalError() }

    /// Commits every offset currently in the local offset store.
    // SETTLED (#250): renamed from `commit()`.
    public func commitStoredOffsets() async throws { fatalError() }

    // OPEN Q3: Do these two methods earn their spot?
    //          They return immediately but silently drop commit-level errors.
    //          Swift-native fire-and-forget is `Task { try? await commit(...) }`.
    //          Alternative: delete both, keep only awaitable commit.
    public func scheduleCommit(_ message: KafkaConsumerMessage) throws { fatalError() }
    public func scheduleCommit() throws { fatalError() }

    // MARK: Position query

    public func committed(
        topicPartitions: [KafkaTopicPartition],
        timeout: Duration
    ) async throws -> [KafkaTopicPartitionOffset] { fatalError() }

    public func position(
        topicPartitions: [KafkaTopicPartition]
    ) throws -> [KafkaTopicPartitionOffset] { fatalError() }

    public var isAssignmentLost: Bool { fatalError() }

    // MARK: Seeking

    public func seek(
        topicPartitionOffsets: [KafkaTopicPartitionOffset],
        timeout: Duration
    ) async throws { fatalError() }
}

// MARK: Consumer sequences

/// Asynchronous sequence of records received from the Kafka cluster.
public struct KafkaConsumerMessages: Sendable, AsyncSequence {
    public typealias Element = KafkaConsumerMessage

    public struct AsyncIterator: AsyncIteratorProtocol {
        public mutating func next() async throws -> Element? { fatalError() }
    }
    public func makeAsyncIterator() -> AsyncIterator { fatalError() }
}

/// Asynchronous sequence of consumer lifecycle events (rebalance, errors).
public struct KafkaConsumerEvents: Sendable, AsyncSequence {
    public typealias Element = KafkaConsumerEvent

    public struct AsyncIterator: AsyncIteratorProtocol {
        public mutating func next() async -> Element? { fatalError() }
    }
    public func makeAsyncIterator() -> AsyncIterator { fatalError() }
}

/// An event emitted by a Kafka consumer.
public enum KafkaConsumerEvent: Sendable, Hashable {
    /// A partition assignment change occurred.
    case rebalance(KafkaConsumerRebalance)
    /// A non-fatal error surfaced from the client.
    case error(KafkaError)
}

/// A partition assignment change reported by the broker.
public struct KafkaConsumerRebalance: Sendable, Hashable {
    public enum Kind: Sendable, Hashable {
        case assign
        case revoke
        case error(String)
    }
    public var kind: Kind { fatalError() }
    public var partitions: [KafkaTopicPartition] { fatalError() }
}

/// A single record received from a Kafka topic.
public struct KafkaConsumerMessage: Sendable {
    public var topic: String { fatalError() }
    public var partition: KafkaPartition { fatalError() }
    public var offset: KafkaOffset { fatalError() }
    public var key: [UInt8]? { fatalError() }
    public var value: [UInt8] { fatalError() }
    public var headers: [KafkaHeader] { fatalError() }
    public var timestamp: KafkaTimestampType? { fatalError() }
    // Note: currently `internal init(messagePointer:)` only — not constructible in unit tests.
    // No API-facing change proposed; test constructor would go behind @_spi(Testing) if needed.
}

// MARK: - Producer

/// Sends messages to a Kafka cluster as part of a service lifecycle.
public final class KafkaProducer: Service, Sendable {

    // MARK: Creation

    public convenience init(config: KafkaProducerConfig, logger: Logger) throws { fatalError() }

    public static func makeProducerWithEvents(
        config: KafkaProducerConfig,
        logger: Logger
    ) throws -> (KafkaProducer, KafkaProducerEvents) { fatalError() }

    // MARK: Service conformance

    public func run() async throws { fatalError() }
    public func triggerGracefulShutdown() { fatalError() }

    // MARK: Sending
    // ------------------------------------------------------------------------
    // OPEN Q5: `KafkaContiguousBytes` protocol constraint.
    //   Franz's comment on #239: "We should really remove this protocol and
    //   start using the various span types instead."
    //   Two candidate replacements:
    //     A. Generic `<Key: Span<UInt8>, Value: Span<UInt8>>` — Swift 6.1+
    //     B. Non-generic taking `RawSpan` for key and value
    //   Also decides whether `Foundation.Data` support is on-by-default
    //   (currently in a separate `KafkaFoundationCompat` target).
    // ------------------------------------------------------------------------

    /// Enqueues a record for delivery. Returns immediately with an identifier
    /// that will appear on the eventual ``KafkaDeliveryReport``.
    public func send<Key: KafkaContiguousBytes, Value: KafkaContiguousBytes>(
        _ message: KafkaProducerMessage<Key, Value>
    ) throws -> KafkaProducerMessageID { fatalError() }

    /// Sends a record and awaits its delivery report.
    public func sendAndAwait<Key: KafkaContiguousBytes, Value: KafkaContiguousBytes>(
        _ message: KafkaProducerMessage<Key, Value>
    ) async throws -> KafkaDeliveryReport { fatalError() }
}

// MARK: Producer sequences

/// Asynchronous sequence of producer events (delivery reports, errors).
public struct KafkaProducerEvents: Sendable, AsyncSequence {
    public typealias Element = KafkaProducerEvent

    public struct AsyncIterator: AsyncIteratorProtocol {
        public mutating func next() async -> Element? { fatalError() }
    }
    public func makeAsyncIterator() -> AsyncIterator { fatalError() }
}

/// An event emitted by a Kafka producer.
public enum KafkaProducerEvent: Sendable, Hashable {
    case deliveryReports([KafkaDeliveryReport])
    case error(KafkaError)
}

/// A record to be sent to a Kafka topic.
public struct KafkaProducerMessage<Key: KafkaContiguousBytes, Value: KafkaContiguousBytes> {
    public var topic: String
    public var partition: KafkaPartition
    public var key: Key?
    public var value: Value
    public var headers: [KafkaHeader]

    public init(
        topic: String,
        partition: KafkaPartition = .unassigned,
        key: Key? = nil,
        value: Value,
        headers: [KafkaHeader] = []
    ) { fatalError() }
}

/// An opaque identifier returned from ``KafkaProducer/send(_:)``. Match it against
/// ``KafkaDeliveryReport/messageID`` to correlate a send with its outcome.
public struct KafkaProducerMessageID: Sendable, Hashable {
    // opaque
}

/// The outcome of a produce request.
public struct KafkaDeliveryReport: Sendable, Hashable {
    public enum Status: Sendable, Hashable {
        case acknowledged(KafkaAcknowledgedMessage)
        case failure(KafkaError)
    }
    public var status: Status { fatalError() }
    public var messageID: KafkaProducerMessageID { fatalError() }
}

/// A record acknowledged by the broker.
public struct KafkaAcknowledgedMessage: Sendable {
    public var topic: String { fatalError() }
    public var partition: KafkaPartition { fatalError() }
    public var offset: KafkaOffset { fatalError() }
}

// MARK: - Configuration types
// ------------------------------------------------------------------------
// OPEN Q1: Delete legacy `KafkaConsumerConfiguration` / `KafkaProducerConfiguration`
//           types entirely at Beta (~670 lines gone, two public types removed),
//           or keep them as `@deprecated` through 1.x?
//
// OPEN Q2: Naming — current is `KafkaConsumerConfig` (short). Ecosystem
//           convention (`swift-nio`, `swift-service-lifecycle`) is
//           `KafkaConsumerConfiguration` (long). Rename?
//           Depends on Q1 outcome.
// ------------------------------------------------------------------------

/// Configuration for a Kafka consumer.
public struct KafkaConsumerConfig: Sendable {
    public init() { fatalError() }

    // Kafka-client basics
    public var bootstrapServers: [String]?
    public var consumptionStrategy: ConsumptionStrategy?
    public var clientId: String?
    public var pollInterval: Duration
    public var metrics: KafkaConfiguration.ConsumerMetrics

    // Offset management
    public var enableAutoCommit: Bool?
    public var enableAutoOffsetStore: Bool?
    public var autoCommitIntervalMs: Int?
    public var autoOffsetReset: KafkaConfig.AutoOffsetReset?

    // ~100 additional properties covering fetch, socket, security, timeouts,
    // logging, and other librdkafka options. Full list generated from
    // Sources/Kafka/Configuration/KafkaConsumerConfig.swift.gyb.
    // ...

    /// Where the consumer reads records from.
    public struct ConsumptionStrategy: Sendable, Hashable {
        public static func group(id: String, topics: [String]) -> Self { fatalError() }
        public static func partition(
            _ partition: KafkaPartition,
            topic: String,
            offset: KafkaOffset
        ) -> Self { fatalError() }
    }
}

/// Configuration for a Kafka producer.
public struct KafkaProducerConfig: Sendable {
    public init() { fatalError() }

    // Kafka-client basics
    public var bootstrapServers: [String]?
    public var clientId: String?
    public var pollInterval: Duration
    public var flushTimeoutMilliseconds: Int
    public var metrics: KafkaConfiguration.ProducerMetrics

    // Producer specifics
    public var acknowledgements: KafkaConfig.Acknowledgements?
    public var compressionCodec: KafkaConfig.CompressionCodec?
    public var enableIdempotence: Bool?
    public var requestRequiredAcks: Int?
    public var requestTimeoutMs: Int?

    // ~100 additional properties. See Sources/Kafka/Configuration/KafkaProducerConfig.swift.gyb.
    // ...
}

/// Typed option values for Kafka client configuration.
public enum KafkaConfig {
    public struct SecurityProtocol: Hashable, Sendable {
        public static let plaintext: Self
        public static let ssl: Self
        public static let saslPlaintext: Self
        public static let saslSsl: Self
    }

    public struct SASLMechanism: Hashable, Sendable {
        public static let gssapi: Self
        public static let plain: Self
        public static let scramSHA256: Self
        public static let scramSHA512: Self
        public static let oauthbearer: Self
    }

    public struct CompressionCodec: Hashable, Sendable {
        public static let none: Self
        public static let gzip: Self
        public static let snappy: Self
        public static let lz4: Self
        public static let zstd: Self
    }

    public struct AutoOffsetReset: Hashable, Sendable {
        public static let earliest: Self
        public static let latest: Self
        public static let error: Self
    }

    public struct Acknowledgements: Hashable, Sendable {
        public static let none: Self
        public static let leader: Self
        public static let all: Self
    }

    public struct GroupProtocol: Hashable, Sendable {
        public static let classic: Self
        public static let consumer: Self  // KIP-848 — deferred integration, see README
    }

    public struct BrokerAddressFamily: Hashable, Sendable {
        public static let any: Self
        public static let v4: Self
        public static let v6: Self
    }

    public struct Debug: Hashable, Sendable {
        public static let generic: Self
        public static let broker: Self
        public static let topic: Self
        public static let metadata: Self
        // ... other librdkafka debug contexts
    }
}

/// Namespace for observability configuration types.
public enum KafkaConfiguration {

    // OPEN Q6: Metrics API shape.
    //   Current: per-gauge opt-in — assign a `Gauge` to each statistic slot.
    //   PR #244 proposal: `metrics = .enabled(prefix:updateInterval:)` auto-registers
    //   every metric under the prefix. Aligns with swift-metrics idioms.
    //   PR #213 (external): adds per-broker / per-topic partition metrics on
    //   the current per-gauge API. Coordinate.

    public struct ConsumerMetrics: Sendable {
        public var updateInterval: Duration?
        public var totalKafkaBrokerRequests: Gauge?
        public var totalKafkaBrokerMessagesReceived: Gauge?
        public var totalKafkaBrokerBytesReceived: Gauge?
        public var queuedOperation: Gauge?
        // ... additional per-metric gauge slots
    }

    public struct ProducerMetrics: Sendable {
        public var updateInterval: Duration?
        public var totalKafkaBrokerRequests: Gauge?
        public var totalKafkaBrokerMessagesSent: Gauge?
        public var totalKafkaBrokerBytesSent: Gauge?
        public var queuedProducerMessages: Gauge?
        // ...
    }
}

// MARK: - Common value types

/// An error thrown by the Kafka client.
///
// OPEN Q7: Keep the current flat shape (code + rdKafkaCode + isFatal + isRetriable),
//           or introduce typed subcases (`.connection`, `.auth`, `.protocol`, `.timeout`)
//           for catch-friendliness? Issue #127.
//
//           Also: currently declared `@unchecked Sendable` — is there a reason we
//           can't make it plain `Sendable`?
public struct KafkaError: Error, CustomStringConvertible, @unchecked Sendable {
    public struct ErrorCode: Hashable, Sendable, CustomStringConvertible {
        public var description: String { fatalError() }
        // Kafka-level classification: `.connectionClosed`, `.config`, `.topicCreation`, etc.
    }

    public struct RDKafkaCode: Hashable, Sendable, CustomStringConvertible {
        public var description: String { fatalError() }
        public var rawValue: Int32 { fatalError() }
        // Full mapping of librdkafka's `rd_kafka_resp_err_t`.
    }

    public var code: ErrorCode { fatalError() }
    public var rdKafkaCode: RDKafkaCode? { fatalError() }
    public var isFatal: Bool { fatalError() }
    public var isRetriable: Bool { fatalError() }
    public var description: String { fatalError() }
}

/// A header attached to a Kafka message.
public struct KafkaHeader: Sendable, Hashable {
    public var key: String
    public var value: [UInt8]?
    public init(key: String, value: [UInt8]?) { fatalError() }
}

/// A partition identifier within a topic.
public struct KafkaPartition: RawRepresentable, Sendable, Hashable {
    public typealias RawValue = Int32
    public var rawValue: Int32 { fatalError() }
    public init(rawValue: Int32) { fatalError() }

    /// Sentinel meaning "let the broker choose".
    public static let unassigned: KafkaPartition = .init(rawValue: -1)
}

/// An offset within a partition.
public struct KafkaOffset: RawRepresentable, Sendable, Hashable {
    public typealias RawValue = Int64
    public var rawValue: Int64 { fatalError() }
    public init(rawValue: Int64) { fatalError() }

    public static let beginning: KafkaOffset = .init(rawValue: -2)
    public static let end: KafkaOffset = .init(rawValue: -1)
    public static let stored: KafkaOffset = .init(rawValue: -1000)
}

/// A `(topic, partition)` pair.
public struct KafkaTopicPartition: Sendable, Hashable {
    public var topic: String
    public var partition: KafkaPartition
    public init(topic: String, partition: KafkaPartition) { fatalError() }
}

/// A `(topic, partition, offset)` triple.
public struct KafkaTopicPartitionOffset: Sendable, Hashable {
    public var topic: String
    public var partition: KafkaPartition
    public var offset: KafkaOffset
    public init(topic: String, partition: KafkaPartition, offset: KafkaOffset) { fatalError() }
}

/// A Kafka message timestamp.
public struct KafkaTimestampType: Hashable, Sendable, CustomStringConvertible {
    public enum Kind: Sendable, Hashable {
        case createTime
        case logAppendTime
    }
    public var kind: Kind { fatalError() }
    public var timestamp: Int64 { fatalError() }
    public var description: String { fatalError() }
}

// MARK: - Byte protocol / span replacement

// OPEN Q5: Replace this protocol with span types per Franz's comment on #239.
//           Candidate replacements:
//             A. Generic `<Value: Span<UInt8>>` — requires Swift 6.1
//             B. Concrete `RawSpan` for both key and value
//           Impact: rewrites `KafkaProducerMessage`, `KafkaProducer.send`, and
//           `KafkaProducer.sendAndAwait`. Also affects `KafkaFoundationCompat`.
public protocol KafkaContiguousBytes {
    func withUnsafeBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R
}
// Current conformances (shipped): Array<UInt8>, ByteBuffer, String.
// Foundation.Data conformance ships in KafkaFoundationCompat target.

// MARK: - Observability: tracing
// OPEN Q8: `swift-distributed-tracing` integration is the third pillar of the
//           observability DoD in the production-readiness checklist.
//           No public API in this proposal yet. Options:
//             A. Beta-block: add `Tracer` injection to consumer and producer inits.
//             B. Defer to 1.0: ship Beta with metrics + logging only, add tracing
//                before 1.0 tag.
//             C. Defer to 1.1: mark as post-1.0.
// ============================================================================

// MARK: - What's intentionally NOT in this proposal (deferred to post-1.0)
//
//   - `KafkaAdminClient` and all admin ops (create/delete/list/describe topics,
//     `rd_kafka_metadata` wrapper, consumer-group admin, ACL management).
//     Issues #156, #31, #75.
//   - Transactional producer (exactly-once semantics). Issue #78.
//   - Batch consume (`rd_kafka_consume_batch_queue`).
//   - `sendBatch(_:)` producer method.
//   - Custom partitioner.
//   - Certificate reloading (blocked upstream in librdkafka).
//   - KIP-848 consumer group protocol integration (config surface exists as
//     `KafkaConfig.GroupProtocol.consumer` but consumer runtime not wired).
//   - `connectAdditional(brokers:)`. Issue #11.
//
// Please confirm this cut list is acceptable for the Beta / 1.0 line.
// ============================================================================

// Placeholder for the swift-metrics Gauge type this proposal references.
// Not part of the proposal — imported from `Metrics` in real code.
public struct Gauge: Sendable {}
