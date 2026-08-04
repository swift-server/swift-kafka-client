//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging

/// A single asynchronous sequence delivering *all* events of one consumer — fetched
/// messages, rebalances, partition EOF and errors — so that the consumer can be driven
/// from a single loop:
///
/// ```swift
/// let stream = try KafkaConsumerStream(configuration: configuration, logger: logger)
/// try stream.subscribe(["topic"])
/// for await event in stream {
///     switch event { ... }
/// }
/// ```
///
/// Iterating the stream *is* the poll loop; there is no separate service task to run.
///
/// The element is ``KafkaConsumerEvent``, *not* a message: records arrive batched inside
/// ``KafkaConsumerEvent/fetch(_:)`` (see ``KafkaFetch/withMessages(_:)``) alongside
/// rebalance, EOF and error events. This is what distinguishes the type from
/// ``KafkaConsumer``, which splits the same information across ``KafkaConsumer/messages``
/// and a separate ``KafkaConsumerEvents`` sequence and needs its `run()` method serviced.
///
/// - Important: The stream subscribes to `.rebalance`, which turns off librdkafka's
///   automatic partition assignment. The caller is therefore responsible for
///   (un)assigning partitions in response to every ``KafkaConsumerEvent/rebalance(_:)``
///   event, otherwise the consumer is never assigned any partitions and consumes nothing.
public struct KafkaConsumerStream: AsyncSequence, Sendable {
    public typealias Element = KafkaConsumerEvent

    /// Internal: used by `KafkaTransaction` to reach the consumer's kafka handle when
    /// committing consumed offsets transactionally (`send(offsets:forConsumer:)`).
    let client: RDKafkaClient
    private let pollInterval: Duration
    private let metrics: KafkaConfiguration.ConsumerMetrics
    private let healthStatusEnabled: Bool

    public struct AsyncIterator: AsyncIteratorProtocol {
        private let client: RDKafkaClient
        private let configPollInterval: Duration
        private var events = [RDKafkaClient.KafkaEvent]()
        private var idx = 0
        private var pollInterval: Duration
        private let metrics: KafkaConfiguration.ConsumerMetrics
        private let healthStatusEnabled: Bool

        init(
            _ client: RDKafkaClient,
            _ configPollInterval: Duration,
            _ metrics: KafkaConfiguration.ConsumerMetrics,
            _ healthStatusEnabled: Bool
        ) {
            self.client = client
            self.configPollInterval = configPollInterval
            self.pollInterval = configPollInterval
            self.metrics = metrics
            self.healthStatusEnabled = healthStatusEnabled
        }

        public mutating func next() async -> KafkaConsumerEvent? {
            while true {
                // Honor structured-concurrency cancellation: end the sequence so callers
                // iterating in a cancelled task (or a cancelled task group) stop cleanly.
                if Task.isCancelled {
                    return nil
                }
                if idx == 0 {
                    let shouldSleep = client.eventPoll(events: &events)
                    if shouldSleep {
                        pollInterval = Swift.min(configPollInterval, pollInterval * 2)
                        let clock = ContinuousClock()
                        try? await clock.sleep(until: clock.now.advanced(by: pollInterval))
                    } else {
                        pollInterval = Swift.max(pollInterval / 3, .microseconds(1))
                        await Task.yield()
                    }

                    // The poll may not have produced any events; go back and poll again.
                    if events.isEmpty {
                        continue
                    }
                }

                let event = events[idx]
                idx += 1

                if idx == events.count {
                    events.removeAll()
                    idx = 0
                }

                switch event {
                case let .fetch(ptr):
                    return .fetch(.init(ptr))

                case let .partitionEOF(topicPartition):
                    return .partitionEOF(topicPartition)

                case .deliveryReport:
                    break

                case let .statistics(statistics):
                    metrics.update(with: statistics)
                    if healthStatusEnabled {
                        return .healthStatus(statistics.consumerHealthStatus)
                    }

                case let .rebalance(action):
                    return .rebalance(action)

                case let .error(error):
                    return .error(error)
                }
            }
        }
    }

    private init(
        _ client: RDKafkaClient,
        _ pollInterval: Duration,
        _ metrics: KafkaConfiguration.ConsumerMetrics,
        _ healthStatusEnabled: Bool
    ) {
        self.client = client
        self.pollInterval = pollInterval
        self.metrics = metrics
        self.healthStatusEnabled = healthStatusEnabled
    }

    public init(configuration: KafkaConsumerConfiguration, logger: Logger) throws {
        // `.rebalance` makes librdkafka deliver assign/revoke events on the queue instead
        // of assigning partitions automatically; the caller must then (un)assign partitions
        // itself in response to each `.rebalance` event (see the `assign`/`incrementalAssign`
        // helpers below).
        var subscribedEvents: [RDKafkaEvent] = [.log, .rebalance]

        // Only listen to offset commit events when autoCommit is false
        if configuration.isAutoCommitEnabled == false {
            subscribedEvents.append(.offsetCommit)
        }

        if configuration.metrics.enabled || configuration.healthStatusInterval != nil {
            subscribedEvents.append(.statistics)
        }

        let client = try RDKafkaClient.makeClient(
            type: .consumer,
            configDictionary: configuration.dictionary,
            events: subscribedEvents,
            logger: logger,
            singleQueue: true
        )

        self.init(
            client,
            configuration.pollInterval,
            configuration.metrics,
            configuration.healthStatusInterval != nil
        )
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(client, pollInterval, metrics, healthStatusEnabled)
    }

    public func subscribe(_ topics: [String]) throws {
        let subscription = RDKafkaTopicPartitionList()
        for topic in topics {
            subscription.add(topic: topic, partition: KafkaPartition.unassigned)
        }
        try client.subscribe(topicPartitionList: subscription)
    }

    // MARK: - Rebalance handling
    //
    // The `.rebalance` event turns off librdkafka's automatic partition assignment
    // (see `rd_kafka_conf_set_rebalance_cb` in rdkafka.h). The caller is therefore
    // responsible for (un)assigning partitions in response to each `.rebalance` event
    // received from the sequence, otherwise the consumer is never assigned any
    // partitions and consumes nothing.
    //
    // For eager assignors (`range`, `roundrobin`) use ``assign(_:)`` on
    // `.assign` and ``unassignAll()`` on `.revoke`. For cooperative assignors
    // (`cooperative-sticky`) use ``incrementalAssign(_:)`` / ``incrementalUnassign(_:)``.

    /// Assign the full partition set to consume (eager assignors).
    public func assign(_ topics: KafkaTopicList) async throws {
        try await client.assign(topicPartitionList: topics.list)
    }

    /// Clear the current assignment (eager `.revoke`, or to sync state on error).
    public func unassignAll() async throws {
        try await client.assign(topicPartitionList: nil)
    }

    /// Incrementally add partitions to the current assignment (cooperative assignors).
    public func incrementalAssign(_ topics: KafkaTopicList) async throws {
        try await client.incrementalAssign(topicPartitionList: topics.list)
    }

    /// Incrementally remove partitions from the current assignment (cooperative assignors).
    public func incrementalUnassign(_ topics: KafkaTopicList) async throws {
        try await client.incrementalUnassign(topicPartitionList: topics.list)
    }
}
