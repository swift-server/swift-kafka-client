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

import Logging
import ServiceLifecycle
import Testing

@testable import Kafka

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Tests for the `RawSpan`-taking `send` and `sendAndAwait` overloads, which let callers hand over bytes
/// held in another type (here `Foundation.Data`) without a conversion to `[UInt8]`.
@Suite struct KafkaProducerSpanTests {
    var config: KafkaProducerConfig

    init() throws {
        self.config = KafkaProducerConfig()
        self.config.useMockBroker()
        self.config.brokerAddressFamily = .v4
    }

    /// Runs `body` with a producer driven by a service group, then shuts it down.
    private func withRunningProducer(
        _ body: @escaping @Sendable (KafkaProducer, KafkaProducer.Events) async throws -> Void
    ) async throws {
        let (producer, events) = try KafkaProducer.makeProducer(config: self.config)
        let serviceGroup = ServiceGroup(
            configuration: ServiceGroupConfiguration(services: [producer], logger: .kafkaTest)
        )
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await serviceGroup.run() }
            try await body(producer, events)
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    private static func firstAcknowledgedMessage(
        from events: KafkaProducer.Events
    ) async -> KafkaProducer.DeliveryReport? {
        for await event in events {
            if case .deliveryReports(let reports) = event, let report = reports.first {
                return report
            }
        }
        return nil
    }

    @Test func sendWithSpansDeliversKeyValueAndHeaders() async throws {
        try await self.withRunningProducer { producer, events in
            let topic: KafkaTopic = "span-topic"
            let key = Data("key".utf8)
            let value = Data("Hello, spans!".utf8)
            let headers = [KafkaHeader(key: "some", value: Array("header".utf8))]

            let messageID = try producer.send(topic: topic, headers: headers, key: key.bytes, value: value.bytes)

            let report = try #require(await Self.firstAcknowledgedMessage(from: events))
            #expect(report.id == messageID)
            guard case .acknowledged(let acknowledged) = report.status else {
                Issue.record("Expected an acknowledged delivery report, got \(report.status)")
                return
            }
            #expect(acknowledged.topic == topic)
            #expect(acknowledged.key == Array(key))
            #expect(acknowledged.value == Array(value))
            #expect(acknowledged.headers == headers)
        }
    }

    @Test func sendWithSpanAndNoKeyDeliversNilKey() async throws {
        try await self.withRunningProducer { producer, events in
            let value = Data("no key".utf8)

            let messageID = try producer.send(topic: "span-topic", value: value.bytes)

            let report = try #require(await Self.firstAcknowledgedMessage(from: events))
            #expect(report.id == messageID)
            guard case .acknowledged(let acknowledged) = report.status else {
                Issue.record("Expected an acknowledged delivery report, got \(report.status)")
                return
            }
            #expect(acknowledged.key == nil)
            #expect(acknowledged.value == Array(value))
        }
    }

    @Test func sendWithEmptySpanValueSucceeds() async throws {
        try await self.withRunningProducer { producer, events in
            let empty = Data()

            let messageID = try producer.send(topic: "span-topic", value: empty.bytes)

            let report = try #require(await Self.firstAcknowledgedMessage(from: events))
            #expect(report.id == messageID)
            guard case .acknowledged(let acknowledged) = report.status else {
                Issue.record("Expected an acknowledged delivery report, got \(report.status)")
                return
            }
            #expect(acknowledged.value.isEmpty)
        }
    }

    @Test func sendAndAwaitWithSpansReturnsAcknowledgedReport() async throws {
        try await self.withRunningProducer { producer, _ in
            let topic: KafkaTopic = "span-topic"
            let key = Data("await-key".utf8)
            let value = Data("await-value".utf8)

            let report = try await producer.sendAndAwait(topic: topic, key: key.bytes, value: value.bytes)

            guard case .acknowledged(let acknowledged) = report.status else {
                Issue.record("Expected an acknowledged delivery report, got \(report.status)")
                return
            }
            #expect(acknowledged.topic == topic)
            #expect(acknowledged.key == Array(key))
            #expect(acknowledged.value == Array(value))
        }
    }
}
