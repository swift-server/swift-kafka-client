//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2023 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// An event received through the ``KafkaProducerEvents`` asynchronous sequence.
///
/// Use the ``kind`` property to determine what type of event occurred,
/// and the ``deliveryReports`` property to access the delivery report results.
public struct KafkaProducerEvent: Sendable, Hashable {
    /// The kind of producer event.
    public struct Kind: Sendable, Hashable, CustomStringConvertible {
        fileprivate enum BackingKind {
            case deliveryReports
        }

        fileprivate let backingKind: BackingKind

        fileprivate init(_ backingKind: BackingKind) {
            self.backingKind = backingKind
        }

        /// A collection of delivery reports received from the Kafka cluster
        /// indicating the status of produced messages.
        public static let deliveryReports = Kind(.deliveryReports)

        public var description: String {
            switch self.backingKind {
            case .deliveryReports:
                return "deliveryReports"
            }
        }
    }

    /// The kind of producer event.
    public let kind: Kind

    /// The delivery reports associated with this event.
    ///
    /// Non-empty when ``kind`` is ``Kind/deliveryReports``.
    public let deliveryReports: [KafkaDeliveryReport]

    private init(kind: Kind, deliveryReports: [KafkaDeliveryReport]) {
        self.kind = kind
        self.deliveryReports = deliveryReports
    }

    /// Create a new ``KafkaProducerEvent`` containing delivery reports.
    ///
    /// - Parameter reports: The delivery reports from the Kafka cluster.
    public static func deliveryReports(_ reports: [KafkaDeliveryReport]) -> KafkaProducerEvent {
        KafkaProducerEvent(kind: .deliveryReports, deliveryReports: reports)
    }

    internal init(_ event: RDKafkaClient.ProducerPollEvent) {
        switch event {
        case .deliveryReport(let results):
            self = .deliveryReports(results)
        case .statistics:
            fatalError("Cannot cast \(event) to KafkaProducerEvent")
        }
    }
}
