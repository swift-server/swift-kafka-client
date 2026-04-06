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

import Crdkafka
import Logging
import NIOConcurrencyHelpers

/// C-compatible rebalance callback passed to `rd_kafka_conf_set_rebalance_cb`.
///
/// Invoked **synchronously** inside `rd_kafka_consumer_poll()` when a consumer
/// group rebalance occurs. The callback must call `rd_kafka_assign` /
/// `rd_kafka_incremental_assign` / `rd_kafka_incremental_unassign` before
/// returning — librdkafka blocks the consumer group protocol until it does.
///
/// The `opaque` parameter is the value set via `rd_kafka_conf_set_opaque`,
/// which we set to an `Unmanaged<RebalanceContext>`.
///
/// - Important: The parameter types must exactly match the C import of
///   `rd_kafka_conf_set_rebalance_cb`'s function pointer typedef.
private func rebalanceCallback(
    _ rk: OpaquePointer?,
    _ err: rd_kafka_resp_err_t,
    _ partitions: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?,
    _ opaque: UnsafeMutableRawPointer?
) {
    guard let opaque else { return }
    let context = Unmanaged<RebalanceContext>.fromOpaque(opaque).takeUnretainedValue()
    context.handleRebalance(kafkaHandle: rk, error: err, partitions: partitions)
}

/// Holds the state needed by the C rebalance callback.
///
/// This object is set as the config opaque pointer via `rd_kafka_conf_set_opaque`
/// and lives for the lifetime of the `KafkaConsumer`. The C callback receives it
/// as the `void* opaque` parameter and calls ``handleRebalance(kafkaHandle:error:partitions:)``.
///
/// Marked `@unchecked Sendable` because all mutable state (`pendingEvents`) is
/// protected by `NIOLockedValueBox`. The class must be a reference type because
/// it is passed to C via `Unmanaged` as the config opaque pointer.
///
/// Thread safety: The rebalance callback fires synchronously inside
/// `rd_kafka_consumer_poll()`, which in our architecture runs on either the
/// cooperative thread pool or a GCD thread. The pending events buffer is
/// protected by `NIOLockedValueBox`. The `logger` is `Sendable`.
final class RebalanceContext: @unchecked Sendable {
    /// Buffer of rebalance events not yet consumed by the Swift event loop.
    private let pendingEvents: NIOLockedValueBox<[ConsumerRebalanceEvent]>
    /// Logger for rebalance operations. Used from the C callback thread.
    private let logger: Logger

    init(logger: Logger) {
        self.pendingEvents = NIOLockedValueBox([])
        self.logger = logger
    }

    /// Called from the C rebalance callback (synchronously inside `rd_kafka_consumer_poll`).
    ///
    /// Performs the assign/unassign operation and buffers the event for the Swift event loop.
    func handleRebalance(
        kafkaHandle rk: OpaquePointer?,
        error err: rd_kafka_resp_err_t,
        partitions: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?
    ) {
        guard let rk else { return }

        let proto = Self.rebalanceProtocol(kafkaHandle: rk)

        switch err {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            let extracted = partitions.map { Self.extractPartitions(from: $0) } ?? []
            Self.performAssign(kafkaHandle: rk, protocol: proto, list: partitions, logger: self.logger)
            self.pendingEvents.withLockedValue {
                $0.append(ConsumerRebalanceEvent(kind: .assign, partitions: extracted))
            }

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            let extracted = partitions.map { Self.extractPartitions(from: $0) } ?? []
            Self.performRevoke(kafkaHandle: rk, protocol: proto, list: partitions, logger: self.logger)
            self.pendingEvents.withLockedValue {
                $0.append(ConsumerRebalanceEvent(kind: .revoke, partitions: extracted))
            }

        default:
            // Unexpected error during rebalance — sync state by unassigning all partitions.
            let errorStr = String(cString: rd_kafka_err2str(err))
            self.logger.warning(
                "Unexpected rebalance error",
                metadata: ["error": "\(errorStr)"]
            )
            let result = rd_kafka_assign(rk, nil)
            if result != RD_KAFKA_RESP_ERR_NO_ERROR {
                self.logger.warning(
                    "Failed to unassign during rebalance error recovery",
                    metadata: ["error": "\(String(cString: rd_kafka_err2str(result)))"]
                )
            }
            self.pendingEvents.withLockedValue {
                $0.append(ConsumerRebalanceEvent(kind: .error(errorStr), partitions: []))
            }
        }
    }

    /// Drain all buffered rebalance events.
    ///
    /// Called by the Swift event loop to retrieve rebalance events that were
    /// buffered by the C callback. Returns an empty array if no events are pending.
    func drainEvents() -> [ConsumerRebalanceEvent] {
        self.pendingEvents.withLockedValue { events in
            let drained = events
            events.removeAll()
            return drained
        }
    }

    /// Inject a rebalance event directly into the buffer for testing.
    /// This simulates what the C rebalance callback does without needing a real `rd_kafka_t` handle.
    ///
    /// - Important: For testing only. Production code should never call this.
    func _testInjectEvent(_ event: ConsumerRebalanceEvent) {
        self.pendingEvents.withLockedValue {
            $0.append(event)
        }
    }

    // MARK: - Internal bridge type

    /// Internal bridge type for rebalance events.
    /// Uses raw tuples to avoid referencing public API types from the bridge layer.
    struct ConsumerRebalanceEvent {
        enum Kind {
            case assign
            case revoke
            /// An unexpected error during rebalance. All partitions have been unassigned.
            case error(String)
        }

        let kind: Kind
        /// Partitions as raw (topic, partition) pairs — converted to public types at Layer 3.
        let partitions: [(topic: String, partition: Int)]
    }

    // MARK: - Private helpers

    private enum RebalanceProtocol {
        case eager
        case cooperative
        case none
    }

    private static func rebalanceProtocol(kafkaHandle rk: OpaquePointer) -> RebalanceProtocol {
        guard let protocolStr = rd_kafka_rebalance_protocol(rk) else {
            return .none
        }
        let proto = String(cString: protocolStr)
        if proto == "COOPERATIVE" {
            return .cooperative
        }
        return .eager
    }

    private static func extractPartitions(
        from listPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>
    ) -> [(topic: String, partition: Int)] {
        let count = Int(listPointer.pointee.cnt)
        var results: [(topic: String, partition: Int)] = []
        results.reserveCapacity(count)
        for i in 0..<count {
            let element = listPointer.pointee.elems[i]
            let topic = String(cString: element.topic)
            let partition = Int(element.partition)
            results.append((topic: topic, partition: partition))
        }
        return results
    }

    private static func performAssign(
        kafkaHandle rk: OpaquePointer,
        protocol proto: RebalanceProtocol,
        list: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?,
        logger: Logger
    ) {
        switch proto {
        case .cooperative:
            if let list {
                let error = rd_kafka_incremental_assign(rk, list)
                if let error {
                    logger.warning(
                        "Incremental assign failed",
                        metadata: ["error": "\(String(cString: rd_kafka_error_string(error)))"]
                    )
                    rd_kafka_error_destroy(error)
                }
            }
        case .eager, .none:
            let result = rd_kafka_assign(rk, list)
            if result != RD_KAFKA_RESP_ERR_NO_ERROR {
                logger.warning(
                    "Assign failed",
                    metadata: ["error": "\(String(cString: rd_kafka_err2str(result)))"]
                )
            }
        }
    }

    private static func performRevoke(
        kafkaHandle rk: OpaquePointer,
        protocol proto: RebalanceProtocol,
        list: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?,
        logger: Logger
    ) {
        switch proto {
        case .cooperative:
            if let list {
                let error = rd_kafka_incremental_unassign(rk, list)
                if let error {
                    logger.warning(
                        "Incremental unassign failed",
                        metadata: ["error": "\(String(cString: rd_kafka_error_string(error)))"]
                    )
                    rd_kafka_error_destroy(error)
                }
            }
        case .eager, .none:
            let result = rd_kafka_assign(rk, nil)
            if result != RD_KAFKA_RESP_ERR_NO_ERROR {
                logger.warning(
                    "Unassign failed",
                    metadata: ["error": "\(String(cString: rd_kafka_err2str(result)))"]
                )
            }
        }
    }
}

// MARK: - RDKafkaConfig extension

extension RDKafkaConfig {
    /// Register the rebalance callback and opaque pointer on a config.
    ///
    /// - Important: `rd_kafka_conf_set_opaque` sets a **single** opaque pointer per config.
    ///   All C callbacks that receive `void* opaque` (rebalance, offset commit, error, etc.)
    ///   share this one slot. Currently only the rebalance callback uses it. If a future
    ///   callback also needs the opaque, introduce a wrapper struct that multiplexes
    ///   (e.g., a context holding both rebalance and other callback contexts).
    ///
    /// - Parameters:
    ///   - configPointer: The `rd_kafka_conf_t` to configure.
    ///   - context: The `RebalanceContext` that will receive callbacks.
    static func setRebalanceCallback(
        configPointer: OpaquePointer,
        context: RebalanceContext
    ) {
        let opaque = Unmanaged.passUnretained(context).toOpaque()
        rd_kafka_conf_set_opaque(configPointer, opaque)
        rd_kafka_conf_set_rebalance_cb(configPointer, rebalanceCallback)
    }
}