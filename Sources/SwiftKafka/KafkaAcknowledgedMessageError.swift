//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-gsoc open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-gsoc project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-gsoc project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// Error caused by the Kafka cluster when trying to process a message produced by ``KafkaProducer``.
public struct KafkaAcknowledgedMessageError: Error {
    /// A raw value representing the error code.
    public var rawValue: Int32
    /// A string describing the error.
    public var description: String?
    /// Identifier of the message that caused the error.
    public var messageID: UInt
}
