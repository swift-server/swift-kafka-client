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

// A minimal executable that forces a full link of the Kafka module. It exists so the static
// Linux (musl) SDK build can be verified end-to-end: the test bundle cannot be built for musl
// (swift-testing isn't in the SDK), but `swift build --swift-sdk <musl>` of this executable
// proves librdkafka resolves against CNIOBoringSSL with no undefined symbols.
import Kafka

var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
_ = try? KafkaProducer.makeProducer(config: config)
print("link check: ok")
