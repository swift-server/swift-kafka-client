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

@testable import SwiftKafka
import XCTest

final class KafkaTopicConfigTests: XCTestCase {
    func testSettingCorrectValueWorks() throws {
        var config = KafkaTopicConfig()

        try config.set("gzip", forKey: "compression.type")

        XCTAssertEqual("gzip", config.value(forKey: "compression.type"))
    }

    func testSettingWrongKeyFails() {
        var config = KafkaTopicConfig()

        XCTAssertThrowsError(try config.set("gzip", forKey: "not.a.valid.key"))
    }

    func testSettingWrongValueFails() {
        var config = KafkaTopicConfig()

        XCTAssertThrowsError(try config.set("not_a_compression_type", forKey: "compression.type"))
    }

    func testGetterHasNoSideEffects() {
        let configA = KafkaTopicConfig()
        let configB = configA

        _ = configA.value(forKey: "compression.type")
        _ = configB.value(forKey: "compression.type")

        XCTAssertTrue(configA == configB)
    }

    func testCopyOnWriteWorks() throws {
        var configA = KafkaTopicConfig()
        let configB = configA
        let configC = configA

        // Check if all configs have the default value set
        [configA, configB, configC].forEach {
            XCTAssertEqual("inherit", $0.value(forKey: "compression.type"))
        }

        try configA.set("gzip", forKey: "compression.type")

        XCTAssertEqual("gzip", configA.value(forKey: "compression.type"))
        XCTAssertEqual("inherit", configB.value(forKey: "compression.type"))
        XCTAssertEqual("inherit", configC.value(forKey: "compression.type"))
        XCTAssertNotEqual(configA, configB)
        XCTAssertNotEqual(configA, configC)
        XCTAssertEqual(configB, configC)
    }
}
