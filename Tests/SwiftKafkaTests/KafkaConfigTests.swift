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

private final class MockClass {}

final class KafkaConfigTests: XCTestCase {
    func testSettingCorrectValueWorks() throws {
        var config = KafkaConfig()

        try config.set("ssl", forKey: "security.protocol")

        XCTAssertEqual("ssl", config.value(forKey: "security.protocol"))
    }

    func testSettingWrongKeyFails() {
        var config = KafkaConfig()

        XCTAssertThrowsError(try config.set("ssl", forKey: "not.a.valid.key"))
    }

    func testSettingWrongValueFails() {
        var config = KafkaConfig()

        XCTAssertThrowsError(try config.set("not_a_protocol", forKey: "security.protocol"))
    }

    func testGetterHasNoSideEffects() {
        let configA = KafkaConfig()
        let configB = configA

        _ = configA.value(forKey: "security.protocol")
        _ = configB.value(forKey: "security.protocol")

        XCTAssertTrue(configA == configB)
    }

    func testSetCopyOnWrite() throws {
        var configA = KafkaConfig()
        let configB = configA
        let configC = configA

        // Check if all configs have the default value set
        [configA, configB, configC].forEach {
            XCTAssertEqual("plaintext", $0.value(forKey: "security.protocol"))
        }

        try configA.set("ssl", forKey: "security.protocol")

        XCTAssertEqual("ssl", configA.value(forKey: "security.protocol"))
        XCTAssertEqual("plaintext", configB.value(forKey: "security.protocol"))
        XCTAssertEqual("plaintext", configC.value(forKey: "security.protocol"))
        XCTAssertNotEqual(configA, configB)
        XCTAssertNotEqual(configA, configC)
        XCTAssertEqual(configB, configC)
    }

    func testMessageCallbackCopyOnWrite() throws {
        var configA = KafkaConfig()
        let configB = configA
        let configC = configA

        configA.setDeliveryReportCallback { (_, _, _: AnyObject?) in }

        XCTAssertNotEqual(configA, configB)
        XCTAssertNotEqual(configA, configC)
        XCTAssertEqual(configB, configC)
    }

    func testConfigOpaquePropertyRetainedAfterDuplication() throws {
        var opaque: MockClass! = MockClass()
        weak var opaqueCopy = opaque

        var configA: KafkaConfig! = KafkaConfig()
        configA.setDeliveryReportCallback(opaque: opaque, callback: { _, _, _ in })
        opaque = nil

        // Increase reference count of internal class
        var configB: KafkaConfig! = configA
        // Triggers duplication of the internal config object
        try configB.set("ssl", forKey: "security.protocol")
        configA = nil

        XCTAssertNotNil(opaqueCopy)
    }
}
