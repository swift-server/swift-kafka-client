// swift-tools-version: 5.6
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

import PackageDescription

let package = Package(
    name: "swift-kafka-gsoc",
    products: [
        .library(
            name: "SwiftKafka",
            targets: ["SwiftKafka"]
        ),
        .executable(
            name: "ErrorPrinter",
            targets: ["ErrorPrinter"]
        ),
    ],
    dependencies: [],
    targets: [
        .target(
            name: "SwiftKafka",
            dependencies: []
        ),
        .executableTarget(
            name: "ErrorPrinter",
            dependencies: ["Crdkafka"]
        ),
        .systemLibrary(
            name: "Crdkafka",
            pkgConfig: "rdkafka",
            providers: [
                .brew(["librdkafka"]),
                .apt(["librdkafka-dev"]),
            ]
        ),
        .testTarget(
            name: "SwiftKafkaTests",
            dependencies: ["SwiftKafka"]
        ),
    ]
)
