// swift-tools-version: 5.7
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

let rdkafkaExclude = [
    "./librdkafka/src/CMakeLists.txt",
    "./librdkafka/src/Makefile",
    "./librdkafka/src/generate_proto.sh",
    "./librdkafka/src/librdkafka_cgrp_synch.png",
    "./librdkafka/src/statistics_schema.json",
    "./librdkafka/src/rdkafka_sasl_win32.c",
    "./librdkafka/src/rdwin32.h",
    "./librdkafka/src/win32_config.h",
]

let package = Package(
    name: "swift-kafka-gsoc",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
        .watchOS(.v9),
        .tvOS(.v16),
    ],
    products: [
        .library(
            name: "SwiftKafka",
            targets: ["SwiftKafka"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.43.1"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.0.0-alpha.1"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        // The zstd Swift package produces warnings that we cannot resolve:
        // https://github.com/facebook/zstd/issues/3328
        .package(url: "https://github.com/facebook/zstd.git", from: "1.5.0"),
    ],
    targets: [
        .target(
            name: "Crdkafka",
            dependencies: [
                "COpenSSL",
                .product(name: "libzstd", package: "zstd"),
            ],
            exclude: rdkafkaExclude,
            sources: ["./librdkafka/src/"],
            publicHeadersPath: "./include",
            cSettings: [
                // dummy folder, because config.h is included as "../config.h" in librdkafka
                .headerSearchPath("./custom/config/dummy"),
                .headerSearchPath("./librdkafka/src"),
            ],
            linkerSettings: [
                .linkedLibrary("curl"),
                .linkedLibrary("sasl2"),
                .linkedLibrary("z"), // zlib
            ]
        ),
        .target(
            name: "SwiftKafka",
            dependencies: [
                "Crdkafka",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
                .product(name: "Logging", package: "swift-log"),
            ]
        ),
        .systemLibrary(
            name: "COpenSSL",
            pkgConfig: "openssl",
            providers: [
                .brew(["libressl"]),
                .apt(["libssl-dev"]),
            ]
        ),
        .testTarget(
            name: "SwiftKafkaTests",
            dependencies: ["SwiftKafka"]
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: ["SwiftKafka"]
        ),
    ]
)
