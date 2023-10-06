// swift-tools-version: 5.9
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
    name: "swift-kafka-client",
    platforms: [
        .macOS(.v14),
        .iOS(.v17),
        .watchOS(.v9),
        .tvOS(.v16),
    ],
    products: [
        .library(
            name: "Kafka",
            targets: ["Kafka"]
        ),
        .library(
            name: "KafkaFoundationCompat",
            targets: ["KafkaFoundationCompat"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.55.0"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.0.0-alpha.1"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        // The zstd Swift package produces warnings that we cannot resolve:
        // https://github.com/facebook/zstd/issues/3328
        .package(url: "https://github.com/facebook/zstd.git", from: "1.5.0"),
        .package(url: "https://github.com/swift-extras/swift-extras-json.git", .upToNextMajor(from: "0.6.0")),
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
                .linkedLibrary("curl", .when(platforms: [.macOS, .linux])),
                .linkedLibrary("sasl2", .when(platforms: [.macOS, .linux])),
                .linkedLibrary("z"), // zlib
            ]
        ),
        .target(
            name: "Kafka",
            dependencies: [
                "Crdkafka",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "ExtrasJSON", package: "swift-extras-json"),
            ]
        ),
        .target(
            name: "KafkaFoundationCompat",
            dependencies: [
                "Kafka",
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
            name: "KafkaTests",
            dependencies: ["Kafka"]
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: ["Kafka"]
        ),
    ]
)
