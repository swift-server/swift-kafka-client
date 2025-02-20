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
    "./librdkafka/src/README.lz4.md",
    "./librdkafka/src/generate_proto.sh",
    "./librdkafka/src/librdkafka_cgrp_synch.png",
    "./librdkafka/src/opentelemetry/metrics.options",
    "./librdkafka/src/rdkafka_sasl_win32.c",
    "./librdkafka/src/rdwin32.h",
    "./librdkafka/src/statistics_schema.json",
    "./librdkafka/src/win32_config.h",
    // Remove dependency on cURL. Disabling `ENABLE_CURL` and `WITH_CURL` does
    // not appear to prevent processing of the below files, so we have to exclude
    // them explicitly.
    "./librdkafka/src/rdkafka_sasl_oauthbearer.c",
    "./librdkafka/src/rdkafka_sasl_oauthbearer_oidc.c",
    "./librdkafka/src/rdhttp.c",
]

let package = Package(
    name: "swift-kafka-client",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
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
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.1.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-metrics", from: "2.4.1"),
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
                .define("_GNU_SOURCE", to: "1"),  // Fix build error for Swift 5.9 onwards
            ],
            linkerSettings: [
                .linkedLibrary("sasl2"),
                .linkedLibrary("z"),  // zlib
            ]
        ),
        .target(
            name: "Kafka",
            dependencies: [
                "Crdkafka",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
            ]
        ),
        .target(
            name: "KafkaFoundationCompat",
            dependencies: [
                "Kafka"
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
            dependencies: [
                "Kafka",
                .product(name: "MetricsTestKit", package: "swift-metrics"),
            ]
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: ["Kafka"]
        ),
    ]
)

for target in package.targets {
    switch target.type {
    case .regular, .test, .executable:
        var settings = target.swiftSettings ?? []
        settings.append(.enableExperimentalFeature("StrictConcurrency=complete"))
        target.swiftSettings = settings
    case .macro, .plugin, .system, .binary:
        break // These targets do not support settings
    @unknown default:
        fatalError("Update to handle new target type \(target.type)")
    }
}

// ---    STANDARD CROSS-REPO SETTINGS DO NOT EDIT   --- //
for target in package.targets {
    if target.type != .plugin && target.type != .system {
        var settings = target.swiftSettings ?? []
        // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0444-member-import-visibility.md
        settings.append(.enableUpcomingFeature("MemberImportVisibility"))
        target.swiftSettings = settings
    }
}
// --- END: STANDARD CROSS-REPO SETTINGS DO NOT EDIT --- //
