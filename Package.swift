// swift-tools-version:6.2.3
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

// Route librdkafka's OpenSSL usage onto swift-nio-ssl's vendored BoringSSL (CNIOBoringSSL) on all
// platforms via custom/openssl_shim, so TLS/crypto is self-contained (no system OpenSSL) and the
// package builds for the fully-static Linux (musl) SDK. We depend on the public `NIOSSL` product,
// which links CNIOBoringSSL transitively, and provide small shims (custom/musl_compat) for the two
// functions librdkafka needs that BoringSSL lacks.
//
// GSSAPI/Kerberos (Cyrus SASL) is opt-in and OFF by default: it needs the system libsasl2 library,
// which isn't available in a fully-static build. Set SWIFT_KAFKA_ENABLE_GSSAPI=1 at build time to
// compile the Cyrus SASL provider and link libsasl2 (requires libsasl2 development headers on the
// host). SCRAM, PLAIN, and TLS/mTLS remain available regardless.
let enableGSSAPI = ["1", "true", "yes"].contains(
    (Context.environment["SWIFT_KAFKA_ENABLE_GSSAPI"] ?? "").lowercased()
)

var rdkafkaExclude = [
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
    "./librdkafka/src/rdkafka_sasl_oauthbearer.c",
    "./librdkafka/src/rdkafka_sasl_oauthbearer_oidc.c",
    "./librdkafka/src/rdhttp.c",
]
if !enableGSSAPI {
    // Cyrus SASL (GSSAPI/Kerberos) unconditionally includes <sasl/sasl.h> and needs libsasl2;
    // exclude it unless GSSAPI is explicitly enabled.
    rdkafkaExclude.append("./librdkafka/src/rdkafka_sasl_cyrus.c")
}

let package = Package(
    name: "swift-kafka-client",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
        .watchOS(.v11),
        .tvOS(.v18),
    ],
    products: [
        .library(name: "Kafka", targets: ["Kafka"]),
        .library(name: "KafkaFoundationCompat", targets: ["KafkaFoundationCompat"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.55.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.29.0"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.1.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.14.0"),
        .package(url: "https://github.com/apple/swift-metrics", from: "2.4.1"),
        .package(url: "https://github.com/facebook/zstd.git", from: "1.5.0"),
    ],
    targets: [
        .target(
            name: "Crdkafka",
            dependencies: [
                // NIOSSL pulls CNIOBoringSSL (vendored BoringSSL, incl. TLS) into the link transitively.
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "libzstd", package: "zstd"),
            ],
            exclude: rdkafkaExclude,
            sources: [
                "./librdkafka/src/",
                "./custom/musl_compat",  // BoringSSL shims
            ],
            publicHeadersPath: "./include",
            cSettings: [
                // openssl_shim redirects <openssl/*.h> onto CNIOBoringSSL; must precede any system openssl.
                .headerSearchPath("./custom/openssl_shim"),
                // dummy folder, because config.h is included as "../config.h" in librdkafka
                .headerSearchPath("./custom/config/dummy"),
                .headerSearchPath("./librdkafka/src"),
            ] + (enableGSSAPI ? [.define("SWIFT_KAFKA_ENABLE_GSSAPI")] : []),
            linkerSettings: [
                .linkedLibrary("z")  // zlib; ssl/crypto come from CNIOBoringSSL
            ] + (enableGSSAPI ? [.linkedLibrary("sasl2")] : [])
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
            dependencies: ["Kafka"]
        ),
        // Minimal executable that forces a final link of librdkafka against CNIOBoringSSL. It lets
        // the static Linux (musl) SDK build be verified with `swift build`, since the test bundle
        // can't be built for musl (swift-testing isn't shipped in that SDK).
        .executableTarget(
            name: "StaticLinkCheck",
            dependencies: ["Kafka"]
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
        break
    @unknown default:
        fatalError("Update to handle new target type \(target.type)")
    }
}

// ---    STANDARD CROSS-REPO SETTINGS DO NOT EDIT   --- //
for target in package.targets {
    switch target.type {
    case .regular, .test, .executable:
        var settings = target.swiftSettings ?? []
        settings.append(.enableUpcomingFeature("MemberImportVisibility"))
        target.swiftSettings = settings
    case .macro, .plugin, .system, .binary:
        ()
    @unknown default:
        ()
    }
}
// --- END: STANDARD CROSS-REPO SETTINGS DO NOT EDIT --- //
