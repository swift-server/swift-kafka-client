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

// TODO: create install script in Libraries

let arch: String
#if arch(arm64)
arch = "arm64"
#elseif arch(x86_64)
arch = "amd64"
#else
fatalError("This package only supports arm64 and x86_64 architectures at the moment")
#endif

let osSpecifier: String
#if os(macOS)
osSpecifier = "darwin"
#elseif os(Linux)
osSpecifier = "glibc_linux"
#else
fatalError("This package only supports macOS and Linux at the moment")
#endif

var rdkafkaLinkerSettings: [PackageDescription.LinkerSetting] = [
    .unsafeFlags(["-LLibraries"]),
    .linkedLibrary("rdkafka_\(osSpecifier)_\(arch)"),
    // SASL2 comes with macOS
    // See: https://github.com/confluentinc/librdkafka/blob/master/packaging/nuget/staticpackage.py
    .linkedLibrary("sasl2", .when(platforms: [.macOS]))
]

let package = Package(
    name: "swift-kafka-gsoc",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .watchOS(.v6),
        .tvOS(.v13),
    ],
    products: [
        .library(
            name: "SwiftKafka",
            targets: ["SwiftKafka"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.43.1"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    ],
    targets: [
        .target(
            name: "SwiftKafka",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
                .target(name: "Crdkafka")
            ]
        ),
        .target(
            name: "Crdkafka",
            linkerSettings: rdkafkaLinkerSettings
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
