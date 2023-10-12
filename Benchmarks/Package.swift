// swift-tools-version: 5.7

import PackageDescription

let package = Package(
    name: "benchmarks",
    platforms: [
        .macOS(.v13),
    ],
    dependencies: [
        .package(path: "../"),
        .package(url: "https://github.com/ordo-one/package-benchmark.git", from: "1.11.1"),
    ],
    targets: [
        .executableTarget(
            name: "SwiftKafkaBenchmarks",
            dependencies: [
                .product(name: "Benchmark", package: "package-benchmark"),
                .product(name: "Kafka", package: "swift-kafka-client"),
            ],
            path: "Benchmarks/SwiftKafkaBenchmarks",
            plugins: [
                .plugin(name: "BenchmarkPlugin", package: "package-benchmark")
            ]
        ),
    ]
)
