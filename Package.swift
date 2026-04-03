// swift-tools-version: 6.2

import Foundation
import PackageDescription

var deps: [Package.Dependency] = [
    .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.5.0"),
    .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.23.0"),
    .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0"),
    .package(url: "https://github.com/jpsim/Yams.git", from: "5.1.0"),
    .package(url: "https://github.com/pointfreeco/swift-parsing.git", from: "0.14.1"),
]

var targets: [Target] = [
    .executableTarget(
        name: "PGSchemaEvoCLI",
        dependencies: [
            "PGSchemaEvoCore",
            .product(name: "ArgumentParser", package: "swift-argument-parser"),
        ]
    ),
    .target(
        name: "PGSchemaEvoCore",
        dependencies: [
            "DataMasking",
            .product(name: "PostgresNIO", package: "postgres-nio"),
            .product(name: "Logging", package: "swift-log"),
            .product(name: "Yams", package: "Yams"),
        ]
    ),
    .target(
        name: "DataMasking",
        dependencies: [
            .product(name: "Parsing", package: "swift-parsing"),
            .product(name: "Yams", package: "Yams"),
        ]
    ),
    .testTarget(
        name: "PGSchemaEvoCoreTests",
        dependencies: ["PGSchemaEvoCore"]
    ),
    .testTarget(
        name: "PGSchemaEvoIntegrationTests",
        dependencies: ["PGSchemaEvoCore"]
    ),
    .testTarget(
        name: "DataMaskingTests",
        dependencies: ["DataMasking"]
    ),
]

// Benchmarks require jemalloc-dev. Enable with: ENABLE_BENCHMARKS=1 swift package benchmark
if ProcessInfo.processInfo.environment["ENABLE_BENCHMARKS"] != nil {
    deps.append(.package(url: "https://github.com/ordo-one/package-benchmark.git", from: "1.31.0"))
    targets.append(
        .executableTarget(
            name: "DataMaskingBenchmarks",
            dependencies: [
                "DataMasking",
                .product(name: "Benchmark", package: "package-benchmark"),
            ],
            path: "Benchmarks/DataMaskingBenchmarks",
            plugins: [
                .plugin(name: "BenchmarkPlugin", package: "package-benchmark"),
            ]
        )
    )
}

let package = Package(
    name: "pg-schema-evo",
    platforms: [.macOS(.v14)],
    products: [
        .executable(name: "pg-schema-evo", targets: ["PGSchemaEvoCLI"]),
        .library(name: "DataMasking", targets: ["DataMasking"]),
    ],
    dependencies: deps,
    targets: targets
)
