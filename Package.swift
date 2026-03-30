// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "pg-schema-evo",
    platforms: [.macOS(.v14)],
    products: [
        .executable(name: "pg-schema-evo", targets: ["PGSchemaEvoCLI"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.5.0"),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.23.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0"),
        .package(url: "https://github.com/jpsim/Yams.git", from: "5.1.0"),
    ],
    targets: [
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
                .product(name: "PostgresNIO", package: "postgres-nio"),
                .product(name: "Logging", package: "swift-log"),
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
    ]
)
