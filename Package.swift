// swift-tools-version:5.7

import PackageDescription

// JetStream currently depends on Apple-only frameworks (CryptoKit, Combine), so
// it is only built on Apple platforms. On other platforms (e.g. Linux CI) the
// Nats core module — the focus of this client — still builds and tests.
#if canImport(Darwin)
    let jetStreamProducts: [Product] = [
        .library(name: "JetStream", targets: ["JetStream"])
    ]
    let jetStreamTargets: [Target] = [
        .target(
            name: "JetStream",
            dependencies: [
                "Nats",
                .product(name: "Logging", package: "swift-log"),
            ]),
        .testTarget(
            name: "JetStreamTests",
            dependencies: ["Nats", "JetStream", "NatsServer"],
            resources: [
                .process("Integration/Resources")
            ]
        ),
    ]
#else
    let jetStreamProducts: [Product] = []
    let jetStreamTargets: [Target] = []
#endif

let package = Package(
    name: "nats-swift",
    platforms: [
        .macOS(.v13),
        .iOS("17.2"),
    ],
    products: [
        .library(name: "Nats", targets: ["Nats"]),
        .library(name: "NatsServer", targets: ["NatsServer"])
    ] + jetStreamProducts,
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.68.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.2"),
        .package(url: "https://github.com/nats-io/nkeys.swift.git", from: "0.1.2"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.0.0"),
        .package(url: "https://github.com/Jarema/swift-nuid.git", from: "0.2.0"),
    ],
    targets: [
        .target(
            name: "Nats",
            dependencies: [
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "NKeys", package: "nkeys.swift"),
                .product(name: "Nuid", package: "swift-nuid"),
            ]),
        .target(
            name: "NatsServer",
            dependencies: [
                .product(name: "Logging", package: "swift-log"),
            ]),

        .testTarget(
                name: "NatsTests",
                dependencies: ["Nats", "NatsServer"],
                resources: [
                .process("Integration/Resources")
                ]
        ),
        .executableTarget(name: "bench", dependencies: ["Nats"]),
        .executableTarget(name: "Benchmark", dependencies: ["Nats"]),
        .executableTarget(name: "BenchmarkPubSub", dependencies: ["Nats"]),
        .executableTarget(name: "BenchmarkSub", dependencies: ["Nats"]),
        .executableTarget(name: "Example", dependencies: ["Nats"]),
        .executableTarget(
            name: "nats-smoke",
            dependencies: [
                "Nats",
                .product(name: "Logging", package: "swift-log"),
            ]),
    ] + jetStreamTargets
)
