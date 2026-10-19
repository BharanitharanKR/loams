// swift-tools-version: 6.0
//
// The Swift SDK's distribution manifest. SwiftPM resolves a package from the
// root of a git repository and versions it by that repository's tags, so this
// file is what makes
//
//     .package(url: "https://github.com/ostrium-labs/loams.git", from: "0.1.0")
//
// work: the release tag `v0.1.0` is the Swift package's version, and
// there is no mirror repository and no second tag to keep in step. It exposes
// the library only, from `sdks/swift/Sources/Loams`. The SDK's own manifest,
// tests and quickstart stay in `sdks/swift/Package.swift`; this file must keep
// the same platforms and target settings, which
// .github/workflows/sdk-release.yml checks by building both.
//
// SwiftPM selects a prerelease only when the requirement names one, so a
// `from: "0.1.0"` requirement does not pick up a prerelease by accident.

import PackageDescription

let package = Package(
    name: "Loams",
    platforms: [
        .iOS(.v15),
        .macOS(.v12),
    ],
    products: [
        .library(name: "Loams", targets: ["Loams"]),
    ],
    targets: [
        .target(
            name: "Loams",
            path: "sdks/swift/Sources/Loams"
        ),
    ],
    swiftLanguageVersions: [.v6]
)
