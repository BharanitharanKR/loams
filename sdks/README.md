![Loams — Your data. Your bucket.](../docs/assets/loams-banner.svg)

# Loams SDKs

This directory contains Loams language implementations, code generation templates, and conformance fixtures. Packages are pre-release and not a stable published SDK family. Check each language’s README and the conformance runner before relying on a runtime or transport.

## Language implementations

| Language | Path | Key Packages / Modules |
| --- | --- | --- |
| **TypeScript** | [`typescript/`](typescript/) | `@loams/client`, `@loams/live` |
| **Python** | [`python/`](python/) | `loams` |
| **Rust** | [`rust/`](rust/) | `loams-sdk` |
| **Go** | [`go/`](go/) | `github.com/ostrium-labs/loams/sdks/go` |
| **C# (.NET)** | [`csharp/`](csharp/) | `Loams` |
| **Java** | [`java/`](java/) | `dev.loams` |
| **Kotlin** | [`kotlin/`](kotlin/) | `dev.loams.kotlin` |
| **Swift** | [`swift/`](swift/) | `Loams` |
| **Ruby** | [`ruby/`](ruby/) | `loams` |
| **C++** | [`cpp/`](cpp/) | `loams` |
| **Dart** | [`dart/`](dart/) | Implementation and runtime fixtures |
| **Objective-C** | [`objc/`](objc/) | Implementation and runtime fixtures |
| **PHP** | [`php/`](php/) | Implementation and runtime fixtures |

## Installing (preview)

Seven SDKs are published, as previews, from each Loams release tag by
[`.github/workflows/sdk-release.yml`](../.github/workflows/sdk-release.yml). A
preview may change between releases: pin the exact version.

| Language | Install |
| --- | --- |
| TypeScript | `npm install @loams/client@preview` |
| Python | `pip install --pre loams` |
| Rust | `cargo add loams@=0.1.0-preview.2` |
| Go (1.25+) | `go get loams.dev/go@v0.1.0-preview.2` |
| C# | `dotnet add package Loams --prerelease` |
| Swift | `.package(url: "https://github.com/ostrium-labs/loams.git", exact: "0.1.0-preview.2")` |
| C++ | `loams-cpp-<version>.tar.gz` on the GitHub release; `find_package(loams CONFIG)` |

Java, Kotlin, Ruby, Dart, Objective-C and PHP are **coming soon**: they build and
pass conformance here, and are not published yet.

## Conformance & Testing

- **[`conformance/`](conformance/)**: Cross-language conformance test runner verifying that all SDKs adhere to the Loams runtime contract (idempotency, consistency tokens, error representations, and pagination).
- **[`fixtures/`](fixtures/)**: Golden test vectors and fixtures consumed by all SDK conformance test suites.
- **[`templates/`](templates/)**: Generator templates used by `scripts/sdk/gen.sh` to produce unified facades and typed clients.
