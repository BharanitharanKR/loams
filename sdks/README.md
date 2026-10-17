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

Every SDK is published, as a preview, from each Loams release tag by
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
| Ruby | `gem install loams --pre` (`0.1.0.pre.2`) |
| PHP | `composer require loams/loams:0.1.0-alpha.2` (Composer has no `preview`; see [`php/`](php/)) |
| Dart | `dart pub add loams:0.1.0-preview.2` |
| Java | `dev.loams:loams:0.1.0-preview.2` (Maven Central) |
| Kotlin | `dev.loams:loams-kotlin:0.1.0-preview.2` (Maven Central) |
| Objective-C | `pod 'Loams', '0.1.0-preview.2'` (module `LoamsObjC`) |

Ruby, PHP, Dart, Java, Kotlin and Objective-C join from the first release tag
cut after their packaging landed; until a registry shows the version, build them
from this directory. Which registries are live is in
[`docs/release/publishing.md`](../docs/release/publishing.md).

## Conformance & Testing

- **[`conformance/`](conformance/)**: Cross-language conformance test runner verifying that all SDKs adhere to the Loams runtime contract (idempotency, consistency tokens, error representations, and pagination).
- **[`fixtures/`](fixtures/)**: Golden test vectors and fixtures consumed by all SDK conformance test suites.
- **[`templates/`](templates/)**: Generator templates used by `scripts/sdk/gen.sh` to produce unified facades and typed clients.
