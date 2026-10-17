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

## Installing

Seven SDKs are published as `0.1.0` from the Loams release tag by
[`.github/workflows/sdk-release.yml`](../.github/workflows/sdk-release.yml), to
npm, PyPI, crates.io, NuGet and the Go proxy; Swift resolves from this
repository's tag and C++ ships as a release tarball. In 0.x an API may change
between minor versions. <!-- verify after publish -->

| Language | Install |
| --- | --- |
| TypeScript | `npm install @loams/client` |
| Python | `pip install loams` |
| Rust | `cargo add loams` |
| Go (1.25+) | `go get loams.dev/go@v0.1.0` |
| C# | `dotnet add package Loams` |
| Swift | `.package(url: "https://github.com/ostrium-labs/loams.git", from: "0.1.0")` |
| C++ | `loams-cpp-0.1.0.tar.gz` on the GitHub release; `find_package(loams CONFIG)` |

Java, Kotlin, Ruby, Dart, Objective-C and PHP are **coming soon**: they build and
pass conformance here, and are not published yet.

## Conformance & Testing

- **[`conformance/`](conformance/)**: Cross-language conformance test runner verifying that all SDKs adhere to the Loams runtime contract (idempotency, consistency tokens, error representations, and pagination).
- **[`fixtures/`](fixtures/)**: Golden test vectors and fixtures consumed by all SDK conformance test suites.
- **[`templates/`](templates/)**: Generator templates used by `scripts/sdk/gen.sh` to produce unified facades and typed clients.
