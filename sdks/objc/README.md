# Loams Objective-C SDK (preview)

> **Pre-release (preview).** This SDK is published as a preview of Loams. Its API
> can change between previews without notice: pin the exact version, and expect
> breaking changes until 1.0. Report problems at
> <https://github.com/ostrium-labs/loams/issues>.

## Install

CocoaPods, pinned exactly:

```ruby
pod 'Loams', '0.1.0'
```

The pod's module is `LoamsObjC` (`@import LoamsObjC;`, or `#import "LoamsObjC-Swift.h"`),
so it can sit next to the Swift SDK's `Loams` module.
iOS 15 and macOS 12 or newer. The podspec is `Loams.podspec` at the repository root.

## Develop

```sh
swift build
swift test --disable-swift-testing
pod lib lint ../../Loams.podspec   # what sdk-release.yml checks (macOS)
```

Licensed under Apache-2.0 (see `LICENSE`).
