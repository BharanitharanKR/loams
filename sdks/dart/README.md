# Loams Dart SDK (preview)

> **Pre-release (preview).** This SDK is published as a preview of Loams. Its API
> can change between previews without notice: pin the exact version, and expect
> breaking changes until 1.0. Report problems at
> <https://github.com/ostrium-labs/loams/issues>.

## Install

```sh
dart pub add loams:0.1.0
```

or in `pubspec.yaml`:

```yaml
dependencies:
  loams: 0.1.0
```

It works with Dart 3 and Flutter.

## Use

```dart
import 'package:loams/loams.dart';
```

## Develop

```sh
dart pub get
dart analyze
dart test
dart pub publish --dry-run   # what sdk-release.yml checks
```

Licensed under Apache-2.0 (see `LICENSE`).
