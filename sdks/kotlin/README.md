# Loams Kotlin SDK (preview)

> **Pre-release (preview).** This SDK is published as a preview of Loams. Its API
> can change between previews without notice: pin the exact version, and expect
> breaking changes until 1.0. Report problems at
> <https://github.com/ostrium-labs/loams/issues>.

## Install

Gradle (Kotlin DSL):

```kotlin
dependencies {
    implementation("dev.loams:loams-kotlin:0.1.0")
}
```

Maven:

```xml
<dependency>
  <groupId>dev.loams</groupId>
  <artifactId>loams-kotlin</artifactId>
  <version>0.1.0</version>
</dependency>
```

It runs on Java 11 or newer, and its only runtime dependencies are
`com.google.protobuf:protobuf-java` and the Kotlin standard library. The Java SDK
is a separate artifact, `dev.loams:loams`; use one or the other.

## Develop

```sh
./build.sh test              # compile and run the suite (needs a JDK and kotlinc)
./build.sh package OUT_DIR   # the jars and POM sdk-release.yml publishes
```

Licensed under Apache-2.0 (see `LICENSE`).
