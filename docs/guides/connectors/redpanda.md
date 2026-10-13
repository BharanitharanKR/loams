# Redpanda

Redpanda speaks the Kafka protocol. It runs on the native
[Kafka connector](kafka.md) through the target profile
`connectors/profiles/redpanda.yaml` (CN2 OR-2, Task 9). Its suites run
against Redpanda itself, never against Apache Kafka.

## Tested against

| System | Version | Where | Suites |
|---|---|---|---|
| Redpanda | v26.2.4 (`redpandadata/redpanda`, `dev-container` mode) | `deploy/fabric/compose.yaml`, profile `connectors-ci-only` (BSL-1.1, so CI only, OR-3); CI job `connectors-ci-only` | contract, roundtrip, kill-restart, dup-check, exactly-once, CloudEvents header pass-through |

## What differs from Kafka

Nothing is narrowed. Transactions are on by default in Redpanda, and
`read_committed` works. Redpanda does not implement KIP-890, so librdkafka
uses the original transaction protocol (R0.18).
`redpanda_ce_headers_pass_through` checks that D270's `ce_` headers survive
byte for byte in both directions. If a Redpanda suite finds a missing
capability, it is added to the profile's `narrows` with a `narrows:` note on
the manifest. It is never skipped.

Configuration is the Kafka connector's keys, in
`connectors/schemas/redpanda.config.json` (as `kafka.config.json`, without
the schema registry, which the native family does not wire in yet, R9.8).
The profile adds no defaults.

## Fallback

None (native, OR-1).
