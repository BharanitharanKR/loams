# CN2 — the connector dependency spike

Task 0 of [the CN2 plan](2026-10-10-cn2-connectors.md): reconcile CN2 with what is built, and check every protocol crate, test image and fallback before a family crate links any of them. Recorded 2026-10-13 on `cn2-t0` (base `origin/dev` at `b555fe73`). The plan's "Rulings made during execution" section holds the rulings (R0.1–R0.30); this file holds the evidence.

**How it was checked.** Crate versions, release dates, SPDX ids, `rust-version` and publishers come from the crates.io API (`https://crates.io/api/v1/crates/<name>`, `/versions` and `/<version>/dependencies`). Capability claims were checked against the published `.crate` sources (`https://static.crates.io/crates/<name>/<name>-<version>.crate`, also browsable at `https://docs.rs/crate/<name>/<version>/source/<path>`). Repository status comes from the GitHub API. Advisories come from the `rustsec/advisory-db` `main` tree on 2026-10-13 (`https://github.com/rustsec/advisory-db/tree/main/crates/<name>`, rendered at `https://rustsec.org/packages/<name>.html`). Image tags come from registry tag APIs: Docker Hub, `mcr.microsoft.com/v2/<repo>/tags/list`, Quay and `gcr.io`. Vendor behaviour comes from vendor documentation and source at the tags named. "Unverified" marks every fact that could not be confirmed, with the reason.

**The 14-day rule.** OR-24 and Global Constraints require each new dependency to be at least 14 days old. The cutoff on 2026-10-13 is a release on or before **2026-09-29**. Where the newest release is younger, the table picks the newest one that passes.

**MSRV.** The `fabric/` workspace is Rust 1.97 (`fabric/Cargo.toml` `rust-version = "1.97"`). No crate below declares a `rust-version` above 1.97. The highest is 1.94.1 (the AWS SDK and `deltalake`).

**Build cost.** The plan asks for one measured build per family crate. No family crate exists yet: `fabric/crates/` holds only `loams-flow`, `loams-flow-proto`, `loams-fabric` and HS1's House crates. A build of a crate that does not exist measures nothing, and the build machine's one-build rule forbids speculative heavy builds. Each family task therefore measures its own crate on its first build (ruling R0.4). The "Build notes" column records what is known from each crate's manifest: native code, the TLS backend and `protoc`.

## 1. Protocol crates (Task 2 and the family tasks consume this table)

### 1.1 Streaming and brokers (Tasks 9, 18)

| Crate | Pin | Released | Latest | SPDX (as published) | MSRV ≤ 1.97 | Maintenance | Advisories (RustSec) | Build notes | Verdict |
|---|---|---|---|---|---|---|---|---|---|
| `rdkafka` | 0.39.0 | 2026-01-25 | 0.39.0 | MIT | 1.74 ✓ | last commit 2026-06-14; slow but live | none on the crate | through `rdkafka-sys` 4.10.0+2.12.1, which bundles **librdkafka 2.12.1** (BSD-2-Clause, with third-party C under MIT, BSD, zlib-style and public-domain terms) built with `make`, or `cmake` with `cmake-build`; `ssl-vendored` builds OpenSSL | **As planned.** The transactional API exists: `init_transactions`, `begin_transaction`, `send_offsets_to_transaction`, `commit_transaction` and `abort_transaction` on the `Producer` trait ([docs.rs](https://docs.rs/rdkafka/0.39.0/rdkafka/producer/trait.Producer.html)) |
| `lapin` | **4.12.0** | 2026-09-18 | 4.12.1 (2026-10-09, under 14 days) | MIT | 1.88 ✓ | very active (commit 2026-10-09) | none | the default `rustls` maps to `aws-lc-rs` (aws-lc-sys, C). Select `rustls--ring` to avoid aws-lc | **Chosen.** Has `confirm_select`/`PublisherConfirm`, `basic_ack`/`basic_nack` and `basic_qos` ([docs.rs](https://docs.rs/lapin/4.12.0/lapin/struct.Channel.html)) |
| `fe2o3-amqp` | **0.18.2** | 2026-09-29T01:11Z | 0.18.2 | `MIT/Apache-2.0` (legacy slash form of `MIT OR Apache-2.0`) | 1.85 ✓ | very active; releases roughly monthly; 0.18.0 yanked | none | pure Rust by default; optional `rustls`/`native-tls` | **Chosen.** It reaches 14 days on 2026-10-13. 0.18.1 (2026-09-13) is the strict alternative, but 0.18.2 fixes a hang in `send`. Settle modes, `accept`/`reject`/`release`/`modify` and SASL PLAIN all exist. Its tests run against `apache/artemis:2.55.0-alpine`, and it ships `service_bus` and `event_hubs` examples ([repo](https://github.com/minghuaw/fe2o3-amqp/tree/main/examples)) |
| `fe2o3-amqp-cbs` | 0.18.0 | 2026-09-08 | 0.18.0 | `MIT/Apache-2.0` | not declared | same repo; the crate calls itself "experimental" | none | pure Rust | **Chosen for Service Bus SAS/Entra tokens.** Task 18 owns a fixture test of the CBS put-token handshake |
| `async-nats` | 0.50.0 | 2026-07-20 | 0.50.0 | Apache-2.0 | 1.88 ✓ | active | RUSTSEC-2023-0027, fixed ≥ 0.29 (not applicable) | `ring` by default | **As planned.** JetStream pull consumers, `ack`/`double_ack`, and `PublishMessage::message_id` set `Nats-Msg-Id` ([docs.rs](https://docs.rs/async-nats/0.50.0/async_nats/jetstream/)) |
| `rumqttc` | 0.25.1 | 2025-11-21 | 0.25.1 | Apache-2.0 | 1.64 ✓ | **stale**: no release for about 11 months; last commit 2026-05-01 | **the default `use-rustls` pins `rustls-webpki ^0.102.8`**, which RUSTSEC-2026-0049, -0098, -0099 and -0104 affect (fixed only in 0.103.x). It also pulls `rustls-pemfile` (RUSTSEC-2025-0134, unmaintained) | `use-native-tls` uses OpenSSL | **Chosen with `default-features = false, features = ["use-native-tls"]`** (R0.9). MQTT 5 (`rumqttc::v5`), QoS 0–2, and `set_clean_start(false)` with a session expiry. In-flight state is held in memory only, so an unacked QoS 1 message is redelivered by the broker after a client restart, not replayed from disk |
| `pulsar` | 6.9.0 | 2026-08-24 | 6.9.0 | MIT OR Apache-2.0 | not declared | active (commit 2026-10-12) | none on the crate. **The default `async-std-runtime` pulls async-std (RUSTSEC-2025-0052, unmaintained)**. `rand` must resolve to ≥ 0.8.6 (RUSTSEC-2026-0097) | `build.rs` runs `prost_build`, so it needs **`protoc`** (already a build requirement of `loams-flow-proto`). The default `compression` builds lz4-sys and zstd-sys | **Chosen with `default-features = false` plus `tokio-runtime`**. Has `ack`, `cumulative_ack` and `nack`, plus producer batching |
| `tokio-tungstenite` | 0.30.0 | 2026-07-11 | 0.30.0 | MIT (tungstenite: MIT OR Apache-2.0) | 1.85 ✓ | quiet but live | none (tungstenite RUSTSEC-2023-0065 fixed ≥ 0.20.1) | pure Rust; TLS opt-in | **As planned** |

Advisories that cut across these crates: `rustls` must resolve to ≥ 0.23.45 (RUSTSEC-2026-0285) and `slab` to ≥ 0.4.11 (RUSTSEC-2025-0047). `fabric/Cargo.lock` already has `rustls` 0.23.45 and `rustls-webpki` 0.103.15.

### 1.2 CDC and databases (Tasks 10, 11, 12, 14, 16)

| Crate | Pin | Released | Latest | SPDX | MSRV ≤ 1.97 | Maintenance | Advisories | Build notes | Verdict |
|---|---|---|---|---|---|---|---|---|---|
| `tokio-postgres` | **0.7.18** | 2026-06-12 | 0.7.18 | MIT OR Apache-2.0 | 1.85 ✓ | active | RUSTSEC-2026-0178 (DataRow panic), **fixed in 0.7.18**, so pin `>=0.7.18` | pure Rust | **As planned**, floor raised to 0.7.18 |
| `postgres-protocol` | **0.6.12** | 2026-06-12 | 0.6.12 | MIT OR Apache-2.0 | 1.85 ✓ | active | RUSTSEC-2026-0179 (unbounded SCRAM iterations, CVSS 8.7) and RUSTSEC-2026-0180 (hstore panic), **both fixed in 0.6.12**, so pin `>=0.6.12` | pure Rust | **Floor 0.6.12** |
| `pg_walstream` (the `pgoutput` client, OR-24) | **0.9.0** | 2026-09-20 | 0.9.0 | **BSD-3-Clause** | 1.87 ✓ | four releases 2026-06 to 2026-09; one maintainer (commit activity unverified: the GitHub API rate-limited this check) | none (not in advisory-db) | default `rustls-tls` uses aws-lc-rs; `libpq` (`pq-sys`) only behind an opt-in feature | **Chosen** (§2, R0.6) |
| `mysql_async` | **0.37.1** | 2026-09-01 | 0.37.1 (0.37.0 is yanked) | MIT OR Apache-2.0 | not declared (edition 2021) | active | none | binlog needs the `binlog` feature. The default `flate2/zlib` builds C zlib; `minimal-rust` avoids it | **As planned** (§3) |
| `mysql_common` | 0.37.3 (what `mysql_async` 0.37.1 resolves) | 2026-06-17 | 0.38.2 | MIT OR Apache-2.0 | edition 2024 | active | none | — | **As resolved by `mysql_async`** (§3) |
| `tiberius` | **0.13.0** | 2026-09-25 | 0.13.0 | `MIT/Apache-2.0` (legacy slash form) | 1.88 ✓ | **revived**: the repo moved to `tiberius-rs/tiberius`, and 0.13.0 follows a two-year gap since 0.12.3 (2024-07-19) | RUSTSEC-2020-0010 covers only ≤ 0.3.2 | the default `native-tls` uses OpenSSL. Use `default-features = false, features = ["rustls", "tds80"]` | **Chosen**, with the maintenance risk recorded (R0.10) |
| `mongodb` | 3.9.1 | 2026-09-10 | 3.9.1 | Apache-2.0 | 1.88 ✓ | active | none | default `rustls-tls` (ring) and `dns-resolver` (hickory) | **As planned** |
| `scylla` | 1.9.0 | 2026-09-15 | 1.9.0 | MIT OR Apache-2.0 | 1.88 ✓ | active | none | no default features; `rustls-023` for TLS | **As planned** |
| `redis` | 1.7.1 | 2026-09-25 | 1.7.1 | BSD-3-Clause | 1.88 ✓ | active | none | `tokio-comp`; TLS through `tokio-rustls-comp` | **As planned** (CN1) |

### 1.3 Cloud, objects, search, analytics, codecs and fleet (Tasks 5, 13, 14, 15, 17, 19, 21, 26)

| Crate | Pin | Released | Latest | SPDX | MSRV ≤ 1.97 | Maintenance | Advisories | Build notes | Verdict |
|---|---|---|---|---|---|---|---|---|---|
| `aws-sdk-sqs` / `-sns` / `-eventbridge` / `-dynamodb` / `-dynamodbstreams` / `-kinesis` / `-lambda` / `-redshiftdata` | 1.113.0 / 1.115.0 / 1.121.0 / 1.128.0 / 1.115.0 / 1.124.0 / 1.148.0 / 1.120.0 | 2026-09-25 (one release batch) | the 2026-10-01 batch (under 14 days) | Apache-2.0 | 1.94.1 ✓ | `awslabs/aws-sdk-rust`, daily | none on the service crates (aws-sigv4 RUSTSEC-2023-0125 is long fixed) | **the defaults build two TLS stacks** (legacy ring/hyper 0.14 and aws-lc/hyper 1). Use `default-features = false, features = ["default-https-client", "rt-tokio"]`. aws-lc-sys (non-FIPS) needs only a C/C++ compiler and must be ≥ 0.39 (RUSTSEC-2026-0044..0048) | **Chosen** (R0.11) |
| `aws-sdk-sesv2` | 1.139.0 | 2026-09-29 | 1.140.1 | Apache-2.0 | 1.94.1 ✓ | same | none | same | Chosen. Task 20 SES may use `aws-sigv4` alone, as the plan says |
| `aws-config` | 1.12.0 | 2026-09-04 | 1.12.0 | Apache-2.0 | 1.94.1 ✓ | `smithy-lang/smithy-rs` | none | same | Chosen |
| `google-cloud-pubsub` (Google's, `googleapis/google-cloud-rust`) | **1.5.0** | 2026-09-25 | 1.6.0 (2026-09-30, under 14 days) | Apache-2.0 | 1.91 ✓ | active | none | rustls + aws-lc-rs; tonic 0.14 / prost 0.14 | **Chosen** (R0.12). Versions 0.1.0–0.30.0 of this name are yoshidan's (MIT), which "donated the crate name to Google"; yoshidan's code continues as `gcloud-pubsub` 1.7.1 (MIT), the alternative. Both are verified on [crates.io owners](https://crates.io/api/v1/crates/google-cloud-pubsub/owners) |
| `object_store` | **0.13.2** (not the plan's 0.14) | 2026-03-24 | 0.14.2 | `MIT/Apache-2.0` | 1.85 ✓ | active | RUSTSEC-2024-0358, fixed ≥ 0.10.2 | the `aws`/`gcp`/`azure` features | **Pinned to 0.13.2** (R0.13): `parquet` 59.3.0 (in `fabric/Cargo.lock`) and `deltalake` 1.1.0 both require `object_store ^0.13.2`; 0.14 goes with arrow/parquet 60 |
| `russh` | 0.63.3 | 2026-09-09 | 0.64.1 (2026-10-05) | Apache-2.0 | 1.89 ✓ | active (`Eugeny/russh`) | RUSTSEC-2026-0154, fixed ≥ 0.60.3 | defaults `aws-lc-rs`; a `ring` feature exists | **Chosen** |
| `russh-sftp` | 3.0.1 | 2026-09-28 | 3.0.1 | Apache-2.0 | not declared | active | none | pure Rust | **Chosen**; 3.0 is new (2026-09-08), so expect API churn |
| `reqwest` | 0.13.5 | 2026-09-08 | 0.13.5 | MIT OR Apache-2.0 | 1.85 ✓ | active | none | rustls + aws-lc-rs by default | **As planned** |
| `qdrant-client` | 1.19.0 | 2026-08-04 | 1.19.0 | Apache-2.0 | not declared | active | none | tonic ^0.14.6 (`tls-ring`), prost ^0.14.3, so **compatible with prost 0.14** | **As planned** |
| `clickhouse` | 0.15.2 | 2026-08-28 | 0.15.2 | MIT OR Apache-2.0 | 1.89 ✓ | active | none | default `lz4` | **As planned** (CN1) |
| `deltalake` (`deltalake-core`) | **1.1.0** | 2026-09-28 | 1.1.1 (2026-10-04) | Apache-2.0 | 1.94.1 ✓ | active | none | arrow/parquet ^59 ✓; **object_store ^0.13.2**; Change Data Feed reads only with the `datafusion` feature (datafusion 55, a heavy build). Pulls `delta_kernel` as `package = "buoyant_kernel"` 0.28 (published by a delta-rs maintainer) | **Chosen** (R0.14). The `txn` action is `CommitProperties::with_application_transaction`, read back by `transaction_version(app_id)` ([source](https://docs.rs/crate/deltalake-core/1.1.0/source/src/kernel/transaction/mod.rs)) |
| `iceberg` (iceberg-rust) | 0.10.1 | 2026-08-01 | 0.10.1 | Apache-2.0 | 1.94 ✓ | active | none | **arrow/parquet ^58**, plus reqwest 0.12 | **Conflict with arrow 59** (R0.15) |
| `arrow-csv`, `arrow-json` | 59.3.0 | 2026-09-01 | 60.0.0 | Apache-2.0 | 1.85 ✓ | active | none | — | **As planned** (59, CN1 Ruling 4) |
| `prost-reflect` | 0.16.5 | 2026-07-09 | 0.16.5 | MIT OR Apache-2.0 | 1.82 ✓ | quiet (last commit 2026-07-09), not archived | none | prost ^0.14 | **As planned** |
| `tonic`, `tonic-reflection` | 0.14.6 | 2026-05-07 | 0.14.6 | MIT | 1.88 ✓ | the repo moved to `grpc/grpc-rust`; active | tonic RUSTSEC-2024-0376, fixed ≥ 0.12.3 | `tls-ring` or `tls-aws-lc` | **As planned.** The reflection client is generated even with `default-features = false` |
| `prost` | 0.14.4 | 2026-06-07 | 0.14.4 | Apache-2.0 | 1.85 ✓ | active | RUSTSEC-2020-0002, fixed ≥ 0.6.1 | — | As in the Tech Stack |
| `snap` | 1.1.2 | 2026-07-15 | 1.1.2 | BSD-3-Clause | 1.39 (README) ✓ | quiet | none | pure Rust | **As planned** |
| `kube` | **4.2.0** | 2026-07-22 | 4.2.0 | Apache-2.0 | 1.89 ✓ | active | none | defaults `client, rustls-tls, ring` | **Chosen** (Task 26) |
| `k8s-openapi` | **0.28.0** | 2026-06-15 | 0.28.0 | Apache-2.0 | not declared | active | none | exactly one version feature, from `v1_32` to `v1_36` | **Chosen with `v1_32`** (`earliest`): supports clusters ≥ 1.32 (R0.16) |

## 2. The `pgoutput` client (OR-24, Task 10)

Upstream `tokio-postgres` has no replication support, in 0.7.18 or on master. PR [#752](https://github.com/rust-postgres/rust-postgres/pull/752) ("Support for physical and logical replication") was never merged: the master tarball has no `copy_both` or `postgres-replication`.

| Candidate | On crates.io | `CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT` | `START_REPLICATION` proto 1 / 2+ with streaming | Standby Status Update with explicit flush LSN | Verdict |
|---|---|---|---|---|---|
| `supabase/etl` (Apache-2.0) | **no**. Its root `Cargo.toml` takes `tokio-postgres` and `postgres-replication` as **git** dependencies (`supabase/rust-postgres` rev `8086f1fd`) | **no**: it uses `USE_SNAPSHOT` / `NOEXPORT_SNAPSHOT` (`crates/etl/src/postgres/client/query.rs:32-58`) | proto 1 only (`raw.rs:774-779`); the fork's decoder has no stream messages | yes (`standby_status_update(write, flush, apply, …)`) | rejected |
| `MaterializeInc/rust-postgres` fork | **no** (git only) | slot SQL is the caller's | proto 1 decoder only | yes | rejected (git dependency, no streaming) |
| `pgwire-replication` 0.4.1 (`Apache-2.0 OR MIT`) | yes | **no slot creation** | proto 1 hard-coded (`src/client/worker.rs:199-205`); returns raw `XLogData` bytes, no pgoutput decoding | write = flush = apply, one value | rejected |
| **`pg_walstream` 0.9.0** (BSD-3-Clause) | **yes** | **yes**, in both the PG14 form (`… LOGICAL "pgoutput" EXPORT_SNAPSHOT`) and the PG15+ form (`(SNAPSHOT 'export')`), with a server-version preflight (`src/sql_builder.rs:259-303, 642`), plus a `SET TRANSACTION SNAPSHOT` + `COPY` handoff (`src/snapshot/mod.rs`) | **yes**: `protocol_version` 1–4 (default 2) and `StreamingMode` off/on/parallel; decodes Stream Start/Stop/Commit/Abort/Prepare (`src/protocol.rs:1067-1130`) | **yes**: `update_flushed_lsn`, `update_applied_lsn`, `send_feedback` (`src/stream.rs:1588, 2255, 2268`); the write LSN is tracked internally | **chosen** |

Sources: <https://crates.io/crates/pg_walstream>, <https://docs.rs/crate/pg_walstream/0.9.0/source/>, <https://github.com/supabase/etl>, <https://crates.io/crates/pgwire-replication>. Risks: one maintainer and a 0.x API. Task 10 pins `=0.9.0` and reviews the vendored source on each bump. If it fails, OR-24's fallback is a minimal CopyBoth client in `loams-connector-postgres` over `postgres-protocol`'s message codecs, since `pg_walstream` shows the full scope that needs.

## 3. `mysql_async` binlog (Task 11)

- **MySQL GTID start: yes.** `Conn::get_binlog_stream(BinlogStreamRequest)` with `with_gtid()` (`COM_BINLOG_DUMP_GTID`) and `with_gtid_set(…)` ([docs.rs](https://docs.rs/mysql_async/0.37.1/mysql_async/struct.BinlogStreamRequest.html)). It needs the `binlog` feature.
- **MariaDB GTID: not supported.**
  - `request_binlog` sends only `SET @master_binlog_checksum='ALL'`, `COM_REGISTER_SLAVE` and the dump. There is no `@slave_connect_state` and no `@mariadb_slave_capability`.
  - `mysql_common`'s `EventType` stops at `TRANSACTION_PAYLOAD_EVENT` (0x28), so `MARIADB_GTID_EVENT` (0xA2), `MARIADB_GTID_LIST_EVENT` (0xA3) and `ANNOTATE_ROWS` are unknown types, and `read_data()` returns `Ok(None)` for them (`src/binlog/events/mod.rs:260-266`).
  - So Task 11 writes MariaDB GTID handling in the family crate (R0.7): set the two session variables on the `Conn` before `get_binlog_stream`, then parse 0xA2/0xA3 from `event.header().event_type_raw()` and `event.data()`. This is untested upstream, so a Task 11 test against real MariaDB 11 is the proof.
- **Row events: yes.**
  - `WRITE/UPDATE/DELETE_ROWS_EVENT` v1 (0x17–0x19) and v2 (0x1E–0x20), plus `PARTIAL_UPDATE_ROWS_EVENT`, decode through `RowsEvent::rows(&TableMapEvent)` into before and after `BinlogRow`s.
  - `BinlogStream::get_tme(table_id)` caches the table maps.
- **`binlog_row_metadata=FULL`: yes.**
  - `TableMapEvent::iter_optional_meta()` yields `OptionalMetadataField::ColumnName` among others ([docs.rs](https://docs.rs/mysql_common/0.37.3/mysql_common/binlog/events/enum.OptionalMetadataField.html)).
  - `BinlogRow` columns are named from that metadata when it is present.

## 4. The P2 protocol table (Task 2 consumes it)

One row per P2 manifest after the Debezium split and MCP's move to P3 (OR-13, OR-15): 73 rows. "Operations" lists what each declared capability in `catalog.csv` needs. Crates and pins are the ones in §1.

| # | P2 row (`id`) | Runtime / family / task | Protocol crate | Wire protocol | Source operations (per declared capability) | Sink operations (per declared capability) |
|---|---|---|---|---|---|---|
| 1 | `redpanda` | native `kafka` / 9 (target) | `rdkafka` | Kafka protocol | streaming: consumer group `subscribe`/`poll`; offset commit after the Fabric acks | streaming: idempotent producer; `exactly_once`: `init_transactions` … `send_offsets_to_transaction` … `commit_transaction` (Redpanda has transactions on by default, §5.1) |
| 2 | `azure-event-hubs` | native `kafka` / 9 (target) | `rdkafka` | Kafka protocol on the Event Hubs Kafka endpoint (SASL PLAIN, `$ConnectionString`, TLS 9093; the emulator uses SASL_PLAINTEXT on 9092) | streaming: consumer group poll, offset commit | streaming: idempotent producer only, **`effectively_once` at most; no `exactly_once`** (§5.2, R0.19) |
| 3 | `cockroachdb` | native `postgres` / 10 (target) | `tokio-postgres` | Postgres wire (CockroachDB) | batch: `SELECT` by key range, or `COPY … TO STDOUT WITH CSV` (**no binary `COPY TO`**); streaming/cdc: **sinkless `CREATE CHANGEFEED FOR TABLE … WITH updated, resolved, cursor = …`** (no `INTO`); `EXPERIMENTAL CHANGEFEED FOR` is deprecated since v25.2 (§5.6, R0.20) | upsert in a transaction with `_loams_checkpoints` |
| 4 | `aurora` | native `postgres` / 10 and `mysql` / 11 (targets, `engine`) | `tokio-postgres` + `pg_walstream`; `mysql_async` | Postgres wire with logical replication; MySQL binlog dump | batch: `COPY`/chunked `SELECT`; cdc: pgoutput slot, or row binlog with GTID | upsert + `_loams_checkpoints` in one transaction |
| 5 | `pgvector` | native `postgres` / 10 (target) | `tokio-postgres` | Postgres wire | batch: `COPY … TO STDOUT (FORMAT binary)`, `vector` → `FixedSizeList<Float32>` | batch: `COPY FROM` / upsert of `vector` columns |
| 6 | `mariadb` | native `mysql` / 11 (target) | `mysql_async` + `mysql_common` + family-crate MariaDB GTID code | MySQL protocol, `COM_BINLOG_DUMP` with `@slave_connect_state` | batch: chunked `SELECT` by PK (DBLog watermarks); cdc/streaming: row events + `TABLE_MAP`, MariaDB GTID list as position (§3) | upsert + `_loams_checkpoints` |
| 7 | `sql-server` | native `sqlserver` / 12 | `tiberius` | TDS 7.4 / 8.0 | batch: key-range `SELECT` under snapshot isolation; cdc/streaming: `cdc.fn_cdc_get_all_changes_<ci>` up to `sys.fn_cdc_get_max_lsn()` | bulk load to a temp table, `MERGE`, `_loams_checkpoints` in the same transaction |
| 8 | `azure-sql` | native `sqlserver` / 12 (target) | `tiberius` | TDS with an Entra ID token | as `sql-server` | as `sql-server` |
| 9 | `debezium-sqlserver` (split, OR-13) | `debezium` fallback / 22 | Debezium Server 3.7.0.Final | SQL Server CDC tables over JDBC; HTTP sink to `ingest` | cdc | — |
| 10 | `debezium-oracle` (split, OR-13) | `debezium` (Oracle's default CDC, `fallback-only`) / 22 | Debezium Server 3.7.0.Final with `ojdbc11` | Oracle LogMiner (`database.connection.adapter=logminer`, the default) | cdc | — |
| 11 | `oracle` | `camel` `sql` (`fallback-only`) / 22–23 | Camel 4.22.1 `sql` + `ojdbc` (user-supplied) | Oracle Net over JDBC | batch: `sql` consumer (polling query) | batch: `sql` producer (upsert) |
| 12 | `minio-rustfs` | native `objects` / 13 (target) | `object_store` (`aws`) | S3 REST | batch: `ListObjectsV2` with a high-water key, `GetObject`; webhook: bucket notifications | `PutObject`/multipart with `If-None-Match: *` (`PutMode::Create`; verified in 0.13.2 `src/aws/mod.rs`) |
| 13 | `cloudflare-r2` | native `objects` / 13 (target) | `object_store` (`aws`) | S3 REST (R2) | batch: list/get (polling) | `PutObject` with `If-None-Match: *` (R2 documents it, §5.7) |
| 14 | `gcs` | native `objects` / 13 | `object_store` (`gcp`) | GCS JSON API | batch: list/get; streaming/webhook: Pub/Sub notifications (nightly) | upload with `x-goog-if-generation-match: 0` |
| 15 | `azure-blob` | native `objects` / 13 | `object_store` (`azure`) | Azure Blob REST | batch: List/Get Blob; streaming/webhook: Event Grid (nightly) | Put Block List with `If-None-Match: *` |
| 16 | `adls-gen2` | native `objects` / 13 | `object_store` (`azure`, flat Blob API) + `reqwest` for the DFS endpoint | Blob REST; DFS REST (`?resource=directory`, `x-ms-rename-source`) | batch: list/read over Blob | write over Blob; **atomic rename through the DFS REST API in the family crate**, because `object_store` renames by copy + delete (R0.21) |
| 17 | `sftp` | native `objects` / 13 | `russh` + `russh-sftp` | SSH 2 / SFTP v3 | batch: `READDIR`, `STAT`, `READ` (polling) | `OPEN`/`WRITE` to a temporary name, then `RENAME` |
| 18 | `valkey` | native `kv` / 14 (target) | `redis` | RESP2/RESP3 | batch: `SCAN` + typed reads; streaming: Streams/keyspace notifications | `SET`/`HSET`/`DEL`, pipelined |
| 19 | `redis-streams` | native `kv` / 14 | `redis` | RESP | streaming: `XREADGROUP`, `XACK` after the Fabric acks, `XAUTOCLAIM` | streaming: `XADD` |
| 20 | `dynamodb` | native `kv` / 14 | `aws-sdk-dynamodb` | DynamoDB JSON over HTTPS (SigV4) | batch: parallel `Scan` segments (`LastEvaluatedKey`); streaming/cdc: through `dynamodb-streams` | `BatchWriteItem` with `UnprocessedItems` retry; conditional `PutItem` on a version attribute |
| 21 | `dynamodb-streams` | native `kv` / 14 | `aws-sdk-dynamodbstreams` | DynamoDB Streams API | cdc: `DescribeStream`, `GetShardIterator`, `GetRecords`, with shard lineage | — |
| 22 | `cassandra` | native `kv` / 14 | `scylla` | CQL native protocol v4 | batch: token-range `SELECT` with paging state | prepared `INSERT` (upsert by primary key) |
| 23 | `opensearch` | native `search` / 15 (target) | `reqwest` | OpenSearch REST | batch/streaming: PIT (`POST /<idx>/_search/point_in_time?keep_alive=`), `search_after`, `DELETE /_search/point_in_time` (PIT since 2.4) | `_bulk` with `_id`, retrying partial failures |
| 24 | `qdrant` | native `search` / 15 | `qdrant-client` | Qdrant gRPC | batch: `Scroll` with an offset | `UpsertPoints` (UUID v5 ids), `DeletePoints` |
| 25 | `mongodb` | native `mongodb` / 16 | `mongodb` | MongoDB wire (OP_MSG) | batch: `_id`-range `find` with `readConcern: snapshot`; cdc/streaming: change streams with a resume token | `bulkWrite`; a transaction with `_loams_checkpoints` on replica sets |
| 26 | `debezium-mongodb` | `debezium` fallback / 22 | Debezium Server 3.7.0.Final | MongoDB change streams | cdc | — |
| 27 | `redshift` | native `analytics` / 17 | `aws-sdk-redshiftdata` + `object_store` | Redshift Data API | batch: `ExecuteStatement`, `GetStatementResult`; bulk: `UNLOAD` to S3 Parquet | Parquet to S3, then `COPY` + `MERGE` through the Data API |
| 28 | `databricks` | native `analytics` / 17 | `reqwest` | SQL Statement Execution API (`/api/2.0/sql/statements`, `ARROW_STREAM`, `EXTERNAL_LINKS`) | batch: statement + chunk fetch | Files API upload to a UC volume, then `COPY INTO`/`MERGE` |
| 29 | `trino` | native `analytics` / 17 | `reqwest` | Trino client REST protocol (`POST /v1/statement`, `nextUri`) | batch | — (source only) |
| 30 | `duckdb` | native `analytics` / 17 (ADBC target) | `adbc_driver_manager` + the DuckDB driver | ADBC C API | batch: query → Arrow | batch: bulk ingest |
| 31 | `delta-lake` | native `lakehouse` / 17 | `deltalake` 1.1.0 (+ `object_store` 0.13) | Delta protocol on object storage | batch: table versions (position = version); cdc: change data feed (`scan_cdf`, needs the `datafusion` feature) | append/merge commit with a `txn` action (`with_application_transaction`) |
| 32 | `rabbitmq-amqp` | native `brokers` / 18 | `lapin` | AMQP 0-9-1 | streaming: `basic.consume`, `basic.qos` prefetch, `basic.ack` after the Fabric | `basic.publish` with publisher confirms |
| 33 | `activemq-jms` | native `brokers` / 18 | `fe2o3-amqp` | AMQP 1.0 | streaming: receiver link, `rcv-settle-mode` **first**, unsettled deliveries, `accepted` after the Fabric (§5.4) | sender link, unsettled, settled on outcome |
| 34 | `azure-service-bus` | native `brokers` / 18 | `fe2o3-amqp` + `fe2o3-amqp-cbs` | AMQP 1.0 (TCP 5671/5672) | streaming: peek-lock receiver (`rcv-settle-mode` first, unsettled), `accepted` after the Fabric | sender link |
| 35 | `nats` | native `brokers` / 18 | `async-nats` | NATS + JetStream API | streaming: durable pull consumer, `ack` after the Fabric; core subjects `resumable: false` | JetStream publish with `Nats-Msg-Id` |
| 36 | `mqtt` | native `brokers` / 18 | `rumqttc` (`v5`, `use-native-tls`) | MQTT 5 | streaming: `SUBSCRIBE` QoS 1, `clean_start = false`, manual `PUBACK` | `PUBLISH` QoS 1 with user properties |
| 37 | `pulsar` | native `brokers` / 18 | `pulsar` (tokio, no async-std) | Pulsar binary protocol | streaming: failover/shared subscription, individual ack | producer with sequence-id dedup |
| 38 | `websocket` | native `brokers` / 18 | `tokio-tungstenite` | RFC 6455 | streaming: frames, `resumable: false` | text/binary frames |
| 39 | `aws-sqs` | native `cloud` / 19 | `aws-sdk-sqs` | SQS JSON protocol | streaming: `ReceiveMessage` long poll, `ChangeMessageVisibility`, `DeleteMessage` after the Fabric | `SendMessageBatch` (FIFO dedup id = `ce_id`) |
| 40 | `aws-sns` | native `cloud` / 19 | `aws-sdk-sns` | SNS API; HTTPS subscription | webhook: subscription confirmation + signature check | `Publish`/`PublishBatch` with attributes |
| 41 | `eventbridge` | native `cloud` / 19 | `aws-sdk-eventbridge` (+ `aws-sdk-sqs`) | EventBridge API | streaming/webhook: a rule → SQS target, read through SQS | `PutEvents` with partial-failure retry |
| 42 | `google-pub-sub` | native `cloud` / 19 | `google-cloud-pubsub` 1.5.0 (Google) | gRPC `google.pubsub.v1` | streaming: `StreamingPull`, ack/nack, client-managed lease extension | `Publish` with ordering keys and attributes |
| 43 | `azure-event-grid` | native `cloud` / 19 | `reqwest` | Event Grid HTTP, CloudEvents 1.0 schema | webhook: validation handshake, CloudEvents pass-through | `POST` a CloudEvents batch (SAS key or Entra ID) |
| 44–63 | `kestra-companion`, `segment`, `sendgrid`, `posthog`, `hubspot`, `shopify`, `gitlab`, `amazon-ses`, `twilio-sms`, `salesforce`, `zendesk`, `servicenow`, `stripe`, `github`, `jira`, `slack`, `google-workspace`, `keycloak`, `vault`, `splunk` | native `http` + an `http-api` profile each / 20 | `reqwest` (+ `aws-sigv4` for SES) | each vendor's REST API; webhooks with vendor signatures | batch: paginated list endpoints (Salesforce SOQL `nextRecordsUrl`, Bulk API 2.0); webhook: a signature-verified receiver; Salesforce `cdc`: the Camel `salesforce` fallback until Task 23 (§6) | REST create/update calls; Splunk HEC with indexer acknowledgement |
| 64 | `grpc` | native `protocols` / 21 | `tonic` + `prost-reflect` + `tonic-reflection` | gRPC over HTTP/2 | streaming: server-streaming calls | unary calls (`sink.reply`) |
| 65 | `graphql` | native `protocols` / 21 | `reqwest` (+ `tokio-tungstenite` for `graphql-ws`) | GraphQL over HTTP; `graphql-transport-ws` | batch: Relay cursor pagination; streaming: subscriptions (`resumable: false`) | mutations |
| 66 | `neo4j` | native `protocols` / 21 | `reqwest` | Neo4j HTTP Query API, `POST /db/{db}/query/v2`, introduced in Neo4j 5.19 and enabled by default on self-managed servers from 5.25 ([docs](https://neo4j.com/docs/query-api/current/)) | batch: cursor-property `MATCH` | parameterised `MERGE` |
| 67 | `grafeo` | native `protocols` / 21 | the generated `loams.graph.v1` client (`connectrpc`) | Connect/gRPC | batch: `ExecuteStream` with a cursor property | `ExecuteBatch` of parameterised GQL |
| 68 | `prometheus` | native `protocols` / 21 | `prost` + `snap` (+ `reqwest`) | Prometheus remote-write 1.0 (snappy protobuf) | streaming: a remote-write receiver on `ingest` | remote-write sender |
| 69 | `lambda-cloud-run-azure-functions` | native `protocols` / 21 | `aws-sdk-lambda`; `reqwest` | Lambda `Invoke`; HTTPS with ID tokens or function keys | webhook: function → HTTP route | `Invoke` / HTTP POST (`sink.reply`) |
| 70 | `openai` | native `protocols` / 21 | `reqwest` | OpenAI REST | — | batch: API calls (`sink.reply`) |
| 71 | `anthropic` | native `protocols` / 21 | `reqwest` | Anthropic Messages API | — | batch: Messages calls (`sink.reply`) |
| 72 | `csv-ndjson` | SDK codecs / 5 | `arrow-csv`, `arrow-json` 59 | — (codec) | decode | encode |
| 73 | `protobuf` | SDK codecs / 5 | `prost-reflect` | — (codec) | decode | encode |
| — | `mcp` | moves to P3 (OR-15) | — | — | — | — |

## 5. Real systems: what each family assumes, and the fallback where an emulator falls short

| # | Assumption | Finding | PR-CI target | Fallback |
|---|---|---|---|---|
| 5.1 | Redpanda transactions and `ce_` headers | **Transactions supported and on by default** (`enable_transactions` defaults to true; `read_committed` consumers work; exactly-once also needs idempotence and `transaction_coordinator_delete_retention_ms ≥ transactional_id_expiration_ms`). Redpanda does not implement KIP-890 (Transactions V2) server-side, so clients fall back to the original protocol ([docs](https://docs.redpanda.com/streaming/25.2/develop/transactions.md), [clients](https://docs.redpanda.com/current/develop/kafka-clients/)). Arbitrary record headers are not reserved or rewritten in any Redpanda doc. **Unverified**: byte-for-byte pass-through of `ce_id`/`ce_type`/`ce_source`, because no doc says so; Task 9's `redpanda_ce_headers_pass_through` is the proof. Current version v26.2.4 | `redpandadata/redpanda` (`ci-service`) | none needed |
| 5.2 | Event Hubs Kafka endpoint and transactions | The **emulator exposes Kafka on 9092** (SASL_PLAINTEXT + PLAIN, `$ConnectionString`). Only producer and consumer APIs work: **no transactions on the emulator** ([overview](https://learn.microsoft.com/en-us/azure/event-hubs/overview-emulator)). In the cloud, Kafka transactions are **public preview, Premium and Dedicated tiers only** ([docs](https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-transactions)). The emulator needs Azurite beside it; its tags are 2.2.1/`latest`. **Unverified**: which emulator tag first shipped Kafka (merged in the installer repo 2024-11-19; no release notes name the tag) | emulator (`ci-service`) | transactional semantics are tested on Kafka and Redpanda; Event Hubs is narrowed to `effectively_once` (R0.19), with a nightly real namespace |
| 5.3 | Service Bus emulator AMQP 1.0 | **AMQP 1.0 on TCP 5672**: queues, topics, subscriptions with filters, sessions, peek-lock `LockDuration`, `MaxDeliveryCount`, dead-lettering. Missing: JMS, partitioned entities, AMQP over WebSockets, large messages (256 KB cap). It has a 10-connection cap and no persistence ([overview](https://learn.microsoft.com/en-us/azure/service-bus-messaging/overview-emulator)). **It needs a SQL Server sidecar** (`mcr.microsoft.com/azure-sql-edge` in the default compose, or `mssql/server` per the quickstart; `ACCEPT_EULA=Y` covers both) | emulator + SQL sidecar (both `ci-service`) | a nightly real namespace for partitioning, large messages and WebSockets |
| 5.4 | ActiveMQ Classic and Artemis AMQP 1.0 acknowledgement | Both brokers **force `rcv-settle-mode` first** (no `second`) and honour the client's `snd-settle-mode`. Classic: `accepted` = ack; `rejected` and `modified{undeliverable-here}` = poison → DLQ; `released` = redeliver without a count; `modified{delivery-failed}` = redeliver with a count. Artemis: `accepted` = ack; `released` = cancel without a count; `rejected` → dead-letter address (unless `amqpTreatRejectAsUnmodifiedDeliveryFailed`); `modified{undeliverable-here}` = excluded from that consumer; `modified{delivery-failed}` = cancel with a count (sources: `AmqpSender.java` in `apache/activemq`; `ProtonServerSenderContext.java` at Artemis 2.57.0). **Images moved**: Artemis is now its own Apache project (`apache/artemis:2.57.0`; `apache/activemq-artemis` is archived), and Classic is `apache/activemq:6.3.2` (`apache/activemq-classic` is archived) | `apache/artemis`, `apache/activemq` | none; Task 18 never relies on `second` |
| 5.5 | OpenSearch point-in-time | PIT **since 2.4**: `POST /<idx>/_search/point_in_time?keep_alive=`, `GET /_search/point_in_time/_all`, `DELETE /_search/point_in_time`, plus `search_after` ([docs](https://docs.opensearch.org/latest/search-plugins/searching-data/point-in-time/)). Current version 3.9.0 (2026-09-29). **Unverified**: any 3.x-specific PIT change; the `/latest/` docs still describe it | `opensearchproject/opensearch:3` | none |
| 5.6 | CockroachDB core changefeeds and binary `COPY TO` | `EXPERIMENTAL CHANGEFEED FOR` is **deprecated since v25.2** ([docs](https://docs.cockroachlabs.com/docs/stable/changefeed-for)); the replacement is a **sinkless `CREATE CHANGEFEED FOR TABLE … WITH updated, resolved`** without `INTO`, which streams to the SQL client and needs `kv.rangefeed.enabled` ([docs](https://docs.cockroachlabs.com/docs/stable/create-changefeed)). **`COPY … TO STDOUT` supports text and CSV only; BINARY is for `COPY FROM`** ([docs](https://docs.cockroachlabs.com/docs/stable/copy)). Licence: CockroachDB Software License from 24.3.0; **single-node clusters need no key and are not throttled** ([FAQ](https://docs.cockroachlabs.com/docs/stable/licensing-faqs)). **Unverified**: whether a v26.x release has already removed `EXPERIMENTAL CHANGEFEED FOR` | `cockroachdb/cockroach` `start-single-node` (`ci-service`) | batch uses `SELECT` (or CSV `COPY`), never binary (R0.20) |
| 5.7 | R2 conditional `PUT` | R2's S3 compatibility table lists PutObject conditional operations (`If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`) as implemented ([docs](https://developers.cloudflare.com/r2/api/s3/api/)). **Unverified**: that `If-None-Match: *` on an existing key returns 412, which the table does not spell out | fixtures recorded from real R2 | a nightly real bucket; the first nightly records the status code into the fixture |
| 5.8 | Azurite hierarchical namespace | **Not supported.** Azurite 3.37.0 covers Blob, Queue and Table. Issues [#409](https://github.com/Azure/Azurite/issues/409) and [#553](https://github.com/Azure/Azurite/issues/553) are open, and PR [#2635](https://github.com/Azure/Azurite/pull/2635) is unmerged | Azurite for flat Blob operations | DFS operations (directories, rename, ACLs) on fixtures in PR CI and a **nightly real HNS account** (R0.21) |
| 5.9 | floci 2.1.0 coverage | floci is at [`floci-io/floci`](https://github.com/floci-io/floci) (MIT); 2.1.0 was released 2026-09-15 (2.2.0 on 2026-10-06). Its README at tag 2.1.0 lists **all eight**: SQS (standard, FIFO, DLQ), SNS (SQS/Lambda/HTTP delivery), EventBridge, Kinesis, DynamoDB (GSI, LSI, TTL, transactions), **DynamoDB Streams**, Lambda (real Docker) and SES (v1 and v2). The image is `floci/floci:2.1.0` on port 4566; `hectorvent/floci` is deprecated. **Unverified**: fidelity beyond the README (performance claims are self-reported) | `floci/floci:2.1.0` | DynamoDB Local (`ci-service`) only if floci's Streams lineage fails a Task 14 test; nightly AWS. LocalStack is not an alternative: since 2026.03 its image requires an auth token |
| 5.10 | Pub/Sub emulator | `gcloud beta emulators pubsub` in `gcr.io/google.com/cloudsdktool/google-cloud-cli:<ver>-emulators` (588.0.0). Ordering, filtering, dead-lettering and retry are supported. Missing: IAM, `UpdateTopic`/`UpdateSnapshot`, configurable retention, protobuf schemas, seek-to-time on ordered subscriptions ([docs](https://docs.cloud.google.com/pubsub/docs/emulator)). **Unverified**: exactly-once delivery on the emulator. `google-cloud-pubsub` 1.5.0 does not read `PUBSUB_EMULATOR_HOST`; it takes `with_endpoint(...)` plus anonymous credentials. **Unverified**: whether a lower layer reads the variable | emulator | a nightly real project for exactly-once and IAM |

## 6. Fallback rows only (OR-4)

- **Camel 4.22.1** is the newest 4.22.x (Maven Central, 2026-09-13), from the `camel-catalog-4.22.1.jar` component JSONs:

  | Component | Directions | Support level |
  |---|---|---|
  | `sql` | consumer + producer | Stable |
  | `salesforce` | consumer + producer | Stable |
  | `jms` | consumer + producer | Stable |
  | `jdbc` | **producer only** | Stable |
  | `amqp` | consumer + producer | Stable |
  | `activemq` | consumer + producer | Stable |
  | `activemq6` | consumer + producer | Stable |
  | `iggy` | consumer + producer | **Preview**, since 4.17.0 |

  - The Salesforce consumer covers CDC both through Streaming API `subscribe:data/ChangeEvents` and through the Pub/Sub API (`pubSubSubscribe:` with `replayPreset`/`pubSubReplayId`), per `salesforce-streaming.adoc` at `camel-4.22.1`.
  - The JDBC ★ row's `camel` assignment can therefore only be a sink. Its source direction needs the `sql` consumer (R0.24).
- **`camel-iggy` commit behaviour** (from `IggyFetchRecords.java` at `camel-4.22.1`):
  - With `autoCommit=true` (the default), the offset is committed server-side **at poll time, before any exchange is processed**. That makes it **at-most-once**: there is no commit on completion and no redelivery on route failure.
  - With `autoCommit=false`, it keeps an in-memory offset from `startingOffset` and **never stores it on the server**, so a restart replays from `startingOffset`.
  - It runs a single consumer task.
  - So CN-R5 stands: camel-iggy is not a delivery path that meets the at-least-once floor (R0.25).
- **Debezium Server 3.7.0.Final**:
  - Released 2026-09-29 ([blog](https://debezium.io/blog/2026/09/29/debezium-3-7-final-released/)), so it is exactly 14 days old on 2026-10-13. The image is `quay.io/debezium/server:3.7.0.Final`, Apache-2.0.
  - Oracle `database.connection.adapter` values:
    - `logminer`: the default, buffered in the connector.
    - `logminer_unbuffered`: buffered in the database.
    - `olr`: OpenLogReplicator.
    - `xstream`: needs a GoldenGate licence and Instant Client's `xstreams.jar`.
  - **The community `debezium-server-dist-3.7.0.Final.tar.gz` already ships `lib/ojdbc11-23.26.0.0.0.jar` and `ojdbc17-23.26.0.0.0.jar`** (plus `xdb`, `orai18n`), verified by listing the Maven Central archive. The server docs still say to copy the driver into `lib/`.
  - The driver is under the **Oracle Free Use Terms and Conditions** ([POM](https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.26.0.0.0/ojdbc11-23.26.0.0.0.pom)). See R0.26 for what this does to CN1 Ruling 8.
  - **Unverified**: the contents of the image itself. It was inferred from its Dockerfile, which unpacks that archive into `/debezium`.

## 7. Test images (OR-3)

Digests are pinned in each family task's commit (Global Constraints); tags below are the lines current on 2026-10-13. `ci-service` = proprietary, source-available or EULA; never in an image Loams builds or in the default compose profile.

| Image | Tag line | SPDX | Terms worth noting | Verdict |
|---|---|---|---|---|
| `apache/kafka` | 4.3.1 | Apache-2.0 | — | service |
| `redpandadata/redpanda` | v26.2.4 | BUSL-1.1 (core) + Redpanda Community License (enterprise features) | BSL forbids offering a commercial streaming service; a 30-day enterprise trial on new clusters. Transactions are not an enterprise feature | ci-service |
| `rabbitmq` | 4.3-management | MPL-2.0 | — | service |
| `apache/artemis` (was `apache/activemq-artemis`) | 2.57.0 | Apache-2.0 | the old repository is archived | service |
| `apache/activemq` (Classic; was `apache/activemq-classic`) | 6.3.2 | Apache-2.0 | the old repository is archived | service |
| `nats` | 2.15.1 | Apache-2.0 | — | service |
| `eclipse-mosquitto` | 2.0.22 | EPL-2.0 OR BSD-3-Clause | — | service |
| `apachepulsar/pulsar` | 4.0.14 (LTS) / 5.0.0 | Apache-2.0 | — | service |
| `mcr.microsoft.com/azure-messaging/eventhubs-emulator` | 2.2.1 | LicenseRef-Microsoft-Emulator-EULA | `ACCEPT_EULA=Y`; development and testing only; needs Azurite | ci-service |
| `mcr.microsoft.com/azure-messaging/servicebus-emulator` | 2.0.1 | LicenseRef-Microsoft-Emulator-EULA | `ACCEPT_EULA=Y`; needs a SQL Server/SQL Edge sidecar (Developer EULA) | ci-service |
| `gcr.io/google.com/cloudsdktool/google-cloud-cli` | 588.0.0-emulators | Apache-2.0 (SDK; no alternate licence file on the pubsub-emulator component) | the emulator itself is a closed-source jar. **Unverified**: a licence statement for the jar specifically | service, treated as `ci-service` until verified (R0.27) |
| `floci/floci` | 2.1.0 (D60) | MIT | — | service |
| `pgvector/pgvector` | pg17 (0.8.7) | PostgreSQL | — | service |
| `mysql` | 8.4.11 | GPL-2.0-only WITH Universal-FOSS-exception-1.0 | — | service |
| `mariadb` | 11.8 (or 11.4) LTS | GPL-2.0-only | — | service |
| `cockroachdb/cockroach` | v26.2.7 / v26.3.2 | LicenseRef-CockroachDB-Software-License | single-node needs no key and is not throttled; multi-node needs a key after 7 days | ci-service |
| `mcr.microsoft.com/mssql/server` | 2022-latest | LicenseRef-Microsoft-SQL-Server-EULA | `ACCEPT_EULA=Y`; Developer edition, non-production; x86-64 only | ci-service |
| `gvenzl/oracle-free` (or `container-registry.oracle.com/database/free`) | 23.26.3 | LicenseRef-Oracle-FUTC | **Unverified**: the current tag on Oracle's registry, which needs a login | ci-service |
| `mongo` | 8.0 | SSPL-1.0 | — | ci-service |
| `cassandra` | 5.0.9 | Apache-2.0 | — | service |
| `valkey/valkey` | 8.1 | BSD-3-Clause | — | service |
| `redis` | 8.x (8.10.2) | AGPL-3.0-only OR LicenseRef-RSALv2 OR SSPL-1.0 | — | ci-service |
| `amazon/dynamodb-local` | 3.3.1 | LicenseRef-AWS-DynamoDB-Local-License | use only "in connection with the Services", with an AWS account in good standing; no redistribution | ci-service |
| `docker.elastic.co/elasticsearch/elasticsearch` | 9.x | AGPL-3.0-only OR SSPL-1.0 OR Elastic-2.0 (the default binaries are Elastic-2.0) | — | ci-service |
| `opensearchproject/opensearch` | 3.9.0 | Apache-2.0 | — | service |
| `qdrant/qdrant` | v1.19.2 | Apache-2.0 | — | service |
| `clickhouse/clickhouse-server` | 26.9 | Apache-2.0 | — | service |
| `trinodb/trino` | 483 | Apache-2.0 | — | service |
| `quay.io/lakekeeper/catalog` | v0.14.0 | Apache-2.0 | (FL1 pins 0.13.6; CN2 follows FL1's pin) | service |
| `rustfs/rustfs` | 1.0.1 | Apache-2.0 | — | service |
| MinIO | **no community image**: `minio/minio` on Docker Hub returns 404; the GitHub repository is archived ("distributed as source code only"); the last source release is `RELEASE.2025-10-15T17-29-55Z` | AGPL-3.0-only | — | ci-service, **built from the last source release or dropped** (R0.28) |
| `fsouza/fake-gcs-server` | 1.56.1 | BSD-2-Clause | — | service |
| `mcr.microsoft.com/azure-storage/azurite` | 3.37.0 | MIT | no DFS/HNS (§5.8) | service |
| `atmoz/sftp` | `alpine` (no version tags) | MIT | pin by digest | service |
| `neo4j` | 5.26-community | GPL-3.0-only | **Unverified**: that the Query API module is in Community. The docs do not say | service |
| `prom/prometheus` | v3.15.0 | Apache-2.0 | — | service |
| `openbao/openbao` | 2.7.1 | MPL-2.0 | — | service |
| `quay.io/keycloak/keycloak` | 26.8.0 | Apache-2.0 | — | service |
| `kestra/kestra` | v2.0.5 | Apache-2.0 | — | service |
| `stripe/stripe-mock` | v0.206.0 | MIT | — | service |
| `splunk/splunk` | 10.6.0 | LicenseRef-Splunk-General-Terms | `SPLUNK_START_ARGS=--accept-license` and `SPLUNK_GENERAL_TERMS=--accept-sgt-current-at-splunk-com`; a 60-day trial at 500 MB/day that converts to Free | ci-service |
| `quay.io/debezium/server` (fallback) | 3.7.0.Final | Apache-2.0 (+ bundled `ojdbc`, Oracle FUTC) | see §6 | fallback runtime |

## 8. Cross-workspace `loams.graph.v1` (OR-20)

There is a precedent already merged: `fabric/crates/loams-house-proto/build.rs` (HS1 Task 8, `20995130`).
- It generates `loams.house.v1` from the **repository root's `proto/`** inside the `fabric/` workspace.
- It reads `CARGO_MANIFEST_DIR` at run time, so the shared target directory does not bake in a worktree path.
- It also generates the `loams.options.v1`, `loams.errors.v1` and `loams.operations.v1` imports.

`proto/loams/graph/v1/graph.proto` imports exactly those three plus well-known types. So a cross-workspace build of the `loams.graph.v1` client is possible (R0.29).

**Unverified**: whether `connectrpc-build` 0.9 can map imported packages to another crate's types (an `extern_path` equivalent). If it cannot, generating the three shared packages a second time in `loams-flow-proto` would give `loams-house-proto` and `loams-flow-proto` two distinct Rust types for `loams.errors.v1.ErrorInfo`. That is harmless unless a crate links both.
