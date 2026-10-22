# HE1 — The Loams House Engine: Rust, ClickHouse-Compatible, Iceberg v3 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task, test first. Where this plan gives exact names, paths, flags or tests, use them verbatim. Record any deviation in the task's rulings file, `docs/plans/he1-rulings/t<N>.md` (indexed in "Rulings made during execution" at the end of this file).
>
> **Status: Approved** (owner, 2026-10-21; stub: HE0 specified, HE1–HE5 at milestone level). Track HE, design [§75](../design/75-loams-house-engine.md) (D1540–D1569, Q1170–Q1187). §75 was approved by the owner on 2026-10-21 with every default (Q1170 as ruled). This plan specifies **HE0** (the owner's Phase 0) task by task. HE1–HE5 are outlined at milestone level and get full task specifications when HE0's exit report is accepted. HE0 writes **no engine code**: it builds the mining tool, the oracle harness and the evidence that decides HE1's scope. It starts after v0.1.1 ships (Q1186).

**Goal (HE0):** know, from real applications rather than guesses, which ClickHouse features Loams House must support; have a harness that compares any House engine against `clickhouse-server` and chDB on those features; and run Langfuse and OpenPanel on today's House (chDB, Iceberg v2). Exit, from §75 §16.1: a mining report for 7 or more applications; Langfuse and OpenPanel run on the Phase 0 House.

**Architecture** (§75 §7, §15):
- **`conformance/clickhouse/apps/<app>/`**: one compose file per mined application, pinned by digest, with a `clickhouse-server` (pinned, `query_log` on) and a driver script that exercises the application (its own end-to-end tests where it has them, else a recorded traffic script).
- **`fabric/crates/loams-house-mine`**: a Rust CLI, `loams-house-mine`, that reads `system.query_log` dumps and migration files and writes a feature inventory (functions, engines, types, formats, settings, syntax forms) with counts per application.
- **`conformance/clickhouse/profile/`**: the generated profile (`profile.toml`) and the report (`docs/house/compat-profile.md`, generated).
- **`conformance/clickhouse/oracle/`**: a runner that executes a corpus on N targets (`clickhouse-server`, the chDB House over HTTP, later the Rust engine), normalises results and writes a triage file.
- **`conformance/clickhouse/stateless/`**: the manifest that filters ClickHouse's `tests/queries/0_stateless` by profile features.

**Tech stack:**
- Rust 1.97, edition 2024, the `fabric/` workspace's lints; `sqlparser` at the version the House pins (0.63.0, HS1 R0.7), `serde`, `toml`, `serde_json`, `clap`.
- Docker Compose v2; images pinned by digest at releases at least 14 days old.
- Python 3.13 with `uv` only where an application's own test runner needs it.
- The reference `clickhouse/clickhouse-server` at the version chDB carries (ClickHouse 26.9.2.1, HS1 R1.1), digest-pinned, CI only; a second pin at 24.3 LTS for the version-range question (Q1170).

**Spec:** [§75](../design/75-loams-house-engine.md) (all of it); [§49](../design/49-loams-house-production.md) §3–§9 and §16; [§32](../design/32-loams-flow-fabric-house.md) §8 and §10 (the surface and the harness, D347–D349); the [HS1 plan](2026-10-08-hs1-house-production.md) and its rulings (what is built).

## Global Constraints

- **Worktrees and branches.** `~/Documents/Ostriumlabs/loams-wt/he0-<task>`, one branch and one PR per task into `dev`. CodeRabbit reviews; merge on green. Commit with `git commit -s`.
- **Light Rust builds.** `cargo build -p loams-house-mine` from `fabric/`; never `--workspace` locally. Build output goes to the shared target in `~/Documents/.cargo/config.toml`; never set `CARGO_TARGET_DIR`, never build in `/tmp`. Stop compose stacks during builds.
- **No engine code in HE0.** No DataFusion dependency enters a merged crate. The dialect spike (Task 10) is a report; its code stays on a branch that is not merged.
- **Licences (§75 §18; FSL as BSL, D1574).** Applications run as unmodified containers pinned by digest; AGPL applications (OpenPanel, Plausible) are never linked, vendored or redistributed. No application source is committed. ClickHouse `.sql`/`.reference` test files may be vendored with attribution (Apache-2.0). ClickBench query text is never committed (D414). Every new crate passes `cargo deny` in `fabric/`.
- **No query text from real users.** Mining runs only on synthetic traffic in CI or local stacks. Dumps are scrubbed of literals before they are committed (`loams-house-mine scrub`).
- **GitHub-hosted runners only.** Application suites that take more than 30 minutes run nightly, not per PR.

## Review Focus

1. **A feature counted for an application that never used it.** Expected: never; every count links to a query id or a migration file line. Test: `inventory_counts_trace_to_sources`.
2. **A literal from a query reaches a committed file.** Expected: never. Test: `scrub_removes_every_literal` (property test over the corpus).
3. **The oracle reports a match when outputs differ only in ways the normaliser should not hide** (for example a different number of rows). Expected: a mismatch. Test: `normaliser_never_hides_row_count_or_types`.
4. **An application's licence is not recorded** before it runs in CI. Expected: the run refuses. Test: `apps_manifest_requires_licence`.

## HE0 — Mining, the oracle and the real applications (Tasks 0–12)

### Task 0 — Rulings

Check and record in "Rulings made during execution":
- **The House as built** at the current `dev`: which HS1 tasks are merged (§75 §3 says Tasks 0–13 and 15–23 have rulings), and what the House answers today for `ALTER … DELETE`, `ALTER … UPDATE`, TTL clauses, `FINAL` on lake tables and `Distributed` engine DDL. These are Langfuse's and OpenPanel's likely gaps.
- **Pins**: `clickhouse-server` 26.9.2.1 and 24.3 LTS digests; each application's image tag and digest at a release at least 14 days old; Langfuse v3 and v4 tags (Q1180).
- **Each candidate application** (Q1185): does it use ClickHouse today, which ClickHouse client and protocol (HTTP or native), its licence (GitHub licence API), and whether it has a runnable end-to-end suite. Record a table; drop any candidate that no longer uses ClickHouse.
- **§75 §4's facts** that HE0 depends on: iceberg-rust's v3 status at its newest release; re-read before Task 9.

Files: this plan. Tests: none. Gate: rulings recorded.

### Task 1 — The apps manifest and compose harness

`conformance/clickhouse/apps/manifest.toml`: per application `name`, `image` (by digest), `licence` (SPDX), `clickhouse_protocol` (`http` | `native`), `driver` (path to its driver script), `suite` (its own test command, if any), `runtime_minutes`. One compose file per application under `conformance/clickhouse/apps/<app>/compose.yaml` that starts the application, its other stores (Postgres, Redis) and a `clickhouse-server` with `query_log` and `part_log` on. `scripts/house/apps.sh <app> up|drive|dump|down`.

Tests: `apps_manifest_requires_licence`, `apps_manifest_pins_by_digest` (static checks in `fabric/crates/loams-house-mine/tests/manifest.rs`); a CI smoke job that brings Langfuse up and down.

Commit: `conformance: the ClickHouse apps manifest and harness (HE0 Task 1)`.

### Task 2 — `loams-house-mine`: the extractor

A CLI with subcommands `extract`, `merge`, `scrub` and `report`:
- `extract --app <name> --query-log <file.tsv> --migrations <dir>`: parses each statement with sqlparser's `ClickHouseDialect`; statements it cannot parse are sent to a pinned `clickhouse-server` as `EXPLAIN AST` and walked from that output. Writes `inventory/<app>.json`: for each feature (`function:<name>`, `engine:<name>`, `type:<name>`, `format:<name>`, `setting:<name>`, `syntax:<form>`) the count and the source ids.
- `merge`: combines inventories and ranks features by application count, then by frequency. A feature is in the profile if 3 or more applications use it, or if a gate application (Langfuse, OpenPanel) uses it (§75 §7.1).
- `scrub`: replaces literals in kept query samples with typed placeholders.
- `report`: writes `conformance/clickhouse/profile/profile.toml` and `docs/house/compat-profile.md`.

Tests: unit tests per feature kind over a hand-written fixture (`fixtures/queries.tsv`); `inventory_counts_trace_to_sources`; `scrub_removes_every_literal`; `unparsed_statements_are_counted_not_dropped`.

Commit: `house: loams-house-mine, the ClickHouse feature extractor (HE0 Task 2)`.

### Task 3 — Mine Langfuse

Run Langfuse v3 and v4 (Q1180) through Task 1's harness: migrations (clustered and unclustered), the application's own end-to-end tests if they run against ClickHouse, plus a driver that ingests traces, observations and scores through its public API and opens the dashboards. Dump, extract, commit the scrubbed inventory.

Check and record: `ReplacingMergeTree` tables and their version columns, `FINAL` use, mutation statements, partitioning, TTL, the formats and settings its client sends.

Commit: `conformance: Langfuse mining run (HE0 Task 3)`.

### Task 4 — Mine OpenPanel

As Task 3 for OpenPanel: its migrations, an event-ingest driver (through its HTTP API so Redis batching runs), and its dashboards, funnels and retention pages. Record whether `windowFunnel`, `retention` or `sequenceMatch` appear (§75 §7.4).

Commit: `conformance: OpenPanel mining run (HE0 Task 4)`.

### Task 5 — Mine five or more further applications

From Task 0's confirmed list (default Q1185: Plausible CE, SigNoz, Umami, PostHog self-hosted, and one more). One commit per application. Then `merge` and `report`: the first `profile.toml` and `docs/house/compat-profile.md`, with Tier 0, Tier 1 and Tier 2 proposals checked against §75 §7.3–§7.5. Disagreements between the mining and §75's tiers are listed for the owner, not resolved silently.

Commit: `conformance: mining runs and the first compatibility profile (HE0 Task 5)`.

### Task 6 — The oracle runner

`conformance/clickhouse/oracle/`: a Rust test binary in `loams-house-mine` (`oracle` subcommand) that runs a corpus of `.sql` cases on a list of targets (`clickhouse-server@26.9`, `clickhouse-server@24.3`, `house-chdb`), each over HTTP with `FORMAT JSONCompactEachRowWithNamesAndTypes`, and compares results after normalisation: float formatting to a declared precision, row order when the case has no `ORDER BY`, and output-format quirks. Writes `oracle/triage.toml` with each mismatch as `bug`, `incompatibility` (documented, with a reason) or `untriaged`. The corpus seeds are the scrubbed mined queries of Tasks 3–5 over generated data of the right schemas.

Tests: `normaliser_never_hides_row_count_or_types`; `unordered_results_compare_as_multisets`; `untriaged_mismatch_fails_the_run`.

Commit: `house: the oracle runner for ClickHouse-compatible engines (HE0 Task 6)`.

### Task 7 — ClickHouse stateless replay manifest

`conformance/clickhouse/stateless/manifest.toml`: the ClickHouse tag (26.9.2.1's), and the filter that keeps a `0_stateless` test when every function, engine, type, format and setting it uses is in `profile.toml` (Task 2's extractor run on the test file). The runner from D349 (HS1 Task 32's, if built; else the oracle runner) runs the kept tests on the chDB House and records the pass rate per category as the HE1 baseline.

Commit: `conformance: stateless tests filtered by the mined profile (HE0 Task 7)`.

### Task 8 — The function and engine matrix

`loams-house-mine matrix`: reads `system.functions` and `system.table_engines` from the pinned chDB and writes `docs/house/compat-matrix.md` with each entry marked `profile` (in Tier 0 or 1), `refused` (Tier 2, `48`), `disabled` (security, `344`) or `unclassified`. A CI check fails on `unclassified` entries for features the mining found.

Commit: `house: the generated function and engine matrix (HE0 Task 8)`.

### Task 9 — Langfuse and OpenPanel on today's House

> **Amended by D1571 (owner, 2026-10-22; [R9.60](he1-rulings/t9.md)).** SigNoz (its MIT build, `signoz/signoz-community`) is the third gate application and runs fully on the House; its gate job is `signoz_on_house_smoke`.

> **Remaining (owner, 2026-10-22: House work stops after the open PRs; [R9.64](he1-rulings/t9.md)).** For SigNoz: refreshable materialized views (`REFRESH EVERY … APPEND TO`, designed in R9.64, not built), L's DDL items from the first SigNoz replay, and the gate job with its House overlay. Done: D1572 and R9.61 (nanosecond times), R9.62 (cluster `system` tables) and R9.63 (`JSON` columns).

Point each application at the chDB House (lake tables, Iceberg v2) instead of `clickhouse-server` and run its suite and driver. For each failure: a fix in the House if HS1 has the piece and it is small; else a ruling that names the gap and the HE phase that closes it (for example mutations, D1552, HE2). Exit for this task: both applications start, ingest and render their main pages on the House, and the remaining gaps are listed.

Tests: `langfuse_on_house_smoke` and `openpanel_on_house_smoke` in a nightly job.

Commit: `conformance: Langfuse and OpenPanel against the House (HE0 Task 9)`.

### Task 10 — Spike: the ClickHouse dialect on DataFusion (report only)

On an unmerged branch: parse every mined statement with sqlparser's `ClickHouseDialect` and record the parse rate; plan 20 representative Tier 0 queries into DataFusion logical plans with stub functions; record what DataFusion lacks (`LIMIT BY`, `WITH TOTALS`, `ARRAY JOIN`, `FINAL`, the combinators) and the cost of each. Pick the versions of DataFusion and iceberg-rust that share an Arrow major (Q1175). Write the result to `docs/plans/he0-dialect-spike.md`.

Commit: `docs: the HE0 dialect spike report (HE0 Task 10)`.

### Task 11 — Spike: iceberg-rust v3 (report and upstream issues)

Write Iceberg v3 tables with Spark (deletion vectors, row lineage, Variant) and read them with the newest iceberg-rust; record what reads, what fails and what is missing for writes. File or link upstream issues for each gap (D1553), with the owner's consent before anything is posted. Record the interop baseline: today's House v2 tables read by DuckDB, Spark and Trino. Write `docs/plans/he0-iceberg-v3-spike.md`.

Commit: `docs: the HE0 Iceberg v3 spike report (HE0 Task 11)`.

### Task 12 — HE0 exit

An exit report in `docs/plans/he0-exit-report.md`: the profile and its tiers, the oracle's first triage, the stateless baseline, Langfuse and OpenPanel on the House, the two spikes, and a proposed HE1 task list with estimates. The owner reviews it before HE1 is specified.

Gate: Tasks 1–11 merged; the report written; §75's open questions that HE0 answers (Q1170, Q1175, Q1180, Q1185) proposed for the owner with evidence.

## Queued gate applications, after SigNoz (Tasks 13–15)

> **Queued by the owner 2026-10-22** ([R13.1](he1-rulings/t13.md), [R14.1](he1-rulings/t14.md), [R15.1](he1-rulings/t15.md)); Laminar replaces Helicone and the order is fixed (owner, 2026-10-22; [R14.2](he1-rulings/t14.md)). These are stubs. **None of them starts** until SigNoz's gate (Task 9, D1571) is done or the owner says so. They are not part of the HE0 exit (Task 12) unless the owner adds them. Evidence for every licence, protocol and stack fact below is in [`docs/research/clickhouse-apps.md`](../research/clickhouse-apps.md) (checked 2026-10-22). When a task starts, its application becomes a gate application under §75 §7.1's rule, and a decision from the reserved D1572–D1579 records that.

Each task follows Task 9's pattern: a manifest entry and compose stack (Task 1), a driver, a mining run (Tasks 2–5) against the 26.9.2.8 oracle, a replay of the oracle traffic against the House (R9.3), then fixes or rulings per gap, and a nightly `<app>_on_house_smoke` job. Supporting images follow R0.4 (Valkey for Redis, RustFS for MinIO).

**Order after SigNoz (owner, 2026-10-22; R13.2, R14.2, R15.2): Task 15 (BI tools) first, then Task 13 (OpenMeter), then Task 14 (Laminar).** Each starts when the one before it is done, unless the owner says otherwise.

### Task 13 — Queued gate: OpenMeter

**Why.** OpenMeter is the model and event format Loams Meter follows (§63 D1121, D1124, D1131). Running OpenMeter on the House dogfoods §63's House half: the same usage events and meter queries, on the store Loams Meter itself uses.

**Application.** `openmeterio/openmeter`, Apache-2.0, no `ee/` split; owned by Kong since 2025-09. Releases are weekly `v1.0.0-beta.N` tags (`v1.0.0-beta.236`, 2026-10-09); pin one at least 14 days old, image `ghcr.io/openmeterio/openmeter` by digest (one image, six commands).

**Protocol: native only.** OpenMeter opens ClickHouse with `clickhouse-go` v2 and no protocol option (`app/config/aggregation.go`, default address `127.0.0.1:9000`), for inserts, migrations and queries. So this gate **needs the native listener built for SigNoz** (D1571); it is the second native-only gate and reuses that work. Whether an `http://` address is accepted is unknown (verify; the config builds `clickhouse.Options` without `Protocol`).

**How OpenMeter's use maps onto §63.**

| OpenMeter | Loams Meter (§63) | What the gate shows |
|---|---|---|
| `om_events`: `MergeTree`, `PARTITION BY toYYYYMM(time)`, `ORDER BY (namespace, type, subject, toStartOfHour(time))`, `data String` (JSON text), `minmax` index on `stored_at` | `_meter.events` (§7.5): `ReplacingMergeTree`, `PARTITION BY toYYYYMM(time)`, `ORDER BY (subject, type, source, id)`, `data String` | The same CloudEvents layout on a lake table. OpenMeter deduplicates **before** insert (Redis; Valkey in the harness); §63 deduplicates in the table with `FINAL` (D1122) |
| Ingest: `POST /api/v1/events` → Kafka → sink worker → batched `INSERT` (optionally `async_insert`) | `_system/_meter` stream → House pipe (`LoamsStream`) exactly once (D769) | The House takes OpenMeter's batched native inserts; the pipe path is not exercised (OpenMeter keeps its Kafka) |
| Meter queries at read time: `tumbleStart`/`tumbleEnd` over minute, hour, day and month windows with a time zone; `sum`, `count`, `uniqExact`, `avg`, `min`, `max`, `argMax` (LATEST) over `JSON_VALUE(data, path)` with `toFloat64OrNull` or `toDecimal128OrNull`, `ifNotFinite`, `nullIf`; group-by on `JSON_VALUE`; optional `PREWHERE` | §8.5: closed windows from `_meter.windows FINAL` plus the open edge aggregated from raw events "directly"; §8.1's aggregations are OpenMeter's | OpenMeter's meter queries **are** the shape of §8.5's open-edge query and of §8.2's close job. Its answers on the House against the oracle check those queries before ME1b writes them |
| Entitlements, grants, plans and billing in Postgres (`ent`); the balance worker reads meter queries | §9 entitlements in the metastore; §12 reconciliation reads House totals | Only the metering path touches the House. Entitlement semantics stay with ME1c's OpenMeter API conformance test (§17.2), not this gate |
| REST `GET /api/v1/meters/{slug}/query` | §17.2's OpenMeter-compatible subset | Once ME1b exists: send the same events to OpenMeter-on-House and to Loams Meter and compare the two `query` answers. That is the dogfood check |

**Scope.** Server, sink worker, balance worker and jobs; the notification service and billing worker start but are not driven; Svix (webhooks) off. Driver: create meters of every aggregation; ingest CloudEvents (`application/cloudevents-batch+json`) including duplicates and late events; query every window size, two time zones, group-by and subject filters; create an entitlement with grants so the balance worker runs its meter queries. OpenMeter's own Go `e2e/` suite needs its source (R0.7: the driver is the default).

**Harness needs.** Kafka (their compose uses `confluentinc/cp-kafka:8.0.3`; use an Apache Kafka image if OpenMeter accepts it, verify), Postgres, Valkey for the Redis dedupe driver, the native port on the House, oracle ClickHouse 26.9.2.8 (OpenMeter pins 25.12.3).

**Expected House gaps** (from the source, to be measured): the native protocol (D1548, D1571); the `minmax` skip index in `CREATE TABLE` (skip indexes are stripped in R9.3's discovery runs); `PREWHERE` on the Rust engine (R10.5; chDB is fine); `tumbleStart`/`tumbleEnd`, `JSON_VALUE`, `toDecimal128OrNull` and `ifNotFinite` in the Tier 0 profile (chDB has them); `async_insert` settings when enabled (R4.5); deletes or mutations, if OpenMeter issues any (not seen; verify).

Tests: `openmeter_on_house_smoke` (nightly). Commit: `conformance: OpenMeter on the House (HE0 Task 13)`.

### Task 14 — Queued gate: Laminar (LLM and agent observability)

> **Laminar replaces Helicone** (owner, 2026-10-22; [R14.2](he1-rulings/t14.md)), with **Opik** as the next candidate. Helicone was queued first (R14.1) and dropped: its public images and releases stop at 2025-08-21 and its development moved to a private repository. Third in the order after SigNoz.

**Application.** `lmnr-ai/lmnr`, Apache-2.0 (`LICENSE.md`), no split in the repository; `frontend-ee` and `app-server-ee` images exist on GHCR from code that is not in the repository (verify), and are never used. Releases `v0.3.2` (2026-10-16), `v0.3.1`, `v0.3.0`; pin one at least 14 days old, `ghcr.io/lmnr-ai/app-server` and `ghcr.io/lmnr-ai/frontend` by digest (their compose uses unpinned tags).

**Protocol: HTTP only.** The Rust app-server uses the `clickhouse` crate 0.15 (HTTP, with `RowBinary` for inserts and typed reads); the Next.js frontend uses `@clickhouse/client` and runs the migrations (`frontend/lib/clickhouse/migrations/*.sql`, over HTTP; verify). Both use `CLICKHOUSE_URL=http://clickhouse:8123`. So this gate runs on today's House and does not wait for the native listener.

**Scope.** app-server and frontend; ingest spans through Laminar's OpenTelemetry endpoint and its SDK's HTTP API (traces, spans, evaluations, events), then open the traces, spans, sessions, evaluations and dashboard pages and the SQL editor (a read-only ClickHouse user, `CLICKHOUSE_RO_USER`).

**Harness needs.** Postgres, Quickwit (full-text search; Apache-2.0 at v0.9.1, verify), and for the full file RabbitMQ and Valkey. Oracle 26.9.2.8 (Laminar pins 26.5).

**Expected House gaps** (from its migrations, to be measured): the `RowBinary` input and output formats over HTTP (chDB has them; the House's HTTP front must pass them through); `ReplacingMergeTree` read with `FINAL` (supported); `tokenbf_v1` skip indexes (stripped in R9.3's discovery runs); a second, read-only user with its own grants (the House classifies `GRANT`/`REVOKE` as unsupported today).

**Next candidate: Opik** (`comet-ml/opik`, Apache-2.0, releases daily): HTTP only, but a clustered schema (`ON CLUSTER`, `ReplicatedReplacingMergeTree`) that needs D1571's cluster-DDL forms, `Map`, `JSON` and `Time` types, Liquibase migrations over JDBC, and up to 14 services with ZooKeeper. Taken up only if the owner adds it after Laminar.

Tests: `laminar_on_house_smoke` (nightly). Commit: `conformance: Laminar on the House (HE0 Task 14)`.

### Task 15 — Queued gate: BI tools (Grafana and Metabase; Superset and Redash optional)

**Why.** Every House user's dashboards and BI tools browse the schema through `system.*` and send generated SQL. These tools need no application stack, so the gate is cheap.

**Tools** (all unmodified containers, D1566):
- **Grafana** (AGPL-3.0) with **`grafana/clickhouse-datasource`** (Apache-2.0, v4.23.1 of 2026-10-21): `clickhouse-go` v2, **native by default, HTTP by its `protocol` setting**. Run it **twice, once per protocol**.
- **Metabase** `metabase/metabase` (the AGPL image; never `metabase-enterprise`), whose ClickHouse driver is in core since Metabase 54 (`modules/drivers/clickhouse`, `clickhouse-jdbc` 0.9.8): **HTTP**.
- Optional: **Superset** (Apache-2.0) with `clickhouse-connect` (HTTP); **Redash** (BSD-2-Clause, HTTP).

**Scope.**
1. Connect: each tool's "test connection" (`SELECT version()`, Metabase's `SELECT currentDatabase()` and `system.settings` `cloud_mode` read).
2. Browse the schema: `SHOW DATABASES`, `SHOW TABLES [FROM db]`, `DESC TABLE`, `SHOW CREATE TABLE`, `SHOW COLUMNS`, `EXISTS DATABASE`/`EXISTS TABLE`, `system.databases`, `system.tables` (`engine`, `engine_full`, `comment`; views and materialized views by `engine`), `system.columns` (`name`, `type`, `table` per database), `system.functions` (Grafana's and Superset's editor completion; Superset also `system.table_functions`), Metabase's JDBC `DatabaseMetaData` (`getTables`, `getColumns`, which `clickhouse-jdbc` answers from `system.*`; verify the exact queries).
3. Standard queries: Grafana's query builder (time series with `$__timeFilter` and `$__timeInterval` macros, tables, logs and traces modes, ad hoc filters, template variables, `Map` key discovery with `mapKeys`, JSON path discovery with `JSONAllPaths` and `distinctJSONPathsAndTypes`); Metabase's question builder (filters, breakouts by date unit, aggregations, joins, native questions) over the sample data loaded into lake tables.
4. Dashboards: Grafana's bundled dashboards (`data-analysis`, `query-analysis`, `cluster-analysis`, OTel logs, traces and service); one Metabase dashboard built by the driver.

**Harness needs.** No application stack: a seeded House namespace (lake tables of several engines and types, a view, a materialized view, the OTel schema for Grafana's OTel dashboards), the oracle with the same data, and a Playwright or API driver per tool (Grafana's `/api/ds/query`, Metabase's `/api/dataset`). Mining (Tasks 2–5) records each tool's statements; answers compared against the oracle as R9.3 does.

**Expected House gaps.**
- `system` tables outside `ALLOWED_SYSTEM_TABLES` (`deny.rs`): `system.clusters` (Metabase's single-node check, which it catches; Grafana's cluster dashboard), `system.merges`, `system.mutations`, `system.replicas`, `system.detached_parts`, `system.disks`. Grafana's `cluster-analysis` and `data-analysis` dashboards fail on these unless the House answers them (empty for a one-node House is the likely ruling).
- `system.query_log` is served from `_house.query_log`: Grafana's `query-analysis` dashboard reads columns that may be blank or missing there (verify).
- `system.functions` lists chDB's functions, including ones the House disables (`344`), so editors offer functions that then fail.
- `system.tables.engine` shows lake engines as the House names them; `engine_full` and `comment` may be default values (`system.rs` fills what it knows).
- The `JSON` type, `JSONAllPaths` and `distinctJSONPathsAndTypes` on lake tables (verify the House's JSON support).
- `SHOW CREATE TABLE` for lake tables (Metabase parses it for skip indexes); the House's answer must parse.
- Native protocol for Grafana's default mode (D1571's listener).

Tests: `bi_grafana_on_house_smoke` (both protocols) and `bi_metabase_on_house_smoke` (nightly). Commit: `conformance: Grafana and Metabase against the House (HE0 Task 15)`.

## HE1 — The read-only Rust engine (milestone level)

- Crates `loams-house-sql` (ClickHouse-dialect frontend to DataFusion logical plans), `loams-house-exec` (Tier 0 functions, combinators, `LIMIT BY`, `WITH TOTALS`, `ARRAY JOIN`, `FINAL` as read-time deduplication) and `loams-house-iceberg` (v2 and v3 reads, deletion vectors applied, pruning by partition, statistics, page index and bloom filters), in `fabric/` (D1550).
- A Rust worker behind the existing front over `hsw1`, selected per namespace (`engine = rust | chdb`), with the oracle comparing both on every PR.
- The foyer cache in process (D1557) and `loams-sandbox` around the worker (D1558).
- Exit: the Tier 0 query set passes against the oracle; at least 50 % of the filtered stateless tests pass.

## HE2 — Writes, deletion vectors and the release gate (milestone level)

- The fresh tier (D1555): a Loams stream as the WAL, in-memory parts, L0 Parquet, compaction, batched commits under a metastore lease (D1554).
- Deletion vectors (Puffin) for `ReplacingMergeTree`, `ALTER … DELETE/UPDATE` and TTL (D1552); upstream iceberg-rust PRs (D1553), with v2 and position deletes as the fallback.
- The NDJSON ingest API with quarantine and endpoint pipes through `loams-gateway` (D1559).
- The commit protocol's TLA+ model in `spec/tla/` and its simulation (D1554).
- Exit: Langfuse and OpenPanel pass their own suites; the Iceberg interop matrix is green; the Rust engine becomes the default (D1542).

## HE3 — Distributed and serverless (milestone level)

- Fragments by file and row group, work stealing, direct and S3 exchanges (D1563); cold-start work (snapshot summaries, NVMe footer cache, warm pools).
- Compute targets (D1562): pods and Fargate first; the Lambda spike (Q1183); microVM pools through `loams-sandbox`.
- Aggregation and join optimisations; the performance gates of §75 §15 (ClickBench internal, D414).
- Exit: scale to zero and burst scale-out demonstrated; the ClickBench target met; libchdb leaves production images (D1567).

## HE4 — Tier 1 (milestone level)

`AggregatingMergeTree`, `SummingMergeTree`, incremental materialized views on row lineage, funnel and retention functions, Variant, skip, full-text and vector indexes with the engine's index code (Q1181); the product layer in beta (D1559–D1561).

## HE5 — On demand (milestone level)

Tier 2 items users ask for; the native protocol (D1548) if users ask for it.

## Rulings made during execution

Rulings live in one file per task under [`he1-rulings/`](he1-rulings/), so tasks running in parallel do not conflict on one table. Each file keeps this plan's columns (`#`, Ruling, Why, Cost if wrong) and its own numbering (R<task>.<n>).

| Task | Rulings |
|---|---|
| 0. Rulings | [t0.md](he1-rulings/t0.md) |
| 1. The apps manifest and compose harness | [t1.md](he1-rulings/t1.md) |
| 2. loams-house-mine, the extractor | [t2.md](he1-rulings/t2.md) |
| 3. Mine Langfuse | [t3.md](he1-rulings/t3.md) |
| 4. Mine OpenPanel | [t4.md](he1-rulings/t4.md) |
| 5. Mine five or more further applications | [t5.md](he1-rulings/t5.md) |
| 6. The oracle runner | [t6.md](he1-rulings/t6.md) |
| 7. ClickHouse stateless replay manifest | [t7.md](he1-rulings/t7.md) |
| 8. The function and engine matrix | [t8.md](he1-rulings/t8.md) |
| 9. Langfuse and OpenPanel on today's House | [t9.md](he1-rulings/t9.md) |
| 10. Spike: the ClickHouse dialect on DataFusion | [t10.md](he1-rulings/t10.md) |
| 11. Spike: iceberg-rust v3 | [t11.md](he1-rulings/t11.md) |
| 12. HE0 exit | [t12.md](he1-rulings/t12.md) |
| 13. Queued gate: OpenMeter | [t13.md](he1-rulings/t13.md) |
| 14. Queued gate: Laminar (was Helicone) | [t14.md](he1-rulings/t14.md) |
| 15. Queued gate: BI tools | [t15.md](he1-rulings/t15.md) |
