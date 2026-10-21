# HE1 — The Loams House Engine: Rust, ClickHouse-Compatible, Iceberg v3 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task, test first. Where this plan gives exact names, paths, flags or tests, use them verbatim. Record any deviation in "Rulings made during execution" at the end of this file.
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
- **Licences (§75 §18).** Applications run as unmodified containers pinned by digest; AGPL applications (OpenPanel, Plausible) are never linked, vendored or redistributed. No application source is committed. ClickHouse `.sql`/`.reference` test files may be vendored with attribution (Apache-2.0). ClickBench query text is never committed (D414). Every new crate passes `cargo deny` in `fabric/`.
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

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| R0.1 | **HS1 as built (checked 2026-10-21 at `3617814c8`).** Tasks 0–13 and 15–23 have rulings (R0.1–R23.16, plus R-ACL.1) and are merged; Task 14 (Fluss tables) and Tasks 24–38 have none and are not built. §75 §3 holds | HS1 plan "Rulings made during execution"; merge commits `hs1-b2-t12` … `t23` on `dev` | Low |
| R0.2 | **What the House answers today for the gate applications' likely gaps** (`fabric/crates/loams-house*`, read, not run): `ALTER … DELETE` is **62** ("outside the House's ClickHouse surface"; sqlparser cannot parse it; `classify.rs` `OUTSIDE_SURFACE`, pinned by `loams-house/tests/classify.rs` and `loams-house-worker/tests/sessions.rs`); `ALTER … UPDATE` is **62** by the same path (not pinned by a test); lightweight `DELETE FROM` is **48** (`Statement::Delete` falls to `Unsupported`; not pinned); a `TTL` clause, table or column, is **48** "TTL is not supported on lake tables (§49 §6.3)" (`ddl.rs`, pinned by `lake.rs`), and `ALTER … MODIFY TTL` most likely **62**; `FINAL` **works** on lake `ReplacingMergeTree` (keeps the greatest `(ver, file seq, row)` per sorting key, drops `is_deleted = 1`) and `SummingMergeTree`, also as `SETTINGS final = 1` (`read.rs`, `views.rs`; pinned by `read.rs` and `compact.rs` tests); `ENGINE = Distributed(…)` is **344** "Engine Distributed is disabled" (`deny.rs` `DENIED_ENGINES`; no test); `ON CLUSTER` and `Replicated*MergeTree('/path', '{replica}', …)` are accepted and ignored (D344; `ddl.rs`). Lake tables take only `MergeTree`, `ReplacingMergeTree` and `SummingMergeTree` (others 48). Materialized views exist only as `CREATE MATERIALIZED VIEW … TO <lake table>` (no `POPULATE`, no inner table). `system.query_log` is served from `_house.query_log`. **So Langfuse's deletes and both applications' TTLs fail today, and `Distributed` would answer 344 where §75 §7.3 wants it accepted and ignored**: Task 9 records each as a gap (D1552, HE2) unless a small fix exists | An Explore pass over `classify.rs`, `ddl.rs`, `deny.rs`, `read.rs`, `views.rs`, `mv.rs`, `system.rs` and their tests | Medium: an inferred answer (UPDATE, DELETE FROM, MODIFY TTL, Distributed) may differ; Task 9 measures them |
| R0.3 | **The ClickHouse pins.** chDB's `SELECT version()` is 26.9.2.1 (HS1 R1.1), and **no `clickhouse-server` image is tagged 26.9.2.1**: the 26.9.2 line's only build is **26.9.2.8** (`docker.io/clickhouse/clickhouse-server:26.9.2.8@sha256:7b9a0b6e9ab7ef79db5c4f8a9808f880258704eb7b4af9309f9029c16e727a80`, created 2026-09-25, `com.clickhouse.build.version` 26.9.2.8), which is the oracle. The floor (Q1170, Langfuse v3) is **24.3.18.7**, the last 24.3 LTS build (`@sha256:85b97f63dcfff47790d26bb5d5801637aaddb2b93e5e9aee27a686c2fb2b9916`, 2025-02-19). Digests are the multi-arch OCI index (amd64, arm64). The 26.9 line has reached 26.9.23.15 (2026-10-20); moving the oracle off chDB's base is a separate ruling when chDB moves | `skopeo inspect` and `list-tags` on Docker Hub, 2026-10-21 | Low: a 26.9.2.1-versus-26.9.2.8 difference is a patch-level bug fix; the oracle runner (Task 6) records `version()` per target |
| R0.4 | **Application pins** (releases at least 14 days old on 2026-10-21; digests are multi-arch indexes): **Langfuse v4 4.54.0** (2026-10-07; `langfuse/langfuse@sha256:ea9f763a…53f2c`, `langfuse/langfuse-worker@sha256:fe7ea9e2…641f2`) and **Langfuse v3 3.225.11** (2026-09-24; `@sha256:a343f64e…dda878`, worker `@sha256:8a28c946…ebd3`), so both lines are mined (Q1180); **OpenPanel 2.3.0** (`lindesvard/openpanel-api`, `-dashboard`, `-worker`, 2026-08-18): OpenPanel's v3.x GitHub releases (from 2026-10-18) are all pre-releases with no images on Docker Hub, so 2.3.0 is its newest stable image. Supporting images, also by digest: `rustfs/rustfs:1.0.0` (the repository's pin) in place of MinIO, which publishes no current images; `valkey/valkey:8.1.3` (BSD-3-Clause) in place of Redis 7.4+, whose licence is not OSI; `postgres:17.6`; `caddy:2.10.2-alpine`; `curlimages/curl:8.16.0`. Full digests are in `conformance/clickhouse/apps/manifest.toml` (Task 1) | GitHub releases API and Docker Hub, 2026-10-21 | Low |
| R0.5 | **Langfuse's migrations use the native protocol** (`CLICKHOUSE_MIGRATION_URL=clickhouse://…:9000`, golang-migrate) while its queries use HTTP (`CLICKHOUSE_URL`, `@clickhouse/client`). The House has no native protocol (moved to HE5, D1548), so **Task 9 cannot run Langfuse's migrator against the House unchanged**: it applies the same migration files over HTTP (from the image, never committed) and records that as a gap. The manifest gets a `migrations_protocol` field for this | Langfuse's `docker-compose.yml` at v4.54.0 | Medium: if no HTTP path works, Task 9's exit for Langfuse needs the owner's call |
| R0.6 | **Candidate applications (Q1185), checked 2026-10-21** (GitHub licence API; client and protocol from each repository's manifests and compose files): | | |
| | • **Langfuse**: ClickHouse core store; HTTP (`@clickhouse/client`), migrations native; MIT with `ee/` dirs; e2e suite in its repository (needs source). **Kept (gate).** | | |
| | • **OpenPanel**: events store; HTTP (`@clickhouse/client`); AGPL-3.0; e2e needs source. **Kept (gate).** | | |
| | • **Plausible CE**: core event store; HTTP (Elixir `ch`/`ecto_ch`), migrations HTTP too; AGPL-3.0; `mix test` and Playwright against ClickHouse 25.11 in CI (needs source); `ghcr.io/plausible/community-edition:v3.2.1` (2026-05-15, `@sha256:33e60bfb…62985`); self-host compose pins ClickHouse 24.12; stack Postgres + ClickHouse. **Kept.** | | |
| | • **SigNoz**: all telemetry; **native** (`clickhouse-go` v2, `tcp://…:9000`) for queries, collector and migrator; MIT except `ee/`, collector AGPL-3.0; pytest integration suite (label-gated); `signoz/signoz:v0.145.0` (2026-10-05), collector `v0.144.12` (pairing unverified); needs ZooKeeper; compose deprecated for Foundry. **Kept** (the only native-protocol app: useful evidence for D1548). | | |
| | • **Umami**: ClickHouse is **optional** (`CLICKHOUSE_URL`), Postgres always; HTTP (`@clickhouse/client`); MIT; plain SQL migrations; `pnpm test:api:clickhouse` (not in CI); `ghcr.io/umami-software/umami:3.4.0` (2026-09-17); its test compose pins ClickHouse 25.8 and notes 26.7+ rejects its schema (`allow_dimensions_outside_sorting_key`). **Kept**, with that incompatibility recorded as a finding for Task 5. | | |
| | • **PostHog (hobby)**: core store; **native** (`clickhouse-driver`), also `clickhouse-connect` (HTTP); MIT except a large `ee/`; CI suites against ClickHouse; **no versioned releases** (rolling `hobby-*` tags, none 14 days old found), and a stack of about 20 services (Redpanda, ZooKeeper, Temporal, Elasticsearch, …). **Kept, pinned by digest of a rolling tag** and run last; the 14-day rule is waived for it only, recorded per run. | | |
| | • **Highlight**: uses ClickHouse (native, `clickhouse-go`), Apache-2.0 except `highlight.io/` and `enterprise/`; last image release 2025-08-08 and rolling `latest` tags; Kafka + ZooKeeper stack. **Dropped** for the stronger candidate below. | | |
| | • **RudderStack**: ClickHouse only as a warehouse **destination**, not its store; ELv2 (banned, D11). **Dropped.** | | |
| | • **HyperDX / ClickStack** (the "one more"): ClickHouse is its only telemetry store; queries HTTP (`@clickhouse/client`), collector and migrations native; MIT; jest integration and Playwright suites against ClickHouse 26.8 in CI; `hyperdx/hyperdx:2.40.0` (2026-10-01, `@sha256:2fcba681…ddf60`) and `clickhouse/clickstack-otel-collector:2.40.0`; stack MongoDB + ClickHouse + collector. **Kept** as Q1185's seventh application | A research pass over each repository (GitHub API, file reads, no clones) and Docker Hub/GHCR | Low: Task 5 re-checks each pin when it runs |
| R0.7 | **The applications' own suites need their source**, which the shared rules forbid cloning without the controller's consent. Tasks 3–5 run each application's **driver** (public API and UI traffic) as the default; `suite` stays empty in the manifest until the controller allows a source checkout (outside the repository, never committed) for a given application | Plan Task 3 ("its own end-to-end tests if they run against ClickHouse"); the no-cloning rule | Medium: drivers may miss queries only the suites send; the mining diff on a later suite run catches them |
| R0.8 | **§75 §4.1 holds on 2026-10-21**: iceberg-rust's newest release is still 0.10.1 (2026-08-01; crates.io max stable 0.10.1); the v3 epic #2411 is open; the deletion-vector read epic #2792 is closed; #2580 (deletion-vector writer and `RowDelta`), #2879 (`_row_id` reads), #3401 (lineage through rewrites), #2923 (REST catalog ignores the format version) and #2546 (Variant writer annotation) are open. Note: #2580 is an **issue**, not a PR as §4.1 says. Re-read before Task 9 | GitHub API and crates.io, 2026-10-21 | Low |
| R10.1 | **Task 10's report is `docs/plans/he0-dialect-spike-report.md`**, not `he0-dialect-spike.md` as Task 10 says, at the controller's instruction; the spike's code is on the unmerged branch `he0-t10-dialect-spike` (`spikes/he0-dialect/`, its own Cargo workspace), so no DataFusion dependency entered a merged crate | The controller's brief for agent D, 2026-10-21 | Low |
| R10.2 | **Versions (Q1175): DataFusion 54.1.0 with iceberg 0.10.1 on Arrow 58** (`cargo tree` shows one `arrow-array`, 58.4.0); DataFusion 55.2.0 beside iceberg 0.10.1 brings Arrow 58 and 59. Move to DataFusion 55.x with iceberg 0.11 (Arrow 59, `fabric/`'s Arrow) when iceberg 0.11 is released and 14 days old (iceberg-rust `main` is on DataFusion 55 and Arrow 59.2 since apache/iceberg-rust#2958). `iceberg-datafusion` is not used (0.10.1 needs DataFusion `^53.1`, and the integration moved to `apache/datafusion-iceberg`, #3228); `loams-house-iceberg` builds its own `TableProvider`. HE1 Task 0 re-checks | crates.io and GitHub, 2026-10-21; spike report §5 | Medium: if HE1 needs iceberg-rust `main`'s deletion-vector reads before 0.11 ships, the fallback is Set B through a `[patch]` git dependency |
| R10.3 | **DataFusion 54.1 and 55.x link sqlparser 0.62, not the House's 0.63**, so HE1's `loams-house-sql` parses with `datafusion_sql::sqlparser`; the front may keep 0.63 for its classifier (the two agree on the spike's corpus but for one statement) or move to 0.62. DataFusion `main` is on 0.63 and Arrow 60 | DataFusion's published manifests; spike report §2.1, §5 | Low |
| R10.4 | **The parse rate was measured on a hand-written 195-statement Tier 0 corpus** (no application source, no ClickBench text), because Tasks 3–5's mined statements had not landed: 77.9 % parse and 75.4 % parse faithfully with sqlparser 0.63 (`SELECT` 87.0 %, `CREATE` 46.4 %, `ALTER` 29.4 %), and the HS1 differential corpus parses 100 %. Re-run on the mined statements when they land (report §8) | Spike report §2 | Medium: the hand-written corpus measures coverage, not application frequency |
| R10.5 | **DataFusion 54.1's planner silently drops `PREWHERE`, `GROUP BY … WITH TOTALS/ROLLUP/CUBE`, function parameters (`quantile(0.95)(x)` → `quantile(x)`) and `SAMPLE`**, and sqlparser reads `FROM t FINAL` as an alias; DataFusion also lowercases every unquoted function name. HE1's first task is a frontend guard that rewrites or refuses (`48`) every such field before planning, with a test that fails on any ignored field | `datafusion-sql` 54.1.0 source; spike report §3.1 | High if skipped: wrong results with no error |
| R10.6 | **Recommendation for HE1: our own frontend and extension nodes on sqlparser and DataFusion as libraries** (DataFusion's `TypePlanner`, `RelationPlanner`, `ExprPlanner`, `HigherOrderUDF`, extension nodes, a ClickHouse coercion rule); upstream only dialect-neutral fixes (sqlparser's ClickHouse syntax gaps; DataFusion errors instead of silent drops, function-name case). Tier 0 dialect work about 100–140 engineer-days (estimate). With the frontend all 20 representative queries plan (6 as shipped, 5 silently wrong). No upstream issue or PR was posted; the candidates (report §7) wait for the owner's consent | Spike report §4, §6, §7 | Medium: revisit at HE1's midpoint if the oracle shows rules a rewrite cannot express |
| R11.1 | **iceberg 0.11 stays on Arrow 58.4 and DataFusion 54, and already reads deletion vectors and row lineage.** The `0.11.x` branch was cut on 2026-09-03 and holds the DV reads (#2866, #2868, #3035) and the lineage reads (#2922, #2966, #2985, #3058). v0.11.0-rc.2 (`859048563bc7`) reads every Spark-written DV table correctly, where 0.10.1 fails (`Corrupt footer`). 0.11.0 has no date (#3034; blocked on licence files; "not before November", #3290), and 0.11.x is the last release governed by the Apache Iceberg project. **This amends R10.2's plan:** DataFusion 55 waits for the first iceberg release on Arrow 59.2 (after 0.11; no date), not for 0.11. If HE1 needs DV reads before 0.11.0 ships, pin iceberg by git at rc.2 or the `0.11.x` head on DataFusion 54.1; `main` on DataFusion 55 only for something 0.11.x lacks | Task 11 spike report §5; GitHub compare API, #3034, #3290, 2026-10-21 | Medium: if 0.11.0 is dropped for a re-cut release, the pin moves to that release (the same Arrow major is not guaranteed) |
| R11.2 | **Task 11's report is `docs/plans/he0-iceberg-v3-spike-report.md`**, not `he0-iceberg-v3-spike.md` as Task 11 says, at the controller's instruction (as R10.1). The code and raw outputs are on the unmerged branch `he0-iceberg-v3-spike` (`spikes/he0-iceberg-v3/`, three standalone reader crates), so no iceberg-rust `main` or rc dependency entered a merged crate | The controller's brief for agent E, 2026-10-21 | Low |
| R11.3 | **The spike's stack:** Lakekeeper **v0.13.6** (`@sha256:d6829722…`, the version the House's REST client was tested with; v0.14.x is under 14 days old or exactly 14), RustFS 1.0.0 (the repository's pin), and Spark `apache/spark:4.0.4` (unmodified) with the Iceberg 1.12.0 runtime and AWS bundle jars from Maven Central, mounted read-only. Warehouses set **`remote-signing-enabled = false`** and no STS, so every client brings its own keys. With Lakekeeper's default remote signing, Spark's ranged reads of Puffin DVs fail with `SignatureDoesNotMatch` against RustFS (report G15) | Measured 2026-10-21 | Low |
| R11.4 | **The interop baseline used the House of `loams-fabric:0.1.0`**, because `loams-house-lake` has no commits between `v0.1.0` and `dev`. It wrote to a bucket-root store (`s3://house`): with `s3://bucket/prefix` the House writes, but chDB's reads through `house-cache` are refused with 403 (report S1, for Task 9 or HS1) | Report §4, §8 | Low for the baseline; S1 is a real bug if anyone configures a prefix |
| R11.5 | **iceberg-rust returns `_last_updated_sequence_number` as `RunEndEncoded(Int32, Int64)`.** HE1's `loams-house-iceberg` casts it to `Int64` or carries run-end-encoded arrays through DataFusion; a test selects both lineage columns under a filter on a DV table and compares with Spark's fingerprint | Report §2 | Medium: an operator that rejects run-end encoding fails at query time |
| R11.6 | **Nothing was posted upstream.** The proposed issues (report §7: a DuckDB `_row_id` bug under filters on DV files, comments on apache/iceberg-rust#2879 and #1652, two conditional issues and one docs issue) and the contribution order (G2 DV serializer and writer, G3 `RowDelta`, G5 lineage through rewrites, G10, G6, then G8 and G9 for Variant) wait for the owner's consent through the controller (D1553, Q1171) | Report §6, §7 | Low |
| R2.1 | **Two feature kinds beyond the plan's six**: `table_function:` (`numbers`, `s3`, `url`; §75 §7.5's security rule is about these) and `combinator:` (`If`, `State`, `Merge`, …; Tier 0 names the combinators). An aggregate with combinators also counts its base function (`countIf` adds `function:countIf`, `function:count` and `combinator:If`) | §75 §7.3 and §7.5 need both told apart from plain functions | Low |
| R2.2 | **Operators are recorded under the ClickHouse function names `EXPLAIN AST` gives them** (`a = b` is `function:equals`, `CASE` is `multiIf`, `x -> y` is `lambda`), so the sqlparser path and the server path name features alike and Task 8 matches them against `system.functions`. An all-capitals function name (`COUNT`) is lowered; `CAST` keeps its capitals | One naming for both parse paths | Low |
| R2.3 | **sqlparser 0.63's ClickHouse dialect refuses much real ClickHouse**: `PARTITION BY` before `ORDER BY`, `INDEX … TYPE`, `ALTER … DELETE/UPDATE`, `ADD COLUMN IF NOT EXISTS`, `t.1`, `ANY`/`ASOF JOIN`, `CREATE DATABASE … ENGINE`. Those go to `EXPLAIN AST` on the pinned server (R0.3's 26.9.2.8). sqlparser also reads `FROM t FINAL` as the alias `FINAL`, which the extractor maps to `syntax:FINAL`, and reads `{p:DateTime64(3)}` as a dictionary holding a call, so query parameters are masked before sqlparser runs and read from tokens | Measured with probes against sqlparser 0.63.0 | Medium: Task 10's parse rate starts from this; HE1's frontend needs these forms |
| R2.4 | **`EXPLAIN AST` refuses unbound query parameters** (`UNKNOWN_QUERY_PARAMETER`), so the extractor binds each `{name:Type}` to a value of its type. The tree does not show setting names, `FORMAT`, `FINAL`, `PREWHERE`, `LIMIT BY`, `WITH TOTALS`, join kinds or table clauses; these are read from the tokens of statements the server accepted. A statement neither path reads is counted under `unparsed` with its id and scrubbed sample, and keeps only the client's settings as features | Measured on 26.9.2.8 | Low |
| R2.5 | **The query-log dump contract** (for Task 1's `apps.sh dump`): `TSVWithNames` with `query_id` and `query` required, and `type`, `is_initial_query` and `Settings` used when present. Rows of type `ExceptionBeforeStart`, rows with `is_initial_query = 0`, second rows for one `query_id` and the harness's reads of `system.query_log`/`system.part_log` are skipped and counted by reason. The harness profile's `log_queries*` settings are never recorded; `--ignore-setting` adds more | The dump query is in `loams-house-mine`'s `querylog` module | Low |
| R2.6 | **An inventory keeps scrubbed samples** (the oracle's corpus seeds, Task 6). Statements with equal scrubbed text and client settings are one sample; a feature's `count` is the executions of its sample sources, checked on write and on merge. Scrubbing turns each literal into a typed ClickHouse query parameter `{pN:String}`, `{pN:Int64}`, `{pN:Float64}`, `{pN:Bool}`, `{pN:Date}` and drops comments; a tuple index (`t.1`) is kept as syntax; text the tokenizer refuses keeps no sample | Review Focus 1 and 2 | Low |
| R2.7 | **`merge` writes a ranking** (JSON, every feature with its applications and frequency); **`report` writes `profile.toml`** (the in-profile features only, which Task 7 filters by) **and the page**. Variants of one application (Langfuse v3 and v4) count as one application | §75 §7.1's rule counts applications | Low |
| R8.1 | **The matrix covers three lists**: `system.functions`, `system.table_engines` and also `system.table_functions`, because most of the security list (`url`, `file`, `remote`, `s3`, …) is table functions | §75 §7.5 names them; the House's deny list is mostly table functions | Low: one more section |
| R8.2 | **chDB's lists are committed as dumps** (`conformance/clickhouse/matrix/chdb/*.tsv`, `source.txt`: chDB 26.9.0, ClickHouse 26.9.2.1), read from a chDB House over HTTP with `loams-house-mine matrix --from-url … --chdb-release 26.9.0`. The CI check reads the dumps, so it needs no libchdb; refresh them on each chDB pin (§75 §15 suite 5's cadence) | Keeps the check light (GitHub-hosted, no 180 MB download); names only, no query text | Low: a stale dump shows as a diff on the next pin |
| R8.3 | **Classes**: the mined profile (`profile.toml`) plus a hand-kept `conformance/clickhouse/matrix/classes.toml` holding only what §75 §7.3–§7.5 and D1558 name (Tier 0/1 engines and functions, Tier 2 lists, the security list). Precedence `disabled` > mined profile > `profile` rules > `refused`; an alias takes its target's class. Every `disabled` name must be on the House's deny list (`disabled_names_are_on_the_house_deny_list`). **For the owner**: §75 differs from today's House on some engines: `Distributed` is `344` today but Tier 0 topology syntax in §75 (marked `profile`); `Kafka`, `RabbitMQ`, `MySQL`, `PostgreSQL`, `NATS`, `MaterializedMySQL` and `Buffer` are `344` today but Tier 2 (`48`) in §75 (marked `refused`); `Memory` is Tier 2 in §75 although the House uses `Memory` temporary tables internally (marked `refused`) | §75 is the target engine's contract; the deny list is today's | Medium: the owner may move an engine; one line in `classes.toml` |
| R8.4 | **The CI check is a test**, `the_committed_matrix_is_up_to_date` in `fabric/crates/loams-house-mine/tests/matrix.rs` (the Fabric workflow's `cargo test --workspace`); its path filter now includes `conformance/clickhouse/**` and `docs/house/**`. It fails on a stale `docs/house/compat-matrix.md` and on an unclassified feature found in `profile.toml` or `conformance/clickhouse/inventory/*.json`. So a PR that adds mined inventories or a new profile regenerates the matrix and classifies what it found | One gate, no second job on the concurrent-job limit | Low |
| R8.5 | **The first matrix has no mined profile** (HE0 Task 5 not merged on 2026-10-21): 1 885 functions, 80 engines, 93 table functions; only §75's named entries are classified. It is regenerated when Task 5's `profile.toml` lands | Task 8 depends on Task 5; the command and its check do not | None |
| R6.1 | **The oracle's pins** are R0.3's: the baseline `clickhouse-server@26.9` is **26.9.2.8** (`@sha256:7b9a0b6e…727a80`, the build of chDB's 26.9.2 line, the same server the extractor uses for `EXPLAIN AST`), the second target **24.3.18.7** (`@sha256:85b97f63…9916`). The runner prints each target's `version()` per run; `house-chdb` answers 26.9.2.1 | No image is tagged 26.9.2.1 (R0.3) | Low: a patch-level difference shows as a triaged mismatch |
| R6.2 | **Comparison rules.** Column names, column types (verbatim, so `Nullable`, `LowCardinality`, width and signedness all count), row count and error codes are never normalised; each difference is its own `kind` (`error`, `names`, `types`, `row_count`, `values`), so triaging one never covers another. Hidden: floats rounded to 12 significant digits (`Float64`) or 6 (`Float32`), `-0` = `0`, `nan`/`inf` as numbers or strings; quoted 64-bit integers; decimal trailing zeros; row order when the outermost query has no `ORDER BY` (rows compare as multisets, multiplicity counted). A case may declare `-- oracle: precision = N` or `-- oracle: order = ordered|unordered`. Error messages are not compared, only codes | Review Focus 3; §75 §15 suite 3's normalisations | Medium: a case with ties under `ORDER BY` needs `order = unordered` |
| R6.3 | **Triage file**: `conformance/clickhouse/oracle/triage.toml`, one `[[mismatch]]` per (case, target, kind) with `status` `bug`, `incompatibility` (needs `reason`) or `untriaged`. The runner keeps statuses it sees again, adds new mismatches as `untriaged`, and drops entries it no longer sees within the cases and targets it ran. It fails on `untriaged` and on an `incompatibility` without a reason; `--check` fails the same way and leaves the file unchanged. An unreachable target stops the run, and a setup statement only the target refuses is a mismatch of case `<corpus>/@setup` | The plan's three statuses | Low |
| R6.4 | **The corpus seed is the House's own select corpus** (`conformance/clickhouse/corpus/select`, 20 cases, FL2/HS1 Task 11) over 1 000 generated rows (`oracle/corpus/select/setup.sql`, the worker test's `corpus_rows()` in SQL). The scrubbed mined queries of Tasks 3–5 aren't merged yet. Each joins `corpora.toml` as one corpus per application, over generated data of its schemas, when its inventory lands (Task 7 uses the same runner) | Task 6 must not wait for Tasks 3–5 | Low |
| R6.5 | **First run, 2026-10-21**: 20 cases on `clickhouse-server` 26.9.2.8 (baseline), 24.3.18.7 and `house-chdb` (`loams-fabric house --single-node`, a local debug build of 2026-10-17, chDB 26.9.0): **no mismatches**, so the committed triage is empty. A probe case (`SELECT version()`) gives the expected `values` mismatch on both other targets, which shows the comparison is live | The seed corpus was written to agree (HS1 Task 11) | None; the mined corpora are the real test |
| R6.6 | **No CI job runs the targets yet.** The runner's tests run per PR (the Fabric workflow); a job that pulls two `clickhouse-server` images and builds the House is nightly material, added when the mined corpora land (HE0 Task 7 or HE1, when the Rust engine is a target). Local ports: 28123 (26.9), 28124 (24.3), 28125 (the House), because 18123 was taken by another stack on the dev machine | GitHub-hosted runner budget (§75 §15: suite 4 nightly) | Low |
| R2.8 | **`query_log` holds a query's text after the server has bound its parameters**: `{p:UUID}` is logged as `_CAST('…', 'UUID')` (measured on Umami, Task 5). The extractor records `_CAST` as `syntax:query parameter` plus the bound type, not as a function. `CODEC(ZSTD(3))` is recorded as `syntax:column CODEC` and `syntax:codec ZSTD`, not as the functions `codec` and `zstd` | A first mining run counted both as functions | Low |
