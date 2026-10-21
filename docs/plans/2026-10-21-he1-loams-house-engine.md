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

