# DX1 — Durable Execution Compatibility: Temporal Protocol, DBOS and Apache Airflow (plan stub)

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task, test first. Where this plan gives exact names, paths, branches or tests, use them verbatim. Record any deviation in "Rulings made during execution" at the end of this file.
>
> **Status: Stub** (2026-10-21). Track DX, design [§74](../design/74-durable-execution-compat.md) (D1500–D1539, Q1150–Q1165, **approved by the owner on 2026-10-21 with every default**, Q1165 open; it amends §68's Temporal and Airflow decisions). This plan fully specifies **DX0**, the baselines. They are the upstream Temporal server on Loams Postgres (compat mode), DBOS conformance, a corpus of real Temporal histories, and Airflow 3.3 with the Edge executor on Loams Postgres. DX0 writes no engine code. DX1–DX5 and the fork tasks FW0–FW10 are listed at milestone level. They get full specifications when DX0 exits (D1534).

**Goal (DX0):**
- the stock Temporal server runs on Loams Postgres 16 through the endpoint mode Task 2 picks, passes Temporal's SQL persistence tests and a soak, and is available as the compose profile `temporal` (compat mode, D1509). Postgres 17 runs as a leg whose failures are expected failures;
- a versioned corpus of real Temporal histories, from pinned Go and TypeScript SDKs against that server, is committed with a manifest, and each SDK's replayer accepts it. This is DX1's differential oracle;
- DBOS Transact's own suites (Python, TypeScript, Go, Java) pass against Loams Postgres, with a crash-recovery case and a polling leg, and the PgDog transaction-mode leg is recorded as an expected failure;
- Airflow 3.3 runs on Loams Postgres with two schedulers and the Edge executor (`edge3`) and passes §68 §4.3's gate tests plus the Edge cases;
- every result is in a strict expected-failure list and in the generated compatibility matrix, and the DX0 exit report gives a go or no-go for DX1 and DX1A.

**Architecture:** no crate changes. DX0 shares §68's OC0 harness (`conformance/compute/`, `deploy/release/compose.compute.yaml`, `scripts/compute/`, `.github/workflows/compute-compat.yml`). It extends OC0 Task 6 (Airflow) and Task 7 (the Temporal spike), and adds `conformance/compute/{dbos-py,dbos-ts,dbos-go,dbos-java}/` and `conformance/compute/temporal/corpus/`. **Shared tasks are done once.** If OC0's task has merged, the DX0 task adds only its delta. If it has not, the DX0 task lands OC0's task as OC1 specifies it, plus the delta, and marks the OC0 task done in OC1's rulings.

**Tech stack:** Docker Compose v2; bash with `set -euo pipefail` and shellcheck; Python 3.12 (stdlib only for scripts); `temporal-sql-tool` and the server images at the pinned 1.32.x tag; Go 1.24+ and Node 22 for the corpus workers; the DBOS SDKs' own test runners (pytest, the TypeScript SDK's test runner, `go test`, Gradle); `psql`.

**Effort (estimate):** 3–3.5 engineer-weeks (14–18 days), or about 2 if OC0 Tasks 1, 2, 6 and 7 have landed first. The owner's draft said 1–2 weeks; the difference is the corpus (Task 4) and four DBOS suites. Per task: 0 (1 d), 1 (2–3 d, or 0), 2 (1 d), 3 (2–3 d), 4 (3–4 d), 5 (2–3 d), 6 (2 d), 7 (1 d). Tasks 3, 5 and 6 can run in parallel after Task 2; Task 4 needs Task 3 (or its `start-dev` fallback).

## Global Constraints

- **Worktrees and branches.** `~/Documents/Ostriumlabs/loams-wt/dx0-<task>`, one branch and one PR per task into `dev` (CodeRabbit reviews; merge on green). `git commit -s`. Never `git stash`.
- **No Rust build, no crate change.** If a task finds a Loams Postgres or gateway defect, it opens an issue against the owning track (§51 for the fork, GW1 for the gateway), records a ruling, and keeps the product's row red or expected-failure.
- **No patches to upstream products.** Temporal, DBOS and Airflow run exactly as released. Only configuration differs. Upstream tests disabled for a reason go in `xfail.toml` with the reason and an issue.
- **Pins (D1274).** Every image by digest and every package by exact version, at a release at least 14 days old when the PR opens (security fixes excepted), in `conformance/compute/pins.env`. On 2026-10-21 that rules out Temporal Go SDK v1.50.0, .NET 1.21.0, Ruby v1.10.0, Rust 1.1.0, `edge3` 5.0.0 and the DBOS releases of 2026-10-15. Task 0 picks the newest eligible ones.
- **Licences (D1271, D1536).** Temporal (MIT), DBOS Transact (MIT) and Airflow (Apache-2.0) only. No DBOS Conductor image or licence key. No BUSL Resonate server. Task 1's `licence_gate_compute_pins` covers the new pins.
- **Never on Loams SQL.** No Temporal or DBOS database is ever created on Loams SQL (TiDB). Test `no_loams_sql_in_dx0` greps the DX0 files for the Loams SQL host and the `mysql8` plugin.
- **Never public.** No new host ports. Products reach Loams on the compose network only.
- **GitHub-hosted runners only.** Suites run nightly and on `workflow_dispatch`. Per-PR jobs only lint and validate files, and run the corpus manifest check (Task 4).
- **Wording (D1512, D1275).** DX0's docs say "runs the Temporal server on Loams Postgres" (compat mode). They never call the gateway "Temporal-compatible", because it does not exist yet. `docs_compute_wording` covers `docs/guides/durable/`.

## Review Focus

1. **A pass that proves nothing.** Each product's own suite runs, and the Loams cases check effects (one execution per step, no double run, histories read back), not exit codes. Tests: `temporal_persistence_suite`, `dbos_py_crash_recovery_exactly_once`, `airflow_edge_worker_kill_retries`.
2. **A corpus that is not reproducible or not real.** Every history in the corpus comes from the pinned real server, with a manifest of SDK, server and scenario versions, and the replayers accept it. Tests: `corpus_manifest_complete`, `corpus_replays_go`, `corpus_replays_ts`.
3. **The wrong endpoint mode.** Each suite runs on exactly the mode its recipe states. Test: `recipe_endpoint_mode_matches_suite`.
4. **A hidden workaround.** No patched product, and no disabled upstream test without an `xfail.toml` entry. Tests: `xfail_entries_have_reason_and_issue`, `xfail_strict_unexpected_pass`.
5. **Loams SQL or a licence slip.** Tests: `no_loams_sql_in_dx0`, `licence_gate_compute_pins`, `no_licence_key_in_compute_files`.

## DX0 — The baselines (Tasks 0–7)

### Task 0 — Rulings

Check and record, in "Rulings made during execution":
- **Pins:**
  - the Temporal server and `admin-tools` images (newest 1.32.x eligible under D1274) and `temporal-sql-tool` from the same tag; the Temporal CLI (v1.9.1);
  - the Go SDK (newest eligible 1.4x) and TypeScript SDK (1.24.0 or newer eligible), plus the `samples-go` and `samples-typescript` commits the corpus builds on;
  - DBOS: `dbos-transact-py`, `-ts`, `-golang` and `-java`, each at the newest release at least 14 days old, with the commit whose test suite runs;
  - Airflow 3.3.2 with its constraints file and the `apache/airflow` image digest, `apache-airflow-providers-edge3` (newest eligible) and `apache-airflow-task-sdk`.
- **Temporal's persistence tests:** the entry point for the SQL persistence tests at the pinned tag (`common/persistence/tests` with plugin `postgres12`, or `postgres12_pgx`), and whether they run from source in a Go container (expected) or from the image.
- **Temporal's default limits** (blob size, gRPC message size, history size and count) and its default namespace retention, read from the pinned tag's `dynamicconfig` defaults. They feed §74 D1508 and Q1163, recorded with file and line.
- **Endpoint modes:** which Loams Postgres endpoints the release compose and Kubernetes offer: PgDog transaction mode through `loams-gateway` `pg`, a session-pooled mode, and a direct compute endpoint. If neither a session nor a direct endpoint exists, Task 2 uses the compute's address on the compose network and files the gap against GW1.
- **Loams Postgres majors** available in the release compose (16; 17 if selectable).
- **DBOS polling:** whether the TypeScript, Go and Java SDKs have an equivalent of Python's `use_listen_notify`. Read their configuration code at the pinned tag.
- **Each DBOS SDK's suite:** the command that runs it against an external Postgres URL, and the tests that need anything beyond Postgres (Kafka, plv8). Those become `xfail.toml` entries with the reason "needs X", not failures.
- **OC0's state:** which of OC0 Tasks 1, 2, 6 and 7 have merged. That decides whether Tasks 1, 3 and 6 below land OC0's part.

Files: this plan. Tests: none. Gate: rulings recorded.

### Task 1 — The harness (shared with OC0 Tasks 1 and 2)

If OC0 Tasks 1 and 2 have merged, this task only adds the new profiles and pins. If not, it lands them exactly as OC1 specifies, plus the following.

Files:
- `conformance/compute/pins.env`: adds `TEMPORAL_*`, `TEMPORAL_SDK_GO`, `TEMPORAL_SDK_TS`, `DBOS_PY`, `DBOS_TS`, `DBOS_GO`, `DBOS_JAVA`, `AIRFLOW_EDGE3`
- `deploy/release/compose.compute.yaml`: profiles `temporal` (the stock server, four services or the auto-setup image per Task 0, on Loams Postgres) and `dbos` (a test-runner container only); the `airflow` profile gains `edge-worker` services (two)
- `deploy/release/compute/temporal/development-sql.yaml`: the server's dynamic config, with `numHistoryShards: 16` fixed
- `conformance/compute/lib.sh`: adds `oc_temporal_wait` (the frontend answers `GetSystemInfo` through `temporal operator cluster health`) and `oc_pg_mode <mode>` (prints the URL for `txn`, `session` or `direct`)

Tests (in `conformance/compute/selftest.sh`): `compose_compute_config_valid` for `temporal`, `dbos` and `airflow`; `compose_compute_requires_profile`; `compose_compute_no_new_host_ports`; `compose_compute_images_pinned_by_digest`; `no_loams_sql_in_dx0`; `licence_gate_compute_pins` (with the new pins).

### Task 2 — Loams Postgres endpoints and databases

Files: `conformance/compute/durable-init.sh` (a one-shot `compute-init` step for the `temporal`, `dbos` and `airflow` profiles), `conformance/compute/endpoint-modes.md`.

- Creates, idempotently, on Loams Postgres:
  - `temporal` and `temporal_visibility` (through `temporal-sql-tool create-database`, `setup-schema` and `update-schema` at the pinned tag);
  - `dbos_py`, `dbos_ts`, `dbos_go` and `dbos_java` (one per SDK, each its own system database, D1520);
  - `airflow`.
- Each in its own database, never a shared schema (D1529).
- `endpoint-modes.md` records, per product, the mode used and why: Temporal session or direct (multi-statement transactions with `FOR UPDATE` and `FOR SHARE`); DBOS session or direct (`LISTEN`/`NOTIFY`); Airflow's scheduler and triggerer session or direct, unless Task 6's transaction-mode leg passes.

Tests: `durable_init_idempotent` (run twice, same result), `durable_init_separate_databases` (`\l` shows one database per product and no shared schema), `pg_modes_reachable` (each mode's URL answers `SELECT 1`; a missing mode fails with the GW1 issue in the message).

### Task 3 — Compat mode: the Temporal server on Loams Postgres (extends OC0 Task 7)

Files: `conformance/compute/temporal/{run.sh,persistence.sh,soak.sh,xfail.toml,README.md,SPIKE.md}`, `conformance/compute/temporal/pg17/xfail.toml`.

Steps:
1. **Persistence suite.** Temporal's SQL persistence tests at the pinned tag, in a Go container, against Loams Postgres 16 on Task 2's mode, then the same against 17.
2. **Soak.** The stock server (frontend, history, matching, worker; `numHistoryShards = 16`) runs the `samples-go` load workflow (Task 0 picks it) for 30 minutes at 50 starts per second (**estimate**, adjusted in Task 0). Halfway through, the history service is restarted. At 20 minutes, a Loams Postgres compute is suspended and resumed.
3. **Transaction-mode leg.** The same smoke runs through PgDog transaction mode and is expected to fail. If it passes, the run fails (strict), and the recipe can recommend transaction mode.
4. **SPIKE.md** records the results, the endpoint mode, the 17 result, and the recommendation for compat mode (D1509).

Tests: `temporal_schema_setup`, `temporal_persistence_suite`, `temporal_persistence_suite_pg17` (expected failures allowed with entries), `temporal_soak_no_failed_workflows`, `temporal_history_restart_recovers`, `temporal_compute_resume_recovers`, `temporal_txn_mode_xfail`.

Gate: if `temporal_persistence_suite` or the soak is red on 16, DX0 exits **no-go** for compat mode. Task 4 then runs its corpus against `temporal server start-dev` (SQLite) as the oracle instead, and the matrix shows Temporal-on-Loams-Postgres red with the reasons.

### Task 4 — The history corpus (Go and TypeScript)

Files:
- `conformance/compute/temporal/corpus/README.md`: the scenario list, the normalisation rules, and how to regenerate
- `conformance/compute/temporal/corpus/workers/go/` (a Go module pinned to Task 0's SDK) and `.../workers/ts/` (an npm package pinned the same way), each with one workflow per scenario
- `conformance/compute/temporal/corpus/generate.sh`: starts each scenario with a **fixed workflow ID** (`corpus-<lang>-<scenario>`), waits for close, and exports with `temporal workflow show --output json` and as binary protobuf (`GetWorkflowExecutionHistory` through a 30-line helper in the Go module)
- `conformance/compute/temporal/corpus/histories/<lang>/<scenario>.{json,binpb}`
- `conformance/compute/temporal/corpus/manifest.json`: per history, the scenario, language, SDK version, server version and image digest, Loams Postgres major, generation date, SHA-256 of both files, and the phase it belongs to (DX1, DX2, DX3, DX4)
- `conformance/compute/temporal/corpus/replay/go/replay_test.go` and `.../replay/ts/replay.test.ts`: replay every history of that language with the SDK's replayer (`worker.NewWorkflowReplayer` with `ReplayWorkflowHistoryFromJSONFile`; `Worker.runReplayHistories`)

Scenarios (each in both languages unless marked):
- **DX1:**
  - `hello` (one activity)
  - `two_activities_sequential`
  - `activity_parallel_fanout_10`
  - `signal_then_complete`
  - `query_while_running`
  - `cancel_requested`
  - `terminated`
  - `workflow_failed_application_error`
  - `activity_heartbeat_details`
- **DX2:**
  - `timer_short`, and `timer_cancelled`
  - `activity_retry_backoff_3_attempts`
  - `activity_start_to_close_timeout`, `activity_schedule_to_start_timeout`, `activity_heartbeat_timeout`
  - `workflow_task_failure_then_fix` (the replay of a nondeterminism fix, Go only)
  - `signal_with_start`
  - `child_workflow`, and `child_workflow_cancelled`
  - `continue_as_new_3`
  - `id_reuse_allow_duplicate_failed_only`, and `id_conflict_fail`
  - `local_activity`
  - `side_effect_and_version_marker`
- **DX3:**
  - `search_attributes_upsert`
  - `memo`
  - `large_payload_1mb`
- **DX4:**
  - `update_accepted_completed`, and `update_rejected` (validator)
  - `schedule_triggered` (Go only)
  - `reset_to_first_workflow_task` (Go only)

Tests:
- `corpus_manifest_complete`: every file is listed, every hash matches, and every scenario exists in both languages unless marked;
- `corpus_replays_go` and `corpus_replays_ts`: every history replays with zero nondeterminism errors;
- `corpus_regenerates_stably`: generating twice produces histories identical after Task 4's normalisation (`normalize.py`: masks timestamps, run IDs, identities, task tokens, request IDs and the worker's binary checksum; keeps event IDs, types, order, batch boundaries and attributes);
- `normalize_selftest`, on fixtures: two histories that differ only in masked fields compare equal, and a reordered pair does not.

Per PR: only `corpus_manifest_complete` and `normalize_selftest` run (no server). The replays and the regeneration run nightly.

### Task 5 — DBOS conformance

Files: `conformance/compute/{dbos-py,dbos-ts,dbos-go,dbos-java}/{run.sh,xfail.toml,README.md}`, `conformance/compute/dbos-common/crash/` (a small app per language for the Loams cases).

Each `run.sh`:
- checks out the SDK's repository at the pinned tag (a read-only `git archive` of the tag through the GitHub API, not a clone kept in the tree);
- points its suite at the Loams Postgres URL for Task 2's mode, writes JUnit and runs `oc_xfail_check`.

Loams cases, per language:
- `dbos_<l>_crash_recovery_exactly_once`: a five-step workflow writes a row per step. `kill -9` of the app after step 3, then a restart. Steps 1–3 are not re-run, and steps 4–5 run once (row counts).
- `dbos_<l>_queue_concurrency`: 1,000 enqueued workflows on a queue with concurrency 10 and two app processes. Each runs once.
- `dbos_<l>_notify_latency`: an event set by one process is received by another in under 1 s on the session or direct endpoint.
- `dbos_<l>_notify_after_compute_resume`: suspend and resume the Loams Postgres compute. After the listener reconnects, a new notification arrives.
- `dbos_py_polling_mode`: a fresh system database created with `use_listen_notify = false`. The suite subset and crash recovery pass. Other languages only if Task 0 found an equivalent switch.
- `dbos_<l>_txn_mode_xfail`: the crash-recovery app through PgDog transaction mode, recorded as an expected failure with the reason (`LISTEN` across transactions).

Tests: `dbos_py_suite`, `dbos_ts_suite`, `dbos_go_suite`, `dbos_java_suite`, and the Loams cases above.

### Task 6 — Airflow 3.3 with the Edge executor on Loams Postgres (extends OC0 Task 6)

Files: `conformance/compute/airflow/{run.sh,dags/,xfail.toml,README.md}` (OC0's), plus `conformance/compute/airflow/edge/{run.sh,dags/}`.

Setup: OC0 Task 6's configuration, plus `[core] executor = airflow.providers.edge3.executors.EdgeExecutor` (the class path is checked in Task 0), the Edge API enabled on the API server, and two `edge-worker` containers with the queue `default`. Then one run with `LocalExecutor` for comparison.

Tests:
- OC0's: `airflow_db_migrate_empty`, `airflow_ha_no_double_run`, `airflow_scheduler_kill`, `airflow_remote_logs_after_worker_gone`, `airflow_xcom_object_storage`, `airflow_git_bundle_version`, `airflow_rest_api_v2_smoke`;
- `airflow_edge_workers_register`: both workers show as online through the Edge API;
- `airflow_edge_ha_no_double_run`: two schedulers and two edge workers run 500 mapped task instances, each with exactly one try;
- `airflow_edge_worker_kill_retries`: `kill -9` of an edge worker mid-task. The task is retried on the other worker within the configured heartbeat timeout, and the try count is 2;
- `airflow_edge_maintenance_mode`: a worker in maintenance takes no new tasks and finishes its running one;
- `airflow_txn_mode_leg`: the scheduler and triggerer through PgDog transaction mode. If it passes, Task 2's `endpoint-modes.md` records that transaction mode is allowed. If it fails, it is an expected failure with the reason.

### Task 7 — Matrix, docs and the DX0 exit report

Files:
- `scripts/compute/matrix.py` (OC0's): a "Durable execution" section with rows for Temporal compat mode (16, 17), the corpus (the scenario count per phase), DBOS per SDK, and Airflow with Edge
- `docs/guides/durable/README.md`: §74 §10's which-engine table and links
- `docs/guides/durable/temporal-compat-mode.md`: running the Temporal server on Loams Postgres. It names the endpoint mode, the shard count fixed at creation, and never Loams SQL
- `docs/guides/durable/dbos.md`: the endpoint, one database per app, polling and scale-to-zero, the expected failures
- `docs/guides/durable/airflow.md`: a link to `docs/guides/compute/airflow.md`, plus the Edge executor section
- `docs/plans/dx0-exit-report.md`: the results; the go or no-go for compat mode; the corpus size; the facts DX1 needs (Temporal's limits and default retention, the endpoint modes, the DBOS polling switches); the recommendations for DX1 and DX1A; the questions for the owner

Tests: `docs_compute_snippets_match_files` (covers `docs/guides/durable/snippets/`), `docs_compute_wording`, `recipe_endpoint_mode_matches_suite`, `matrix_from_fixture` (with durable rows).

**DX0 exit:**
- every product has a nightly run with a committed `xfail.toml`;
- the corpus has at least the DX1 and DX2 scenarios in both languages and replays clean;
- `SPIKE.md` and `dx0-exit-report.md` give the go or no-go;
- the owner reviews the report before DX1 starts.

## Fork work: `ostrium-labs/resonate` (FW0–FW10; §74 §7.4)

These tasks run in the fork, not in this repository. Each is one branch on the fork's `main` and one upstream PR to `resonatehq/resonate`, with DCO sign-off. §74 §7.4 holds each task's acceptance criteria. The Loams side of each is one pin-bump PR here (`Cargo.toml` `rev`, plus the code that needs it).

| Task | When | Loams-side follow-up |
|---|---|---|
| FW0: shepherd #1162–#1168, `CARRIES.md`, fork CI | With DX0 | none |
| FW1: the TiKV store in the fork, keeping the 0.1.0 format | With DX0 | Pin bump; delete `crates/loams-durable/src/tikv.rs`; `durable-tikv` uses the fork's plugin |
| FW2: TiDB upstream | With DX0 | none |
| FW3: `resonate-client` and the public router entry | Before DX1 | `loams-temporal-resonate` and `loams-worker` depend on it |
| FW7: the gateway-facing protocol profile | Before DX1 Task 0 | DX1 Task 0 reads it |
| FW8: poll groups with many consumers | Before DX1A | none |
| FW5: settled-promise retention | DX2 | Gateway retention (Q1163) |
| FW6: tag-indexed search | DX3 | Console Durable page |
| FW9: several instances on one TiKV keyspace | DX3 | Cluster HA tests |
| FW10: benchmarks and tracing | DX3 | DX3's load comparison |
| FW4: batch request (only if DX3 measures the need) | DX3 | The relay uses it |

## DX1–DX5 (milestone level; full tasks after DX0's exit)

| Milestone | Content (§74) | Gate |
|---|---|---|
| **DX1** | Gateway MVP. Vendored protos; `loams-temporal-{proto,history,statemachine,store,matching,resonate,frontend,testkit}`; the `temporal` module in `loams-gateway`; the store with compare-and-set and outbox on Loams Postgres; the relay; the matching fast path; the MVP method set; Go and TypeScript hello-world; the replayers accept gateway histories; `capabilities_match_impl` | DX0 exit; FW3 and FW7; durable-listener auth (D111) or the compose network-namespace path |
| **DX1A** | `LoamsResonateExecutor` in `ostrium-labs/loams-airflow`; `crates/loams-worker`; provider v0; chaos (worker kill, lease expiry, scheduler failover); the Airflow version matrix | DX0 Airflow; FW3, FW8 |
| **DX2** | Timers, timeouts, retries, signals, queries, cancel and terminate, child workflows, continue-as-new, ID policies, local activities; the differential harness in CI; Java and Python; DBOS console preset and views | DX1 exit; FW5 |
| **DX3** | Visibility and the list-filter grammar; sticky queues; shards and partitions; outbox batching; load tests against Temporal; Resonate throughput on TiKV | DX2 exit; FW6, FW9, FW10 (FW4 if measured) |
| **DX4** | Updates, versioning, Schedules, reset, `OperatorService`; Airflow asset watchers, auth manager, state store; the console's Durable section; an AIP-108 Rust prototype | DX3; AIP-108 out of experimental for the Rust part |
| **DX5** | TLA+ specs `spec/tla/temporal/{WorkflowTask,Activity,Timer,OutboxCas}.tla`; deterministic simulation (1,000 seeds nightly); porcupine; SDK-suite soak | Parallel from DX1 |

## Owner actions

- §74 and its defaults (D1500–D1539, Q1150–Q1164), including the amendments to §68, were approved on 2026-10-21; nothing further for the design.
- Q1165: the design-partner terms with ResonateHQ. The fork-work list (§74 §7.4) is the engineering input.

## Rulings made during execution

(none yet)
