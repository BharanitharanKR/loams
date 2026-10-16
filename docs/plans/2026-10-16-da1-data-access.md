# DA1 — Data Access and HTAP (plan stub)

> **Status: Stub** (2026-10-16). Track DA, design [§58](../design/58-data-access-and-htap.md) (D1030–D1061, Q910–Q925). The owner approved the direction on 2026-10-16. This stub only lists tasks. **It specifies no implementation.** Each part gets a full plan, written against the code as it stands, when its dependency lands. That plan will have files, interfaces, named tests and commit messages, as in [GW1](2026-10-16-gw1-loams-gateway.md).

**Global constraints (for the full plans):**
- Worktrees go in `~/Documents/Ostriumlabs/loams-wt/da1-<part>`, with branches cut from a fresh `origin/dev`.
- One PR per task into `dev`, with one CodeRabbit round. Merge only on a green `CI required`.
- Rust builds use the shared target directory. Never build in `/tmp`.
- PgDog stays unmodified (D708, D1023).
- Fork work (`loams_request_attrs`, phase 2) follows §51's patch-queue rules.
- No usage names in docs or metrics labels (D548, D1021).

## DA1a — Part A: Loams protocol extensions in `loams-gateway` (D1032–D1038)

Waits on GW1 Tasks 1–8.

| Task | Scope |
|---|---|
| A0 | Reconcile with GW1 as built. Check PgDog's protocol 3.2 support, `pg_stat_statements` query-id stability under leading comments, and `ParameterStatus` handling in libpq, JDBC, node-postgres, psycopg and pgx for unknown names |
| A1 | The extension spec (`docs/spec/pg-extensions/`): ALPN `loams-pg/1`, the `_pq_.loams.*` names and versions, the message type bytes, error codes, grease |
| A2 | ALPN recognition and the protocol-aware relay (frontend and backend message parsing, forwarding unchanged), fuzzed |
| A3 | Negotiation: strip, `loams.extensions` acknowledgement, `NegotiateProtocolVersion` for unknowns, the hard errors, grease, protocol 3.0 and 3.2 |
| A4 | Long cancel keys per client and `LoamsCancel` on the same connection |
| A5 | GoAway (`loams.goaway`) on drain, compute move and replica rebalance, wired to `pg-control` |
| A6 | Request attributes: the `LoamsAttributes` message, the sqlcommenter injection, spans; the fork extension `loams_request_attrs` |
| A7 | Resumable sessions: state recording, the sealed token, `loams.resumable`, resume with replay, and the conformance matrix per statement class |
| A8 | SDK support (Rust first, then the Postgres drivers the SDKs wrap), with the strictness tests of §58 §3.3 |

## DA1b — Part B: plan control as an API (D1039–D1041)

| Task | Scope |
|---|---|
| B1 | `loams.plan.v1` protos, reasons and the catalogue row |
| B2 | `PlanService` on `pg-control` with `pg_hint_plan` pins (written at compute start), scopes, roles and audit events |
| B3 | Advice sets selected by `plan.advice` (needs A6) |
| B4 | TiDB global bindings through `loams-sqlgate`'s control path |
| B5 | `DiffPlans` and `CheckRegressions`, plus the upgrade-preflight hook (§54) and the merge and restore check |
| B6 | PG19: `pg_plan_advice` and `pg_stash_advice` (after NF1f's 19 images), migrating pins |

## DA1c — Part C: the Loams Data API (D1042–D1050)

Waits on API2 Task 1, the gateway's `http` plan and PG2 Task 22.

| Task | Scope |
|---|---|
| C1 | The `data-api-1` spec, `proto/loams/data/v1/`, and the public conformance suite |
| C2 | `DataService` for `engine:` targets in `loams`, with API2's `SqlService` as a shim |
| C3 | The gateway's `data` module for `postgres:` targets: the authenticator role and `SET LOCAL ROLE`, `Execute`, `Call` and batons |
| C4 | Idempotency rows in the same transaction, and the error taxonomy |
| C5 | `Explain` with the write set, and budgets |
| C6 | Results: JSON, Arrow IPC, and presigned partitions |
| C7 | Named operations: the registry, the determinism check, injected clock and seed, and the call log with replay to a branch |
| C8 | Live queries on the change streams |
| C9 | MCP tools and the agent safety profile, in the gateway's `mcp` module |
| C10 | `sql:` targets through `loams-sqlgate` |
| C11 | The WebSocket transport |

## DA1d — Part D: HTAP (D1051–D1054)

| Task | Scope |
|---|---|
| D1 | Phase 1: `loams pg analytics enable` (publication, bridge, stream, lake table, pipe), replace-on-key lake tables, and lag in `system.loams_pipes` |
| D2 | Phase 1 for Loams SQL, after `loams-sqlcdc` (Q922) |
| D3 | Phase 2 (after Loams Postgres GA): pageserver tuple decoding at the image LSN, Parquet writes, and the Iceberg commit through Lakekeeper; the per-table reloption; and a checksum check against a `SELECT` at the same LSN |
| D4 | Phase 3: the tail decoder to Arrow, tail merge in DataFusion and the House, freshness reporting, and the `fresh` and `snapshot` modes |

## DA1e — Part E: ADBC (D1055, D1056)

| Task | Scope |
|---|---|
| E1 | `adbc_driver_loams` over the Flight SQL driver |
| E2 | The ADBC 1.1 server features: statistics, incremental execution, partitioned results, cancellation, progress |
| E3 | The Power Query custom connector, and the Power BI Desktop and Fabric smoke tests. Certification only on the owner's word (Q923) |

## DA1f — The shared bucket-segment library (D1060)

| Task | Scope |
|---|---|
| F1 | **`loams-seglog`.** Extract create-only segment writes with `If-None-Match` fencing, unknown-outcome resolution, header, body and CRC32C framing, checkpoints, gap-stopping readers, compaction and GC from `loams-git`'s `StoreWalStore` (`crates/loams-git/src/wal.rs`, GT1 Tasks 7 and 11), generalised over the naming scheme. Move Loams Git's WAL onto it with its tests unchanged. Then build PG2 Task 40's `.lwal` offload on it. Done with PG2 Task 40 (Q924) |
