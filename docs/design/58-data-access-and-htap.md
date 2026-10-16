# 58 — Data Access and HTAP: Postgres Wire First, a Loams Data API, Analytics on the Same Copy

Status: **Approved direction** (owner, 2026-10-16) · details **Proposed** · 2026-10-16. Source: the owner's approval of 2026-10-16 (§1.1). Decisions **D1030–D1061** and open questions **Q910–Q925** are in the [decision log](13-decision-log.md). Rows that restate an approved recommendation are marked *Approved direction*. Rows that add detail to one are *Proposed* until the owner confirms them. Plan stub: [DA1](../plans/2026-10-16-da1-data-access.md). **This document writes no code.**

This document builds on these earlier designs. §9 says where it amends each one:
- [§57](57-loams-gateway.md): `loams-gateway`, the Postgres front door (D1010–D1024). Part A lives in its `pg` module, and Part C in its later `http` and `mcp` modules.
- [§46](46-loams-postgres-production.md) §8 and §9: PgDog as the front door (D708–D711), and `loams-wal` (D714) with the laptop results of §9.2.
- [§47](47-loams-sql-production.md): Loams SQL on TiDB and TiKV keyspaces. TiKV's Raft log is the WAL (D725), and CDC runs through `loams-sqlcdc` (D738).
- [§36](36-loams-git.md) §4–§5: Loams Git's bucket WAL (D388–D391). The open PR #668 adds §20 and D1025 (a TiKV-backed `RefLog` for hot repositories).
- [§32](32-loams-flow-fabric-house.md) and [§49](49-loams-house-production.md): the House, Iceberg on the bucket, and pipes from Loams streams (D769).
- [§05](05-query-engine.md) §8: Flight SQL and ADBC bulk ingest (D49, as built in M1.2).
- [§44](44-unified-api-and-sdks.md) and the [API2 plan](../plans/2026-10-10-api2-unified-api-sdks.md): the planned `loams.sql.v1.SqlService/Query`. That proto does not exist yet.
- [§34](34-protocol-gateway-and-standards.md): the standards charter (D360). It covers pinned specs, CloudEvents and W3C Trace Context.

Markers, as in §47 and §49:
- **(verify)** means the claim is not yet checked against a primary source or running code. The task that depends on it checks it first.
- **(estimate)** means the number was reasoned, not measured.
- **(measured)** cites §46 §9.2's laptop results.

---

## 1. Summary

### 1.1 The owner's approval (2026-10-16, binding)

> "i agree with all of your recommendations, write all of this"

The recommendations were made in conversation on 2026-10-16, and this document writes them down. There are seven:
1. Postgres wire is the SQL protocol. Loams does not build a TDS server now.
2. Part A: Loams protocol extensions in `loams-gateway`, which clients opt into.
3. Part B: plan control as an API, not as hints in the wire protocol.
4. Part C: a Loams Data API, published as an open spec.
5. Part D: HTAP in three phases.
6. Part E: ADBC as the way in for BI tools.
7. Durability: TiKV Raft and `loams-wal` stay separate by role, and they share common code.

### 1.2 The landscape in one paragraph

Postgres wire has won as the protocol new databases speak (CedarDB, Aurora DSQL, Firebolt, PlanetScale's Neki, Supabase's Multigres). TiDB opened a PostgreSQL-protocol public preview on TiDB Cloud Starter on 2026-10-15. Serious databases on object storage keep **commits off S3**:
- Neon and Databricks Lakebase acknowledge a commit on a quorum of Paxos safekeepers.
- TiDB X persists the Raft log to local disk first and uploads WAL chunks to S3 in the background.
- OceanBase acknowledges on its LogService.
- Azure HorizonDB acknowledges on a WAL service.

S3 holds history and branches. HTAP comes in four shapes:
1. **One engine with a hybrid format:** CedarDB (proprietary; Cloud GA 2026-10-14), SingleStore, OceanBase.
2. **A synced columnar replica:** TiFlash as a Raft learner, Spanner's columnar engine (GA 2026-04-17), AlloyDB.
3. **Columnar at the storage layer:** Databricks LTAP. It was announced on 2026-06-16 and is "coming soon". The pageserver transcodes pages to open columnar files on object storage, and queries merge the newest changes at read time.
4. **CDC into a lake.**

**Nobody ships all four of these together: open source, Postgres wire, HTAP, and S3 as the source of truth.** That gap is what Part D aims at. Appendix A has the details and the sources.

### 1.3 Decisions

| # | Decision | § | Status |
|---|---|---|---|
| D1030 | **Postgres wire is the SQL protocol for Loams' Postgres product.** Loams adds capability through opt-in protocol extensions and through APIs around the wire protocol, never through a new wire protocol | 1, 3 | Approved direction (owner, 2026-10-16) |
| D1031 | **No TDS (SQL Server) server now.** Loams revisits this only for a paying SQL Server migration. Even then it ports Babelfish onto `loams-postgres` rather than writing T-SQL in Rust | 2 | Approved direction (owner, 2026-10-16) |
| D1032 | **Part A: Loams protocol extensions in `loams-gateway`** (opt-in through `_pq_.loams.*`). A plain client sees stock Postgres | 3 | Approved direction (owner, 2026-10-16) |
| D1033 | **Recognising a Loams-aware client:** it offers ALPN `loams-pg/1` in its ClientHello. The gateway then terminates TLS (as on D1014's direct path) and relays with protocol awareness. Every other client is spliced or terminated exactly as in §57 | 3.2 | Proposed |
| D1034 | **Strict negotiation:** the extensions are startup parameters `_pq_.loams.<ext>=<version>`. The gateway strips them before PgDog and acknowledges them in ParameterStatus `loams.extensions`. Any surprise is a hard error. Grease keeps the mechanism honest | 3.3 | Proposed |
| D1035 | **Resumable sessions:** the gateway sends session-state deltas and a sealed resume token. After a reconnect (scale-to-zero, a compute move, a gateway restart) the gateway replays the session state; a session that cannot be replayed is marked as such | 3.4 | Proposed |
| D1036 | **GoAway and redirect:** an asynchronous notice tells the client to reconnect (and resume) by a deadline, optionally to another endpoint, because of a drain, a compute move or a read replica | 3.5 | Proposed |
| D1037 | **Cancel on the same connection:** a Loams frontend message, which the gateway turns into a Postgres `CancelRequest` with the backend key it saw. Loams-aware clients on protocol 3.2 get their own long cancel keys | 3.6 | Proposed |
| D1038 | **Request attributes:** key–value attributes on each request (`traceparent`, `tracestate`, `app.tenant`, `app.user_agent`, `plan.advice`), like MySQL's query attributes. They reach the compute as a sqlcommenter-format comment that a `loams-postgres` extension reads | 3.7 | Proposed |
| D1039 | **Part B: plan control is an API.** There are no hints in the wire protocol. Advice is stored on the server, keyed by query id | 4 | Approved direction (owner, 2026-10-16) |
| D1040 | **`loams.plan.v1.PlanService`:** `ExportPlan`, `Pin`, `Unpin`, `ListPins`, `DiffPlans`, `CheckRegressions`. Postgres uses `pg_hint_plan` until Loams ships PG19 images, then `pg_plan_advice` and `pg_stash_advice`. Loams SQL uses TiDB SQL plan bindings | 4.2, 4.3 | Proposed |
| D1041 | **Plan safety:** every pin has a scope (a branch, optionally a database and role). Protected branches need `plan:admin`. Every change is audited as a CloudEvent. `CheckRegressions` gates engine upgrades and branch merges | 4.4 | Proposed |
| D1042 | **Part C: the Loams Data API, published as an open spec** (`data-api-1`). SQL stays the language. It is the proposed answer to Q720: Loams adopts no PostgREST | 5 | Approved direction (owner, 2026-10-16) |
| D1043 | **The request model:** a statement batch or a named operation. Unary Connect over HTTP, plus an optional WebSocket that carries the same messages. Interactive transactions are opt-in through a baton (the Hrana model). Each request names its target branch and carries W3C trace context | 5.2 | Proposed |
| D1044 | **Idempotency and errors:** an idempotency key per request. For Postgres writes it is deduplicated **in the same transaction** as the writes. Errors are typed as `RETRYABLE_CONFLICT`, `RETRYABLE_UNAVAILABLE`, `COMMIT_UNKNOWN`, `BUDGET_EXCEEDED` or `PERMANENT`, mapped from SQLSTATE and TiDB codes | 5.3 | Proposed |
| D1045 | **Dry run and budgets:** a dry run returns the plan, the estimates and the write set without executing. Budgets cap time, rows, bytes and cost units, and are enforced at planning and while streaming | 5.4 | Proposed |
| D1046 | **Results in three sizes:** JSON rows for point reads, Arrow IPC for bulk, and presigned URLs to Arrow or Parquet partitions for very large results | 5.5 | Proposed |
| D1047 | **Named operations are registered SQL functions.** They are deterministic: the server injects the clock and the seed as recorded arguments. They are replayable from a request log. This is the model of Convex mutations, Zero custom mutators and SpacetimeDB reducers | 5.6 | Proposed |
| D1048 | **Live queries** run on the existing CloudEvents change streams (D154, D270): an initial result, then diffs, on a server-streaming RPC | 5.7 | Proposed |
| D1049 | **MCP tools generated from the schema**, with an agent safety profile: read-only by default, writes only on a branch, budgets on, named tools and no raw SQL writes, and results marked as untrusted data | 5.8 | Proposed |
| D1050 | **Where it is served, and how it relates to API2:** a `data` module of `loams-gateway` for Postgres and Loams SQL, and the `loams` binary for engine SQL. API2's planned `SqlService/Query` ships as planned and becomes the engine target's shim over `DataService`. **Amends §44 §5.1's SQL row** | 5.9 | Proposed |
| D1051 | **Part D: HTAP in three phases.** (1) CDC into Iceberg, as already planned. (2) The pageserver writes columnar files when it writes image layers. (3) Fresh analytics that merge the recent tail. Phases 2 and 3 come after Loams Postgres GA | 6 | Approved direction (owner, 2026-10-16) |
| D1052 | **Phase 1:** pgoutput (D154) and `loams-sqlcdc` (D738) feed Loams streams, House pipes (D769) feed Iceberg, and offsets commit in the same Iceberg snapshot as the rows. Postgres first | 6.2 | Proposed |
| D1053 | **Phase 2:** the `loams-postgres` pageserver writes an Iceberg table of Parquet files per relation as part of image-layer creation. That gives one copy and no pipeline. It is off by default, per table | 6.3 | Proposed |
| D1054 | **Phase 3:** the House and DataFusion read the Iceberg snapshot as of an LSN and merge the WAL tail after it, decoded from `loams-wal` or the pageserver. Freshness is bounded and reported | 6.4 | Proposed |
| D1055 | **Part E: ADBC is the way in for BI tools.** It builds on the engine's existing Flight SQL and ADBC ingest (D49) | 7 | Approved direction (owner, 2026-10-16) |
| D1056 | **ADBC details:** a thin Loams ADBC driver over the Flight SQL driver; a Power Query custom connector for Power BI and Fabric; ADBC 1.1 statistics, incremental execution and partitioned results on the server | 7.2 | Proposed |
| D1057 | **Durability substrates stay separate by role.** TiKV Raft is the substrate for small transactions and metadata. `loams-wal` is the high-volume ordered byte log for Postgres WAL. D714 and D725 are unchanged | 8 | Approved direction (owner, 2026-10-16) |
| D1058 | **The proposed default for the launch store (a lean, not a ruling):** Arm A for production. The TiKV store stays behind `WalStore` for small and single-node deployments. PG2 Task 42's bake-off and Q652 still decide | 8.3 | Proposed |
| D1059 | **Git does not use `loams-wal`.** Objects stay on the bucket, and the bucket WAL stays the default `RefLog` (D388, D389). The TiKV `RefLog` of D1025 is opt-in for hot repositories | 8.4 | Proposed |
| D1060 | **One bucket-segment library, `loams-seglog`,** shared by `loams-wal`'s bucket offload (`pgwal/*.lwal`) and Loams Git's WAL (`wal/*.lgw`). It provides create-only segments with `If-None-Match` fencing, unknown-outcome resolution, framing, checkpoints and compaction | 8.5 | Proposed |
| D1061 | **The plan is DA1**, with parts DA1a–DA1f. Each part waits on the dependency named in §10 | 10 | Proposed |

### 1.4 What it looks like

```
 psql / JDBC / Prisma (plain)        Loams SDK, Loams-aware driver          HTTP / edge / agents         BI tools
   │ TLS, ALPN postgresql or none       │ TLS, ALPN loams-pg/1, postgresql      │ Connect, WebSocket, MCP     │ ADBC (Flight SQL)
   ▼                                    ▼                                       ▼                             ▼
 loams-gateway ─ pg module ───────────────────────────────┐        loams-gateway ─ http / data / mcp     loams (engine)
   splice or terminate (§57, unchanged)                    │          DataService, PlanService            Flight SQL :8082
   Loams-aware: terminate, negotiate, resume, GoAway,      │          (Postgres, Loams SQL)               DataService (engine)
   cancel, request attributes (Part A)                     │                 │
   ▼                                                       ▼                 ▼
 PgDog (unmodified) ──► loams-postgres computes ──► pageserver ──► bucket: layers + (phase 2) Iceberg/Parquet
                                │                         ▲
                                └─► loams-wal ────────────┘ ──► bucket: pgwal/*.lwal
                                       │ CDC (pgoutput)
                                       ▼
                         Loams streams ──► House pipes ──► Iceberg (phase 1) ◄── House / DataFusion (phase 3: + tail)
```

## 2. TDS: why not now (D1031)

### 2.1 Where TDS is better than Postgres wire

| TDS feature | What it gives | The Postgres-wire answer here |
|---|---|---|
| **In-band cancel** (an `ATTENTION` packet on the same connection) | No second connection and no key lookup | D1037: a Loams frontend message on the same connection |
| **Feature negotiation with explicit acknowledgement** (`FEATUREEXT` in LOGIN7, `FEATUREEXTACK` in the response) | The client knows exactly what the server agreed to | D1034: `_pq_.loams.*` plus the `loams.extensions` acknowledgement |
| **Session-state deltas and session recovery** (`SESSIONRECOVERY`, `SESSIONSTATE` tokens) | A dropped connection is rebuilt with its state, and the application does not see it | D1035 |
| **Server-initiated routing and redirect** (`ENVCHANGE` type 20) | Azure SQL sends clients to the right node or to a read replica | D1036 |
| **One-round-trip typed RPC and table-valued parameters** (`RPC` requests, `TVP`) | A procedure call with typed, set-valued arguments in one round trip | D1047's named operations in the Data API; Postgres pipelining and arrays on the wire |
| **Rich column metadata** (`COLMETADATA`: collation, nullability, identity, updatable) | Tools need fewer catalog queries | The Data API's result schema (D1046) carries nullability, origin table and column, and key flags |

### 2.2 Why Loams does not build it now

- **Licence and patents.** The MS-TDS specification (revision 42.0, 2026-06-17) lets anyone copy it to build an implementation under Microsoft's copyright, but says that Microsoft "has patents that might cover your implementations" and grants no patent licence by the notice itself. Whether TDS falls under the Open Specifications Promise needs a check of the Patent Map (verify). One known patent is **US 9,003,054**, which compresses null columns in TDS rows (`NBCROW`). It is listed as active, with an adjusted expiry of 2031-09-10. A server can avoid it by never sending `NBCROW`, which costs bandwidth on wide, sparse rows.
- **The tools want a SQL Server, not a protocol.** SSMS and most SQL Server tooling query `sys.*` catalog views, `sp_*` procedures and server properties as soon as they connect. Serving them means emulating a large catalog, not a wire format.
- **T-SQL semantics are the real cost.** Babelfish for PostgreSQL has been open source since 2021. Its current limitations page still lists bulk copy (`BULK INSERT`, `bcp`) as unsupported, along with partitioning, CLR, Service Broker, full-text search and most XML. Loams would face the same long tail.
- **The BI market is moving to Arrow.** Power BI and Fabric are replacing their embedded ODBC drivers with ADBC drivers (§7). Dremio's replacement driver there is the Flight SQL ADBC driver, which Loams' engine already serves (D49).
- **There is no Rust TDS server to build on.** No production TDS server crate exists. Microsoft's `mssql-rs` has a `mssql-mock-tds` test server, and the `tds-protocol` crate is a codec. Loams would write and fuzz all of it.

### 2.3 What would change the answer

A paying customer's SQL Server migration in which application rewrites are not an option. In that case Loams **ports Babelfish** (its extensions are Apache-2.0 and PostgreSQL-licensed, and they need a patched Postgres with protocol and parser hooks) onto `loams-postgres`, under §51's patch-queue rules, rather than writing T-SQL in Rust. The TDS listener would become a `tds` module of `loams-gateway` that splices to Babelfish's own TDS endpoint, as the `pg` module does for PgDog. Q-level detail waits for that customer. Nothing is planned.

## 3. Part A: Loams protocol extensions in `loams-gateway` (D1032–D1038)

### 3.1 The rule

A client that asks for nothing gets stock Postgres through §57's paths, unchanged. A client that asks for Loams extensions gets them from the gateway, and **PgDog and the compute never see an extension on the wire**. PgDog stays unmodified (D708, D1023), and the compute sees ordinary protocol 3.0 or 3.2 traffic.

### 3.2 Recognising a Loams-aware client (D1033)

The gateway splices `SSLRequest` clients without reading anything after the ClientHello (D1014), so it cannot see their startup parameters. A Loams-aware client therefore says so **in the ClientHello**, by offering ALPN `loams-pg/1` before `postgresql`:

| ClientHello ALPN | Gateway path |
|---|---|
| none (after `SSLRequest`) | splice (§57 §7.2, unchanged) |
| `postgresql` (direct TLS) | terminate and relay (§57 §7.3, unchanged) |
| `loams-pg/1`, `postgresql` (either negotiation) | **terminate**, select `loams-pg/1`, and run the **protocol-aware relay**: it parses frontend and backend messages, handles the extensions, and forwards everything else unchanged over a TLS connection to PgDog |

- Terminating needs the gateway to present PgDog's own certificate, for SCRAM channel binding (§57 §7.3). So Part A works only in `tls_mode = auto` or `terminate`. In `passthrough` mode the gateway does not select `loams-pg/1`, and the client falls back to plain Postgres.
- A client that offers `loams-pg/1` to a server that does not select it (stock Postgres, PgDog directly, an old gateway) proceeds as a plain client. The extensions are always optional for the connection to work. The SDK reports which extensions it got.
- ALPN is visible in clear in the ClientHello. It reveals only that the client is Loams-aware.

### 3.3 Negotiation (D1034)

- **Request.** In its `StartupMessage` the client sends one parameter per extension, `_pq_.loams.<ext>=<version>[;required]`, with `<ext>` from `resume`, `goaway`, `cancel`, `attrs`. This uses Postgres' reserved `_pq_.` namespace for protocol extensions.
- **Strip.** The gateway removes every `_pq_.loams.*` parameter before it forwards the startup to PgDog. PgDog never sees one.
- **Acknowledgement.** After the upstream `AuthenticationOk`, the gateway inserts a `ParameterStatus` named `loams.extensions`, for example `resume=1,goaway=1,cancel=1,attrs=1`. This is the explicit acknowledgement that `FEATUREEXTACK` gives TDS. ParameterStatus is a standard backend message, and clients that do not know the name ignore it.
- **Unknown extensions.** An extension name or version the gateway does not know is listed in a `NegotiateProtocolVersion` message (the standard answer for unrecognised `_pq_` options), and the connection continues without it.
- **Hard errors on surprises:**
  - The client closes the connection if a `;required` extension is not acknowledged.
  - The client closes the connection if the server acknowledges an extension it did not request.
  - The gateway refuses (`FATAL 08P01`) a Loams frontend message for an extension that was not negotiated.
  - The SDKs test these three cases in conformance.
- **Grease.** Loams clients add a parameter `_pq_.loams.grease_<random 4 hex>=1`. The gateway must list it as unsupported in `NegotiateProtocolVersion`. A middlebox or gateway that chokes on unknown `_pq_` parameters is found at once, not when the first real extension ships. PostgreSQL 19's beta period ran protocol grease in libpq for the same reason. The 19 release stops sending it, but the parameter reservations remain.
- **Protocol 3.2.** The gateway accepts protocol 3.0 and 3.2 from the client and speaks the same version to PgDog. PgDog's support for 3.2 is unknown (verify; GW1 Task 0 checks it). Until it is confirmed, the gateway downgrades to 3.0 upstream and keeps the long key (§3.6) on its own side only.

### 3.4 Resumable sessions (D1035)

- **What the gateway records** on a negotiated connection:
  - the startup parameters;
  - every `ParameterStatus` from upstream;
  - every named prepared statement (`Parse` with a name, and its `Close`);
  - each `SET`/`RESET` statement it recognises. The recogniser is deliberately conservative: a top-level `SET` or `RESET` of one setting, in a simple query or an unnamed `Parse`.
- **What makes a session non-resumable:**
  - an open transaction (`ReadyForQuery` status `T` or `E`);
  - temporary tables (`CREATE TEMP`);
  - session advisory locks;
  - `LISTEN`;
  - cursors `WITH HOLD`;
  - any `SET` the recogniser cannot classify, such as `SET` inside a function or `set_config` in a query.
  
  The gateway sends `ParameterStatus loams.resumable=off`, and `on` again when the condition clears (for example, `COMMIT`, or `UNLISTEN *` with no temp tables).
- **Deltas and the token.** The session state is a small, sealed blob: AEAD with a regional key, at most 16 KiB (Q911). It is sent as `ParameterStatus loams.session=<token>` whenever it changes. The SDK keeps the latest token.
- **Resume.** The client reconnects with `_pq_.loams.resume=<token>`, to any gateway replica in the region:
  1. The gateway authenticates the new connection normally. A resume never skips authentication, and the token is bound to the role and the database.
  2. It replays the recorded `SET`s and `Parse`s upstream, consuming their responses itself.
  3. It acknowledges `resume=ok`.
  
  A token past its window (default 300 s, Q912), or one that fails to open, gets `resume=expired`, and the client sees a fresh session.
- **When it helps:** a gateway or PgDog restart, a compute move (§46 §9.5's quiesced move), a compute that scaled to zero while the client's connection was cut, a network change on a mobile client. It never hides a lost transaction. A cut in the middle of a transaction is reported as before, or as `COMMIT_UNKNOWN` through the Data API.

### 3.5 GoAway and redirect (D1036)

- **The message.** The gateway sends an asynchronous `ParameterStatus loams.goaway=<reason>;<deadline_ms>[;<host:port>]` to clients that negotiated `goaway`. ParameterStatus may arrive at any time, so this needs no new backend message type. Reasons: `drain`, `compute_move`, `rebalance`, `read_replica`.
- **The client's duty.** Finish the current transaction, then reconnect (with resume) before the deadline, to the redirect target if one was given. The connection keeps working until the deadline. After it, the gateway closes the connection between transactions.
- **When it is sent:**
  - the gateway is draining (§57 §9.1's rolling restart);
  - `pg-control` plans a compute move or a pool change;
  - a read-only session (database `__ro`, D708) should move to another replica.
- **Upstream.** A Postgres-side `GoAway` message (backend byte `g`) is being discussed on pgsql-hackers. Version 3 of the patch makes it a protocol extension, and it is listed for the PG20 commitfest. When a released Postgres has it, the gateway also accepts the compute's own `GoAway` and forwards it as `loams.goaway`. Clients keep one code path.
- **Plain clients** get §57's behaviour: a drain finishes live relays for up to 60 s and then closes them.

### 3.6 Cancel (D1037)

- **On the same connection.** A negotiated client sends a Loams frontend message `LoamsCancel`. Its type byte is assigned in the spec from bytes that protocol 3.2 does not use, and it is valid only after negotiation. The gateway knows the upstream `BackendKeyData` for that connection, opens a cancel connection to the same PgDog address, and sends a `CancelRequest` (encrypted where PgDog supports it). The client never needs the key and never opens a second connection. This closes the gap of §57 §7.7, where a plaintext cancel on the shared port has no SNI and is dropped.
- **Long cancel keys.** Protocol 3.2 (PostgreSQL 18) allows cancel keys of up to 256 bytes. The gateway gives each Loams-aware client that speaks 3.2 **its own** 32-byte random key in `BackendKeyData` and maps it to the upstream key, so a guessed or leaked key is worth nothing outside its own connection. This holds even while PgDog speaks 3.0 upstream. A protocol 3.0 client keeps the 4-byte key, and a spliced client keeps PgDog's key unchanged, because the gateway cannot see inside its TLS.

### 3.7 Request attributes (D1038)

- **On the wire.** A negotiated client sends a `LoamsAttributes` frontend message before a `Query` or `Parse`/`Bind`. It carries key–value pairs from a fixed list:
  - `traceparent` and `tracestate` (W3C Trace Context, D360);
  - `app.tenant`, the application's own end-user tenant. The Loams tenant still comes from the credential, never from the request (D364);
  - `app.user_agent`;
  - `plan.advice`, which selects an advice set (§4);
  - up to 8 `app.*` keys of the application's own, with names up to 64 bytes and values up to 256 bytes.
  
  Attributes apply to the next request only. The model is MySQL 8's query attributes.
- **To the compute.** PgDog passes only stock messages, so the gateway prepends a comment in **sqlcommenter** format (`/*traceparent='…',app_tenant='…'*/`) to the `Query` text or the `Parse` text. The comment does not change `pg_stat_statements`' query id, because jumbling works on the parse tree (verify). A `loams-postgres` extension, `loams_request_attrs`, reads the leading comment the way `pg_hint_plan` reads its hints. It:
  - exposes the attributes to the planner hook (`plan.advice`, §4.3);
  - writes them into the log line prefix and `pg_stat_activity`'s `application_name` suffix (verify that this is safe);
  - links the backend's OTel span to `traceparent`.
- **Bind-only executions** of an existing named statement carry no SQL text. Q913 asks how their attributes reach the compute. The default is that they get the gateway's span only.
- **At the gateway.** The attributes label the span of the request. Following D1021, they never become metric labels.

## 4. Part B: plan control as an API (D1039–D1041)

### 4.1 Why not hints in the wire

Hints in the protocol or in application SQL tie a plan to the code that sends it. They cannot be changed without a deploy, and agents would write them freely. Both Postgres 19 and TiDB keep advice **on the server, keyed by the query's normalised identity**. Loams does the same, and puts an API around it.

### 4.2 `loams.plan.v1.PlanService` (D1040)

AP0 conventions apply: `NO_SIDE_EFFECTS` reads, `idempotency_key` on mutations, `ErrorInfo.reason`.

| RPC | Does |
|---|---|
| `ExportPlan(target, sql \| query_id, params?)` | Plans the statement without running it. Returns the plan, its advice string (`EXPLAIN (PLAN_ADVICE)` on PG19, a `pg_hint_plan` hint set before it, a TiDB binding's `USING` statement on Loams SQL), the estimates and the query id |
| `Pin(target, scope, query_id, advice, reason)` | Stores advice for that query id in that scope. Pins are versioned and append-only |
| `Unpin(target, scope, query_id)` | Removes the pin, keeping it in the history |
| `ListPins(target, scope?)` | Lists the pins with their history |
| `DiffPlans(a, b)` | Compares two plans (two branches, two engine versions, pinned against free) node by node, with estimate deltas |
| `CheckRegressions(target, against)` | Re-plans every pinned query, and the top-N queries by total time from `pg_stat_statements` (or TiDB's statement summary), on `against` (a branch, a new engine version, a schema change). Reports the plans that changed and whether their estimated cost rose by more than a threshold (default 2×) |

### 4.3 Engines (D1040)

| Product | Until | Advice mechanism |
|---|---|---|
| Loams Postgres on 14–18 | Loams ships PG19 images (§51 §8: 19 is beta in every Loams document) | `pg_hint_plan` (BSD-3-Clause, already in the fork's **core** extension catalogue, §51 §7). Pins go into its hint table, which is keyed by query id in current releases (verify the pinned version). `pg-control` writes it on every compute start |
| Loams Postgres on 19+ | — | `pg_plan_advice` (the advice language: `JOIN_ORDER`, `HASH_JOIN`, `INDEX_SCAN`, `SEQ_SCAN`…; feedback `matched`, `not matched`, `conflicting`…) and `pg_stash_advice` (per-query-id advice applied automatically). Advice constrains the planner and never forces a plan it would not consider |
| Loams SQL (TiDB) | — | SQL plan bindings: `CREATE GLOBAL BINDING FOR … USING …`, applied by `loams-sqlgate`'s control path per branch keyspace |

**Advice sets.** A pin may belong to a named set, for example `reporting`. A request selects a set with the `plan.advice` attribute (§3.7) or the Data API's `plan_advice` field. `loams_request_attrs` switches the set before planning. Without one, only the default set applies.

### 4.4 Safety (D1041)

- **Every pin has a scope:** a branch is required, and a database and a role are optional. There are no global pins.
- **Scopes for writing:**
  - `plan:write` may pin on unprotected branches.
  - Protected branches (D713) need `plan:admin`.
  - Agents get `plan:read` by default.
- **Every change is audited** as a CloudEvent `io.loams.dev.plan.pin.v1` (who, scope, query id, before, after, reason) on the namespace's audit stream.
- **`CheckRegressions` is a gate.** It runs in the upgrade preflight (§54) and before a branch is merged or swapped in a restore (D713). A regression is reported, never applied automatically.
- **Pins survive branching.** A child branch inherits its parent's pins at branch time.

## 5. Part C: the Loams Data API (D1042–D1050)

### 5.1 What it is

The Data API is an open, versioned specification (`data-api-1`), together with Loams' implementations of it. It lets an application, an edge function or an agent run SQL against a Loams database over HTTP, without a Postgres or MySQL driver and without holding a connection. SQL stays the language: Postgres' dialect for Postgres, TiDB's for Loams SQL, and DataFusion's for the engine.

**Why SQL, and not a new language.**
- Fauna's FQL went with Fauna, which shut down its service in 2025.
- EdgeQL's company, Gel, joined Vercel and closed Gel Cloud on 2026-01-31. The language stays open source.
- PRQL remained a compiler to SQL with little production adoption.
- XTDB moved from XTQL to SQL as its primary language in 2.x (verify).

The improvements worth having arrive *inside* SQL:
- pipe syntax (`FROM t |> WHERE … |> AGGREGATE …`), from Google's paper, shipped in GoogleSQL and adopted elsewhere. Loams accepts it where the engine parses it (DataFusion; verify);
- SQL/PGQ graph queries (`GRAPH_TABLE`, `CREATE PROPERTY GRAPH`), committed to Postgres master on 2026-03-16 for PostgreSQL 19 (verify that it is in the 19 release).

**Relation to Q720.** NF1 dropped Neon's private `subzero` broker (D816), and Q720 asked whether to adopt PostgREST after PG2 GA. D1042 proposes this API instead. It is RPC-shaped rather than REST over tables, it covers all three products, and it is designed for agents.

### 5.2 The request model (D1043)

`loams.data.v1.DataService`:

| RPC | Shape |
|---|---|
| `Execute(ExecuteRequest) → ExecuteResponse` | **A batch:** an ordered list of statements with positional or named parameters, run in one implicit transaction (all or nothing) unless `mode = AUTOCOMMIT_EACH` |
| `Call(CallRequest) → CallResponse` | **A named operation** (§5.6) with typed arguments |
| `Begin` / `Continue` / `Commit` / `Rollback` | **Interactive transactions, opt-in.** `Begin` returns a **baton**, an opaque token that pins the transaction to its upstream connection, and each later call returns the next one. This is the Hrana model: the calls stay unary, so D420's rule (no client- or bidi-streaming in the application API) holds. A baton expires after `idle_timeout` (default 10 s, maximum 60 s), and the transaction is then rolled back |
| `ExecuteArrow` | As `Execute`, with an Arrow IPC result (§5.5) |
| `Subscribe(SubscribeRequest) → stream SubscribeEvent` | A live query (§5.7), server-streaming |
| `Explain(ExecuteRequest)` | Dry run (§5.4) |

- **Transports.** Connect unary over HTTP/1.1 and HTTP/2 (JSON or protobuf), so `curl` and `fetch` work from edge runtimes. An optional **WebSocket** at `/loams.data.v1/ws` carries the same messages, one per frame, with a request id. It lowers per-call latency for interactive transactions, and it is not a gRPC stream, so D420 is unchanged. The model is Neon's serverless driver (HTTP for one-shot queries, WebSocket for sessions) and libSQL's Hrana.
- **Every request carries:**
  - `target`: `postgres:<project>/<branch>/<database>`, `sql:<database>/<branch>`, or `engine:<namespace>`;
  - an optional `idempotency_key`;
  - `budget` (§5.4);
  - `consistency` (`STRONG`, `AT_LEAST <token>`, or `REPLICA_OK` to allow a read replica);
  - `plan_advice`;
  - W3C trace context in the `traceparent` header.
- **Authentication** uses Loams credentials (MT1). How a credential becomes a Postgres role is Q917.

### 5.3 Idempotency and errors (D1044)

- **Idempotency.** The key is 16–64 bytes, and the window is 24 hours, as API2-Q4.
  - **For Postgres and Loams SQL writes, the key is recorded inside the transaction** as a row in `_loams.requests(key, digest, result, expires_at)`, written just before `COMMIT`. A retry after an unknown outcome finds the row and gets the stored result, or finds no row and knows the first attempt did not commit. That makes "exactly once" true even across a commit whose acknowledgement was lost.
  - Reads, and the engine target, use API2's metastore ledger.
  - A retry whose request digest differs from the stored one is refused with `idempotency_key_reused`.
- **The error taxonomy.** Each error reaches the client as `ErrorInfo.reason` and a category:

| Category | Means | Mapped from (examples) |
|---|---|---|
| `RETRYABLE_CONFLICT` | Retry the whole transaction or batch | SQLSTATE `40001` (serialization failure), `40P01` (deadlock); TiDB `9007` (write conflict), `8028` (schema changed) |
| `RETRYABLE_UNAVAILABLE` | Retry later. Nothing was applied | `57P01`–`57P03` (admin shutdown, cannot connect now), `53300` (too many connections), a compute that is waking |
| `COMMIT_UNKNOWN` | The commit was sent, and its outcome was lost. Retry with the same idempotency key to learn it | A connection error (`08xxx`) after `COMMIT` was sent |
| `BUDGET_EXCEEDED` | A budget in §5.4 was hit. Nothing was applied, or the transaction was rolled back | `57014` (query canceled by the budget's timer), the server's row and byte caps, a planning cost over budget |
| `PERMANENT` | Do not retry | `23xxx` (integrity), `42xxx` (syntax, privilege), `22xxx` (data) |

- **The SDK retries** `RETRYABLE_*` with backoff, and `COMMIT_UNKNOWN` only when the request has a key. It never retries `PERMANENT`.

### 5.4 Dry run and budgets (D1045)

- **`Explain`** runs nothing. It returns:
  - the plan and its estimates (rows, cost);
  - the query id, and the pins that would apply;
  - the **write set**: each table written, with the estimated rows and the kind of change (`INSERT`, `UPDATE`, `DELETE`).
  
  For Postgres it is `EXPLAIN (FORMAT JSON)` without `ANALYZE`, run in a transaction that is always rolled back. For a named operation, the write set is the operation's declared write set (§5.6).
- **Budgets.** `budget { max_duration_ms, max_rows, max_bytes, max_cost_units }`. The defaults are set per credential type in config. An agent defaults to 5 s, 10 000 rows and 16 MiB.
  - **Cost units** are the planner's total cost estimate (Postgres) or the estimated rows times width (TiDB, engine). A plan over budget is refused at planning, before it runs.
  - Time is enforced with `statement_timeout`, set per transaction.
  - Rows and bytes are counted while the result streams. Hitting a cap cancels the statement and returns `BUDGET_EXCEEDED`, after rolling back if anything was written.
  - The model is PlanetScale's traffic control: budgets on queries, not only on connections.

### 5.5 Results (D1046)

| Size | Format | How |
|---|---|---|
| Small: point reads, ≤ 10 000 rows and ≤ 1 MiB (defaults) | JSON rows (or protobuf) with a result schema: name, type, nullability, origin table and column, and whether the column is part of a key | `Execute` |
| Bulk: up to 256 MiB (default) | **Arrow IPC** stream | `ExecuteArrow`; the engine target can also use Flight SQL directly |
| Very large | **Presigned URLs** to Arrow IPC or Parquet partitions | The server writes partitions under `ns/<ns>/results/<request_id>/part-<n>.parquet` and returns presigned GET URLs that expire after 1 hour, with the schema and row counts. GC removes the objects after 24 hours. On stores without presigning (some RustFS setups) the URLs point at a Loams download endpoint instead (Q918) |

The Arrow and Parquet type mapping follows §34 §4's events-to-Arrow rules where they overlap, and Postgres' own type OIDs are kept in Arrow field metadata.

### 5.6 Named operations (D1047)

- **A named operation is a SQL function** in the database: Postgres `CREATE FUNCTION` in a schema the application exposes (default `api`), or a TiDB stored routine where one exists. For Loams SQL, a registered parameterised statement until TiDB has functions (verify). It is registered with the Data API by `RegisterOperation(target, function, options)`, which records:
  - its argument and result types (from the catalog);
  - its declared write set: the tables it may write, checked at execution;
  - its idempotency rules;
  - who may call it.
- **Deterministic.** The function may not call `now()`, `random()`, `gen_random_uuid()` or `clock_timestamp()` directly. The registry rejects functions whose bodies use them (verify how the check is done: a parser pass over `prosrc` or a lint on `pg_proc`). Instead the server passes `_loams_now timestamptz` and `_loams_seed bytea` as **recorded arguments**, and `loams.uuid(_loams_seed, n)` derives ids.
- **Replayable.** Every call is logged as a CloudEvent `io.loams.dev.data.call.v1` with the operation, its arguments, the injected clock and seed, and the result digest. Replaying the log on a branch reproduces the same writes, which is how an agent's changes are tested before they are merged.
- **Why functions.** They live with the schema, they version with migrations, they branch with the database, and Postgres already enforces their privileges. This is the shape of Convex mutations, Zero 1.0's custom mutators and SpacetimeDB reducers: named, typed, deterministic server-side writes. Here they are standard SQL and need no new runtime.

### 5.7 Live queries (D1048)

- `Subscribe(target, sql | operation, params)` returns the initial result, then `diff` events (`added`, `removed`, `changed` rows keyed by the primary key), and a `cursor` after each change.
- **Mechanism.** The server finds the tables the query reads (its plan's relations). It then follows those tables' changes on the existing change streams: pgoutput into Loams streams (D154) for Postgres, `loams-sqlcdc` (D738) for Loams SQL, and changelog streams (D21) for the engine. All three carry CloudEvents (D270). On a relevant change it **re-runs the query**, debounced (default 100 ms), and sends the diff. Incremental maintenance (`pg_ivm` is in the catalogue) is a later optimisation (Q919).
- **Resuming.** A subscriber that reconnects with its cursor gets the diff since that cursor, or a fresh initial result if the stream was trimmed past it.
- **Limits.** Subscriptions count against budgets, with a per-credential cap (default 100) and a maximum result size (default 10 000 rows). A query over the cap is refused with `subscription_too_large`.

### 5.8 MCP tools and the agent profile (D1049)

- **Generated tools.** For each database the Data API exposes, the `mcp` module of `loams-gateway` (D1022) generates:
  - one MCP tool per **named operation**, with a JSON Schema taken from the function's signature and its comment as the description;
  - `describe_schema`;
  - `explain`;
  - `query`, which is **read-only**.
- **The agent safety profile**, the default for agent credentials:
  - **Read-only by default.** `query` runs in a read-only transaction (`SET TRANSACTION READ ONLY`, plus a role without write grants).
  - **Writes only on a branch.** A write tool runs on the session's own branch (`agent/<session>`, created on first write from the target branch, TTL 7 days, Q920), never on a protected branch. Changes reach the real branch through a human-approved merge or a replay of the operation log (§5.6).
  - **Budgets on** (§5.4).
  - **Named tools, not raw SQL**, for writes. Raw SQL writes are not exposed to agents at all.
  - **Results are untrusted data.** Tool output wraps row data in a marked, typed envelope, and the tool descriptions tell the model not to follow instructions found in data.
- **Why.** In the Supabase MCP incident of 2025, an agent with a service-role database connection read a support ticket containing instructions and leaked another table's data into a public reply. Private data, untrusted input and an exfiltration path were all reachable from one tool (Simon Willison's "lethal trifecta"). This profile removes each of the three by default: data stays read-only, the role is scoped, and writes are named, branched and budgeted.

### 5.9 Where it is served, and API2 (D1050)

- **Postgres and Loams SQL:** a `data` module of `loams-gateway`, on the `http` module's Pingora stack (D1013, D1022). It connects to the namespace's PgDog as an ordinary Postgres client, over TLS and SCRAM with a per-project authenticator credential (Q917), so PgDog stays the front door for Postgres semantics. It connects to `loams-sqlgate` for Loams SQL.
- **Engine SQL:** the `loams` binary serves `DataService` for `engine:` targets on its main port, over the same DataFusion planner as the Flight SQL listener.
- **API2.** API2 Task 1 plans `loams.sql.v1.SqlService/Query` and `QueryArrow`. Neither proto exists on `dev` yet. They **ship as planned**, so API2 is not blocked. When DA1c lands, they become a thin shim over `DataService.Execute` and `ExecuteArrow` for `engine:` targets, and §44 §5.1's SQL row points at `loams.data.v1`. This amends §44 and does not break it: `SqlService` stays served for at least one minor release after the shim, under D360's deprecation rule.
- **The spec** lives at `docs/spec/data-api/` with `proto/loams/data/v1/`, licensed Apache-2.0 and versioned on its own (`data-api-1.0`). The conformance suite is public, so another implementation can claim compatibility (Q916).

## 6. Part D: HTAP in three phases (D1051–D1054)

### 6.1 The shape

| Phase | What | When |
|---|---|---|
| 1 | **CDC into Iceberg**, assembled from pieces already designed | As planned: PG2 and HS1 |
| 2 | **The pageserver writes columnar files** with its image layers: one copy and no pipeline | After Loams Postgres GA |
| 3 | **Fresh analytics:** read Iceberg as of an LSN and merge the recent tail | After phase 2 |

### 6.2 Phase 1: CDC into Iceberg (D1052)

```
loams-postgres ── pgoutput (D154) ──► Loams stream (CloudEvents, D270) ──► House pipe (D769) ──► Iceberg (lake table)
Loams SQL      ── loams-sqlcdc (D738) ─┘
```

- Nothing new is invented. The bridge from D154 confirms the slot only after the append. The House pipe commits the consumed offsets in the same Iceberg snapshot as the rows, which makes the whole path exactly-once.
- **What DA1 adds:**
  - `loams pg analytics enable <project>/<branch> [--tables …]`, which creates the publication, the bridge, the stream, the lake table and the pipe in one step;
  - a replace-on-key mode for the lake table (D765's `Replacing` merge on the primary key with a delete marker), so the table reflects updates and deletes;
  - lag in the House's `system.loams_pipes`.
- **Postgres goes first.** Loams SQL follows when `loams-sqlcdc` is built (Q922).

### 6.3 Phase 2: the pageserver writes columnar (D1053)

- **The idea.** The pageserver already reconstructs whole pages when it creates **image layers**. For a table that opts in, it also decodes the heap tuples of those pages into Arrow and writes Parquet files. It then commits an Iceberg snapshot of the relation, through Lakekeeper, tagged with the image layer's LSN. That is one copy of the data, written by the component that already writes it, with no pipeline.
- **The data:**
  - **Tuple decoding** needs the relation's descriptor at that LSN, which the pageserver reads from the catalog pages at the same LSN.
  - **Visibility** is decided at the image LSN: tuples whose `xmin` committed at or before it and whose `xmax` did not. That needs the commit log (`pg_xact`) at the LSN, which the pageserver also stores.
  - TOASTed values are de-TOASTed from the TOAST relation at the same LSN.
- **The format.** Iceberg v2 with Parquet, partitioned like the relation's own partitions, with the row's `ctid` dropped and the primary key kept. Updates between image layers are equality deletes plus inserts. When the pinned iceberg-rust and Lakekeeper support v3, they become deletion vectors (Q921).
- **Off by default, per table** (`ALTER TABLE … SET (loams.columnar = on)`, a reloption the fork's extension registers). It costs pageserver CPU and bucket writes, which §46 §13's usage hooks must be able to see.
- **This is the `loams-postgres` fork's work** (§51, patch-queue rules). It is pageserver code and touches no Postgres file.

### 6.4 Phase 3: fresh analytics (D1054)

- **The read.** The House (chDB) and the engine's DataFusion read the relation's Iceberg snapshot as of LSN *L*, the latest image LSN. They then merge the **tail**, the changes after *L*. The tail is decoded from the WAL that `loams-wal` holds, or from the pageserver's delta layers, through the fork's WAL decoder (`loams-wal-decoder`) into Arrow batches with the same schema. The tail is applied as upserts and deletes by primary key at read time. This is the same rule Loams already uses for its engine's tail (§01's glossary: data committed to the log but not yet in the durable indexed form, merged into every read).
- **Freshness** is reported with every result: the LSN read up to, and the lag. The client chooses `fresh` (merge to the current commit LSN) or `snapshot` (Iceberg only, cheaper).
- **Bounds.** A tail longer than a configured size (default 256 MiB of WAL) forces `snapshot` with a warning, and the pageserver is asked to create an image layer sooner.

### 6.5 Positioning against LTAP

Databricks' LTAP is the same idea as phases 2 and 3: the pageserver transcodes pages into open columnar files, and queries merge the newest changes from the pageserver at read time. Loams' difference is that **all of it is open source**, on the customer's own bucket, with the Iceberg REST catalog (Lakekeeper), and Postgres wire in front of it. LTAP was announced on 2026-06-16 as "coming soon", so the difference rests on shipping it, not on the idea.

## 7. Part E: ADBC as the way in for BI tools (D1055, D1056)

### 7.1 What exists

The engine serves Flight SQL on its own listener (`:8082`). It serves queries and `DoPut` bulk ingest, including ADBC's `adbc_ingest` (`CommandStatementIngest`). The ADBC Flight SQL drivers for Python and Go are an M1 exit gate (D49, §05 §8, as built in M1.2).

### 7.2 What DA1 adds (D1056)

- **A thin Loams ADBC driver** over the Arrow project's Flight SQL driver. It adds Loams credentials (API key, OIDC token), namespace and branch selection, and consistency tokens. It is published for the languages the ADBC driver manager supports (C/C++, Go, Python, R, Java via JNI, C#), as `adbc_driver_loams` (Q925).
- **Power BI and Fabric.** Microsoft is moving their embedded ODBC connectors to ADBC drivers. The tenant setting rolls out from July 2026, Microsoft plans to turn it on by default from October 2026, and it plans to remove the ODBC drivers from the service from early 2027. Dremio's replacement there is already the Flight SQL ADBC driver. Loams ships a **Power Query custom connector** (`.mez`) that uses the Flight SQL ADBC driver against the engine and, after phase 1, against the House's lake tables through the engine. Certification by Microsoft is outward-facing, so the owner decides it (Q923).
- **ADBC 1.1 features on the server:**
  - **statistics** (`GetStatistics`: row counts, distinct counts and null counts from the manifest, Iceberg and Lance statistics);
  - **incremental execution** (`adbc.statement.exec.incremental`, which returns partitions as they are ready);
  - **partitioned results** (`ExecutePartitions`, which returns Flight endpoints a client may fetch in parallel, mapped to scan-plan fragments, D53);
  - cancellation and progress.

## 8. Durability substrates: TiKV Raft and `loams-wal` (D1057–D1060)

The owner asked: "loams-wal vs TiKV Raft — which is best, keep separate?" The answer is to keep them separate by role and share the common pieces.

### 8.1 The roles (D1057)

| Substrate | Is for | Today's users | Not for |
|---|---|---|---|
| **TiKV (Raft, Percolator transactions)** | Small transactions and metadata: keyed records, CAS, leases, votes, sequencers | Loams SQL's data (TiKV *is* TiDB's storage, D725 unchanged); Loams Live (D130); the scale-out `MetaStore` (D124); `loams-wal`'s votes, leases and timeline metadata (D268, D269); `pg-control`'s store; control planes; Git refs of hot repositories (D1025, proposed in PR #668) | A high-volume ordered byte log |
| **`loams-wal`** | The high-volume ordered byte log for Postgres WAL: group commit, quorum ack, streaming to the pageserver, offload to the bucket | Loams Postgres (D714 unchanged) | Records with keys, transactions, metadata |
| **The bucket (create-only objects)** | History, branches, archives, and logs whose commit rate fits object-store latency | The engine's WAL objects (D25), Git's WAL (D388), `.lwal` archives (D269), Iceberg, Lance, pageserver layers | Millisecond commits |

### 8.2 Why not one of them for everything

- **TiKV as the Postgres WAL** pays, per append:
  - a TSO round trip;
  - a Raft log write;
  - a RocksDB apply, and its compaction later.
  
  That is write amplification on bytes that are read once and trimmed. That cost is fine for records and wrong for a byte log. §46 §9.2's laptop results (rf3, three runs, measured) show it:
  - `commit-1`: 58–87 TPS on TiKV against 109–117 for the safekeeper reference;
  - `commit-16`: 474–712 against 814–983;
  - `bulk`: 8–18 MB/s.
- **`loams-wal` as general metadata** would need keys, transactions and range reads, which TiKV already has and has tested. Building them again is the trap D725 avoided for Loams SQL.
- **Neither for Loams SQL.** D725 stands: a second log in front of TiKV adds latency and no durability.

### 8.3 The launch store: the lean (D1058)

- **Arm A** (walproposer's Paxos over three acceptors on local NVMe, D264) passes `commit-16` and `tpcb-16` against the reference in every tier (measured). It also passes `commit-1` with SQPOLL. **Its `bulk` is 8–32 MB/s against 45–228**, which PG2d's Task 33 is attacking (§46 §9.4).
- **The lean:** Arm A for production, and the TiKV store kept behind `WalStore` for small and single-node deployments, where one fewer stateful service matters more than throughput.
- **This is a proposed default, not a ruling.** D714 named the TiKV hot tier as the default launch candidate, on the owner's directive. PG2 Task 42's bake-off against §46 §9.3's absolute targets, and the owner's answer to Q652, decide. If TiKV wins the bake-off, D1058 is withdrawn.

### 8.4 Git (D1059)

- Loams Git does not use `loams-wal`. A repository's commit rate is pushes, not WAL bytes. Its objects (packs) are immutable and belong on the bucket.
- The bucket WAL (create-only `wal/<seq>.lgw` segments, D388, D389) stays the **default** `RefLog`.
- D1025 (PR #668) proposes a TiKV-backed `RefLog` for hot repositories, mirrored into the bucket WAL. That is consistent with §8.1: TiKV for small transactions, the bucket as the record.

### 8.5 One bucket-segment library (D1060)

- **Two logs on the bucket do the same things:**
  - `loams-wal`'s offload writes `pgwal/<tenant>/<timeline>/<begin>-<end>.lwal` under a TiKV lease (D269), with `loams-log`'s framing conventions;
  - Loams Git writes `wal/<seq:020>.lgw` with `If-None-Match: *` (D389).
  
  Both need: create-only segment writes; fencing by the create condition; resolving an unknown outcome (GET the name, and compare the batch id); header, body and CRC32C framing; checkpoints; readers that stop at the first gap; compaction; and GC after a grace period.
- **A new crate `loams-seglog`** (Apache-2.0) holds those pieces, extracted from `loams-git`'s `StoreWalStore` (`crates/loams-git/src/wal.rs`) and generalised over the naming scheme (sequence numbers for Git, LSN ranges for `.lwal`). Both logs keep their own formats and their own version numbers. Only the code is shared.
- **The traits.** Both crates have a trait named `WalStore`: Git's over the bucket, `loams-wal`'s over hot-tier stores. The library adds a third concept, `SegmentLog`, and neither existing trait is renamed.
- **When.** Extract when the second user is built, in PG2 Task 40 (the offload), not before (Q924). That extraction is DA1 Task F1, and PG2 Task 40 and GT1's WAL tasks carry pointers to it.

## 9. Relation to earlier decisions

| Decision | Effect |
|---|---|
| **D708** (PgDog is the front door, unmodified) | Kept. Extensions end at the gateway (§3.1), and the Data API is a PgDog client (§5.9) |
| **D710, D1014** (TLS on both hops; splice `SSLRequest` clients, terminate direct ones) | Kept for plain clients. **Extended by D1033:** clients that offer ALPN `loams-pg/1` are also terminated, with PgDog's own certificate, and re-encrypted to PgDog |
| **D1017, Q895** (cancels: ordered upstreams; plaintext cancels on the shared port dropped) | D1037's same-connection cancel removes the need for a plaintext cancel for Loams-aware clients. Q895's default stands for the others |
| **D1021** (no tenant labels on metrics) | Kept. Request attributes label spans only |
| **D1022** (later `http`, `llm`, `mcp` modules) | The `data` module and the generated MCP tools (§5.8, §5.9) are added to the `http` and `mcp` plans |
| **§44 §5.1, API2 Task 1** (`loams.sql.v1.SqlService/Query`, planned, no proto yet) | **Amended by D1050:** it ships as planned, then becomes the engine target's shim over `loams.data.v1` |
| **D420** (no client- or bidi-streaming in the application API) | Kept. Interactive transactions use batons over unary calls, and the WebSocket carries unary messages |
| **Q720** (Data API: drop `subzero`; PostgREST after GA?) | Proposed answer: D1042, the Loams Data API, and no PostgREST |
| **D154, D738, D769** (pgoutput bridges, `loams-sqlcdc`, House pipes) | Composed as HTAP phase 1 (D1052), unchanged |
| **D49** (Flight SQL and ADBC) | Extended by D1056 |
| **D714, D725** (`loams-wal` for Postgres; TiKV Raft as Loams SQL's WAL) | Unchanged (D1057) |
| **D234, D264, Q652** (the TiKV hot tier and Arm A; the launch-store question) | Unchanged. D1058 records a lean as a proposed default, and Q652 still decides |
| **D388, D389, D1025** (Git's bucket WAL; the TiKV `RefLog`, PR #668) | Consistent (D1059). D1060 shares their segment code |
| **D360** (the standards charter) | New pinned standards, each behind one crate: Postgres protocol 3.0 and 3.2, and the `_pq_.loams.*` extension spec (`loams-gateway`); ADBC 1.1 (the driver); sqlcommenter (`loams_request_attrs`); MCP 2026-07-28 (the `mcp` module) |
| **D548** (usage stays outside this repository) | Kept. Budgets and live-query caps are limits, not accounts, and per-namespace numbers leave only through observer hooks |
| **D816, §51** (the fork's private dependencies dropped; patch-queue rules) | Phase 2 and `loams_request_attrs` are fork work under §51's rules |

## 10. The plan (D1061)

[DA1](../plans/2026-10-16-da1-data-access.md) is a stub that lists tasks only. Each part gets a full plan when it starts.

| Part | Scope | Waits on |
|---|---|---|
| **DA1a** (Part A) | The extension spec; ALPN recognition; the protocol-aware relay; negotiation and grease; long cancel keys; same-connection cancel; GoAway; request attributes and `loams_request_attrs`; resume | GW1 Tasks 1–8 (the gateway), and PgDog's protocol 3.2 support checked |
| **DA1b** (Part B) | `loams.plan.v1`; `pg_hint_plan` pins; TiDB bindings; `CheckRegressions` in the upgrade preflight; PG19 `pg_plan_advice` and `pg_stash_advice` | DA1a's attributes for advice sets; NF1f (PG19 images) for the 19 path |
| **DA1c** (Part C) | The `data-api-1` spec and conformance suite; `DataService` for engine targets (API2's shim); the `data` module for Postgres; idempotency rows; errors; budgets; results; named operations; live queries; MCP tools and the agent profile | API2 Task 1; the gateway's `http` plan (Pingora); PG2 Task 22 |
| **DA1d** (Part D) | Phase 1 `loams pg analytics enable`; then phases 2 and 3 | PG2 and HS1 for phase 1; Loams Postgres GA for phases 2 and 3 |
| **DA1e** (Part E) | `adbc_driver_loams`; the ADBC 1.1 server features; the Power Query connector | None for the driver; the owner for certification (Q923) |
| **DA1f** (§8.5) | Task F1: the `loams-seglog` extraction, then Loams Git's WAL and `loams-wal`'s offload on it | PG2 Task 40 (Q924) |

## 11. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Extensions fork the ecosystem: Loams-only behaviour that drivers must special-case | Medium | Strictly opt-in. Every extension degrades to stock Postgres. GoAway adopts the upstream message when one ships. The spec is public |
| The conservative `SET` recogniser misses state, so a resumed session differs | Medium | Unknown statements mark the session non-resumable (§3.4). Conformance tests run resume after each statement class |
| sqlcommenter comments change query ids or break statement caching | Low (verify) | DA1a's first task tests `pg_stat_statements` ids with and without comments. Attributes are opt-in |
| The Data API's idempotency rows add a write to every keyed transaction | Medium | Only keyed requests pay. The table must be a logged table to survive a crash, so the extra write is real; DA1c measures it |
| Phase 2's tuple decoding in the pageserver is hard to get right (TOAST, visibility, schema changes) | High | Phase 1 ships first and stays as the fallback. Phase 2 is per table and off by default, with a checksum check against a `SELECT` at the same LSN in CI |
| LTAP or another vendor ships the same thing first | Medium | Loams' position is open source on the customer's bucket, not novelty (§6.5) |
| The Power BI timeline moves | Medium | The ADBC driver is useful without Power BI. The connector follows Microsoft's dates |

## 12. Open questions (Q910–Q925)

| # | Question | Default |
|---|---|---|
| Q910 | How does a Loams-aware client announce itself: ALPN `loams-pg/1` in the ClientHello, or only `_pq_.loams.*` startup parameters (which would make the gateway terminate every client to read them)? | ALPN `loams-pg/1`; plain clients stay spliced (D1033) |
| Q911 | Where does resumable session state live: a sealed token held by the client (any replica can resume), or the gateway's memory (no size limit, but one replica only)? | A sealed token of at most 16 KiB; over that, the session is resumable only on the same replica |
| Q912 | The resume window | 300 s |
| Q913 | How do request attributes reach the compute for Bind-only executions of a named statement under PgDog's transaction pooling? | Only the gateway's span gets them; revisit if PgDog passes a protocol extension |
| Q914 | GoAway as an asynchronous `ParameterStatus`, or a new backend message now? | `ParameterStatus loams.goaway`; also accept upstream's `GoAway` message once a released Postgres has it |
| Q915 | Where do `pg_hint_plan` pins live before PG19, and what is their source of truth? | `pg-control`'s store is the record; the hint table is written on every compute start |
| Q916 | The Data API spec's licence and home | Apache-2.0, `docs/spec/data-api/` and `proto/loams/data/v1/`, versioned on its own as `data-api-1.0`, with a public conformance suite |
| Q917 | How does a Loams credential become a Postgres role in the Data API? | The PostgREST pattern: a per-project authenticator role, `SET LOCAL ROLE <mapped role>`, and the claims in a `loams.claims` setting for RLS policies |
| Q918 | Presigned result URLs: their lifetime, where the objects live, and the threshold | 1 h URLs, `ns/<ns>/results/`, GC after 24 h, used only for results over 256 MiB; a Loams download endpoint where the store cannot presign |
| Q919 | Live queries: re-run on invalidation, or incremental view maintenance? | Re-run, debounced by 100 ms; `pg_ivm` later, if measurements need it |
| Q920 | The agent profile's branch for writes: created per session automatically, or named by the caller? | Created automatically (`agent/<session>`, TTL 7 days) unless the caller names one |
| Q921 | Phase 2's format: Iceberg v2 with equality deletes now, or wait for v3's deletion vectors? | v2 now; v3 when the pinned iceberg-rust and Lakekeeper support it |
| Q922 | Does HTAP phase 1 for Loams SQL wait for `loams-sqlcdc`? | Yes; Postgres goes first |
| Q923 | Power BI: a custom connector only, or Microsoft certification too? Certification is outward-facing, so the owner decides | A custom connector first; certification when customers ask |
| Q924 | When to extract `loams-seglog`: now, from `loams-git` alone, or when `loams-wal`'s offload is built? | When the offload is built (the second user) |
| Q925 | A Loams ADBC driver, or documented options for the Flight SQL driver? | A thin `adbc_driver_loams` over the Flight SQL driver, for auth and defaults |

---

## Appendix A. Research (2026-10-16)

### A.1 Postgres wire won

| System | What it is (the protocol it speaks) | Source |
|---|---|---|
| CedarDB | HTAP database from TUM's Umbra research. "PostgreSQL-compatible"; existing drivers and tools work. CedarDB Cloud is generally available as of 2026-10-14. The engine is proprietary, and its storage is SSD-based (the Cloud post does not say) | ([source](https://cedardb.com/blog/cedardb_launches_htap/)), ([source](https://cedardb.com/blog/cedardb_cloud/)) |
| Aurora DSQL | AWS's distributed SQL, Postgres-compatible. It added foreign-key constraints in 2026-08 | ([source](https://aws.amazon.com/about-aws/whats-new/2026/08/aurora-dsql-foreign-key-constraints/)) |
| PlanetScale Neki | Sharding for ordinary Postgres behind a router that is "a single Postgres wire-protocol endpoint"; durability per shard (async, sync, cross-zone sync). Platform preview (post 2026-09-18) | ([source](https://planetscale.com/blog/the-architecture-of-neki)) |
| Supabase Multigres | Vitess-style scale-out for Postgres, announced with Supabase's 2026 "scale without limits" launch | ([source](https://supabase.com/blog/select-2026-scale-without-limits)) |
| TiDB | "PostgreSQL support on TiDB Cloud Starter enters public preview today" (2026-10-15). No production SLA, Starter only. Also: TiDB X's core code is to be open-sourced by the end of 2026 | ([source](https://www.pingcap.com/blog/tidb-cloud-scaile-2026-product-announcements/)) |
| Firebolt | Cloud analytics engine with a Postgres-compatible dialect and driver protocol (verify the wire details) | — |

### A.2 Commits stay off S3; S3 holds history

| System | Commit path | What S3 holds | Source |
|---|---|---|---|
| Neon / Databricks Lakebase | "a transaction is committed once a quorum acknowledges the record", through Paxos safekeepers | The pageserver writes page history to object storage asynchronously, off the commit path (post 2026-08-27) | ([source](https://www.databricks.com/blog/object-storage-wal-lakebase-postgres-agentic-era)) |
| TiDB X | "The Raft log is first persisted to local disk"; WAL chunks are "uploaded to object storage in the background" | "The single source of truth for all data": SSTs and the Raft WAL chunks. Whether the ack waits for S3 is not stated (it does not appear to) | ([source](https://docs.pingcap.com/tidbcloud/tidb-x-architecture/)) |
| OceanBase | Its LogService (Paxos log streams) acknowledges commits; shared-storage mode puts data on object storage | Data and backups | ([source](https://en.oceanbase.com/docs/common-oceanbase-database-10000000003679868)) |
| Azure HorizonDB | A separate WAL service acknowledges; page servers and storage are behind it (verify the details) | Pages and history | — |
| Loams Postgres | `loams-wal`'s quorum, then the bucket every 250 ms (D714) | `.lwal` archives, pageserver layers | §46 §9 |

### A.3 HTAP patterns

| Pattern | Systems | Notes | Source |
|---|---|---|---|
| One engine, hybrid format | CedarDB, SingleStore, OceanBase | CedarDB: proprietary, Cloud GA 2026-10-14 | ([source](https://cedardb.com/blog/cedardb_launches_htap/)) |
| Synced columnar replica | TiFlash (Raft learner), Spanner columnar engine, AlloyDB columnar engine | Spanner: "Columnar engine for Spanner is now generally available (GA)" on 2026-04-17, with PostgreSQL-interface databases supported | ([source](https://docs.cloud.google.com/spanner/docs/release-notes)) |
| Columnar at the storage layer | Databricks LTAP on Lakebase | Announced 2026-06-16, "coming soon as a part of Lakebase". Lakebase stores data in Unity Catalog in Delta and Iceberg; the pageserver transcodes pages from row format into open columnar form as it materialises them; analytical queries read object storage as of an LSN and fetch the newest changes from the pageserver | ([source](https://www.databricks.com/company/newsroom/press-releases/databricks-launches-ltap-first-lake-transactionalanalytical)), ([source](https://www.databricks.com/blog/object-storage-wal-lakebase-postgres-agentic-era)) |
| CDC into a lake | Debezium + Iceberg sinks, Loams' phase 1 | The common default; minutes of lag | — |

### A.4 TDS

| Fact | Source |
|---|---|
| MS-TDS revision 42.0, 2026-06-17 (Major). The notice lets anyone copy the documentation "in order to develop implementations"; "Microsoft has patents that might cover your implementations"; the notice grants no patent licence; some documents are covered by the Open Specifications Promise or the Community Promise | ([source](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/b46a581a-39de-4745-b076-ec4dbb7d13ec)) |
| US 9,003,054, "Compressing null columns in rows of the tabular data stream protocol" (Microsoft). Listed as active; adjusted expiry 2031-09-10 (Google Patents' assumption, not a legal conclusion). The claims cover marking null columns with a bitmap and sending only non-null data when the column count exceeds a threshold | ([source](https://patents.google.com/patent/US9003054)) |
| TDS 8.0: TLS before the login (strict encryption), with ALPN `tds/8.0` | ([source](https://learn.microsoft.com/fi-fi/sql/relational-databases/security/networking/tds-8)) |
| Babelfish: "Functionality related to bulk copy is not supported"; `BULK INSERT` "is not supported"; also no partitioning, CLR, Service Broker, full-text search or SQL graph, and most XML | ([source](https://babelfishpg.org/docs/usage/limitations-of-babelfish)) |
| Microsoft `mssql-rs`: a Rust client, with `mssql-mock-tds` as a test server; no production server crate exists | ([source](https://github.com/microsoft/mssql-rs/wiki)) |

### A.5 Protocol and planner facts

| Fact | Source |
|---|---|
| PostgreSQL 19 adds `pg_plan_advice` ("to stabilize and control planner decisions") and `pg_stash_advice` ("to allow per-query-id advice to be specified"). Protocol grease was active during the 19 beta period and is not sent by the release; the reservations remain. 19 RC1 was released on 2026-10-15 | ([source](https://www.postgresql.org/docs/release/19.0/)) |
| `pg_plan_advice`: the advice tags (`JOIN_ORDER`, `HASH_JOIN`, `NESTED_LOOP_*`, `MERGE_JOIN_*`, `SEQ_SCAN`, `INDEX_SCAN`, `GATHER`…); `EXPLAIN (PLAN_ADVICE)`; feedback (`matched`, `partially matched`, `not matched`, `inapplicable`, `conflicting`, `failed`); advice constrains the planner and does not dictate a plan | ([source](https://www.postgresql.org/docs/devel/pgplanadvice.html)) |
| GoAway: Jelte Fennema-Nio's proposal (October 2025) for an advisory backend message `g` asking clients to reconnect, for poolers and switchovers. It moved from a protocol bump to a protocol extension in v3, and it is in the PG20 commitfest, "Needs review" | ([source](https://hackorum.dev/topics/52517)) |
| SQL/PGQ (`GRAPH_TABLE`, `CREATE PROPERTY GRAPH`) committed to master on 2026-03-16 by Peter Eisentraut, with Ashutosh Bapat | ([source](https://www.postgresql.org/message-id/E1w247I-0000Tk-2Y%40gemulon.postgresql.org)) |
| MySQL's X Protocol compared with the classic protocol (pipelining, expectations, CRUD messages); MySQL 8's query attributes as the model for D1038 | ([source](https://dev.mysql.com/doc/dev/mysql-server/8.4.7/mysqlx_protocol_comparison.html)) |

### A.6 Arrow, ADBC and BI

| Fact | Source |
|---|---|
| The ADBC specification (1.1: statistics, incremental execution, partitioned results, cancellation, progress) | ([source](https://arrow.apache.org/adbc/current/format/specification.html)) |
| Power BI and Fabric move embedded ODBC connectors to ADBC (Databricks, BigQuery, Snowflake, Impala, Spark; Dremio's replacement is the Flight SQL ADBC driver). The tenant setting rolls out from July 2026, with default-on planned from October 2026, ODBC removal from the service from early Q1 2027, and no ODBC drivers in Desktop or the gateway from spring 2027 (page updated 2026-09-17) | ([source](https://learn.microsoft.com/en-us/power-query/transition-to-adbc)) |
| "Data wants to be free": the case for Arrow end to end instead of row protocols | ([source](https://arrow.apache.org/blog/2025/02/28/data-wants-to-be-free/)) |

### A.7 Data APIs and agents

| Fact | Source |
|---|---|
| Google's pipe syntax for SQL ("SQL has problems. We can fix them") | ([source](https://research.google/pubs/sql-has-problems-we-can-fix-them-pipe-syntax-in-sql/)) |
| Gel (EdgeDB) joined Vercel on 2025-12-02; Gel Cloud closed on 2026-01-31; the project stays open source | ([source](https://www.geldata.com/blog/gel-joins-vercel)) |
| Convex: deterministic mutations, scaled horizontally | ([source](https://stack.convex.dev/horizontally-scaling-functions)) |
| Zero 1.0: custom mutators on the server | ([source](https://infoq.com/news/2026/06/zero-version-1)) |
| Neon's serverless driver: HTTP for one-shot queries, WebSocket for sessions and interactive transactions | ([source](https://neon.com/docs/serverless/serverless-driver)) |
| libSQL Hrana over HTTP v2: pipelined requests with a baton for stream continuity | ([source](https://github.com/tursodatabase/libsql/blob/main/docs/HTTP_V2_SPEC.md)) |
| PlanetScale's database traffic control: budgets and limits per query pattern | ([source](https://planetscale.com/blog/introducing-database-traffic-control)) |
| The Supabase MCP incident and the "lethal trifecta" (private data, untrusted content, an exfiltration channel) | ([source](https://simonwillison.net/2025/Jul/06/supabase-mcp-lethal-trifecta/)) |

## Appendix B. Not verified here

Taken from the owner's brief and not re-checked on 2026-10-16. Each one is marked (verify) where it is used:
- Firebolt's and Azure HorizonDB's protocol and WAL details.
- XTDB 2.x's move from XTQL to SQL.
- PgDog's protocol 3.2 support.
- `pg_hint_plan`'s hint table keyed by query id.
- Query-id stability under comments.
