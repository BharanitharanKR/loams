# LC1 — Loams Cloud on TiDB X: Dev on Your Own Devices, Prod in Your Own Cloud — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task. Where this plan gives exact names, paths, flags or tests, use them verbatim, and record any deviation in "Rulings made during execution" at the end.
>
> **Status: Approved** (owner, 2026-10-23, all defaults; stub). Track **LC**, design [§78](../design/78-loams-cloud-on-tidb-x.md) (D1700–D1735, Q1260–Q1277). **LC0 starts once the owner has created the no-card TiDB Cloud org (Q1277).** The public opening of LC2 waits on counsel (Q1276); D1701, which amends D1323, is approved.
>
> **Coverage:** this plan specifies **LC0**, the spikes, task by task. LC0 writes no product code. LC1–LC5 are outlined at milestone level; each gets full task specifications after the LC0 report.
>
> **Inputs:**
> - the research report [`docs/research/tidb-x-and-hcp.md`](../research/tidb-x-and-hcp.md) (2026-10-23);
> - §70 and the [BY1 plan](2026-10-21-by1-byoc-clouds.md), which §70 and §71 share;
> - the [ER1 plan](2026-10-20-er1-edge-reach.md);
> - the [IA1 plan](2026-10-21-ia1-identity-authz.md) and the IA0 spike report;
> - the [SD1 plan](2026-10-13-sd1-studio-ade.md).

**Goal (LC0):** before any product code, find out whether TiDB X can carry Loams' dev state the way §78 assumes, and at what cost:
- tenant cells on Starter;
- workspace handoff on Filesystem;
- the control plane on a Worker through the HTTP driver;
- device enrolment on IA0's Zitadel stack;
- whether TiDB Cloud Zero can hold try-out SQL;
- whether the defaults hold on a managed work laptop.

**Exit (LC0):** `docs/plans/lc0-report.md`, with:
- a go or no-go for each spike;
- the measured numbers (per-tenant storage visibility, RU attribution, drain latency, writes per agent session, Worker request volume per device);
- answers to §78's (verify) items;
- recommended values for Q1263–Q1265.

## Global Constraints

- **Owner actions first.** The owner creates the TiDB Cloud organization for LC0, **with no card** (Q1277), and gives the agent an API key pair with the least role that works. Agents never sign up for or create accounts on TiDB Cloud, Cloudflare or HCP. The Cloudflare account is Ostrium's existing one, and the owner grants a scoped API token.
- **Cost:** every LC0 spike stays inside free allowances.
  - Starter: the 5 free instances.
  - Filesystem: no card, so the no-card caps apply (1 file system per region, 2,000 files, 2 GB).
  - Workers: the Free plan, unless Task 3 needs Paid, which the owner approves.
  - Teardown at the end of each task is part of the task.
- **No product code in LC0.** Spike code lives under `spikes/lc0-<task>/`, is marked `publish = false` or `"private": true`, is never wired into a crate, the CLI or the release, and is deleted or kept as a fixture by LC1's first task.
- **Secrets:** TiDB API keys, Filesystem tokens and SQL passwords are never committed, logged or written into the report. Spikes read them from the environment.
- **Worktrees and branches:** `~/Documents/Ostriumlabs/loams-wt/lc0-<task>`, with one branch and one PR per task into `dev`. Commit with `git commit -s`. Never stash.
- **Light builds:** no Rust build in LC0 except Task 5's, which runs `cargo test -p` on one spike crate. Never `--workspace`, and never in `/tmp`.
- **Review:** routine spike PRs get no AI review. Task 5 (identity) and the LC0 report get the `review:greptile` label.
- **GitHub-hosted runners only.** No LC0 spike runs in CI, because they need the owner's accounts.

## Review Focus

1. **A tenant reads or writes another tenant's database in a cell.** Expected: never. Test: `cell_grants_isolate_tenants` (Task 1).
2. **A scoped Filesystem token reaches outside its prefix.** Expected: never. Test: `scoped_token_cannot_escape_prefix` (Task 2).
3. **A handoff loses drained work.** Expected: never; undrained work is reported, not lost silently. Test: `handoff_after_drain_is_complete` (Task 2).
4. **A device token outlives deactivation by more than a few seconds.** Expected: under 5 s. Test: `deactivated_device_is_denied` (Task 5).
5. **A spike leaves resources behind.** Expected: never. Each task ends with `teardown_leaves_nothing`.

## LC0 — Spikes (Tasks 0–7)

### Task 0 — Rulings and pins

Check each item and record the answer in "Rulings made during execution":

- **Owner rulings.** Whether the owner has answered Q1260–Q1277, and which defaults changed.
- **Pins.** The `ti` CLI release used (install script URL and version); the `@tidbcloud/serverless` version (it must be at least 14 days old); Zitadel and OpenFGA at IA0's pins.
- **Dependencies.** Whether IA2 and IA4 (machine users) have landed; whether SD1's remote agentd (D909) is on `dev`; whether ME1 has started.
- **Re-reads.** Re-read the report's Starter, Filesystem and PG-preview pages. Note any limit that changed since 2026-10-23.

Files: this plan. Commit: `plans(lc1): LC0 Task 0 rulings and pins`.

### Task 1 — Starter tenant cells (spike)

**What to build:** `spikes/lc0-cells/`, a script in TypeScript or Python using a MySQL driver over TLS. Against one free Starter instance it:
1. creates *N* tenant databases (N = 50, 200, 500), each with its own SQL user and grants only on its database;
2. runs a small mixed workload per tenant;
3. measures the following.

| Measure | How |
|---|---|
| Isolation | Each tenant user tries `SELECT`, `INSERT`, `SHOW DATABASES` and `information_schema` reads on another tenant's database |
| Per-tenant storage visibility | `information_schema.TABLES` `DATA_LENGTH` and `INDEX_LENGTH` per schema, against the console's figure |
| Per-tenant activity | Whether `PROCESSLIST`, `CLUSTER_PROCESSLIST`, `SLOW_QUERY` or `STATEMENTS_SUMMARY` give per-user activity on Starter (the report says `STATEMENTS_SUMMARY` is Premium-only) |
| RU attribution | Whether any per-user or per-database RU figure exists |
| Connection caps | Whether `MAX_USER_CONNECTIONS` (per user) is enforced |
| Credential rotation | Whether `ALTER USER … PASSWORD EXPIRE` and password rotation work |
| The spending limit | With the instance at its free quota (or a throttled test), confirm new connections are denied and existing ones throttled |
| DDL cost | `CREATE DATABASE` and `CREATE USER` latency at each N |

**Tests:** `cell_grants_isolate_tenants`, `cell_storage_is_visible_per_tenant`, `teardown_leaves_nothing`.

**Report section:** the cell size to use (§78 D1717's default of 200), whether per-tenant fairness is observable, and a go or no-go for cells.

Commit: `spikes(lc0): Starter tenant cells (LC0 Task 1)`.

### Task 2 — Filesystem workspaces and handoff (spike)

**What to build:** `spikes/lc0-fs/`, scripts driving `ti fs` on two machines:
- a Linux VM or container with `fuse3` and `/dev/fuse`;
- a macOS machine (WebDAV, and FUSE if macFUSE is installed).

The runs:
1. Create a file system; clone a real repository into it with `ti fs-git clone-git-workspace`; mount it with the `coding-agent` profile.
2. Run a scripted "agent session" against it: 30 edits, a test run, a commit. Count the **write operations** it caused (from the console's usage, or the billing API, read before and after) and the files it created.
3. **Handoff:** create a layer for the "run"; drain; checkpoint; fork the layer on the second machine; hydrate Git; check that the tree matches. Then repeat **without** draining (kill the mount on machine A), and record what machine B sees.
4. **Scoped tokens:** a token for `/t/a` with `read,list,write`, TTL 1 h. It must fail on `/t/b`, fail to mount `/`, and fail after `disable-file-system-token`. Time the propagation.
5. **Limits:** fill to the no-card limits (2,000 files, 2 GB) and confirm writes stop and reads work.
6. **Usage visibility:** check whether per-file-system usage is readable by API or CLI, or only in the console.

**Tests:** `scoped_token_cannot_escape_prefix`, `handoff_after_drain_is_complete`, `undrained_handoff_is_detected`, `teardown_leaves_nothing`.

**Report section:**
- writes per session and the cost per session at $0.50 per 1,000;
- drain latency;
- whether the 5-minute drain interval (D1713) is right;
- whether per-file-system limits or usage are API-reachable;
- a go or no-go for the hosted cohort (Q1263).

Commit: `spikes(lc0): Filesystem workspaces, handoff and token scoping (LC0 Task 2)`.

### Task 3 — The control plane on a Worker (spike)

**What to build:** `spikes/lc0-worker/`, a Worker with `@tidbcloud/serverless`. It:
- creates and reads device and lease records in a free Starter instance;
- runs one Durable Object per user, holding hibernating WebSockets from 20 simulated devices that ping every 30 s.

**Measure:**
- the latency of single-statement writes;
- the idempotency-key pattern under retries;
- the requests and duration billed per device per day, with and without hibernation, read from the Workers dashboard or analytics;
- whether the Free plan's limits hold for the 20 devices.

**Tests:** `lease_is_exclusive_under_races`, `device_reconnects_after_sleep`, `teardown_leaves_nothing`.

**Report section:** requests per device per month, whether Workers Free or Paid is needed (§78 D1727), and the driver's behaviour on transient errors.

Commit: `spikes(lc0): the control plane on a Worker with the HTTP driver (LC0 Task 3)`.

### Task 4 — TiDB Cloud Zero for try-out SQL (research, plus one owner-run test)

Read TiDB Cloud Zero's documentation and terms:
- Can a third party (Loams Cloud) create Zero databases on users' behalf, programmatically?
- Is there an API, or only the SKILL command?
- What are the rate limits?
- How does the claim flow move a database into the user's own account?

The owner runs one manual creation and claim, to confirm the flow. No agent creates an account.

**Report section:** whether Q1264's Zero option is allowed and practical.

Commit: `docs(lc0): TiDB Cloud Zero for try-out SQL (LC0 Task 4)`.

### Task 5 — Device enrolment on IA0's stack (spike)

**What to build:** `spikes/lc0-device/`, a small Rust binary (one crate, `publish = false`) against the `conformance/identity/` compose (Zitadel and OpenFGA at IA0's pins). It:
1. runs the **OIDC device authorization grant** for a person;
2. creates a machine user of kind `device` under the person's org;
3. registers a key pair, with the private key in the OS keychain through the `keyring` crate where available, and a file with mode 0600 otherwise;
4. gets 15-minute access tokens by private-key JWT;
5. deactivates the device and measures how long until a token request and a protected call are denied.

Also check whether OpenFGA's MySQL datastore runs against a free Starter instance (§78 §17).

**Tests:** `device_flow_issues_tokens`, `deactivated_device_is_denied` (under 5 s), `device_cannot_act_for_another_org`.

**Provenance:** this spike may read IA0's report and `conformance/identity/`. It must not be read by loams-identity's clean-room agents (memory `loams-identity-zitadel`).

Commit: `spikes(lc0): device enrolment on Zitadel (LC0 Task 5)`.

### Task 6 — The company-device check (research and a manual run)

On a managed-like laptop (a macOS or Windows user account without admin rights, behind an HTTPS proxy with a re-signing CA, `mitmproxy` or similar), install Loams Desktop's per-user daemon (D784) and confirm:
- no admin prompt;
- the outbound WebSocket and HTTPS through the proxy;
- the corporate CA from the OS store;
- MySQL over TLS on 4000 through the proxy, or its failure, with the HTTP fallback;
- no inbound listener except loopback;
- clean removal.

Then confirm that `tsnet` inside a test daemon joins a second tailnet while the system Tailscale client stays on the first. The report §4.3 says a device is on one tailnet at a time.

**Report section:** §78 §10's table, row by row, as pass or fail.

Commit: `docs(lc0): the company-device check (LC0 Task 6)`.

### Task 7 — The LC0 report

Write `docs/plans/lc0-report.md` with:
- each task's go or no-go and numbers;
- the answers to §78's (verify) items;
- recommended values for Q1263, Q1264 and Q1265, and the cell size;
- a costed estimate of LC2 at 100, 1,000 and 10,000 users against D1730's ceiling;
- any change §78 needs, as a list for the owner.

Label the PR `review:greptile`.

Commit: `docs(lc0): the LC0 report`.

## LC1–LC5 outline

| Milestone | Scope | Depends on |
|---|---|---|
| **LC1: the `tidbx` target and multi-device dev (BYOG)** | `deploy/targets/<id>.toml` manifests for all six targets plus `devices` (D1707, D1735), generated into §78 §13's tables and joined to §71's `free-tiers.json`; `loams target list\|show`; `[environments.*]` in `loams.toml` and `loams env list`; the `tidbcloud` backend for `loams.sqldb.v1` (BYOG: instance per database, TiDB branches) with its conformance column; agentd workspaces on Filesystem (a layer per run, drain on approval and handoff, `coding-agent` profile) using the user's own `ti` session; `loams run move` between the user's own devices over their tailnet; optional `tsnet` in agentd; the `tidbx` conformance suite (D1734), nightly and non-blocking on the owner's no-card org | LC0 report; SD1 remote agentd |
| **LC2: the hosted try-out** | `deploy/loams-cloud/` (a Worker, per-user Durable Objects, the control DB schema); `loams cloud login\|device\|status\|export\|move`; device machine users (IA4); cells and the cell map; cohort Filesystem with budgets and the kill switch; the try-out plan in the Meter (ME1); abuse controls; `loams.dev/cloud` pages in loams-site (a separate PR in that repository, CodeRabbit-reviewed there); the threat model; `LOAMS_CORS_ORIGINS` default change and the networking map; Ostrium's OpenTofu root with R2 state (D1729). Invite-only first, public after Q1276 | D1701 approved; IA2, IA4; ME1's enforcement path; counsel |
| **LC3: Cloudflare** | The `cloudflare` target wired to BY0's Cloudflare template (§70 §7, D1367) and AB0's Cloudflare adapter (§71); Tunnel-based device-to-device as the documented non-tailnet path (§69 ER0) | BY0 Cloudflare, AB0, ER0 |
| **LC4: Vercel** | The `vercel` target wired to AB1's Vercel adapter and §70 §8's integration; Vercel Sandbox as an `agents` option for short runs | AB1 |
| **LC5: production targets and `loams promote`** | `aws`, then `gcp`, then `azure` targets wired to BY0/BY1 (§70) and AB0/AB1 (§71); `loams promote --from --to` opening a PR in the company's infrastructure repository; company orgs and the identity mapping (D1725); the company-run control plane guide (D1726) | BY1, AB1, IA3 |

## Rulings made during execution

(none yet)
