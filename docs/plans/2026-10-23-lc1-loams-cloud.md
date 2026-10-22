# LC1 — Loams Cloud on TiDB X: Dev on Your Own Devices, Prod in Your Own Cloud — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task. Where this plan gives exact names, paths, flags or tests, use them verbatim, and record any deviation in "Rulings made during execution" at the end.
>
> **Status: Approved** (owner, 2026-10-23, all defaults; stub). Track **LC**, design [§78](../design/78-loams-cloud-on-tidb-x.md) (D1700–D1735, Q1260–Q1277). **LC0 starts once the owner has created the no-card TiDB Cloud org (Q1277).** The public opening of LC2 waits on counsel (Q1276); D1701, which amends D1323, is approved.
>
> **Coverage:** this plan specifies **LC0**, the spikes, task by task, and **LC2a**, invite-only access (the owner's amendment of 2026-10-23; the first phase users see), task by task. LC0 writes no product code. LC1, the rest of LC2 and LC3–LC5 are outlined at milestone level; each gets full task specifications after the LC0 report.
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

## LC2a — Invite-only access: the waitlist and email invites (Tasks 0–11)

> **The first LC phase that users see.** It specifies the owner's invite-only rule of 2026-10-23 (D1736, approved) and its design in [§78 §25](../design/78-loams-cloud-on-tidb-x.md#25-invite-only-access-the-waitlist-and-email-invites-d1736d1749-owner-amendment-of-2026-10-23). D1737–D1749 and Q1278–Q1289 are **Proposed**.
>
> **Prerequisites before any task starts:**
> - the owner approves §25's API plan;
> - LC0 Tasks 3 and 5 report a go;
> - IA2 has landed, along with the Zitadel bundle at `auth.loams.dev`.
>
> **Before the waitlist page goes live:** counsel's privacy policy (Q1276) and the sending domain's DNS (Q1278) must be in place.
>
> **This plan writes no code.** It specifies the tasks for the agents who will.

**Goal:** someone can join the waitlist, confirm the address, receive an invite, redeem it with GitHub or Google, and land in a Loams Cloud account. Only an Ostrium admin can invite, and every step is audited.

**Exit:**
- the end-to-end test `invite_only_happy_path` passes against Miniflare and the `conformance/identity/` compose (Zitadel v4.19.4);
- every Review Focus test passes;
- the LC2a section of the LC threat model is written.

**Architecture:**
- **The Worker:** `deploy/loams-cloud/` is the Worker of D1727, with routes under `/cloud/v1/`. It has a `send_email` binding, a Turnstile secret, rate-limit bindings, two HMAC/AES keys, the Zitadel service account's key, and the Redeem client's secret.
- **The schema:** `deploy/loams-cloud/schema/0001_waitlist.sql`, applied to the control DB.
- **The CLI:** `crates/loams` gains `loams cloud admin …`.
- **Pages** (in loams-site, a separate PR there): `loams.dev/cloud`, `/cloud/confirm`, `/cloud/leave`, `/cloud/invite`, `/cloud/invite/error`, `/cloud/welcome`.

**Global constraints, in addition to LC0's:**
- **No real mail in tests.** Use Email Service's local simulation, where Wrangler supports it (check in Task 0), plus a recorded fake.
- **Real sends** go only to addresses the owner controls, until the owner opens the waitlist.
- **No plaintext address** in logs, audit rows, metrics, errors or test snapshots. A test greps for that.
- **Greptile:** every LC2a PR carries `review:greptile` (security, auth, personal data).

**Review focus:**
1. **Enumeration.** Every waitlist response is identical across new, pending, confirmed, suppressed and refused addresses: the same status code, body and timing within noise. Test: `waitlist_responses_do_not_enumerate`.
2. **Gate.** A Zitadel user without the `tryout` role gets no Loams Cloud session and no API access. Test: `no_role_no_session`.
3. **Single use.** An invite redeems at most once, even under concurrent callbacks. Test: `invite_redeems_once_under_race`.
4. **Admin only.** Every `/admin/*` call without `cloud_admin`, without MFA or with stale authentication is refused, and an agent principal can never hold the role. Tests: `admin_requires_role_and_mfa`, `agents_cannot_be_cloud_admin`.
5. **Deletion.** After "remove me" or `DELETE /me`, no readable address remains in any table. Test: `deletion_leaves_only_hmac`.
6. **Tokens.** Token values never appear in logs, including `GET /invites/{token}`'s path. Test: `tokens_are_redacted_from_logs`.

### Task 0 — Rulings, pins and the (verify) items

Check each item on the pinned versions and record the answer in "Rulings made during execution". Where a finding changes §25, stop and report it to the owner rather than working around it.

- **Zitadel v4.19.4:**
  1. that `projectRoleCheck` blocks a login with no role, for a user just created by an external IdP;
  2. the login-policy settings that let GitHub and Google create users on first sign-in in one org, and the `urn:zitadel:iam:org:id:{id}` scope that places them there;
  3. that `CreateAuthorization` works on v4.19.4 (the forum reported an error on v4.2.0), and the least-privilege role for the service account;
  4. which `ListIDPLinks` field carries GitHub's numeric user id;
  5. whether access tokens carry `amr` and `auth_time` (else the ID-token path of §25.8);
  6. the HTTP email provider's payload schema and signature header;
  7. Actions v2 target signing for the wake-up;
  8. Zitadel's guidance on an org per individual (Q1284).
- **Cloudflare:**
  1. Email Service's status and whether it onboards a subdomain (`mail.loams.dev`);
  2. the `send_email` binding's shape from the generated Workers types (`wrangler types`);
  3. local simulation;
  4. the new-account daily quota (`GET …/email/sending/limits`);
  5. the rate-limiting binding's status and limits;
  6. Turnstile Siteverify's single-use rule.
- **Disposable-domain list:** choose one, record its licence (it must be compatible with D11) and pin a revision.

Files: this plan. Commit: `plans(lc1): LC2a Task 0 rulings and pins`.

### Task 1 — The control-DB schema

`deploy/loams-cloud/schema/0001_waitlist.sql` holds the tables of §25.3:
- `waitlist`, `waitlist_tokens`, `waitlist_suppressed`, `invites`, `redemption_sessions` and `audit`;
- unique `email_hmac` on `waitlist`, and unique `token_hash` on `waitlist_tokens` and `invites`;
- an index on `invites(state, expires_at)` for the expiry sweep.

The schema must stay inside TiDB X's limited features (§78 §14.1), so it uses no `time_to_live` attribute (the expiry sweep is a Cron Trigger) and no partitioning.

**Tests**, against a local MySQL-compatible TiDB in CI, plus one run on the owner's free Starter instance:
- `schema_applies_cleanly`;
- `email_hmac_is_unique`;
- `no_plaintext_email_column_outside_email_enc`, a schema lint.

Commit: `loams-cloud: the waitlist and invite schema (LC2a Task 1)`.

### Task 2 — Email: sender, templates and the Zitadel bridge

- **The `send_email` binding:** restricted to the sender address of Q1278, using Email Service's send-binding restrictions.
- **Four templates,** each with a text part and an HTML part: `confirm`, `already_listed`, `invite` and `removal`.
  - Every template has a footer removal link (`wlr_` token) and a `List-Unsubscribe` header pointing at it.
  - No tracking pixels and no link rewriting.
  - Plain links on `loams.dev` only.
- **The pacer:** sends at about 1 a second through a Durable Object queue, with the daily cap of Q1288.
- **Results:** permanent bounces from the send result go into `waitlist_suppressed` and revoke any open invite.
- **`POST /internal/zitadel/notify`:** verifies the HTTP provider's signature as Task 0 found it, then sends through the same binding.
- **Owner actions:** DNS onboarding of the sending domain, and the DMARC record (`p=quarantine` with `rua`).

**Tests:**
- `templates_render_text_and_html`;
- `every_email_has_removal_link`;
- `pacer_respects_daily_cap`;
- `hard_bounce_suppresses_and_revokes`;
- `zitadel_notify_rejects_bad_signature`.

Commit: `loams-cloud: transactional email through Email Service (LC2a Task 2)`.

### Task 3 — The public waitlist endpoints

These are `POST /waitlist`, `POST /waitlist/confirm` and `DELETE /waitlist`, exactly as in §25.10.
- Turnstile Siteverify is called server-side.
- The rate limits are §25.7's.
- Addresses are normalised and run through the disposable and no-MX check.
- The responses are enumeration-safe.
- `email_hmac` and `email_enc` are written with the Worker's keys.
- Tokens are hashed with SHA-256.
- Log redaction applies to every token-bearing path.

**Tests:**
- `waitlist_responses_do_not_enumerate`, which compares status, body and the median of 50 timings for each address state;
- `confirm_is_single_use`;
- `turnstile_token_reused_is_refused`;
- `disposable_domain_is_silently_dropped`;
- `rate_limits_apply_per_prefix_and_address`;
- `tokens_are_redacted_from_logs`.

Commit: `loams-cloud: the waitlist API with double opt-in (LC2a Task 3)`.

### Task 4 — Admin authorisation and the admin API

- **The endpoints:** `GET /admin/waitlist`, `DELETE /admin/waitlist/{id}` and `GET /admin/audit`.
- **The checks:**
  - the Zitadel access token is verified;
  - `loams-authz` checks `platform:loams-cloud#cloud_admin`, with a model addition in `deploy/openfga/core.fga` that excludes `agent` and `service_account` principals, and `BuiltinAuthorizer` parity;
  - MFA and authentication within 15 minutes are required (§25.8).
- **The CLI:** `loams cloud admin waitlist list|delete` and `loams cloud admin audit`. It gets its token through the OIDC device flow at `auth.loams.dev`.

**Tests:**
- `admin_requires_role_and_mfa`;
- `agents_cannot_be_cloud_admin`, in the model test and in `builtin_matches_openfga`;
- `admin_list_pages_and_filters`.

Commit: `loams-cloud: cloud_admin and the admin waitlist API (LC2a Task 4)`.

### Task 5 — Invites

- **The endpoints:** `POST /admin/invites` (batches of up to 50, with an `Idempotency-Key`), `GET /admin/invites`, and `POST /admin/invites/{id}/revoke` and `/resend` (at most 3 resends, with `--extend`).
- **The sweep:** an hourly Cron Trigger marks expired invites.
- **The CLI:** `loams cloud admin invite …`.

**Tests:**
- `batch_skips_unconfirmed_and_suppressed`;
- `resend_invalidates_previous_token`;
- `revoke_makes_link_unusable`;
- `expiry_sweep_marks_expired`;
- `idempotent_batch_creates_once`.

Commit: `loams-cloud: admin invites, resend, revoke and expiry (LC2a Task 5)`.

### Task 6 — Zitadel configuration as code

In `deploy/loams-cloud/zitadel/`:
- **The Loams Cloud project:** roles `tryout` and `cloud_admin`, `projectRoleCheck` on, and the app's OIDC client.
- **The Loams Cloud Redeem project:** no role check, and one confidential client with PKCE.
- **The `loams-cloud` org** for try-out users (Q1284), with GitHub and Google as IdPs and registration through them only.
- **A service account** holding only `user.grant.write` on the Loams Cloud project.
- **The HTTP email provider** pointing at `/internal/zitadel/notify`.
- **An Actions v2 target** for `/internal/zitadel/wake`.

All of it is applied by a script against the Admin and Management APIs, idempotent, run first against the `conformance/identity/` compose.

**Tests:**
- `zitadel_config_applies_idempotently`;
- `no_role_no_session`, run end to end on the compose.

Commit: `loams-cloud: Zitadel projects, clients and providers as code (LC2a Task 6)`.

### Task 7 — Redemption

- **The endpoints:** `GET /invites/{token}`, `POST /invites/{token}/redeem` and `GET /redeem/callback`, exactly as §25.5 and §25.10 give them.
- **The callback:**
  - verifies the ID token;
  - runs the account checks: existing account, GitHub age through a GitHub App token, the D1730 ceiling;
  - spends the invite and creates the account records in one transaction;
  - calls `CreateAuthorization`, with compensation if it fails;
  - creates the Durable Object by first contact;
  - writes the audit events;
  - redirects into the app's normal sign-in.

**Tests:**
- `invite_only_happy_path`;
- `invite_redeems_once_under_race`;
- `different_email_redeems_and_records_both`;
- `existing_account_does_not_spend_invite`;
- `young_github_account_is_refused_without_spending`;
- `grant_failure_rolls_back`;
- `status_check_reveals_nothing`.

Commit: `loams-cloud: invite redemption through Zitadel (LC2a Task 7)`.

### Task 8 — Lifecycle and the Zitadel event feed

- **`DELETE /me`:** deletion within 7 days, D1731's rule.
- **The feed:** an event-list reader with a stored position, by D1622's method, for `user.removed`, `user.deactivated`, `user.reactivated` and `user.grant.removed`. It is woken by `/internal/zitadel/wake` and also polls every few seconds.
- **Retention jobs** for §25.6's table.

**Tests:**
- `deletion_leaves_only_hmac`;
- `zitadel_user_removed_suspends_account`;
- `wake_is_only_a_hint` (a lost webhook still converges by polling);
- `retention_jobs_delete_on_schedule`.

Commit: `loams-cloud: account lifecycle and the Zitadel event feed (LC2a Task 8)`.

### Task 9 — The pages (in loams-site)

A separate PR in `ostrium-labs/loams-site`, reviewed there by CodeRabbit.
- **The pages:**
  - the waitlist form, with Turnstile, the two optional fields and §25.6's notice linking the privacy policy;
  - confirm, leave, invite (it reads the fragment and calls the API), error and welcome.
- **No analytics** on `/cloud/*`.
- **The `(console)` group** becomes `cloud`, as §8 says.
- **Accessibility:** WCAG 2.2 AA on every form.

Tests: the site's own checks, plus a Playwright flow against a mocked API.

Commit, in loams-site: `cloud: the waitlist and invite pages (LC2a Task 9)`.

### Task 10 — End to end, and the abuse drill

- **The run:** Miniflare with the Worker, plus the `conformance/identity/` compose, plus a fake GitHub IdP. It drives the happy path and every refusal path.
- **The drill:** 1,000 scripted sign-ups from 10 IP prefixes, checking that limits, caps and enumeration-safety hold.

Tests: `invite_only_happy_path` (end to end) and `abuse_drill_stays_within_caps`.

Commit: `loams-cloud: end-to-end and abuse tests for invite-only access (LC2a Task 10)`.

### Task 11 — Threat model and the go-live checklist

- **The threat model:** `docs/security/loams-cloud-threat-model.md`, LC2a section.
  - The threats: enumeration, token theft and guessing, invite forwarding, Turnstile bypass, admin takeover, Zitadel misconfiguration (role check off), the email provider's signature, data-subject requests, and bounce storms.
- **The go-live checklist,** which the owner ticks:
  - counsel's policy is published;
  - DNS and DMARC are in place;
  - Zitadel's config has been applied to `auth.loams.dev`;
  - the first admin has been granted `cloud_admin`, with MFA;
  - the daily caps are set.

Label the PR `review:greptile`.

Commit: `docs(security): Loams Cloud invite-only threat model and go-live checklist (LC2a Task 11)`.

## LC1–LC5 outline

| Milestone | Scope | Depends on |
|---|---|---|
| **LC1: the `tidbx` target and multi-device dev (BYOG)** | `deploy/targets/<id>.toml` manifests for all six targets plus `devices` (D1707, D1735), generated into §78 §13's tables and joined to §71's `free-tiers.json`; `loams target list\|show`; `[environments.*]` in `loams.toml` and `loams env list`; the `tidbcloud` backend for `loams.sqldb.v1` (BYOG: instance per database, TiDB branches) with its conformance column; agentd workspaces on Filesystem (a layer per run, drain on approval and handoff, `coding-agent` profile) using the user's own `ti` session; `loams run move` between the user's own devices over their tailnet; optional `tsnet` in agentd; the `tidbx` conformance suite (D1734), nightly and non-blocking on the owner's no-card org | LC0 report; SD1 remote agentd |
| **LC2: the hosted try-out** | `deploy/loams-cloud/` (a Worker, per-user Durable Objects, the control DB schema); `loams cloud login\|device\|status\|export\|move`; device machine users (IA4); cells and the cell map; cohort Filesystem with budgets and the kill switch; the try-out plan in the Meter (ME1); abuse controls; `loams.dev/cloud` pages in loams-site (a separate PR in that repository, CodeRabbit-reviewed there); the threat model; `LOAMS_CORS_ORIGINS` default change and the networking map; Ostrium's OpenTofu root with R2 state (D1729). Invite-only throughout (D1736); its first slice is LC2a above | D1701 approved; IA2, IA4; ME1's enforcement path; counsel |
| **LC3: Cloudflare** | The `cloudflare` target wired to BY0's Cloudflare template (§70 §7, D1367) and AB0's Cloudflare adapter (§71); Tunnel-based device-to-device as the documented non-tailnet path (§69 ER0) | BY0 Cloudflare, AB0, ER0 |
| **LC4: Vercel** | The `vercel` target wired to AB1's Vercel adapter and §70 §8's integration; Vercel Sandbox as an `agents` option for short runs | AB1 |
| **LC5: production targets and `loams promote`** | `aws`, then `gcp`, then `azure` targets wired to BY0/BY1 (§70) and AB0/AB1 (§71); `loams promote --from --to` opening a PR in the company's infrastructure repository; company orgs and the identity mapping (D1725); the company-run control plane guide (D1726) | BY1, AB1, IA3 |

## Rulings made during execution

(none yet)
