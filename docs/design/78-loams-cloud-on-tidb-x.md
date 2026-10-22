# 78 — Loams Cloud on TiDB X: Dev on Your Own Devices, Prod in Your Own Cloud

Status: **Approved** · 2026-10-23. The owner approved the design with all defaults on 2026-10-23: every decision (D1700–D1735), including D1701's amendment of D1323 and the wording updates to D1351, D1378 and D1615, and every question (Q1260–Q1277) at its recommended default. The owner's next actions are a no-card TiDB Cloud org for LC0 (Q1277) and counsel review before the public try-out (Q1276).

Source: the owner's directions of 2026-10-23.
- "TiDbX, we will deploy loams cloud/platform on this, so in deploy add everything to deploy the studio and software factory on it, use only services provided by them etc."
- On who hosts: "both. For loams cloud i have hashicorp free tier, so my plan is to let developers try out easily by signup and also connect their homelabs, laptops etc."
- On compute, since TiDB Cloud has none: "research first".
- Later the same day, two more rulings:
  - **The deploy order is TiDB X, then Cloudflare, Vercel, AWS, GCP and Azure**, each mapped mostly onto that provider's free-tier services, behind one target abstraction.
  - **The product thesis** in §1, which drives the phases.

The owner's notes on TiDB X were treated as unverified. The research report checks each claim against primary sources: [`docs/research/tidb-x-and-hcp.md`](../research/tidb-x-and-hcp.md) ("the report" below). §4 lists the corrections.

**Numbering.** Decisions **D1700–D1735** and questions **Q1260–Q1277** are declared in the [decision log](13-decision-log.md), approved by the owner on 2026-10-23. This document reserves **D1700–D1749** and **Q1260–Q1289**.
- The highest numbers in use on `dev` (`7e83717d9`) sit in §77's blocks D1640–D1679 and Q1220–Q1244.
- Open PRs use D1119 (#1082), D1552 and D1570 (#1055), D1603–D1616 (#1009) and one number in §77's free block D1666–D1679 (#1057), all below this block.
- Design number 78 is the next free one; no open PR takes it.

Plan: [LC1](../plans/2026-10-23-lc1-loams-cloud.md), track **LC** (a stub; LC0 is specified task by task).

This document builds on the designs below. §20 says where it amends each one, and the amended documents carry a note.
- [§41](41-multitenant-byoc-control-plane.md): the control plane and its outbound-only agent (D543).
- [§43](43-private-networking.md): the tailnet (D580–D589).
- [§53](53-studio-ade.md): Studio, the per-user `loams-agentd`, and remote agentd (D909, D921).
- [§50](50-loams-desktop-daemon.md): the per-user service (D784).
- [§63](63-loams-meter.md): Loams Meter.
- [§69](69-edge-reach.md): edge reach and no hosted relay (D1311, D1317).
- [§70](70-byoc-clouds.md): data-plane BYOC (D1350–D1384).
- [§71](71-app-byoc-free-tiers.md): app BYOC on free tiers (D1390–D1410).
- [§76](76-identity-and-authorization.md): Zitadel and OpenFGA (D1600–D1621).
- [§47](47-loams-sql-production.md): Loams SQL (D720–D739).
- [§72](72-software-factory-loop.md): the Factory loop.
- [§39](39-software-factory-and-loams-bot.md): the Factory's embedded apps.
- The Cloudflare BYOC research (`~/Documents/Ostriumlabs/pitch/research/cloudflare-byoc.md`, 2026-10-21).

Markers, as in §69–§77:
- **(verify)**: not checked against a primary source; the task named next to it checks it first.
- **(estimate)**: judged, not measured.
- **(V)**: checked on the primary source linked in the report, on 2026-10-23.

Soil names follow the 2026-10-23 branding (memory `loams-branding`). They appear next to the technical names and never replace them.
- *the ground* is the machines a user owns.
- *a plot* is one deployment.
- *BYOG* is bring your own ground.
- *the Field* is Studio.
- *the Greenhouse* is the Software Factory.
- *the Commons* is Loams Cloud (Q1261, approved).

---

## 1. The product thesis (D1700)

The owner's thesis, 2026-10-23, drives everything below.

- **The user** is an employee at a company who has several machines: a work laptop, a personal laptop, a homelab, perhaps a desktop.
- **Stage 1, local and dev:** the Loams stack runs on those machines for **local and dev environments**. Loams Cloud and the tailnet or edge reach join the machines together. This is both the try-out and the daily driver for development.
- **Production** goes into **the company's own AWS, Google Cloud or Azure account**: BYOC/BYOG as designed in §70 and §71.
- **Then:** the stack gets stronger by supporting real customer deployments and learning from them.

Five consequences shape the rest of this document:

1. **Dev on the user's own devices is the first-class path.** A user has several devices. Laptops sleep and devices come and go. Work moves between devices. Each piece of state has a defined home (§9).
2. **The path from dev to prod is explicit.** The same artefacts and configuration are promoted from a dev environment on laptops into the company's cloud account. Identity and tenancy map so that **the company owns prod and individuals own dev** (§11, §12).
3. **The deploy-target order serves this.** TiDB X holds dev state. Cloudflare and Vercel are edge and app targets. AWS, Google Cloud and Azure are the production targets (§13).
4. **Company devices have rules.** Work laptops are managed by IT. Defaults must be safe there: no system-wide tailnet client required, no inbound port, no code leaving the device unless the user turns it on (§10).
5. **TiDB X is the state layer, not the whole platform.** TiDB Cloud has no general compute (report §2.5). Compute is the user's ground.

## 2. Summary

### 2.1 The problem

Trying Loams today means installing the compose stack on one machine and joining it to the user's own Tailscale tailnet (memory `loams-preview-release-model`). Nothing joins a user's several machines into one dev environment. Nothing lets work started on a laptop continue on a homelab. A developer can't sign up and be running in minutes.

The owner wants a hosted, free way in: sign up, connect your machines, and use Studio (the Field) and the Software Factory (the Greenhouse) across them. The state that must follow the user lives on TiDB X.

Three earlier rulings stand in the way as worded, so this document proposes amending them:
- D1323 removed the services offering.
- D1351 and D1378 say Ostrium runs no data plane.
- D1615 says Ostrium runs only the console and auth.

### 2.2 The answer

- **Three places, one stack** (D1700, D1716):
  - **Loams Cloud** (hosted by Ostrium, or by anyone, since the code is open) runs the control plane and holds shared dev state on TiDB X.
  - **The ground** (the user's own devices) runs the engine, Studio's agents and the Factory's apps.
  - **The company's cloud account** runs production, as in §70 and §71.
- **Loams Cloud is a free, hosted try-out** (D1701, amends D1323). It has no paid tier, no SLA and no support contract, and it has published limits. It holds only accounts, devices, coordination and the dev state a user opts into. Ostrium still hosts no customer data plane (D1378 stands for BYOC) and no data relay (D1317 stands). §7 has the amendment text.
- **TiDB X is the state target** (D1708):
  - Loams SQL dev databases are plain MySQL on Starter (D1709).
  - Agent workspaces are on Filesystem (D1713).
  - Postgres has a trial adapter only (D1710).
  - House stays Loams' own (D1711).
  - Live, the metastore, Durable and the Git bucket stay on the ground (D1712).

  TiDB services alone cannot run Loams: there is no compute, no KV API, no bucket API and no queues (§5).
- **TiDB Cloud BYOG comes first** (D1728). A user can point Loams at **their own TiDB Cloud organization**. Ostrium then holds none of their state, and the user's own free allowances apply. This ships before the hosted try-out (LC1, then LC2), because it needs no new hosted surface.
- **Devices join outbound only** (D1714, D1715):
  - `loams cloud login` uses the OIDC device flow at `auth.loams.dev`.
  - Each device becomes a machine identity with 15-minute tokens.
  - One outbound WebSocket per device carries presence and leases (§41's D543 pattern).
  - Runs are pinned to a device. Handoff is explicit: drain, checkpoint, continue elsewhere (§9.4).
  - Device-to-device traffic (terminals, app previews) goes over the user's own tailnet or Tunnel (D1722).
- **Company devices are safe by default** (D1723):
  - The daemon is per user, with no admin rights (D784).
  - No tailnet client is required. A company that already runs Tailscale keeps its tailnet; a device is on only one tailnet at a time (V).
  - Nothing inbound.
  - Cloud sync is **off** for a device marked as work.
- **Dev to prod** (D1724, D1725):
  - The same images by digest, `loams.toml`, release set and OpenTofu modules.
  - `loams promote` opens a pull request in the company's infrastructure repository. Data does not move.
  - The individual's personal org owns dev, and the company's org and issuer own prod.
- **One deploy-target abstraction for six targets** (D1706, D1707). A target manifest maps Loams' **slots** (control, SQL, Postgres, KV, Durable, House, bucket, workspace, secrets, agent compute, app compute, edge, identity, metering) to each provider's services.
  - The order: **TiDB X → Cloudflare → Vercel → AWS → Google Cloud → Azure.**
  - It reuses §70's install tiers and §71's app ports and free-tier table rather than repeating them.
  - §13 has a free-tier table per provider.
- **Sign-up and the console** (D1705):
  - `console.loams.dev` is retired.
  - Sign-up and account pages live at **`loams.dev/cloud`**, the existing site Worker's `(auth)` and `(console)` routes, renamed.
  - The product console is the one each plot already bundles (#1064), and Loams Desktop.
- **The control plane's compute** (D1727): a Cloudflare Worker, with a Durable Object per user for presence and leases, and its records on a TiDB Starter instance through the HTTP driver. Zitadel stays on its own host and Postgres (§76).
- **HashiCorp** (D1729):
  - HCP has no compute, and its only lasting free tier is HCP Terraform Free: 500 resources and one concurrent run (V).
  - Nothing in Loams depends on HCP. Ostrium's own infrastructure uses OpenTofu, with state on R2 by default.
  - HCP Boundary and Vault are not used: there is no free tier, and the tailnet already does the job.
- **Phases:**
  - **LC0:** spikes and reports, no product code.
  - **LC1:** TiDB X BYOG and multi-device dev.
  - **LC2:** the hosted try-out, invite-only first.
  - **LC3:** Cloudflare.
  - **LC4:** Vercel.
  - **LC5:** AWS, Google Cloud and Azure as production targets, with `loams promote`.

### 2.3 Phases at a glance

| Phase | What ships | Hosted by Ostrium | Builds on | Effort (estimate) |
|---|---|---|---|---|
| **LC0** | Spikes: Starter tenant cells, Filesystem handoff and cost, the HTTP driver from a Worker, Zero's terms, device enrolment on IA0's stack, a company-device check; the LC0 report | Nothing new | IA0, SD1 | 2–3 engineer-weeks |
| **LC1** | The target model; the `tidbx` target (the user's own TiDB Cloud org): SQL backend, workspaces on Filesystem, handoff between the user's own devices over their tailnet; `loams target`, `loams env` | Nothing new | §53, §47, LC0 | 5–7 engineer-weeks |
| **LC2** | The hosted try-out: `loams.dev/cloud` sign-up and account pages, the control plane Worker, device enrolment, presence and leases, cells on Ostrium's TiDB org, quotas through the Meter, abuse controls; invite-only, then open | **Yes** (D1701) | IA2/IA4, ME1, LC1 | 6–8 engineer-weeks |
| **LC3** | The Cloudflare target: the user's own Cloudflare account as ground for edge, apps and a checkpointed node; Tunnel for device-to-device | Nothing new | BY0 Cloudflare (§70 §7), AB0 (§71), ER0 | Mostly §70's and §71's own estimates; 2 weeks for the target glue |
| **LC4** | The Vercel target: apps and agents on the user's or company's Vercel team | Nothing new | AB1 (§71 §7.5), §70 §8 | 1–2 weeks of glue |
| **LC5** | AWS, then Google Cloud, then Azure as production targets; `loams promote` dev → prod; the company org mapping | Nothing new | BY0/BY1 (§70), AB0/AB1 (§71) | 4–6 weeks of glue on top of BY1 |

## 3. Scope

**Goals:**
- A developer can sign up, enrol their laptop and homelab, and run Studio agents and the Factory's apps on them within minutes. State that should follow them lives in Loams Cloud on TiDB X, or in their own TiDB Cloud organization.
- One target abstraction covering TiDB X, Cloudflare, Vercel, AWS, Google Cloud and Azure, built on §70 and §71.
- An explicit dev → prod promotion into the company's own cloud account.
- Honest limits: what is free, what is hosted, what is never hosted, and what cannot run on TiDB services.

**Non-goals:**
- A paid tier, an SLA, a support plan or "we run it for you" (D1323 stands).
- Hosting anyone's production.
- Hosted compute for agents or apps.
- A relay for device-to-device data traffic (D1317).
- A hosted Headscale (Q1269).
- Running Live, the metastore or Durable on TiDB Cloud (D1712).
- Replacing House with Lake (D1711).
- Replacing Loams Postgres with the PG preview (D1710).
- Any LC2 deploy code before counsel's review (Q1276) for the public try-out; LC0 writes spikes only.

## 4. What TiDB X actually offers (summary of the report)

The report (§2) has every fact with its source. These are the ones this design depends on, and the corrections to the owner's notes.

| Area | What is true on 2026-10-23 | Correction to the owner's notes |
|---|---|---|
| TiDB X | Object storage is the source of truth; SQL is stateless; Starter, Essential, Premium and BYOC run on it (V). PingCAP plans to open-source the core by the end of 2026 (V, a plan) | "TiKV becomes stateless" is the CTO's claim; the docs still keep Raft logs and cache on local disk |
| Starter | GA. 5 free instances per organization; 25 GiB row, 25 GiB columnar and 250M RUs a month in total; a hard **spending limit**; a default cap of 100 instances. No resource groups, PITR, changefeeds, CMEK or audit logs; transactions under 30 minutes (V) | — |
| Essential, Premium | Essential is preview, from $400 a month in compute; Premium is GA from November 2026, from $1,800 a month in compute, with CMEK, private connectivity and a 99.99% SLA (V) | Prices are starting compute only. **New Essential instances have had no branching since 2026-07-14** |
| Branching | Preview, Starter only, **5 per organization** by default, copy-on-write, minutes to create (V) | "Branch APIs on Starter and Essential" is now Starter only; branching is not instant |
| Filesystem | Preview since 2026-10-15. POSIX when mounted (FUSE on Linux, WebDAV or FUSE on macOS, **no Windows mounts**); layers, checkpoints, forks; owner and scoped tokens; no database needed. A $5 monthly allowance; with no card, 1 file system per region, 2,000 files and 2 GB; **with a card, no spending cap**; writes cost $0.50 per 1,000; writes are buffered locally until drained (V) | Add: preview, no SLA, the cost model and the drain rule |
| PG preview | pgwire v3; B-tree, GIN, HNSW; no partitioning, FDW, logical replication or custom extensions; no SLA; AWS only; 10 free instances, each 50 GiB and 500M RUs (V) | `pg_cron` isn't named; custom extensions are refused |
| Lake | Preview; from $1.60 an hour; **no free quota**; its own MCP server (V) | — |
| API and MCP | The API (preview) covers clusters, **billing, IAM and Data Service** (V). The TiDB MCP Server is **SQL-level only** (V) | No control-plane MCP; audit isn't in the API docs |
| Compute | **None.** Data Service runs SQL only; Lake's UDFs are external; Zero is temporary databases (V) | Answers "research first": compute must come from elsewhere |
| Credits | The TiDB AI Startup Program, "up to $100,000" (V); a $300 Essential trial credit (unverified) | **No $500 TiDB programme found.** HCP's trial credit is $500 for 6 months (V), which may be the source of the figure |
| Customers | Manus has more than 1.5 million clusters; Moonshot (Kimi) has more than a million file systems (V, PingCAP's own figures) | Vendor claims, not independently checkable |

## 5. What TiDB services alone can and can't run (D1708)

The owner asked to "use only services provided by them". For the state layer, that holds. For the platform as a whole, it can't.

| Loams need | On TiDB services? | Where it runs instead |
|---|---|---|
| Relational state for dev databases and the control plane's records | **Yes:** Starter (MySQL wire) | — |
| Agent workspaces shared across devices | **Yes:** Filesystem (preview) | Local disk when off |
| A Postgres trial | **Yes:** the PG preview (no SLA) | Loams Postgres on the ground is the product |
| SQL-only HTTP endpoints | **Yes:** Data Service (Starter on AWS) | Not used in LC (Q1272) |
| Analytics | **Paid only:** Lake | House on the ground (D1711) |
| Compute for the engine, agents and apps | **No** | The ground; the user's Cloudflare, Vercel or cloud account (§13) |
| Control-plane compute | **No** | A Cloudflare Worker (D1727) |
| Transactional KV (Live, the metastore, Durable on TiKV) | **No:** no TiKV or keyspace API | The ground's embedded stores (D1341) |
| An S3-compatible bucket (the engine bucket, Git truth) | **No:** Filesystem is not an object-store API | The ground's local folder, or the user's R2 or S3 |
| Queues and schedules | **No** | Durable and Jobs on the ground; §71's ports for apps |
| Identity | **No:** Zitadel needs Postgres 14–18 | `auth.loams.dev`, as in §76 |

So **Loams Cloud on TiDB X** means: TiDB X holds the state, a Worker runs the control plane, Zitadel signs people in, and the user's ground does the work.

## 6. Architecture

```
                    loams.dev/cloud (site Worker)        auth.loams.dev (Zitadel, §76)
                        sign-up, account pages               OIDC, device flow
                               │                                   │
                               ▼                                   │
        ┌──────────────── Loams Cloud control plane (Worker) ◄─────┘
        │  API: orgs, devices, projects, environments, runs, quotas, audit
        │  per-user Durable Object: presence, leases, run index (hibernating WebSockets)
        │            │                          │                          │
        │            ▼                          ▼                          ▼
        │   TiDB Starter: control DB    TiDB Starter cells:          TiDB Filesystem:
        │   (Ostrium's own records)     tenant dev SQL DBs,          workspaces (scoped
        │                               transcripts (opt-in)         tokens per device)
        │                                        ▲                          ▲
        └── outbound WebSocket, 15-min token ────┼──────────────┐           │
                                                 │ MySQL/TLS    │           │ HTTPS/FUSE
   ┌──────────── the ground: the user's devices ─┴──────────────┴───────────┴───────────┐
   │  work laptop: agentd (per-user), Studio; cloud sync off by default (D1723)          │
   │  personal laptop: agentd, light compose (engine, House, Postgres, Git), Desktop    │
   │  homelab: light compose + the Factory's apps (Forgejo, Plane, Zulip, SigNoz)        │
   │  device to device: the user's own tailnet or Cloudflare Tunnel only (D1722)         │
   └─────────────────────────────────────────────────────────────────────────────────────┘
                                   │ loams promote (a PR, same artefacts)
                                   ▼
             the company's AWS / Google Cloud / Azure account: prod (§70, §71)
```

The same picture with BYOG state (LC1) replaces Ostrium's TiDB org with the user's own organization. The control plane then is only the device registry, or nothing at all: a user with one tailnet and their own TiDB org needs no Loams Cloud account.

## 7. The D1323 amendment: Loams Cloud as a free, hosted try-out (D1701–D1704)

### 7.1 The amendment text (D1701)

> **Loams Cloud is a free, hosted try-out run by Ostrium Labs** (amends D1323, and the hosting wording of D1351, D1378, D1615, D1461 and D1317's rationale). Ostrium runs Loams Cloud so that developers can sign up and connect their own devices.
>
> - **What it is:** free and a preview. Help is best effort, through GitHub issues and Discord, and its limits are published. There is no paid tier, no SLA, no support contract and no "we run it for you" offering. **D1323's removal of the services offering stands.**
> - **What Ostrium hosts:**
>   - the sign-up and account pages;
>   - the Loams Cloud control plane: accounts mirrored from Zitadel, devices, projects, environments, the run index, leases, quotas and audit;
>   - the dev state a user opts into, on Ostrium's TiDB Cloud organization: session transcripts, small try-out SQL databases and, for an invited cohort, agent workspaces.
> - **What Ostrium never hosts:**
>   - the engine;
>   - compute for agents or apps;
>   - anyone's production data;
>   - a customer's data plane in any cloud account (**D1378 stands for BYOC**);
>   - a relay for data-plane or device-to-device traffic (**D1317 stands**).
> - **Leaving:** a user can leave at any time with their state, by export or by moving it to their own TiDB Cloud organization (D1728).

### 7.2 What is hosted, what is free, what is never hosted (D1702, D1703)

| | Hosted by Ostrium | Free? | Limits (defaults, Q1265) |
|---|---|---|---|
| Sign-up, account, devices and projects pages (`loams.dev/cloud`) | Yes | Yes | — |
| Identity (`auth.loams.dev`, Zitadel) | Yes (already, D1615) | Yes | GitHub or Google sign-in (Q1266) |
| Control plane: devices, presence, leases, run index, quotas, audit | Yes | Yes | 5 devices per user; 60 API calls a minute per device |
| Session transcripts and run metadata | Yes, on opt-in (on by default for personal devices, off for work devices) | Yes | Counted in the user's SQL storage quota |
| Try-out SQL databases (Loams SQL dev, MySQL wire) | Yes, in shared cells; or on TiDB Cloud Zero if LC0 allows it (Q1264) | Yes | 2 databases, 100 MiB in total, 1M RUs a month |
| Agent workspaces (Filesystem) | Only for an invited cohort (Q1263); everyone else uses BYOG | Yes, in the cohort | 1 GiB, 5,000 files, 2,000 write operations a month |
| The engine, House, Postgres, Git, Live, Durable | **No:** on the user's ground | — | — |
| Agents' and apps' compute | **No:** on the user's ground or their own accounts | — | — |
| Production | **No:** in the company's cloud account | — | — |
| Device-to-device traffic | **No:** over the user's tailnet or Tunnel | — | — |

**The global ceiling** (D1730): Ostrium's total monthly spend on Loams Cloud has a hard ceiling (default $50 a month, Q1265). It is enforced by each cell's TiDB spending limit and by Workers' plan limits.
- When a cell reaches its limit, its tenants' try-out databases deny new connections until the month resets. That is TiDB's own behaviour (V).
- The account pages then say "the try-out is full this month" and offer the BYOG path.

### 7.3 Branding (D1704)

- **The tagline:** "Don't rent a cloud. Loam it." still holds, because Loams Cloud is not rented.
  - It is free, and it holds only coordination and the dev state a user chooses to put there.
  - The work runs on ground the user owns.
  - The state can move to ground the user owns at any time: their own TiDB Cloud organization, or the company's cloud.
- **Copy for the site:**
  - "Loams Cloud is a free try-out with limits. Your machines do the work; Loams Cloud keeps them in step."
  - "Take your state with you whenever you like."
- **Pricing copy** stays "Free and open source. Runs on ground you own." Add one line: "Loams Cloud: free, with limits, no SLA." Never write "pay if you want us to run it".
- **The soil name** proposed is **the Commons**: shared land anyone may use, under rules everyone can read. It sits next to "Loams Cloud", never in place of it (Q1261). It fits the planned blog post on the history of land, enclosure included.

### 7.4 What this does to the earlier rulings

| Ruling | Today | After D1701 |
|---|---|---|
| D1323 | No services offering | Unchanged; a free, hosted try-out is not a service offering |
| D1351 principle 2, "Ostrium runs no data plane" | For BYOC | Unchanged for BYOC installs. Loams Cloud is a try-out control plane and dev-state store, not a data plane: no engine, no production |
| D1378 | No customer data plane, including a managed BYOC | Unchanged. Loams Cloud is not BYOC in Ostrium's account |
| D1615 | Ostrium hosts `loams.dev`, `console.loams.dev`, `auth.loams.dev` | `loams.dev` (with `/cloud`), `auth.loams.dev`, and the Loams Cloud control plane; `console.loams.dev` retired (D1705) |
| D1461 | "Ostrium hosts only the console and auth (plus static site files)" | The rationale reads "plus Loams Cloud (D1701)"; the index decision is unchanged |
| D1317 | No hosted relay; keeps "Ostrium hosts only the console and auth" | The decision stands; its rationale's hosting list is updated |
| Networking map, public endpoints | `loams.dev`, `auth.loams.dev`, `console.loams.dev` | `loams.dev`, `auth.loams.dev` and the control plane's API host (Q1262) |

**Ostrium becomes a data processor** for try-out state. That needs a privacy policy, terms of use and an acceptable use policy before LC2 opens to the public. It is an owner action with counsel (Q1276), in line with Q424's precedent for `push.loams.dev`.

## 8. Sign-up and the console (D1705)

The owner is removing `console.loams.dev` (memory `loams-branding`). What remains is cleaner.

- **Sign-up and account pages:** `loams.dev/cloud/*` on the existing loams-site Worker.
  - That Worker already serves `(auth)/sign-in`, `(auth)/sign-up` and a `(console)/console` preview with keys, usage, billing and settings pages, behind `run_worker_first` (loams-site `wrangler.jsonc`).
  - The `(console)` group becomes `cloud`, with pages for devices, projects and environments, quotas, data export and account deletion. "Billing" is removed.
  - Sign-in moves to Zitadel at `auth.loams.dev` as §76 already plans for the site.
  - **No product console is hosted.**
- **The product console** (Console and Studio modes) runs where it already ships:
  - bundled with each plot's compose bundle (#1064), served by the engine on loopback or the tailnet;
  - in Loams Desktop.
  - Studio's web edition picks a host from the device registry (D909's registry id), not a URL.
- **The control plane's API host** is `api.loams.dev/cloud/v1` (default, Q1262), or a path on `loams.dev`.
  - It is a public HTTPS endpoint, like `auth.loams.dev`.
  - The engine stays never-public: the API is the control plane, not an engine listener.
- **`LOAMS_CORS_ORIGINS`** loses its `https://console.loams.dev` default. A plot's own console is same-origin, and Desktop uses its own origin. Done in LC2, with the networking map updated.
- **Rejected options:**
  - *Keep `console.loams.dev` for Loams Cloud users:* it brings back the hosted console the owner removed, and duplicates the bundled console.
  - *A new `cloud.loams.dev` app:* a second site and a second Auth.js setup for a handful of pages.

## 9. Devices: the ground

### 9.1 Enrolment (D1714)

```
loams cloud login            # OIDC device authorization flow at auth.loams.dev
loams cloud device add [--name homelab] [--work]   # registers this machine
loams cloud device list | rename | remove
loams cloud status           # presence, leases, quotas
```

1. **Sign-in:** `loams cloud login` runs the OIDC device authorization grant against `auth.loams.dev` (Zitadel supports it, verify in LC0 Task 5) and stores a refresh token in the OS keychain.
2. **Registration:** `device add` creates a **device machine user** under the user's personal org. It is the same kind of identity as §76's agent machine users (IA4), with `device` as its kind.
   - The device holds a key pair. The private key stays in the keychain.
   - The device gets **15-minute access tokens** by private-key JWT. Deactivating the device denies its next token, as IA0 measured (memory `loams-identity-zitadel`).
3. **The link:**
   - The device opens **one outbound WebSocket** to the control plane, authenticated with that token and refreshed before expiry. It carries presence, leases and typed commands.
   - The command set is closed: `Claim`, `Release`, `Drain`, `Checkpoint`, `Report`, `RotateKey`.
   - There is never a shell or a passthrough. This is D543's pattern, applied to devices.
4. **No inbound:** the device opens no inbound port. The control plane never dials a device.
5. **What a device can do:** a device token can only mint the device's own scoped credentials:
   - a SQL user for the user's cells or BYOG org;
   - Filesystem scoped tokens for the user's prefix, read-write, with a TTL of at most 1 hour;
   - its own lease operations.

### 9.2 Presence and leases: devices that come and go (D1715)

- **Presence:** a device is `online` while its WebSocket is open. It becomes `asleep` after 2 missed 30-second pings, and `offline` after 24 hours. Laptop sleep is the normal case, not an error.
  - The per-user Durable Object holds presence with hibernating WebSockets, so an idle device costs almost nothing. This needs verifying on Workers pricing in LC0 Task 3.
- **Runs are pinned:** an agent run, or a Factory app instance, runs on one device under a **lease** (60 s, renewed by ping).
  - Sleep freezes the process. On wake, the device renews the lease if nobody else holds it and the run continues.
  - Nothing moves a run automatically, because moving a half-written file tree silently is worse than waiting.
- **Assignment is pull:** a user starts a run "on homelab" from any device. The control plane records the intent, and the homelab claims it over its link. A run aimed at a sleeping device waits, and says so.
- **Always-on duties:** the user picks one device for them, usually the homelab: the Factory's apps, Durable schedules, Git's remote of record (§9.3). Laptops are never the default for always-on duties.

### 9.3 What lives where (D1716)

| State | Home | Follows the user across devices? |
|---|---|---|
| Account, org, devices, projects, environments, quotas, audit | Loams Cloud control DB (Ostrium's Starter instance), or the company-run control plane (D1726) | Yes |
| Run index: ids, device, status, timestamps, branch names | Loams Cloud | Yes |
| Session transcripts (prompts, tool calls, outputs) | The user's tenant space: Loams Cloud cell or BYOG TiDB. On by default for personal devices, **off for work devices**, opt-in per project there | Yes, when on |
| Agent workspace (working tree of a run) | Filesystem layer per run: BYOG org, or the hosted cohort. Otherwise local disk | Yes, when on |
| Code history | Git: the project's remote of record (GitHub, the homelab's Forgejo, or Loams Git on the user's bucket). WIP refs `refs/loams/wip/<run>` | Yes, by push |
| Loams SQL dev databases | Starter: cells or BYOG instances and branches | Yes |
| Engine stores (metastore, Live, Durable, Graph), the bucket, House, Loams Postgres | The device's plot (D1341: embedded stores, local folder) | No: one plot per device. Shared data is for the user to choose, not synced |
| Secrets (model API keys, `env/` secrets) | The device: keychain, `secrets/`, Loams Secrets on the plot | **Never** uploaded. Each device enrols its own keys |
| Factory app data (Forgejo, Plane, Zulip, SigNoz) | The always-on device's plot | Reached over the tailnet (D1722) |

**What leaves a device by default:** its identity, a name the user picks, OS and architecture, Loams version, presence, the run index, and quota counters. **Never by default:** code, prompts, transcripts on work devices, files, secrets or model keys.

### 9.4 Handoff between devices (D1713)

A run started on the work laptop continues on the homelab:

1. The user picks "continue on homelab" in Studio, or runs `loams run move <run> --to homelab`.
2. **The source drains**, if it is awake:
   - it stops the agent at a turn boundary;
   - runs `ti fs drain-file-system`, which uploads buffered writes (V: FUSE buffers locally);
   - creates a checkpoint on the run's layer;
   - pushes the WIP ref;
   - releases the lease.
3. **The target forks** the layer from the checkpoint into its own mount. That keeps the source's layer intact, because forks are independent (V).
   - It hydrates Git (`ti fs-git hydrate-git-workspace`, or fetches the WIP ref).
   - It loads the transcript and resumes the session.
4. **If the source is asleep or gone**, the target resumes from the **last synced checkpoint**, and the UI shows how old it is. Unsynced writes stay on the sleeping laptop, which pushes them to a side branch when it wakes. Nothing is lost silently.
5. **Mount profile:** the run's mount uses Filesystem's `coding-agent` profile, which keeps dependencies, build output and Git internals local (V). Every device rebuilds them, which is cheap compared with syncing `node_modules`.

**Limits, stated in the UI:**
- Windows can't mount Filesystem (V). A Windows device hands off through Git only.
- Concurrent writes to one file are not merged (V). A run has one writer at a time, which the lease ensures.

### 9.5 Device to device (D1722)

Interactive paths need a network between devices: Studio's terminal on a remote device, a Factory app's UI on the homelab, an agent's preview port. **Loams Cloud does not carry them** (D1317 stands). The user picks one of:

- **Their own tailnet:** Tailscale or their Headscale, as today (D580–D589). The free Personal plan is **non-commercial** (V). A company should use its own tailnet.
- **`tsnet` inside agentd** (proposed in LC1 Task 6): the daemon joins the tailnet as its own userspace node, with no TUN device, no root and no system client (V, §69 §5). This also solves the one-tailnet-at-a-time limit (V) on a work laptop that is already on the company's tailnet: the daemon's own node can join the user's personal tailnet, but only if company policy allows it (§10).
- **Their own Cloudflare Tunnel**, per §69, with the gateway's edge listener.
- **Nothing:** the runs and transcripts still move through Loams Cloud's state; only live terminals and previews need a network.

**A hosted Headscale** ("Loams Mesh") is not proposed (Q1269).
- It would need a public VM with 443/tcp and 3478/udp, because Headscale doesn't work behind Cloudflare (V, D583).
- Ostrium would be coordinating every user's network.
- Revisit it when three or more try-out users can't use any of the above.

## 10. Company devices (D1723)

Work laptops are managed by IT. Loams must be acceptable there without exceptions.

| Concern | Default | Why |
|---|---|---|
| Admin rights | **None.** agentd is a per-user service (D784): systemd `--user`, LaunchAgent, or an HKCU Run entry | Most managed laptops forbid installing system services |
| Network client | **No tailnet client required.** The link is outbound HTTPS and WebSocket on 443. MySQL over TLS on 4000 only when SQL is used (verify that proxies allow it; the HTTP driver is the fallback) | Corporate firewalls allow 443; a second VPN client is often forbidden |
| An existing corporate Tailscale | Untouched. Loams never runs `tailscale switch`, and never asks the user to leave the company tailnet (one active tailnet per device, V). `tsnet` (§9.5) is offered only if the user confirms policy allows it | Leaving the company tailnet breaks the user's work access |
| Inbound | **None** | — |
| Data leaving the device | **Off for a device marked work.** `--work` at `device add`, or Desktop asks once. No transcripts, workspaces or SQL databases in Ostrium's cells. Only identity, presence and the run index (§9.3). Opt-in per project, after a warning that company policy may forbid it | Company code in a third party's account is a common policy breach |
| Better for companies | **The company runs the control plane** (D1726) or points its employees at its own TiDB Cloud org (BYOG, D1728). Then the state stays in the company's accounts | The company owns it, so it passes policy |
| Proxies and TLS inspection | `HTTPS_PROXY`/`NO_PROXY` respected; the OS trust store used, so a corporate inspection CA works; certificate pinning only to `auth.loams.dev`'s issuer, never to a leaf | Corporate proxies re-sign TLS |
| Model keys | Stay on the device, in the OS keychain; the company's own LLM gateway can be the configured endpoint | Keys are the company's secret |
| Background activity | The daemon starts at login and stops at logout; no wake locks; the menu-bar or tray icon shows when agents are running; Settings › Background agents can turn it off (§50) | Visible and controllable |
| Telemetry | None by default (D1375's rule, extended to devices) | — |
| Removal | `loams cloud device remove`, then uninstall; per-user files listed (§50 §5) | IT can check it is gone |
| MDM | A documented, silent per-user install and a managed-preferences key `CloudSync=off` (LC2) | IT can enforce the default |

## 11. Dev to prod (D1724)

**The rule:** the same artefacts, promoted; never rebuilt and never copied by hand.

| Artefact | Dev (the ground) | Prod (the company's account) |
|---|---|---|
| Images | `ghcr.io/ostrium-labs/{loams,loams-fabric,loams-agentd}` by digest | The same digests |
| Release set | The plot's release set and upgrade bundle (D1352, §54) | The same, pinned in the infrastructure repository |
| App code and `loams.toml` (`[app]`, §71 D1391) | The project repository | The same commit |
| Environment config | `loams.toml` `[environments.dev]` | `[environments.prod]`: target, region, sizes |
| Secrets | `env/dev` secrets as code (D839) or the device's keychain | `env/prod` in the cloud's secret store (D1358). **Re-issued, never copied** |
| Infrastructure | None (compose on the device) | §70's OpenTofu modules (D1362) and §71's generated roots (D1406) |
| Schema | SQL migrations, Live schema, Postgres migrations in the repository | Applied by the deploy, in order |
| Data | Dev data | **Does not move.** A seed script may run; a one-off import is an explicit, separate command |

**The flow:**

```
loams env list                             # dev (devices, state: loams-cloud | tidbx), prod (aws)
loams promote --from dev --to prod [--plan-only]
```

1. `loams promote` resolves the dev environment's release set and app commit, and renders the prod environment's inputs for the target (§13). It **opens a pull request** in the company's infrastructure repository (GitHub, or the company's Forgejo or Loams Git).
2. The company's CI plans and applies it under the company's review rules (§64's plan runs, D1164). Promotion never holds the company's cloud credentials.
3. A target the company hasn't set up yet fails with the `loams byoc deploy --cloud <c>` command to run first (D1361).

**Environments in `loams.toml`:**

```toml
[environments.dev]
ground = "devices"          # the user's enrolled devices
state  = "loams-cloud"      # or "tidbx" (own org) or "ground"

[environments.prod]
ground = "aws"              # target id (§13); tier from §70
region = "eu-west-1"
tier   = "vm"
state  = "ground"           # the data plane's own stores, in the company's account
issuer = "https://id.example.com"   # the company's OIDC issuer (D1725)
```

## 12. Identity and tenancy (D1725, D1726)

- **People:**
  - A Loams Cloud user is a Zitadel user at `auth.loams.dev`, signing in with GitHub or Google (D1615).
  - Each user gets a **personal org**: Loams org → Zitadel org 1:1, as in D1605.
  - Devices are machine users in it.
- **Companies:**
  - A company may create a **company org** on Loams Cloud and invite employees, with membership from project user grants (IA0 F3).
  - Its projects' **dev** environments may stay personal, with each employee owning their own devices and dev state, or belong to the company org.
  - **Prod** is never on Loams Cloud. It lives in the company's account with the company's own issuer: its IdP, or a Zitadel the company runs.
  - `auth.loams.dev` is the prod issuer only if the company explicitly chooses it (§76 §12.3).
- **Authorization:**
  - The control plane uses one OpenFGA store with `org` at the root, plus the cross-org tuple test (D1612).
  - Dev resources (devices, runs, cells, file-system prefixes) are objects under the org.
  - The control plane never holds tuples for a company's prod install (§76 §12.3).
- **Company-run Loams Cloud** (D1726):
  - The control plane is Apache-2.0 code in this repository (§41's control plane, extended).
  - A company can run it for its employees, with its own Zitadel or IdP and its own TiDB Cloud org (or its own TiDB on TiKV).
  - Its employees enrol devices against `cloud.example.com` instead of Ostrium's. The CLI takes `--control-plane <url>`.
  - This is the cleanest answer to §10's policy question.
- **Moving from personal to company:** the project's Git repository moves, and its dev SQL databases can be exported and imported. Transcripts stay with the person unless they choose to share them.

## 13. Deploy targets: one abstraction, six providers (D1706, D1707, D1735)

### 13.1 The model

- **A target** is a provider plus a role. It is described by a manifest, `deploy/targets/<id>.toml`. The manifest is data: it maps each **slot** to a service, `ground`, or `gap`, with the free limit, its source and the date it was read.
- **The consumers:**
  - `loams target list|show <id>`;
  - `loams byoc deploy --cloud <id>` (§70 D1361), for data-plane tiers;
  - `loams app deploy --target <id>` (§71 D1406), for app ports;
  - `loams promote` (§11).
- **The docs tables** in §13.3 and in §71 §7 are generated from these manifests. The nightly free-tier check (D1401) reads them.
- **Nothing is duplicated:**
  - §70 stays the source for data-plane install tiers.
  - §71 stays the source for app ports and their free-tier numbers.
  - The manifest *references* §71's `free-tiers.json` entries for app ports instead of copying them.

**Slots** (D1707):

| Slot | Meaning | Loams part |
|---|---|---|
| `control` | Control plane and its records | §41, this document |
| `sql` | MySQL-wire relational state | Loams SQL (§47) |
| `postgres` | Postgres | Loams Postgres (§46, §51) |
| `kv` | Transactional KV: metastore, Live | §18, §45 |
| `durable` | Durable execution store | §21, Resonate |
| `house` | Analytics | §49, §75 |
| `bucket` | Object store with conditional writes: engine bucket, Git truth | §02, §36 |
| `workspace` | Shared, mountable file trees for agents | §53, this document |
| `secrets` | Secret store | D1358, §64 |
| `agents` | Compute for Studio's agents | §53, §72 |
| `apps` | Compute for the Factory's apps and built apps | §39, §71 |
| `edge` | Reach from off the tailnet | §69 |
| `identity` | Sign-in and tokens | §76 |
| `meter` | Usage and limits | §63 |

**Roles and order** (D1706). The order is the owner's, and the roles come from the thesis (§1).

| Order | Target | Role | Phase |
|---|---|---|---|
| 1 | `tidbx` | **State** for dev: `sql`, `workspace`, a `postgres` trial. BYOG (the user's org) or Loams Cloud's cells | LC1, LC2 |
| 2 | `cloudflare` | **Edge and apps;** an optional single checkpointed plot (§70 §7) | LC3 |
| 3 | `vercel` | **Apps and agents** (§71 §7.5); no data plane (D1373) | LC4 |
| 4 | `aws` | **Production** (§70 §6, §71 §7.2) | LC5 |
| 5 | `gcp` | **Production** (§70 §6, §71 §7.3) | LC5 |
| 6 | `azure` | **Production** (§70 §6, §71 §7.4) | LC5 |
| — | `devices` | **The ground:** the user's enrolled machines; every slot except `control` | LC1 |

### 13.2 The per-provider mapping

`G` means the slot stays on the ground (the user's devices, or the data plane's own stores in the company's account). `—` means not offered.

| Slot | tidbx | cloudflare | vercel | aws | gcp | azure |
|---|---|---|---|---|---|---|
| `control` | Starter (records only; compute elsewhere) | Workers + Durable Objects | — | G | G | G |
| `sql` | **Starter** (plain MySQL, D1709) | G | G | G (Loams SQL, `tikv` profile) or TiDB Cloud Premium (Q1271) | G | G |
| `postgres` | PG preview, **trial only** (D1710) | Hyperdrive to an external Postgres (D1371) | Marketplace Neon (§71) | G (Loams Postgres) | G | G |
| `kv` | **gap** | DO SQLite metastore, phase 1 (D1368) | — | G; DynamoDB metastore when D58 ships (D1355) | G; Postgres metastore (D1355) | G; Postgres metastore (D1355) |
| `durable` | **gap** (Q1268) | DO-backed Resonate store (D1368) | — | G | G | G |
| `house` | **gap** (Lake not adopted, D1711) | Container, if its spike passes (D1367) | — | G | G | G |
| `bucket` | **gap** (Filesystem is not an object API) | R2 | — (Blob for apps only) | S3 | GCS | Blob Storage |
| `workspace` | **Filesystem** | Sandbox / Containers disk + R2 (verify) | Vercel Sandbox (ephemeral) | G (EBS) | G | G |
| `secrets` | Filesystem vault (BYOG only; Q1274) | Worker secrets, Secrets Store | Sensitive env vars | Secrets Manager | Secret Manager | Key Vault |
| `agents` | **gap** | Sandbox SDK / Containers (Workers Paid) | Vercel Sandbox | G (VM) | G | G |
| `apps` | **gap** | Workers + D1 + R2 + Queues (§71) | Functions (§71) | Lambda + DynamoDB + SQS (§71) | Cloud Run + Firestore (§71) | Functions Flex + Cosmos DB (§71) |
| `edge` | — | Tunnel, Workers VPC, Access (§69) | Vercel OIDC, PrivateLink (§70 §8) | PrivateLink (§70) | PSC (§70) | Private Link (§70) |
| `identity` | — | Access (D1372) | Vercel OIDC (D1308) | GetWebIdentityToken (ER2) | Metadata ID token | Managed identity |
| `meter` | Billing API for the org's cost (verify the API scope) | G | G | G | G | G |

### 13.3 Free tiers per provider

App-port allowances are in §71 §7, read 2026-10-21, and are not repeated. These tables add the slots §71 doesn't cover, and the gaps. "(verify)" marks a limit not confirmed on a primary page.

**TiDB X** (read 2026-10-23, report §2):

| Service | Free limit | Loams slot | Gap |
|---|---|---|---|
| Starter (MySQL) | 5 free instances per org, 25 GiB row + 25 GiB columnar + 250M RUs a month in total; hard spending limit | `sql`, `control` records | No resource groups, so no per-tenant fairness; transactions under 30 minutes; no changefeeds or PITR |
| Branching | 5 branches per org; 10 GiB each on free instances | `sql` branches (BYOG) | Not per user; minutes to create |
| Filesystem | $5 a month per org; with no card, 1 file system per region, 2,000 files, 2 GB | `workspace`, `secrets` (vault) | No spending cap with a card; writes $0.50 per 1,000; no Windows mounts; preview |
| PG preview | 10 free instances, each 50 GiB and 500M RUs a month | `postgres` (trial) | No SLA; AWS only; no extensions, partitioning or logical replication |
| Data Service | Preview, Starter on AWS | — (Q1272) | SQL only |
| Lake | **None** | — | From $1.60 an hour |
| TiDB Cloud Zero | Temporary databases, 30 days, claimable | Try-out `sql` (Q1264) | Terms for third-party provisioning (verify) |
| **Missing** | — | — | Compute, KV, bucket, queues, identity |

**Cloudflare** (report §4.2; §71 §7.1; §70 §7):

| Service | Free limit | Loams slot | Gap |
|---|---|---|---|
| Workers Free | 100,000 requests a day, 10 ms CPU | `control` (small), `apps` | Heartbeat volume needs Paid (§17) |
| Workers Paid | $5 a month: 10M requests, 30M CPU-ms | `control` | Not free |
| Durable Objects | SQLite-backed on Free (verify limits) | `control` presence; `kv`/`durable` phase 1 (D1368) | — |
| Containers / Sandbox | **Workers Paid only**; 375 vCPU-minutes, 25 GiB-hours and 200 GB-hours of disk included | `agents`, a checkpointed plot (D1354) | Ephemeral disk; HTTP-only inbound; at most 4 vCPU and 12 GiB |
| R2 | 10 GB-month, 1M Class A, 10M Class B, free egress (§71) | `bucket`, Git truth (D1369) | — |
| Tunnel | All plans; $0 (verify) | `edge`, device to device | — |
| Access | Free up to 50 users (verify) | Admin routes (D1372) | — |
| Workers VPC | Beta, free during beta | Workers → ground | Beta |
| **Missing** | — | — | Postgres (D1371), TiKV, durable block disks |

**Vercel** (§71 §7.5; report §4.4):

| Service | Free limit (Hobby) | Loams slot | Gap |
|---|---|---|---|
| Functions | 1M invocations, 4 hours of Active CPU (§71) | `apps` | **Hobby is non-commercial**; a company uses Pro ($20 a month) |
| Sandbox | 5 active-CPU hours, 45-minute sessions, 10 at once | `agents` (short runs) | Sessions too short for long agent runs |
| Blob, Cron | (verify) | `apps` blobs, schedules | — |
| Queues | First 1M operations | `apps` queue | — |
| **Missing** | — | — | Any data plane (D1373) |

**AWS** (§71 §7.2; report §4.4):

| Service | Free limit | Loams slot | Gap |
|---|---|---|---|
| Free plan credits | $100 plus up to $100; the plan ends after 6 months and the account closes | BYOC tests | A trial, not production |
| Lambda, DynamoDB (provisioned), SQS | Always free (§71) | `apps` | — |
| EC2 | Credits only | Production plot (§70 VM tier; about $73 a month for a small install, estimate) | **The data plane is paid**, by the company |
| S3 | Credits only (§71) | `bucket` | Cents a month |

**Google Cloud** (§71 §7.3; report §4.4):

| Service | Free limit | Loams slot | Gap |
|---|---|---|---|
| e2-micro | One instance; 1 GB of memory (verify) | — | **Too small** for the light stack (about 1 GB idle) |
| Cloud Run, Firestore, GCS (5 GB, three US regions) | §71 | `apps`, small `bucket` | — |
| Credit | $300 for 90 days | BYOC tests | — |
| Compute Engine | Paid | Production plot (about $60 a month, estimate, §70) | **The data plane is paid**, by the company |

**Azure** (§71 §7.4):

| Service | Free limit | Loams slot | Gap |
|---|---|---|---|
| Functions Flex Consumption | 250,000 executions, 100,000 GB-s | `apps` | — |
| Cosmos DB free tier | 1,000 RU/s and 25 GB, one account per subscription | `apps` items | — |
| B1s VM | 750 hours for 12 months on a free account (verify) | — | 1 GiB of memory, too small |
| Credit | $200 for 30 days (§71) | BYOC tests | — |
| VM | Paid | Production plot (about $82 a month, estimate, §70) | **The data plane is paid**, by the company |

**The honest summary:**
- Apps fit free tiers everywhere (§71).
- **No provider's free tier holds an always-on Loams data plane** except a user's own machine, or Oracle's Always Free A1 (2 OCPU and 12 GB, with idle reclaim, V).
- That is why dev runs on the user's own ground, and why production is paid by the company in its own account.

## 14. Loams' parts on TiDB X

### 14.1 Loams SQL: plain MySQL on Starter (D1709)

- **Why not as built:** Loams SQL as designed (§47) runs adapted TiDB on **Loams' own TiKV**, with one API v2 keyspace per database or branch. TiDB Cloud exposes no TiKV, PD or keyspace API. Keyspaces are its internal mechanism, not an interface. So **Loams SQL can't host its keyspaces on TiDB Cloud.**
- **The `tidbcloud` backend:** a new backend for `loams.sqldb.v1` (§47 §11):
  - **BYOG:**
    - a database is a Starter instance in the user's org;
    - a branch is a TiDB Cloud branch (5 per org, so `CreateBranch` beyond the quota fails with the TiDB error, unwrapped);
    - PITR, changefeeds and resource classes are refused on Starter.
  - **Loams Cloud:**
    - a database is a MySQL database in a shared cell, with one SQL user per tenant database and grants only on it (§15.1);
    - no branches.
  - Credentials are minted per device (§9.1) and expire with `ALTER USER … PASSWORD EXPIRE` rotation (verify on Starter).
- **The compatibility contract:** for this backend it is TiDB X's limited-features list (report §2.2), not §47's:
  - no placement policies, resource groups, `IMPORT INTO`, `BACKUP`, or the `time_to_live` attribute;
  - an enforced password policy;
  - transactions under 30 minutes.

  The SQL conformance runs (§47 §17) get a `tidbcloud` column that records these as expected differences.
- **Production SQL** in the company's account stays §47's Loams SQL (the `tikv` profile, or the operator). A company that prefers a managed service can use **TiDB Cloud Premium** in its own account through the same backend (Q1271). It is paid, from $1,800 a month (V).

### 14.2 Loams Postgres: the PG preview as a trial target only (D1710)

- **The product stays our fork:** Loams Postgres is the Neon fork (§51). The PG preview is a different engine.
  - It has no SLA (V).
  - It can't load our extensions (V).
  - It has no logical replication (V), and so no CDC into Loams streams.
  - Its triggers run per row (V).
- **What the PG preview gets:**
  - a `postgres` adapter used **only when a user asks for it**, for example to try Mastra's Postgres storage against a cloud Postgres without running one;
  - a nightly, non-blocking conformance run that records which of Loams Postgres' tests pass (Q1271).
- **Never a default.** The quota is generous (10 free instances of 50 GiB, V), but "excessive usage may face restrictions or suspension" (V).

### 14.3 Live, the metastore, Durable and Git stay on the ground (D1712)

- **Live, the metastore and Durable:**
  - Live and the metastore need transactional KV (`loams-kv`, §18), and Durable needs its Resonate store.
  - Starter offers SQL only. A TiDB metastore backend was superseded by D124 and D260, and is not revived here.
  - On the ground, the embedded stores are supported for single-node installs (D1341).
  - A Resonate store on MySQL is Q1268 (default: not before LC3). It would let a durable run outlive the device that started it.
- **Git:**
  - Truth is a bucket with create-only PUTs (§36), and TiDB offers no bucket API.
  - The project's remote of record is wherever the user keeps it (§9.3).
  - Filesystem's Git workspaces hold working trees and worktrees for runs, never truth (V: `ti fs-git clone/hydrate/add-git-worktree`).

### 14.4 House: our own; Lake not adopted (D1711)

- **The owner's ruling holds:** House is our own (§75).
- **Lake:** it has no free quota, compute starts at $1.60 an hour, it is a preview, and it is a separate warehouse (V). It adds nothing that a try-out needs.
- **Revisit** if a BYOG user already on Lake wants House to read from it. That would be a connector, not a dependency.

### 14.5 Workspaces on Filesystem (D1713)

- **One layer per agent run**, forked from the project's base path. The run commits to the base only after the person approves, matching the Factory's approval gate (§72 §7). Otherwise the run is rolled back.
- **Scoped tokens per device:**
  - the path is the project's prefix;
  - the operations are `read,list,search,write,delete` for the writer, and `read,list` for a viewer;
  - the TTL is at most 1 hour, minted by the control plane (Loams Cloud) or by the user's own CLI session (BYOG);
  - **the owner token never leaves the control plane** (D1718).
- **Mounts:**
  - FUSE on Linux and on macOS when macFUSE is installed, WebDAV otherwise on macOS, none on Windows (V).
  - agentd mounts under `$HOME`, which also avoids Ubuntu 26.04's AppArmor rule for `fusermount3` (V).
- **Durability:** a run's state is durable once drained (V). agentd drains before every handoff, every approval gate, and every 5 minutes while the run is active (an estimate, tuned in LC0 Task 2).

### 14.6 Metering (D1721)

- **The Meter enforces the try-out:** Loams Meter (§63) runs in the control plane with **the try-out plan as code** (D1137). It covers devices, databases, storage, RUs, file-system bytes, files and writes, with a hard stop: a tenant with no payment method gets a hard stop, which here means every tenant.
- **Counters come from three places:**
  1. the control plane's own operations (device, token and lease counts, API calls);
  2. TiDB:
     - per-database storage from `information_schema.TABLES` (verify on Starter);
     - the org's cost from the billing API (V that the API covers billing; verify its granularity);
     - per-tenant RUs only if LC0 finds a way, since Starter has no resource groups (V);
  3. devices' reports, which are **untrusted**. They are used for warnings, never for limits that protect Ostrium.
- **What Ostrium's money rests on** is the hard caps TiDB enforces itself (spending limits per cell, V) and the cohort size for Filesystem (§15.2), not the device reports.

## 15. Tenancy, quotas and abuse on the hosted try-out

### 15.1 SQL cells (D1717)

- **The control DB** is its own Starter instance. It is never shared with tenants.
- **Tenant cells:**
  - A **cell** is a Starter instance with a spending limit, holding up to *N* tenants' databases (default 200, tuned in LC0 Task 1).
  - Each tenant gets its own MySQL databases and its own SQL users, with grants only on them. Users have no `SUPER` and no cross-database grants.
  - The control plane keeps the cell map. A full cell gets no new tenants, and a new cell opens.
- **The instance budget:**
  - The first 5 instances are free (V).
  - More need a card on Ostrium's org and a spending limit each.
  - The default cap is 100 instances (V), enough for 99 cells at 200 tenants each, so about 19,800 tenants (estimate).
- **Fairness:**
  - Starter has no resource groups (V), so one tenant can use up a cell's RUs.
  - The guards:
    - small cells;
    - a per-tenant connection cap through `MAX_USER_CONNECTIONS` (verify that TiDB X enforces it);
    - a watcher that flags a tenant whose storage or active connections jump;
    - moving a noisy tenant to a cell of its own.
  - The cell's spending limit bounds the damage to one month and one cell (V: new connections are denied, existing ones throttled).
- **An alternative:** if TiDB Cloud Zero's terms allow third-party provisioning (Q1264), try-out SQL could be Zero databases that users later claim into their own TiDB Cloud org. Ostrium would then hold no tenant SQL at all.

### 15.2 Filesystem in the hosted try-out (D1718)

**The money problem:** with a card on file, Filesystem has **no spending cap** (V). Writes cost $0.50 per 1,000 (V). The org's free allowance is $5 a month (V), shared by every file system. A free service can't give that to everyone.

**The proposal:**
- **BYOG is the default for workspaces:** each user's own TiDB Cloud org has its own $5 monthly allowance, and with no card it can never be billed (V). That is about 10,000 free writes a month for each user (estimate, at $0.50 per 1,000).
- **The hosted cohort:**
  - An **invited cohort** of at most 25 users (Q1263) gets hosted workspaces.
  - Each user gets their own file system, with per-file-system storage and file limits set (V: console only, so LC0 Task 2 checks whether an API exists).
  - Each user gets a monthly write budget of 2,000 (about $1, estimate).
  - The org-level cost is read daily from the billing API.
  - At the cohort budget (default $25 a month, inside D1730's ceiling), the control plane **disables the cohort's scoped tokens** until the month resets. Disabling takes about 10 s (V).
- **Never:** an owner token on a device, a token without a TTL, or a path prefix wider than the tenant's.

### 15.3 Quota defaults (D1719, Q1265)

| Per personal org | Default |
|---|---|
| Devices | 5 |
| Projects | 10 |
| Try-out SQL databases | 2, with 100 MiB in total (transcripts included) |
| RUs | 1M a month |
| Concurrent SQL connections | 10 |
| Hosted workspace (cohort only) | 1 GiB, 5,000 files, 2,000 writes a month |
| Control-plane API | 60 calls a minute per device; one WebSocket per device |
| Company orgs | Invited only in LC2; employees count against their own personal quotas |

**The global ceiling** (D1730) is $50 a month across TiDB and Workers (Q1265). At 1,000 users at full quota, the RUs come to about 1B a month, which costs about $75 after the free 250M (estimate). That is above the ceiling, so the hard caps bite first and the try-out is honest about being full.

### 15.4 Abuse controls (D1720)

- **Sign-up:**
  - GitHub or Google through Zitadel only; no password accounts in the try-out (Q1266).
  - Cloudflare Turnstile on the sign-up page.
  - One personal org per identity.
  - An invite code for the first 200 users.
  - A GitHub account at least 30 days old (Q1266).
- **Nothing public:** users get no public endpoints on Loams Cloud. It serves no content, hosts no sites and runs no code, so phishing, hosting and mining abuse have no surface.
- **Storage abuse:** caught by the quotas (§15.3) and the cells' hard limits. Large files are refused by the per-file-system size limit (500 MiB, V).
- **Inactivity:** an org idle for 60 days is suspended, with its data kept. After 90 days it is deleted, with emails at 45 and 75 days (Q1267). Export is always available (D1731).
- **The kill switch:** an operator can suspend an org. That disables its device tokens and Filesystem tokens and locks its SQL users, in about 10 s for Filesystem (V). Every action is audited in the control DB.
- **Terms:** an acceptable use policy and terms of use, written with counsel before LC2 is public (Q1276).

### 15.5 Data handling (D1731)

- **No SLA and no backups promised.** The try-out says so. TiDB Starter has no PITR (V), and Filesystem has no SLA in preview (V).
- **Export:**
  - `loams cloud export` writes the user's SQL databases (logical dump over TLS; Starter refuses `SELECT … INTO OUTFILE` (V), so the dump is client-side), their transcripts, and `ti fs pack-file-system` archives (V) to the device.
  - `loams cloud move --to tidbx` copies them into the user's own org.
- **Account deletion** removes the tenant databases, file systems and Zitadel user within 7 days, and is audited.
- **Region:** one region at LC2: `aws-us-east-1`, where Starter, Filesystem, the PG preview and Data Service all exist (V for Filesystem and the PG preview's AWS limit). EU follows on demand (Q1275).

## 16. HashiCorp (D1729)

The owner's plan said "for loams cloud i have hashicorp free tier". The findings (report §3):

- **HCP can't host Loams Cloud.** HCP has no compute service. Its only lasting free tier is **HCP Terraform Free**: 500 managed resources and one concurrent run (V). Everything else is a one-time $500 credit for 6 months, then a hard stop (V).
- **The use that fits** is infrastructure as code for Ostrium's own Loams Cloud resources: the Worker, DNS, the TiDB Cloud cells (TiDB Cloud has a Terraform provider, V; its OpenTofu registry listing needs verifying), and the R2 buckets.
  - **The default:** **OpenTofu** (MPL-2.0, CNCF, V), with state in an R2 bucket on Ostrium's Cloudflare account, plan and apply in CI with OIDC. This is design 64's preference (OpenTofu over Terraform) and needs no third-party free tier.
  - **The option:** HCP Terraform Free as a **state-only** backend for OpenTofu through the `cloud` block (V that OpenTofu supports it, with a version check it warns about). HCP's remote runs execute Terraform, not OpenTofu (V), so runs stay in CI either way (Q1273).
- **Not used:**
  - **HCP Boundary:** no free allowance, billing per identity, a worker per private site (V); the tailnet already does this.
  - **HCP Vault:** Secrets retired, Dedicated has no free tier (V); Worker secrets and later Loams Secrets (§64) do this.
  - **Packer, Waypoint, Consul and Vagrant:** retired, closing, or not needed (V).
- **Licences:** HashiCorp's tools are BSL 1.1 with IBM as licensor (V).
  - D11 and D1574 govern code and dependencies: Loams never copies, ports or links BSL code, and none of Loams' artefacts depends on HCP.
  - Using a hosted service for Ostrium's own operations is not a dependency of Loams. It's allowed, kept optional, and replaceable by the default.
- **Reconciled with design 64:** design 64 takes over each HashiCorp product's role inside Loams. Its "HCP" row mapped hosted operation to Ostrium services, which D1323 superseded. Nothing in 64 forbids using HCP Terraform as an operations tool, and nothing here adds an HCP dependency to the product.

## 17. Control-plane compute and cost (D1727, D1730)

- **The API:** a Cloudflare Worker (`loams-cloud`, in this repository under `deploy/loams-cloud/`, LC2).
  - It reaches TiDB with `@tidbcloud/serverless` over HTTP (beta, V), since Workers can't open TCP.
  - Interactive transactions are experimental (V), so the API uses single-statement writes and idempotency keys.
- **Presence and leases:** one Durable Object per user, holding presence, leases and the live run index with **hibernating WebSockets**. It writes durable records to the control DB.
- **The plan:** Workers Paid ($5 a month). Free's 100,000 requests a day and 10 ms of CPU don't fit device links beyond a few dozen devices (estimate).
- **Zitadel:** unchanged. It runs on its own host with its own Postgres (§76 §12.3) and is backed up nightly.
  - Zitadel needs Postgres 14–18, so it can't use Starter.
  - The PG preview has no SLA, so it isn't used for identity.
- **OpenFGA:** the control plane's OpenFGA could use its MySQL datastore on a dedicated Starter instance. Whether OpenFGA's MySQL datastore works on TiDB X needs verifying in LC0 Task 5. The default is OpenFGA on Postgres beside Zitadel.
- **The monthly cost at LC2** (estimate):

  | Item | Cost |
  |---|---|
  | Workers Paid | $5 |
  | Control DB | $0 (a free Starter instance) |
  | Cells | $0 up to 4 free instances, then spending-limited |
  | Filesystem cohort | At most $25 |
  | Zitadel's host | Whatever it costs today |

  The total stays inside D1730's $50 ceiling.

## 18. Security notes

- **Threat model:** the control plane is a new public surface, so LC2 adds `docs/security/loams-cloud-threat-model.md`. The main threats:
  - stolen device keys, mitigated by keychain storage, 15-minute tokens and deactivation;
  - cross-tenant reads in a cell, mitigated by per-tenant SQL users, the grants test and no shared users;
  - Filesystem token leaks, mitigated by scoped tokens, a TTL of at most 1 hour and path prefixes;
  - the control plane commanding devices, mitigated by a closed command set with no shell (D543);
  - abuse, mitigated by §15.4;
  - Ostrium's own TiDB API keys, held only in Worker secrets and rotated.
- **The engine stays never-public** (D1311). Devices dial out. Nothing listens.
- **Greptile review:** LC2's PRs carry the `review:greptile` label, because they touch security and auth (memory `loams-branding`).

## 19. What not to build (D1733)

- A paid tier, an SLA, or Loams Cloud as a production host.
- Hosted compute for agents or apps.
- A relay for device or data-plane traffic.
- A hosted Headscale in LC0–LC5 (Q1269).
- Live, the metastore or Durable on TiDB Cloud in LC0–LC2.
- A TiDB metastore backend.
- Lake as House.
- The PG preview as Loams Postgres.
- Our own filesystem service.
- An MCP control-plane server for TiDB Cloud (PingCAP's SQL MCP server and the API are enough).
- Copying any BSL or FSL code.
- Automatic migration of runs between devices.
- Syncing secrets or model keys through Loams Cloud.

## 20. Amendments to earlier designs (approved by the owner, 2026-10-23)

| Design | Amendment |
|---|---|
| [§39](39-software-factory-and-loams-bot.md) | The Factory's apps (Forgejo, Plane, Zulip, SigNoz) run on the user's always-on device in dev (§9.2) and in the company's account in prod; Loams Cloud never hosts them |
| [§41](41-multitenant-byoc-control-plane.md) | The control plane gains a hosted try-out instance (Loams Cloud) and a device kind: devices enrol with D543's outbound pattern (§9.1). The control plane stays opt-in for BYOC (D1377) |
| [§43](43-private-networking.md) | A device may join the user's tailnet through `tsnet` in agentd (§9.5); a hosted Headscale stays out (Q1269) |
| [§47](47-loams-sql-production.md) | A `tidbcloud` backend for `loams.sqldb.v1`: plain MySQL on Starter, with its own compatibility column (§14.1) |
| [§53](53-studio-ade.md) | Studio's host registry (D909) is Loams Cloud's device registry; run workspaces may be Filesystem layers; runs hand off between devices (§9.4) |
| [§63](63-loams-meter.md) | The Meter enforces the try-out plan (§14.6) |
| [§64](64-loams-infra-stack.md) | HCP is reconciled (§16): no product dependency; OpenTofu with R2 state for Ostrium's own infrastructure |
| [§69](69-edge-reach.md) | D1317 stands; its rationale's hosting list now includes the Loams Cloud control plane (D1701). Devices never take inbound traffic through Loams Cloud |
| [§70](70-byoc-clouds.md) | D1351 and D1378 stand for BYOC; their wording is clarified by D1701. `tidbx` joins the target list as a state target, and the deploy order is TiDB X → Cloudflare → Vercel → AWS → Google Cloud → Azure |
| [§71](71-app-byoc-free-tiers.md) | The target manifests (§13.1) reference §71's free-tier table for app ports; `loams app deploy --target` reads the same target ids |
| [§73](73-agent-marketplace.md) | D1461's rationale ("Ostrium hosts only the console and auth") reads "plus Loams Cloud (D1701)" |
| [§76](76-identity-and-authorization.md) | D1615: `console.loams.dev` is retired (D1705), and the control plane is added to what Ostrium hosts; devices are machine users of kind `device` |
| [Networking map](networking-map.md) | The public endpoints list: `loams.dev`, `auth.loams.dev`, the control plane's API host; `console.loams.dev` removed; the `LOAMS_CORS_ORIGINS` default changes in LC2 |

## 21. Risks

| Risk | Mitigation |
|---|---|
| Filesystem is in preview, with no SLA, and its interfaces may change (V) | BYOG default; cohort only for hosted; Git stays the code's truth; a version pin of `ti` in agentd |
| No per-tenant RU fairness on Starter (V) | Small cells, connection caps, a watcher, hard spending limits (§15.1) |
| Filesystem's uncapped bill with a card (V) | Cohort, per-user write budget, daily billing read, token kill switch (§15.2) |
| Ostrium becomes a data processor | Counsel before public LC2 (Q1276); work devices off by default; export and deletion |
| PingCAP changes free tiers or terms | Manifests are dated data with a nightly check (D1401); BYOG and the ground keep working without Loams Cloud |
| Users treat the try-out as production | No SLA, said everywhere; inactivity deletion; `loams promote` is the path to prod |
| Work-laptop policy violations | §10's defaults; company-run control plane (D1726) |
| Laptops sleep mid-run | Pinned runs, leases, explicit handoff, last-checkpoint resume (§9.2, §9.4) |
| The HTTP driver is beta (V) | Single-statement writes and idempotency keys; a TCP fallback from a container if needed |
| The cost ceiling makes the try-out "full" | Honest message, BYOG offered, the ceiling raised only by the owner |

## 22. Decisions (D1700–D1735, approved by the owner, 2026-10-23)

| # | Decision | § |
|---|---|---|
| D1700 | **The product thesis:** dev on the user's own devices (the ground), joined by Loams Cloud and the tailnet or edge; prod in the company's own AWS, Google Cloud or Azure (§70, §71); the stack strengthens from real deployments. Phases follow it | 1 |
| D1701 | **Loams Cloud is a free, hosted try-out run by Ostrium** (amends D1323 and the hosting wording of D1351, D1378, D1615, D1461 and D1317's rationale); text in §7.1 | 7.1 |
| D1702 | **What Ostrium hosts:** sign-up and account pages, the control plane, and opted-in dev state (transcripts, try-out SQL, cohort workspaces) on Ostrium's TiDB Cloud org | 7.2 |
| D1703 | **What Ostrium never hosts:** the engine, agents' or apps' compute, production, a customer's data plane, a data or device relay | 7.2 |
| D1704 | **Branding:** "Loams Cloud" with the soil name "the Commons" (Q1261); "free, with limits, no SLA"; no "we run it" copy | 7.3 |
| D1705 | **`console.loams.dev` is retired;** sign-up and account pages at `loams.dev/cloud` on the site Worker; the product console only bundled with each plot and in Desktop | 8 |
| D1706 | **One target abstraction, six providers,** in the order TiDB X → Cloudflare → Vercel → AWS → Google Cloud → Azure, with roles state, edge and apps, apps, production ×3; plus `devices` | 13.1 |
| D1707 | **Slots:** `control`, `sql`, `postgres`, `kv`, `durable`, `house`, `bucket`, `workspace`, `secrets`, `agents`, `apps`, `edge`, `identity`, `meter`; manifests at `deploy/targets/<id>.toml` map each to a service, `ground` or `gap` | 13.1 |
| D1708 | **TiDB X is the state target,** not the platform: `sql` on Starter, `workspace` on Filesystem, `postgres` trial; gaps for compute, KV, bucket, queues, identity | 5, 13.2 |
| D1709 | **Loams SQL on TiDB Cloud is plain MySQL** through a `tidbcloud` backend for `loams.sqldb.v1`; no keyspace hosting; TiDB X's limited-features list as its contract | 14.1 |
| D1710 | **The PG preview is a trial adapter and a non-blocking conformance target only;** Loams Postgres stays the product | 14.2 |
| D1711 | **House stays Loams' own;** Lake is not adopted | 14.4 |
| D1712 | **Live, the metastore, Durable and Git truth stay on the ground;** no TiDB metastore backend | 14.3 |
| D1713 | **Workspaces on Filesystem:** a layer per run, commit on approval, drain before handoff and approval, the `coding-agent` profile, Git for durable code | 9.4, 14.5 |
| D1714 | **Device enrolment:** OIDC device flow, a device machine user with a keychain key, 15-minute tokens, one outbound WebSocket with a closed command set; no inbound | 9.1 |
| D1715 | **Presence and leases:** online, asleep, offline; runs pinned under 60-second leases; pull assignment; one always-on device for always-on duties | 9.2 |
| D1716 | **What lives where** (§9.3); secrets and model keys never leave a device | 9.3 |
| D1717 | **SQL cells:** a control DB instance; tenant cells of up to 200 databases with per-tenant users and grants; a spending limit per cell; noisy tenants moved | 15.1 |
| D1718 | **Filesystem tenancy:** BYOG by default; a hosted cohort with a file system per user, write budgets and a token kill switch; owner tokens never leave the control plane; scoped tokens of at most 1 hour | 15.2 |
| D1719 | **Quota defaults** (§15.3) | 15.3 |
| D1720 | **Abuse controls** (§15.4) | 15.4 |
| D1721 | **Metering:** the Meter with the try-out plan as code; counters from the control plane, TiDB, and untrusted device reports used only for warnings | 14.6 |
| D1722 | **Device to device** only over the user's tailnet, `tsnet` in agentd, or their own Tunnel; D1317 stands; no hosted Headscale | 9.5 |
| D1723 | **Company-device defaults** (§10): per-user daemon, no tailnet client, no inbound, cloud sync off for work devices, proxy and corporate-CA support, MDM key | 10 |
| D1724 | **Dev to prod:** the same artefacts; `loams promote` opens a pull request in the company's infrastructure repository; data doesn't move; secrets re-issued | 11 |
| D1725 | **Identity mapping:** personal org per user, devices as machine users, optional company org; prod uses the company's issuer; `auth.loams.dev` for prod only by choice | 12 |
| D1726 | **The control plane is open source and self-hostable;** a company can run Loams Cloud for its employees (`--control-plane <url>`) | 12 |
| D1727 | **Control-plane compute:** a Cloudflare Worker with per-user Durable Objects on Workers Paid; TiDB through the HTTP driver; Zitadel unchanged | 17 |
| D1728 | **TiDB Cloud as a BYOG target:** the user's or company's own org, keys on their device or in their CI; Ostrium holds none of that state; ships first (LC1) | 2.2, 13 |
| D1729 | **HashiCorp:** no product dependency; OpenTofu with R2 state for Ostrium's infrastructure; HCP Terraform Free optional as a state-only backend; no Boundary or Vault | 16 |
| D1730 | **A hard monthly ceiling** on Ostrium's Loams Cloud spend (default $50), enforced by spending limits and plan caps | 7.2, 17 |
| D1731 | **Data handling:** no SLA, export and move to BYOG at any time, deletion within 7 days, one region at LC2 | 15.5 |
| D1732 | **Phases LC0–LC5** (§2.3), in the owner's order | 2.3 |
| D1733 | **Not built** (§19) | 19 |
| D1734 | **Conformance:** a `tidbx` target suite (SQL backend, workspace handoff, token scoping, cell isolation) nightly on a free Starter org and a no-card Filesystem, non-blocking until LC2 | 23 |
| D1735 | **Target manifests are dated data** with sources, read by the nightly free-tier check (extends D1401 to data-plane slots) | 13.1 |

## 23. Open questions (Q1260–Q1277), answered by the owner on 2026-10-23: every default

| # | Question | Default (approved) |
|---|---|---|
| Q1260 | Approve the D1323 amendment (D1701): a free, hosted try-out? | **Yes**, invite-only first (LC2), public after counsel (Q1276) |
| Q1261 | Name and soil name? | **"Loams Cloud"**, with **"the Commons"** as the soil name. Alternatives: "Loams Commons" as the product name; "the Exchange" (clashes with the Seed Exchange) |
| Q1262 | The control plane's API host? | **`api.loams.dev/cloud/v1`**, with account pages at `loams.dev/cloud`; alternative `cloud.loams.dev` |
| Q1263 | Hosted workspaces (Filesystem) in the try-out? | **An invited cohort of at most 25** with per-user budgets and a $25 cohort ceiling; BYOG for everyone else |
| Q1264 | Try-out SQL: Ostrium's cells, or TiDB Cloud Zero claimed by the user? | **Zero if LC0 Task 4 confirms that its terms allow third-party provisioning;** cells otherwise |
| Q1265 | Quota defaults and the global ceiling? | **§15.3's table and $50 a month**; the owner raises it |
| Q1266 | Sign-up gates? | **GitHub or Google only, Turnstile, one org per identity, invite codes for the first 200, GitHub account at least 30 days old** |
| Q1267 | Inactivity policy? | **Suspend at 60 days, delete at 90**, emails at 45 and 75 |
| Q1268 | A Resonate store on MySQL so durable runs outlive a device? | **Not before LC3;** a spike then |
| Q1269 | A hosted Headscale ("Loams Mesh")? | **No;** revisit when three try-out users can't use a tailnet, `tsnet` or a Tunnel |
| Q1270 | Work-device default for cloud sync? | **Off,** asked once at enrolment; MDM key to enforce |
| Q1271 | TiDB Cloud Premium as a production SQL option, and the PG preview's conformance run? | **Yes to both, documented and non-blocking;** Premium is the company's own paid choice |
| Q1272 | Use Data Service for anything? | **No** in LC0–LC2 |
| Q1273 | HCP Terraform Free for Ostrium's infrastructure? | **No by default** (OpenTofu, R2 state); allowed as a state-only backend if the owner prefers |
| Q1274 | Filesystem's vault for BYOG secrets? | **No;** secrets stay in the device keychain and Loams Secrets |
| Q1275 | The region? | **`aws-us-east-1`** at LC2; EU on demand |
| Q1276 | Privacy policy, terms of use and AUP for the try-out? | **Counsel writes them before public LC2** (owner action) |
| Q1277 | Apply to the TiDB AI Startup Program, and create the TiDB Cloud org used by LC0's spikes? | **Yes, both owner actions:** create the org with no card for LC0; apply after incorporation (memory `company-formation`) |

## 24. Sources

All vendor facts are in the [research report](../research/tidb-x-and-hcp.md) with their URLs, read on 2026-10-23. In this repository:
- `deploy/release/compose.yaml` and the light default (D1340–D1343);
- the decision log rows cited above;
- loams-site's `wrangler.jsonc` and `app/(auth)`, `app/(console)` (read at `0f8577b`, 2026-10-23);
- memory notes `loams-preview-release-model`, `loams-fully-oss`, `loams-product-focus`, `loams-branding`, `loams-edge-reach`, `loams-identity-zitadel`, `loams-light-compose` and `company-formation`.
