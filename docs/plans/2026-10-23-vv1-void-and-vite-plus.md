# VV1 — Void and Vite+ — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task. Where this plan gives exact names, paths, flags or tests, use them verbatim. Record any deviation in "Rulings made during execution" at the end.
>
> **Status: Stub.** Track **VV**, design [§79](../design/79-void-and-vite-plus.md).
> - **D1770 is approved** (owner, 2026-10-23): adopt Vite+ and Void wherever each fits.
> - **The mapping (D1771–D1789, Q1300–Q1310) is Proposed.** Nothing below starts until the owner approves it.
>
> **Coverage:** this plan specifies **VV0**, the spikes, task by task. VV0 writes no product code. VV1–VV3 are outlined at milestone level, and each gets full task specifications after the VV0 report.

**Goal (VV0):** before any migration, find out where Void and Vite+ hold for Loams:
- which `@loams/app` and Void seams work on Cloudflare and on Node;
- how the shims call Void's handlers;
- whether the monorepo can take one `vite-plus` pin;
- whether the Loams Cloud control plane is better on Void than on `wrangler`.

**Exit (VV0):** `docs/plans/vv0-report.md`, with:
- a go or no-go for each task;
- the measured numbers (build times against today's, bundle sizes, cold starts on Lambda);
- the answers to §79's (verify) items;
- recommended answers for Q1300, Q1304 and Q1308.

## Global Constraints

- **Worktrees and branches.** Work in `~/Documents/Ostriumlabs/loams-wt/vv0-<task>`. Each spike's throwaway code lives under `spikes/vv0/<task>/`, which is not a workspace member. Its PR into `dev` carries only the report section.
- **Sign off every commit:** `git commit -s`. Each PR carries the label `review:greptile`.
- **No accounts and no deploys** without the owner.
  - Tasks 2, 3 and 7 deploy only to the owner's Cloudflare Free test account and AWS credit account (§70 §12.2), with scoped tokens the owner provides.
  - Every resource is deleted at the task's end, and the deletion is logged in the report.
- **Exact pins:** `vite-plus` 1.3.0 and `void` 1.2.1, or the newest release at least 14 days old when the task starts.
- **Node 24.21.0 or later** for Void tasks.
- **Never** use `curl | bash` installers in CI. Never use Void Cloud.

## VV0 tasks

| Task | Question | How | Pass |
|---|---|---|---|
| 1 | Does the console build, test and lint under Vite+ with no behaviour change? | Branch `web/apps/console` and `@loams/ui` onto `vite-plus` 1.3.0: `vp build`, `vp test`, `vp check`. Diff `dist/`, time each step against Vite 8.3.1, Vitest 5.0.3 and Biome. Draft the Biome-to-Oxlint parity table | The same test results, `dist/` equivalent (hashes may differ), and a parity table with every gap named |
| 2 | Do `@loams/app` ports on Cloudflare work through Void's bindings? | A portable Void app with an `items` adapter on D1, `blobs` on R2 and `queue` on Queues. A generated `void.config.ts` overlay sets `inference.bindings` and the `cloudflare` binding arrays. Then `void deploy --platform cloudflare`, a second deploy, and `void project rollback`. Check whether a Queues dead-letter queue can be bound explicitly | Void provisions exactly the declared resources, records them in `void.lock.json`, and rolls back. The DLQ answer is recorded |
| 3 | Can Loams' shims wrap Void's `node` build? | `target: "node"`. Wrap `dist/ssr/app.js` for Lambda behind a Function URL. Call a `crons/` handler from the shim's `scheduled` entry through EventBridge Scheduler. Try calling a `queues/` handler from SQS | HTTP, cron and either a queue handler or a documented no-go, with cold-start numbers |
| 4 | Does the `node` build run as an OCI image in the light compose stack? | `loams app build --target node --image` (a script in the spike), in the `apps` profile, with ports through `env.native` on the local Postgres and the bucket | The app serves and persists across a restart |
| 5 | Can Hyperdrive reach a private Loams Postgres over Cloudflare Tunnel, and TiDB Starter as MySQL? | A Void `database: "pg"` app on Hyperdrive to Loams Postgres behind the owner's Tunnel. A `database: "mysql"` app on TiDB Starter's free instance. Also note what Workers' `connect()` means for §78 §17's HTTP-driver rationale | Both connect, with read-after-write within one request and TLS mode recorded |
| 6 | Can the whole workspace take one `vite-plus` pin with `vite` aliased? | The catalog entry and override, `pnpm install`, `nx run-many -t build test typecheck`. Check electron-vite 6 (PR #1049) against `vite-plus-core` | No peer warnings left unexplained, and every target green. The desktop's answer is recorded |
| 7 | Is the Loams Cloud control plane better on Void? | Port LC2a's skeleton (one route, the per-user Durable Object, a cron, a secret, staged deploy) to Void. Compare it with `wrangler` and `@cloudflare/vitest-pool-workers` under `vp test` | A recommendation for Q1304 with the trade-offs. LC2a is not delayed |

## VV1: the toolchain (milestone)

- **Pins:** the `vite-plus` catalog pin and the `vite` alias; Node 24 in CI and `.nvmrc` (Q1303).
- **Targets:** `vp` in `web/`, `sdks/typescript/` and `plugins/`; `vp test` and `vp check` in the desktop.
- **Lint and format:** Biome and ESLint to Oxlint and Oxfmt (Q1301), with the formatting commits in `.git-blame-ignore-revs`.
- **Libraries:** tsup to `vp pack` in `@loams/ui`, and `vp pack` for SDK packages where the output is byte-compatible.
- **The fallback lane** (Q1309).
- **Docs:** `docs/monorepo.md` and `CONTRIBUTING.md` updated.
- **loams-site** on Vite+, in its own PR.

## VV2: apps on Void (milestone, inside AB0 and AB1)

- **The templates:** the portable and Cloudflare-native Void templates, and `web/examples/app-byoc-notes` as a portable Void app.
- **The deploy:** the `@loams/app` overlay; `loams app deploy --target cloudflare` through `void deploy` (D1779).
- **The `node` shims** for AWS (AB0), then Google Cloud, Azure and Vercel (AB1).
- **The Void conformance lanes** (D1787).

## VV3: options (milestone)

- **The `void-platform` target** (Q1306).
- **The control plane on Void** (Q1304).
- **loams-site on `void deploy`** (Q1305).
- **Void's Cloudflare build on Loams Functions** (Q1308).

## Rulings made during execution

None yet.
