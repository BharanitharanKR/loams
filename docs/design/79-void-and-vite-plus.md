# 79 — Void and Vite+: the JavaScript Toolchain and the App Deployer, on Every Target

Status: **D1770 Approved** (owner, 2026-10-23); **the mapping (D1771–D1789, Q1300–Q1310) is Proposed** and waits for the owner. No code is written by this document.

Source: the owner's direction of 2026-10-23.

> "use Void and Vite+. We can adopt it for every cloud and self-host option."

The owner's notes on Void were treated as unverified. §3 checks each claim against primary sources: the LICENSE files and repository contents through the GitHub API, the npm registry, and Void's own documentation in its repository. No repository was cloned, no account was made, and nothing was deployed.

**Numbering.** This document reserves **D1770–D1799** and **Q1300–Q1319**. It uses D1770–D1789 and Q1300–Q1310 and keeps the rest free.
- The highest numbers in use on `dev` (`3b750c6bd`) are §78's second reservation, D1750–D1769 and Q1290–Q1299. The highest declared rows are D1758 and Q1297.
- No open PR or remote branch uses D1759–D1799 or Q1298–Q1319 (checked on 2026-10-23 across every remote branch).
- Design number 79 is the next free one, and no open PR takes it.

Plan: [VV1](../plans/2026-10-23-vv1-void-and-vite-plus.md), track **VV** (a stub; VV0 is listed task by task).

This document builds on:
- [§70](70-byoc-clouds.md): data-plane BYOC (D1350–D1384). The console on Workers static assets (§70 §7.2).
- [§71](71-app-byoc-free-tiers.md): app BYOC on free tiers (D1390–D1410). The portable contract (D1391), frameworks (D1392), ports (D1393), the `loams` target (D1396), the deploy (D1406).
- [§72](72-software-factory-loop.md): the Factory loop and its deploy stage (D1434).
- [§73](73-agent-marketplace.md): agent templates (D1458).
- [§78](78-loams-cloud-on-tidb-x.md): Loams Cloud, its control plane on a Worker with a Durable Object per user (§17, D1727), and the target abstraction (D1706, D1707).
- [§24](24-cpu-time-runtime.md): Loams Functions on workerd (D171, D181).
- [`docs/monorepo.md`](../monorepo.md): the pnpm and Nx workspace, and the current rule that Vite+ stays scoped to `plugins/`.

Markers, as in §69–§78:
- **(V)**: checked on the primary source in §16, on 2026-10-23.
- **(verify)**: not checked; the VV0 task named beside it checks it first.
- **(estimate)**: judged, not measured.

---

## 1. Summary

### 1.1 The direction (D1770, approved)

Adopt **Vite+** as the JavaScript and TypeScript toolchain, and **Void** for building and deploying apps, wherever each fits, across every cloud and self-host option.

### 1.2 What the research found

- **Vite+ is MIT and stable.** 1.0.0 shipped on 2026-09-28, and 1.3.0 on 2026-10-16. It bundles Vite, Rolldown, Vitest, tsdown, Oxlint and Oxfmt, plus a task runner, behind one `vp` command (V).
- **Void is MIT, and it is young.** Its first open-source release, v1.1.11, came out on 2026-10-15. The third-party claim is right. The LICENSE's copyright holder is Cloudflare, Inc. (V).
- **Void deploys only to Cloudflare.** It has exactly two deploy targets: your own Cloudflare account, and a Void Platform, which itself runs in a Cloudflare account (V).
- **Void can build for Node.js, Bun and Deno.** The output is a standalone Hono server. But on those targets, Void's resource APIs fail at compile time: database, KV, storage, auth, AI, sandbox and env. `void deploy` doesn't handle them, and you host the output yourself (V).
- **The Void Platform needs Cloudflare, and a paid plan.** It needs Workers for Platforms, R2, Analytics Engine, and an external hosted Postgres (V).

### 1.3 The answer

"Every cloud and self-host" is met in three layers. Only the first is Void-everywhere.

| Layer | Every target | How |
|---|---|---|
| **Toolchain** | Yes | Vite+ for dev, check (lint, format, types), test, build and pack, everywhere (D1772–D1777) |
| **App authoring** | Yes | Void's routes, pages and typed client are the authoring layer. §71's ports (`@loams/app`) stay the portable state layer (D1778) |
| **Deploy** | Cloudflare only | `void deploy` for Cloudflare targets (D1779, D1782). Elsewhere, Void's `node` build output plus §71's shims and §71's deploy (D1780, D1781) |

**Plainly:** Void is a Cloudflare deployer. It is not a multi-cloud one. For AWS, Google Cloud, Azure, Vercel and self-hosting, Loams uses Vite+ and Void's Node build, then deploys with the design-70 and design-71 targets that are already approved. Loams does not claim otherwise (D1771).

### 1.4 Per target

| Target | Toolchain | Authoring | Build output | Deploy | App state |
|---|---|---|---|---|---|
| **Cloudflare, the customer's account** | Vite+ | Void | Void's Cloudflare build (a Worker plus assets) | `void deploy --platform cloudflare`, driven by `loams app deploy` | `@loams/app` ports on D1, R2 and Queues; or Void's own APIs in a Cloudflare-native app |
| **Cloudflare, a team Void Platform in the customer's account** | Vite+ | Void | Same | `void deploy --platform void` with a project deploy token | As above, on the platform's resources |
| **AWS** | Vite+ | Void (portable) | Void `target: "node"`, plus Loams' Lambda shim | `loams app deploy` and OpenTofu (D1406) | Ports on DynamoDB, S3 and SQS |
| **Google Cloud** | Vite+ | Void (portable) | `node`, in a Cloud Run container | `loams app deploy` and OpenTofu | Ports on Firestore, GCS and Cloud Tasks |
| **Azure** | Vite+ | Void (portable) | `node`, plus the Functions Flex shim | `loams app deploy` and OpenTofu | Ports on Cosmos DB, Blob and Queue Storage |
| **Vercel** | Vite+ | Void (portable) | `node`, plus the Vercel Functions shim (Build Output API) | `loams app deploy` and the Vercel CLI (D1406) | Ports on Marketplace storage, Vercel Blob and Queues |
| **Self-host (the ground, compose)** | Vite+ | Void (portable) | `node`, in an OCI image | Compose `apps` profile on the user's own device | Ports on the `loams` target (Live, the bucket, Jobs) |
| **A Loams data plane (`loams` target, D1396)** | Vite+ | Void (portable) | Later: Void's Cloudflare build on Loams Functions (workerd) | `loams app deploy --target loams` | Ports on Live, the bucket and Jobs |

Loams' Rust engine, its data-plane BYOC (§70) and the Electron desktop's build are out of Void's reach. §70 is unchanged, except that the console is built with Vite+.

### 1.5 What cannot be met

- **`void deploy` to AWS, Google Cloud, Azure, Vercel or a self-hosted server.** Void has no adapter for them. Its `DeployTarget` type is `'cloudflare' | 'void'` (V). §71's deploy covers them instead.
- **Void's resource APIs off Cloudflare.** These are `void/db`, `void/kv`, `void/storage`, `void/auth`, `void/ai`, `void/sandbox` and `void/env`. Also off: WebSocket route files, inbound email, edge prerendering, and a built-in cron scheduler (V). Portable apps use `@loams/app` ports instead.
- **A Void Platform anywhere but Cloudflare.** It also needs Workers for Platforms, which is a paid plan (V). It can't run on the free tier, and it can't run on the ground.
- **Void on Node 22.** Void needs Node 24.21.0 or later (V), but CI runs Node 22. D1775 moves the JavaScript jobs to Node 24.
- **Void's queue dead-letter queues.** "Void-managed queues do not configure a dead-letter queue" (V). §71's `dead_letter = true` needs an explicit Cloudflare binding (verify, VV0 Task 2).

---

## 2. Scope

**In scope:**
- the monorepo's JavaScript workspace (`web/`, `sdks/typescript/`, `plugins/`, `apps/desktop-electron/`, `deploy/loams-cloud/` once it exists);
- the apps that the Software Factory, Studio and the marketplace scaffold, build and deploy (§71, §72, §73);
- loams-site (`ostrium-labs/loams-site`);
- the Loams Cloud control plane (§78 §17), as an option.

**Out of scope:**
- the Rust engine and its builds (Cargo, Nx, and `loams-build`, the Buck2 research fork);
- data-plane BYOC installs (§70), except the console's build;
- native mobile and desktop apps;
- forking Void or Vite+.

---

## 3. What was verified (2026-10-23)

### 3.1 Vite+ (`voidzero-dev/vite-plus`)

- **Licence:** MIT. `LICENSE` reads "Copyright (c) 2026-present, VoidZero Inc." (blob `cf067b1`). The npm package `vite-plus` declares MIT from 0.1.0 (2026-03-06) through 1.3.0 (V).
- **Maturity** (V):
  - the repository has existed since 2025-03-06 and has 6,449 stars;
  - 1.0.0 shipped on 2026-09-28, promoting rc.1 with no code changes;
  - 1.1.0 followed on 2026-10-07, 1.2.0 on 2026-10-14 and 1.3.0 on 2026-10-16;
  - the CLI needs Node `^22.18.0 || ^24.11.0 || >=26.0.0` (since rc.0).
- **What 1.3.0 bundles** (from its release notes, V):

  | Tool | Version | Licence (V) |
  |---|---|---|
  | Vite | 8.3.4 | MIT (`vitejs/vite`) |
  | Rolldown | 1.2.14 | MIT (`rolldown/rolldown`) |
  | tsdown | 0.23.1 | MIT (`rolldown/tsdown`) |
  | Vitest | 5.0.5 | MIT (`vitest-dev/vitest`) |
  | Oxlint | 1.88.0 | MIT (`oxc-project/oxc`) |
  | oxlint-tsgolint | 7.0.2004 | (type-aware lint; not checked separately) |
  | Oxfmt | 0.73.0 | MIT (`oxc-project/oxc`) |
  | Vite Task (`vp run`) | — | MIT (`voidzero-dev/vite-task`) |

- **Commands:** `vp env`, `install`, `dev`, `check` (format, lint and type checks), `test`, `build`, `run` (cached monorepo tasks), `pack` (libraries), `create`, `migrate`, `staged` and `hooks`. All of them are configured from one `vite.config.ts` (V, README).
- **One packaging detail matters.** `vite-plus` depends on `vite` as `npm:@voidzero-dev/vite-plus-core@1.3.0`, which is VoidZero's MIT build of Vite (V). `docs/monorepo.md` already records the pnpm peer warnings that this alias causes in `plugins/`.
- **Loams already uses it.** `plugins/` and `plugins/core/dashboard-ui` are on Vite+ 1.0.0, with `vp test`, `vp lint` and `vp fmt`. The console stays on plain Vite 8.3.1 and Vitest 5.0.3 (`docs/monorepo.md`).

### 3.2 Void (`voidzero-dev/void`)

- **Licence:** MIT. `LICENSE` reads "Copyright (c) 2026-present, Cloudflare, Inc." (blob `d6ac074`). It was added in commit `437213ec6` on 2026-09-15 (V).
  - The npm package `void` declares MIT from 1.1.11.
  - VoidZero's earlier npm versions (0.7.0 on 2026-05-07, and later) declare no licence. The older `void` versions from 2014–2022 belong to an unrelated, earlier package of the same name.
- **The open-source date:** release v1.1.11 on 2026-10-15 says "Initial open source release of Void." **The third-party claim (open-sourced 2026-10-15, MIT) is correct** (V).
- **Maturity** (V, 2026-10-23):
  - the repository was created on 2026-02-23;
  - it has 3,724 commits, mostly from Christoph Nakazawa (`cpojer`, 1,886) and Evan You (`yyx990803`, 1,254);
  - at least 100 commits landed since 2026-10-01, and 85 since 2026-10-16;
  - it has 554 stars, 52 forks and 31 open issues and PRs;
  - its GitHub releases are v1.1.11 (2026-10-15), v1.1.12 (2026-10-19), v1.2.0 and v1.2.1 (both 2026-10-22).
- **Its parts** (README, V):
  - **The SDK:** file-based API routes, server-rendered pages (React, Vue, Svelte or Solid), a typed fetch client, and typed APIs for databases, storage and AI.
  - **The CLI:** build, provision Cloudflare resources, apply migrations, and deploy in one command, plus logs and rollback.
  - **The Platform:** a shared deployment environment installed in a team's Cloudflare account.
- **Its packages:**
  - `void`, `create-void`, `@void/deploy-core`, `@void/deploy-cloudflare` and `@void/platform`;
  - `@void/edge`, `@void/isr` and `@void/md`;
  - `@void/react`, `@void/vue`, `@void/svelte` and `@void/solid`.

  `void` 1.2.1 depends on `@cloudflare/vite-plugin`, `miniflare`, `@cloudflare/workers-auth`, `@cloudflare/sandbox`, `hono`, `better-auth`, `drizzle-orm` and the Postgres and MySQL drivers. It needs Node 24.21.0 or later, and `vite ^8` as a peer (V).
- **Its toolchain:** "Vite+ is the default toolchain", and Void's own repository builds with `vp` (V).
- **Frameworks:** Void works with TanStack Start, React Router 7, SvelteKit, Nuxt, Analog and Astro 6. Its vinext (Next.js on Vite) adapters are "experimental" (V).

### 3.3 Void's runtimes and targets

**Deploy targets.** `@void/deploy-core` defines `type DeployTarget = 'cloudflare' | 'void'` and a `DeployTargetAdapter` boundary (V). The docs say: "`void deploy` handles Cloudflare and Void platform deployments" (V).

**Runtime targets.** `void.config.ts` `target` is one of these (V):

| `target` | Runtime | `void deploy` | Resource APIs (`void/db`, `kv`, `storage`, `auth`, `ai`, `sandbox`, `env`) |
|---|---|---|---|
| `"cloudflare"` (default) | Workers | Yes | Yes |
| `"node"` | Node.js with `@hono/node-server` | No: "deploy `dist/` using your hosting provider's workflow" | **No:** a compile-time error |
| `"bun"` | `Bun.serve()` | No | No |
| `"deno"` | `Deno.serve()` | No | No |

**What works on every runtime** (V):
- file-based routing and middleware;
- SSR pages;
- the typed fetch client;
- headers and redirects;
- static generation;
- `vite preview`.

**What is missing on `node`, `bun` and `deno`** (V):
- the resource APIs above;
- the `void/env` schema and the secret workflow;
- WebSocket route files (`*.ws.ts`), which compile to Durable Objects;
- a cron runtime. Handlers in `crons/` compile into the bundle, but "there is no built-in scheduler";
- inbound email and `sendEmail`;
- edge prerendering and edge revalidation;
- Void-managed auth.

**The build output on `node`** (V):
- `dist/ssr/app.js` default-exports a Hono app, so it has a `fetch(Request) → Response`;
- `dist/ssr/index.js` starts the server on `PORT`;
- `dist/client/` holds the static assets.

The docs add that the app can be embedded in "serverless environments that accept a `fetch` handler" (V). That is the hook §71's shims need (D1780).

**Off-Cloudflare state is only a proposal.** Issue [#454](https://github.com/voidzero-dev/void/issues/454) (2026-10-16, open, no reply) asks for `void/kv` and `void/storage` on `node`, backed by Redis and S3. It is a community request, not a plan.

**Can Void run without Cloudflare?**
- The SDK can, as a Node, Bun or Deno server, without its Cloudflare resource APIs.
- The CLI's deploy cannot.
- The Platform cannot.

### 3.4 What the Void Platform needs (V)

- **A Cloudflare account** with Workers for Platforms (a paid plan), R2 and Analytics Engine.
- **"An empty hosted PostgreSQL database"**, dedicated to the platform. It must be reachable from the installer's computer and from Cloudflare, through Hyperdrive. With a private certificate authority, it needs a separately managed Hyperdrive.
- **A login provider:** GitHub, Google, generic OIDC or Cloudflare Access.
- **A domain**, or `workers.dev` for testing.
- **The footprint it creates:**
  - five Workers (API, proxy, tail ingestion, dispatch and email gateway);
  - three KV namespaces, one R2 bucket and three Queues;
  - one Workers for Platforms dispatch namespace;
  - one Hyperdrive and one AI Gateway;
  - four Durable Object classes.
- **What the core install leaves out:** the user dashboard, GitHub builds and webhooks, build Containers, and custom app domains (opt-in).
- **Its features:** access control, per-app request and CPU limits (HTTP 429 when an app goes over), retained logs, deployment history, rollback, and "connected applications" that request deployments a user approves.

### 3.5 Cloudflare and VoidZero

- **Cloudflare announced the acquisition of VoidZero on 2026-06-04** ([Business Wire](https://secure.businesswire.com/news/home/20260604108073/en/Cloudflare-Acquires-VoidZero-to-Build-the-Future-of-the-AI-Native-Web); also [Techzine](https://www.techzine.eu/news/devops/141866/cloudflare-acquires-vite-developer-voidzero/)).
  - It says Vite, Vitest, Rolldown, Oxc and Vite+ remain open source under MIT.
  - It commits $1 million to an independent Vite ecosystem fund.
  - The team joins Cloudflare's Emerging Technology and Incubation group.
  - These are press statements, not licence terms. The licences are what §3.1 and §3.2 read.
- **Void's LICENSE names Cloudflare, Inc.,** while Vite+'s names VoidZero Inc. Void is a Cloudflare product, and it is built to sell Cloudflare.

### 3.6 VoidZero's repositories (`voidzero-dev/*`, V)

| Repository | Licence | Stars | What it is |
|---|---|---|---|
| `vite-plus` | MIT | 6,449 | The toolchain |
| `void` | MIT | 554 | The framework, CLI and platform |
| `vite-task` | MIT | 479 | The task runner behind `vp run` |
| `oxc-angular-compiler` | MIT | 244 | An experimental Angular compiler on Oxc |
| `setup-vp` | MIT | 125 | A GitHub Action that installs Vite+ with caching |
| `pkg-pr-registry-bridge` | MIT | 15 | Installs `pkg.pr.new` preview builds as npm versions |
| `vite-plus-extension-pack` | MIT | 10 | An editor extension pack |
| `setup.viteplus.dev` | MIT | 8 | The installer host |
| `vite-plus-remote-cache-cloudflare` | MIT | 6 | A remote cache server for `vp run`, on Cloudflare (since 2026-10-06) |
| `vibe-dashboard`, `.github` | none | — | Not software Loams would use |
| `community-design-resources` | NOASSERTION | — | Design assets |
| `vite-plus-discussions` | none | — | Archived |

Vite, Vitest, Rolldown and Oxc live in their own organisations (`vitejs`, `vitest-dev`, `rolldown`, `oxc-project`), all MIT (V). Cloudflare's `vinext`, which loams-site uses, is MIT too (V).

---

## 4. Where Loams is today (`dev` at `3b750c6bd`)

- **The workspace:** pnpm 11.27.1 and Nx 23.2.1 at the root, with `nx run-many` for build, test and typecheck (`package.json`, `nx.json`). Nx Cloud is the remote cache. Nx Rust projects are in open PRs #1101 and #1102.
- **Lint and format:** Biome 2.5.15 in `web/` and at the root. `plugins/` uses `vp lint` and `vp fmt` (Oxlint and Oxfmt), plus ESLint 9.
- **Tests:** Vitest in 49 packages: 5.0.3 in `web/`, and through `vp test` in `plugins/`.
- **Builds:**
  - the console uses Vite 8.3.1;
  - `@loams/ui` uses tsup 8.5.1;
  - most packages use `tsc`;
  - the desktop uses electron-vite 5.0.0 on Vite 7.3.7, with Vite 8 and electron-vite 6 in open PR #1049.
- **Node:** `engines` is `>=22`, and all 33 `node-version` entries in the workflows are 22.
- **Cloudflare:** the console deploys as Workers static assets with `wrangler` (`web/apps/console/wrangler.jsonc`, assets only). The control plane `deploy/loams-cloud/` is designed (§78 §17) but not built.
- **App BYOC:** §71 is approved and AB0 is not built. `@loams/app` doesn't exist yet. D1406 bundles with esbuild and deploys Cloudflare apps with `wrangler`.
- **loams-site:** Next.js 16.3.5 on vinext 1.0.1, Vite 8, `@cloudflare/vite-plugin` and `wrangler deploy`, with Biome for lint.

---

## 5. Vite+ as the toolchain (D1772–D1777)

### 5.1 In the monorepo (D1772)

- **Each JavaScript package's targets call `vp`:**
  - `vp check` for format, lint and types;
  - `vp test`;
  - `vp build` for apps;
  - `vp pack` (tsdown) for libraries.

  The scripts keep their names (`build`, `test`, `typecheck`, `lint`), so Nx's `targetDefaults` and cache inputs don't change.
- **Nx stays the orchestrator.** The repository is polyglot: Rust, Kotlin, Swift, Go and Python sit beside TypeScript. `vp run` sees only `package.json` scripts, so it isn't adopted as the monorepo runner (Q1302). Nx Cloud stays the remote cache, and Vite+'s Cloudflare remote cache isn't used.
- **One version.** `vite-plus` is pinned in the pnpm catalog at one exact version, and `vite` is aliased to the matching `@voidzero-dev/vite-plus-core`, workspace-wide.
  - This replaces `docs/monorepo.md`'s "Do not apply a global Vite override": the override becomes the catalog entry, applied in one PR, so the peer warnings disappear (verify, VV0 Task 6).
  - The 14-day age rule for catalog bumps applies.
- **The libraries:**
  - `@loams/ui` moves from tsup to `vp pack`;
  - `sdks/typescript/packages/*` move from `tsc` emit to `vp pack` only where their output stays byte-compatible for consumers: the same `exports` map and `.d.ts` paths (VV1 Task 4).
- **CI installs Vite+ through npm** (`pnpm install`), not through `curl https://vite.plus | bash`. `voidzero-dev/setup-vp` (MIT) is allowed in workflows, pinned by commit SHA.

### 5.2 Lint and format (D1773)

- Biome is replaced by `vp check`, which runs Oxlint (with type-aware rules through oxlint-tsgolint) and Oxfmt. This happens in `web/`, at the root and, by a separate PR, in loams-site.
- **The order:**
  1. A rule-parity table: Biome's enabled rules against Oxlint's. A gap is an Oxlint rule to turn on, or a recorded loss.
  2. One formatting commit per workspace, listed in `.git-blame-ignore-revs`.
  3. Removing `biome.json`.
- ESLint 9 in `plugins/` goes at the same time, if `vp lint` covers its rules.
- Q1301 asks the owner. The default is yes.

### 5.3 The exit path (D1774)

The toolchain must be reversible in a day.
- **Configs keep tool sections standard.**
  - `test` is Vitest's config, and `lint` and `fmt` are Oxlint's and Oxfmt's.
  - No package depends on a `vp`-only behaviour without a note in its README.
- **A weekly fallback lane** (Q1309) builds and tests the console and `@loams/ui` with plain Vite, Rolldown and Vitest from npm, without `vite-plus`. It fails when a Vite+-only feature has crept in.
- **If Vite+ is relicensed or abandoned:**
  - Loams pins the last MIT release;
  - Loams moves to the plain tools the fallback lane already proves;
  - Loams may fork the last MIT release, as MIT allows.

  The same rule holds for Void (D1786).

### 5.4 Node (D1775)

- JavaScript CI jobs and `.nvmrc` move to **Node 24 LTS**. The workspace `engines` becomes `>=24.11`.
  - Vite+ supports `^22.18 || ^24.11 || >=26` (V).
  - Void needs `>=24.21.0` (V).
- **Published packages keep their own runtime floor.** `@loams/client`, `@loams/app` and the other SDKs still support Node 22 at runtime, tested in one matrix cell. Only the toolchain needs 24.

### 5.5 The desktop (D1776)

- `apps/desktop-electron` keeps electron-vite for its main, preload and renderer builds until electron-vite runs on `vite-plus-core` (verify, VV0 Task 6).
- It uses `vp test` and `vp check` now.
- PR #1049 (Vite 8 and electron-vite 6) lands first.

### 5.6 What the Factory and Studio scaffold (D1777)

- Every JavaScript project that the Factory, Studio, `loams agent new` or a marketplace template creates is a Vite+ project:
  - one `vite.config.ts`;
  - the scripts `dev`, `check`, `test` and `build` mapped to `vp`;
  - `vp staged` as the pre-commit hook.
- **The CI workflow these templates generate** runs `vp install --frozen-lockfile`, `vp check`, `vp test` and `vp build`. The Factory's CI stage (§72 stage 5) waits on those checks, so they are the same on every target.
- **The templates:**
  - `web/examples/app-byoc-notes` (§71 AB0);
  - the Mastra template `loams-mastra-template` (D1458), when it is a TypeScript project;
  - the JavaScript starters in `loams-agent-templates` (MK1).

---

## 6. Void as the app framework (D1778)

**Void is the authoring layer for Loams web apps.** That means:
- file routes (`routes/`, `middleware/`);
- pages with SSR, in React by default to match the console;
- the typed client (`void/client`);
- `crons/` and `queues/` handlers;
- `void.config.ts`.

**The rule that keeps apps portable.** In a **portable** app, the default, code doesn't import `void/db`, `void/kv`, `void/storage`, `void/auth`, `void/ai`, `void/sandbox` or `void/env`, and it has no `*.ws.ts` routes.
- State goes through `@loams/app` ports (`env.items`, `env.blobs`, `env.queue`) and `env.loams` (D1393, D1397).
- Auth goes through Better Auth directly, as Void's own Node guide says (V), or through the Loams identity of §76.
- A lint rule in the template (`no-restricted-imports`) enforces this.

**A Cloudflare-native app** declares `[app.targets] allow = ["cloudflare"]` (or `["cloudflare", "void-platform"]`) and uses Void's APIs freely. This is §71 §6.6's escape hatch, made first-class for Cloudflare. The deploy marks it so.

**Which is the default** for a new Factory or Studio app is Q1300. The default proposed is **portable**, with Cloudflare-native as an explicit choice at creation.

**How `@loams/app` works under Void:**
- **On Cloudflare,** the ports' Cloudflare adapter reads the bindings that Void provisions. `loams app deploy` writes `inference.bindings` and the `cloudflare` binding arrays into a generated `void.config.ts` overlay, so Void creates D1, R2 and Queues and records them in `void.lock.json` (verify, VV0 Task 2).
- **On `node`,** the ports use their AWS, Google, Azure, Vercel or `loams` adapters, exactly as §71 designs them.

§71's portable contract (D1391, the Workers module shape) stands. A Void app's Cloudflare build already is that shape. Its `node` build exposes `fetch`, and Loams' shim adds `queue` and `scheduled` (D1780).

---

## 7. Deploying with Void

### 7.1 Cloudflare: the customer's own account (D1779)

`loams app deploy --target cloudflare` on a Void app (one with a `void.config.ts`) runs these steps:
1. §71's cost guards (D1400–D1402) against the free-tier table. Void knows nothing about free tiers. Void says that native apps "work on Workers Free within its quotas" (V).
2. The overlay (§6).
3. `void deploy --platform cloudflare`. Void builds, provisions, saves a D1 Time Travel restore point, applies migrations, uploads, checks readiness, then moves traffic (V).
4. Recording a `loams.deploy.v1` record (D1167, D1434). It holds the Worker version, the `void.lock.json` digest and the restore-point command.

**Rules for this path:**
- **Credentials** are the customer's own: a scoped `CLOUDFLARE_API_TOKEN` held in Loams Secrets for the Factory, or the developer's `void cloudflare login` session (D1404).
- **Rollback** is `void project rollback`, recorded on the deploy record. Rollback doesn't reverse migrations (V), so the Factory's deploy approval (§72 §7.2) shows the pending migrations.
- **Apps without Void** keep D1406's `wrangler` path.
- **The fit with "ground you own":** the app, its D1, R2 and Queues live in the customer's account. Loams and Ostrium host nothing (D1378, D1702).

### 7.2 Cloudflare: a team Void Platform (D1782)

- A team that has installed a Void Platform **in its own Cloudflare account** can name it as a target: `void-platform`, with the platform's API URL and a project deploy token in Loams Secrets.
- `loams app deploy --target void-platform` runs `void connect <url> --no-login` and then `void deploy --platform void --project <slug>` (V, the documented CI flow).
- The platform's own access controls, limits and usage reporting apply. Loams adds the deploy record.
- **Loams doesn't install, run or host a Void Platform.** The docs carry a recipe for installing one, pointing to Void's own guide. Its Postgres can be a Loams Postgres reached through a separately managed Hyperdrive over Cloudflare Tunnel (verify, VV0 Task 5). Otherwise it is a managed Postgres.
- Q1306 asks whether to offer this target at all. The default is yes, in AB2.

### 7.3 AWS, Google Cloud, Azure and Vercel (D1780)

- A portable Void app is built with `target: "node"`.
- Loams' entry shim for each target wraps `dist/ssr/app.js` (a Hono app with `fetch`):
  - Lambda behind a Function URL;
  - a Node container on Cloud Run;
  - Azure Functions Flex Consumption;
  - Vercel Functions through the Build Output API.

  `dist/client/` goes where §71 §5.1 puts static assets.
- **Infrastructure and deploy** are D1406's, unchanged: OpenTofu for AWS, Google Cloud and Azure; the Vercel CLI for Vercel.
- **Crons:** Void compiles the `crons/` handlers into the bundle with no scheduler (V). The shim's `scheduled` entry calls them by their `cron` export, and §71's `schedule` port provisions EventBridge Scheduler, Cloud Scheduler, a timer trigger or Vercel Cron (verify the export shape, VV0 Task 3).
- **Queues:** Void's `defineQueue` consumers target Cloudflare Queues (V). Portable apps use §71's `queue` handler, which the shim maps to SQS, Cloud Tasks, Queue Storage or Vercel Queues. Whether a `queues/` handler can be called by the shim on `node` is checked in VV0 Task 3.
- **The bundling step** (D1406 step 1) uses Vite+ (`vp build`, Rolldown) instead of esbuild, for Void and non-Void apps alike (Q1310 covers Vercel).

### 7.4 Self-hosting (D1781)

**On the ground** (a user's own laptop or homelab, §78):
- a portable Void app is built with `target: "node"` into an OCI image (`loams app build --target node --image`);
- it runs in the light compose stack's `apps` profile, beside the Factory's apps (§78 §20, §39);
- its ports use the `loams` adapters once F1 and JB1 land (D1396). Until then they use the local stack's Postgres and the bucket through `env.native` (verify, VV0 Task 4).

**On a Loams data plane** (the `loams` target):
- Loams Functions runs the Workers module shape on workerd (§24, D171). A Void app's Cloudflare build is such a Worker.
- Running it there, with §24's KV and R2 bindings mapped onto Loams, is a later option, after F1 and a spike (Q1308).
- The default is the `node` image.

**No Void Platform off Cloudflare.** §1.5 says why.

### 7.5 Databases (D1785)

Void's `database: "pg"` and `database: "mysql"` connect through Hyperdrive on Cloudflare (V). For a Cloudflare Void app, that means:
- **Loams Postgres** (§46, §51) and **Loams SQL** (§47), which speak the Postgres and MySQL wire protocols, through Hyperdrive. Hyperdrive reaches a private data plane over the customer's Cloudflare Tunnel, which is §69's default reach (D1305), so the never-public rule (D1311) holds (verify that Hyperdrive reaches a database over Tunnel, VV0 Task 5).
- **TiDB Starter** (§78's `tidbx` state target) as `database: "mysql"` through Hyperdrive (verify that Hyperdrive works with TiDB Starter's TLS, VV0 Task 5).

On `node` targets, the app uses its driver directly with a `DATABASE_URL`, so the `void/db` API isn't available (V). Portable apps use Drizzle directly, or `env.loams`.

---

## 8. The Loams Cloud control plane (D1783)

§78 §17 builds the control plane as a Worker (`deploy/loams-cloud/`) with a Durable Object per user, the TiDB HTTP driver and WorkOS. LC2a is specified task by task.

- **Vite+ now.** `deploy/loams-cloud/` is a Vite+ package from its first commit: `vp check`, plus `vp test` with Cloudflare's Workers Vitest pool (verify that it works under `vp test`, VV0 Task 7).
- **Void as a spike, not on LC2a's critical path.**
  - Void supports native Durable Objects in `durable-objects/` and Hono routes. It also supports secrets, crons, staged deploys with readiness checks, and rollback. These fit the control plane (V).
  - But LC2a's tasks are written against `wrangler`, and Void's DB layer would push the control DB toward Hyperdrive MySQL instead of §78's HTTP driver.
  - VV0 Task 7 ports the LC2a skeleton to Void and compares the two.
  - The default (Q1304) is that LC2a ships on `wrangler`, and the control plane moves to `void deploy` only if the spike passes and LC2a isn't delayed.
- **§78's reason for the HTTP driver needs a fresh look.** It says Workers "can't open TCP". Workers have the `connect()` socket API, and Hyperdrive supports MySQL (verify, VV0 Task 5). This doesn't change D1727 now. It is an input to Q1304.
- **The console's static-assets deploy** (`web/apps/console/wrangler.jsonc`) stays on `wrangler`. Void can deploy SPAs (V), and moving there is a VV3 option with no benefit yet.

---

## 9. loams-site (D1784)

- **Vite+ now.** The site is Next.js on vinext with Vite 8. It moves to Vite+ (`vp check`, `vp build` through vinext's Vite config, `vp test`), and Biome goes as in D1773. This is one PR in `ostrium-labs/loams-site`.
- **Void later.** Void's vinext adapters are experimental (V). The site keeps `wrangler deploy` until they aren't, then moves to `void deploy` if the site's Worker routes (`run_worker_first`), Auth.js and redirects carry over (Q1305).

---

## 10. Lock-in, and the rules against it (D1786)

**Void is a Cloudflare product with an MIT licence.** That makes the code free to use and fork. The direction is a different matter: Void exists to put apps on Cloudflare, and nothing obliges it to grow other targets. The risks and the rules:

| Risk | Rule |
|---|---|
| Void stays Cloudflare-only | Loams' portability lives in Loams: `loams.toml` `[app]`, the ports, the shims, `loams.deploy.v1`. Void is used where it is good (authoring, Cloudflare deploys), and never as the only path to a target |
| Void's resource APIs tie apps to Cloudflare | Portable is the default (Q1300). Native apps are marked Cloudflare-only on the deploy and in the marketplace (§71 §11) |
| Void Cloud (the hosted platform at void.cloud) | Not used. No Loams path depends on a VoidZero- or Cloudflare-hosted service. Deploys go to the customer's own account or the customer's own Void Platform only |
| Young code: eight days public, four releases | Exact pins. Upgrades only after the 14-day age rule and a green conformance lane (D1787). No auto-merge of Void bumps |
| Relicensing | The last MIT release stays usable. Loams may fork it. Loams can fall back to Hono, Vite and `wrangler` without rewriting apps, because the portable rule keeps app code off Void-only APIs |
| Vite+ swaps in VoidZero's own Vite build (`vite-plus-core`) | The weekly fallback lane on plain Vite (D1774) |
| Cloudflare tokens in the Factory | Scoped API tokens in Loams Secrets, the customer's own (D1404). Never Ostrium's |

---

## 11. Conformance (D1787)

- **§71's ports suite (D1407) gains a Void lane.**
  - `web/examples/app-byoc-notes` is written as a portable Void app.
  - In PRs, it is built for `cloudflare` and `node` and run on Miniflare and in Node with DynamoDB Local.
  - Nightly, it is deployed with `void deploy` to the Cloudflare Free test account, and with the Lambda shim to the owner's AWS account (§70 §12.2).
- **A Cloudflare-native example** (`web/examples/void-native-notes`) runs nightly on the Cloudflare account only.
- **A Void or Vite+ bump PR** must pass both lanes and the fallback lane (D1774).

---

## 12. Phases (D1788)

| Phase | Content | Depends on | Effort (estimate) |
|---|---|---|---|
| **VV0: spikes** (no product code) | Seven checks, task by task in [VV1](../plans/2026-10-23-vv1-void-and-vite-plus.md). They are the VV0 report's go or no-go items | — | 4–6 agent-days |
| **VV1: the toolchain** | The catalog pin and `vite` alias; Node 24; `vp` targets in `web/`, `sdks/typescript/`, `plugins/` and the desktop's tests; Biome to Oxlint and Oxfmt; tsup to `vp pack`; the fallback lane; loams-site on Vite+ | VV0 Tasks 1, 6 | 6–9 agent-days |
| **VV2: apps on Void** | The portable and native templates; the `@loams/app` overlay for Void; `loams app deploy --target cloudflare` through `void deploy`; the `node` shims for AWS (AB0), then Google Cloud, Azure and Vercel (AB1); the Void conformance lanes | §71 AB0 (`@loams/app`); VV0 Tasks 2–4 | Part of AB0 and AB1, plus 5–7 agent-days |
| **VV3: options** | The `void-platform` target (Q1306); the control plane on Void (Q1304); loams-site on Void (Q1305); Void's Cloudflare build on Loams Functions (Q1308) | Each one's question and spike | Per option |

VV1 can start right after the owner approves. It doesn't block 0.1.x work, and it lands as one PR per workspace.

---

## 13. What not to build (D1789)

- A fork of Void or Vite+, or patches carried on them. Fixes go upstream first.
- A non-Cloudflare `void deploy` adapter, in Loams or upstream, in VV0–VV2. `@void/deploy-core` has an adapter boundary, but its `DeployTarget` type is closed. Q1307 asks whether to propose one upstream later.
- A Loams-hosted Void Platform, or any use of Void Cloud.
- `vp run` as the monorepo's task runner in place of Nx, or Vite+'s Cloudflare remote cache in place of Nx Cloud.
- A Void-only app template as the default.
- A Void path for the Rust engine, data-plane BYOC (§70) or the native apps.

---

## 14. Amendments to earlier designs (proposed with this design; D1770 itself is approved)

| Design | Amendment |
|---|---|
| [§70](70-byoc-clouds.md) | The console in every BYOC tier is built with Vite+ (D1772). Data-plane BYOC is otherwise unchanged; Void does not deploy the data plane |
| [§71](71-app-byoc-free-tiers.md) | D1392: Void is the recommended authoring framework (portable by default), beside Hono, which Void apps already run on. D1406: for Void apps on Cloudflare, `void deploy` replaces `wrangler`; step 1 bundles with Vite+ (Rolldown) instead of esbuild; Void's `node` output is the artefact the AWS, Google Cloud, Azure and Vercel shims wrap. New targets: `void-platform` (D1782) and the self-hosted `node` image (D1781). The `@loams/app` Cloudflare adapter works through Void's bindings |
| [§72](72-software-factory-loop.md) | Stage 5 waits on the `vp check`, `vp test` and `vp build` checks that the templates' CI runs for JavaScript projects (D1777). Stage 6 deploys Void apps on Cloudflare through `void deploy` and records it in `loams.deploy.v1` (D1779). Templates are Vite+ projects, and portable Void apps for web apps (D1778, Q1300) |
| [§73](73-agent-marketplace.md) | JavaScript agent templates (D1458) are Vite+ projects. A listing built on Void's Cloudflare-only APIs shows "Cloudflare only" |
| [§78](78-loams-cloud-on-tidb-x.md) | §17: `deploy/loams-cloud/` is a Vite+ package. Void for the control plane is a VV0 spike, and LC2a stays on `wrangler` (D1783, Q1304). §13.2: the `cloudflare` column's `apps` slot reads "Workers, deployed by Void". `void-platform` is a Cloudflare sub-target |
| [`docs/monorepo.md`](../monorepo.md) | "Vite+ stays scoped to plugins" and "Do not apply a global Vite override" are replaced by D1772 when VV1 lands |
| `ostrium-labs/loams-build` `LOAMS.md` | Vite+ is the recommended toolchain. Plain Vite, Rolldown and Vitest are the fallback (done 2026-10-23 on `loams/main`) |

---

## 15. Decisions and open questions

### 15.1 Decisions (D1770–D1789)

| # | Decision | § | Status |
|---|---|---|---|
| D1770 | **Adopt Vite+ as the JavaScript and TypeScript toolchain, and Void for building and deploying apps, wherever each fits, for every cloud and self-host option** (the owner, 2026-10-23: "use Void and Vite+. We can adopt it for every cloud and self-host option.") | 1.1 | **Approved (owner, 2026-10-23)** |
| D1771 | **Void is read as what it is:** an MIT framework whose deploy targets are only Cloudflare and a Void Platform on Cloudflare. "Every cloud" is met by Vite+ everywhere, Void authoring everywhere, `void deploy` on Cloudflare, and §71's shims and deploy for AWS, Google Cloud, Azure, Vercel and self-hosting. No Loams page claims Void deploys elsewhere | 1.3, 3.3 | Proposed |
| D1772 | **Vite+ in the monorepo:** each JavaScript package's targets call `vp check`, `vp test`, `vp build` or `vp pack`. Nx stays the orchestrator and Nx Cloud the cache. One exact `vite-plus` pin in the catalog, with `vite` aliased to `vite-plus-core` workspace-wide. Installed through npm, and `setup-vp` pinned by SHA | 5.1 | Proposed |
| D1773 | **Biome and ESLint give way to `vp check` (Oxlint and Oxfmt)** after a rule-parity table, with one formatting commit per workspace in `.git-blame-ignore-revs` | 5.2 | Proposed |
| D1774 | **Reversible in a day:** standard tool sections in `vite.config.ts`, and a weekly fallback lane on plain Vite, Rolldown and Vitest. Pin and, if needed, fork the last MIT release if either project is relicensed | 5.3 | Proposed |
| D1775 | **Node 24 LTS for JavaScript CI and `.nvmrc`** (`engines >=24.11`). Published SDK packages keep a Node 22 runtime floor, tested in one cell | 5.4 | Proposed |
| D1776 | **The desktop keeps electron-vite for its build** until electron-vite runs on `vite-plus-core`. It uses `vp test` and `vp check` now | 5.5 | Proposed |
| D1777 | **Everything the Factory, Studio and templates scaffold is a Vite+ project.** Their generated CI runs `vp install --frozen-lockfile`, `vp check`, `vp test` and `vp build`, which the Factory's CI stage waits on | 5.6 | Proposed |
| D1778 | **Void is the authoring layer for Loams web apps, portable by default:** no Void Cloudflare-only imports, state through `@loams/app` ports and `env.loams`, enforced by lint. A Cloudflare-native app opts in with `[app.targets] allow = ["cloudflare"]` | 6 | Proposed |
| D1779 | **Cloudflare deploys of Void apps go through `void deploy --platform cloudflare`** into the customer's own account, driven by `loams app deploy`: cost guards first, a generated `void.config.ts` overlay, a `loams.deploy.v1` record, `void project rollback` for rollback. Non-Void apps keep `wrangler` | 7.1 | Proposed |
| D1780 | **AWS, Google Cloud, Azure and Vercel run Void's `node` build:** Loams' shims wrap `dist/ssr/app.js`, call `crons/` handlers from `scheduled`, and map §71's `queue`. D1406's OpenTofu and the Vercel CLI deploy it. Bundling moves from esbuild to Vite+ (Rolldown) | 7.3 | Proposed |
| D1781 | **Self-hosting runs Void's `node` build as an OCI image** in the compose `apps` profile on the ground. Void's Cloudflare build on Loams Functions (workerd) is a later option (Q1308) | 7.4 | Proposed |
| D1782 | **A `void-platform` target:** deploy into a Void Platform that the customer installed in its own Cloudflare account, with a project deploy token in Loams Secrets. Loams never installs, runs or hosts one, and never uses Void Cloud | 7.2 | Proposed |
| D1783 | **The Loams Cloud control plane is a Vite+ package now. Void for it is a VV0 spike;** LC2a ships on `wrangler` unless the spike passes without delaying it | 8 | Proposed |
| D1784 | **loams-site moves to Vite+ now, and to `void deploy` only when Void's vinext adapters are no longer experimental** | 9 | Proposed |
| D1785 | **Cloudflare Void apps reach Loams Postgres, Loams SQL and TiDB Starter through Void's `pg` and `mysql` dialects on Hyperdrive,** over the customer's Tunnel for a private data plane (D1305, D1311) | 7.5 | Proposed |
| D1786 | **The lock-in rules of §10:** portability lives in Loams' contract, never in Void; no Void Cloud; exact pins with the age rule and green lanes; Cloudflare-only apps are marked | 10 | Proposed |
| D1787 | **A Void conformance lane:** the notes example as a portable Void app on Miniflare and Node in PRs, nightly on Cloudflare Free and AWS. A native example on Cloudflare. Toolchain bumps must pass these and the fallback lane | 11 | Proposed |
| D1788 | **Phases VV0–VV3:** spikes; the toolchain; apps on Void within AB0 and AB1; options | 12 | Proposed |
| D1789 | **Not built:** forks of Void or Vite+; a non-Cloudflare `void deploy` adapter in VV0–VV2; a hosted Void Platform; `vp run` in place of Nx; a Void-only default template; Void for the engine, data plane or native apps | 13 | Proposed |

### 15.2 Open questions (Q1300–Q1310), each with a recommended default

| # | Question | Recommended default |
|---|---|---|
| Q1300 | New Factory and Studio web apps: portable Void app, or Cloudflare-native Void app? | **Portable** (Void routes and pages, with `@loams/app` ports). Cloudflare-native is a choice at creation |
| Q1301 | Replace Biome (and ESLint in `plugins/`) with Oxlint and Oxfmt through `vp check`, in the monorepo and loams-site? | **Yes,** after a rule-parity table, with one formatting commit per workspace |
| Q1302 | Nx or `vp run` as the monorepo task runner? | **Nx,** because the repository is polyglot. `vp` runs inside each package's targets |
| Q1303 | Node 24 for JavaScript CI and `.nvmrc`? | **Yes, Node 24 LTS.** SDK packages keep a Node 22 runtime floor |
| Q1304 | The Loams Cloud control plane on Void? | **A VV0 spike. LC2a ships on `wrangler`;** switch only if the spike passes and LC2a isn't delayed |
| Q1305 | loams-site on Void? | **Vite+ now. Void when its vinext adapters leave experimental** |
| Q1306 | Offer a `void-platform` target (deploy into a customer's own Void Platform)? | **Yes, in AB2,** as a documented target and recipe. Loams never hosts one |
| Q1307 | Propose a non-Cloudflare deploy adapter (Node, containers, other clouds) to Void upstream? | **Not now.** Open an upstream discussion after VV2, with the shim findings |
| Q1308 | The self-host runtime for Void apps: a `node` container, or Void's Cloudflare build on Loams Functions (workerd)? | **A `node` container now.** Try workerd after §24's F1 and a spike |
| Q1309 | A weekly fallback lane on plain Vite, Rolldown and Vitest? | **Yes,** weekly, for the console and `@loams/ui` only |
| Q1310 | Vercel: Void's `node` output through Loams' shim and the Build Output API, or Vercel's own framework detection? | **Loams' shim and the Build Output API** (D1406), so every target uses the same artefact |

---

## 16. Sources (all read on 2026-10-23)

**GitHub API**, no clone:
- `voidzero-dev/void`:
  - `LICENSE`, blob `d6ac0741`, and its commit history (`437213ec6`, 2026-09-15);
  - the README;
  - the [v1.1.11 release](https://github.com/voidzero-dev/void/releases/tag/v1.1.11) ("Initial open source release of Void") and the releases v1.1.12, v1.2.0 and v1.2.1;
  - [`packages/deploy-core/src/target.ts`](https://github.com/voidzero-dev/void/blob/main/packages/deploy-core/src/target.ts);
  - `docs/guide/deployment.md`, `docs/integrations/nodejs-bun-deno.md`, `docs/integrations/cloudflare.md` and `docs/reference/config.md` (`target`);
  - `docs/guide/self-hosted-platform.md`, `docs/guide/platform/installation/prerequisites.md`, `docs/guide/platform/installation/domains.md` (the Cloudflare footprint) and `docs/guide/platform/development/runtime.md`;
  - `docs/guide/database.md` and `docs/guide/database/postgresql.md`;
  - `docs/guide/queues.md`, `docs/guide/jobs.md`, `docs/guide/durable-state.md`, `docs/guide/sandboxes.md` and `docs/integrations/frameworks/overview.md`;
  - the contributors and commits endpoints;
  - issue [#454](https://github.com/voidzero-dev/void/issues/454).
- `voidzero-dev/vite-plus`:
  - `LICENSE`, blob `cf067b10`;
  - the README;
  - the releases [v1.0.0](https://github.com/voidzero-dev/vite-plus/releases/tag/v1.0.0) and [v1.3.0](https://github.com/voidzero-dev/vite-plus/releases/tag/v1.3.0), with their bundled versions.
- `orgs/voidzero-dev/repos`: every repository with its licence, stars and last push.
- The licences of `vitejs/vite`, `vitest-dev/vitest`, `rolldown/rolldown`, `rolldown/tsdown`, `oxc-project/oxc`, `cloudflare/vinext`, `cloudflare/workerd` (Apache-2.0) and `honojs/hono`.

**npm registry:**
- `void`: versions, dates, the licence per version, 1.2.1's `engines` and dependencies;
- `vite-plus`: 1.3.0's dependencies and licence;
- `@voidzero-dev/vite-plus-core`: 1.3.0.

**Docs:**
- [void.cloud](https://void.cloud) (the same Markdown as the repository's `docs/`);
- [viteplus.dev](https://viteplus.dev).

**The acquisition:**
- [Cloudflare's press release on Business Wire](https://secure.businesswire.com/news/home/20260604108073/en/Cloudflare-Acquires-VoidZero-to-Build-the-Future-of-the-AI-Native-Web), 2026-06-04;
- [Techzine](https://www.techzine.eu/news/devops/141866/cloudflare-acquires-vite-developer-voidzero/).

**Loams, `dev` at `3b750c6bd`:**
- `package.json`, `nx.json`, `pnpm-workspace.yaml`;
- `web/package.json`, `web/apps/console/{package.json,wrangler.jsonc}`;
- `plugins/{package.json,vite.config.ts}`, `apps/desktop-electron/package.json`;
- `docs/monorepo.md`, `.github/workflows/*.yml` (`node-version`);
- designs 70, 71, 72, 73 and 78.

**loams-site, `main`:** `package.json` and `wrangler.jsonc`.
