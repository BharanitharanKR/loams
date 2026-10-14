# SD1 — Loams Studio (an Agentic Development Environment) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans.
>
> - Work task by task, **test first**: write the named tests, run them, watch them fail, then implement until they pass.
> - Each task lists the interfaces it must produce and the tests that must exist and pass before it is done.
> - Where this plan gives exact values (names, paths, methods, flags, reasons), use them verbatim.
> - The code is not pre-written in this plan (M0.3 Ruling 1). Record every deviation in "Rulings made during execution" at the end of this file.
>
> **Status: Planned** (2026-10-13). This is **Track SD**, Loams Studio. The design is [§53](../design/53-studio-ade.md) (proposed D900–D922, Q800–Q819); the inventory is [studio-vendor-inventory.md](../design/studio-vendor-inventory.md). It carries out the owner's direction of 2026-10-13:
> - one console shell with two modes, Console and Studio;
> - the factory apps under Studio;
> - UI vendored from T3 Code (MIT) and rebuilt on our stack, backed by `loams-agentd`;
> - the 2026-10-13 UI stack rulings;
> - a remote agentd for the web edition, and the local one on the desktop.
>
> There are five milestones, SD1a–SD1e, run in the order given under "Execution order"; some tasks inside a milestone run in parallel. Branches are `sd1<m>-t<N>` (for example `sd1b-t9`), stacked per milestone and based on `dev`. **Each task goes up as its own PR into `dev`.**

**Goal:** Loams Studio ships in the cordis console on the desktop and in the browser:
- a Console | Studio mode switcher; plugins declare their modes;
- Studio's projects, threads with any harness, a worktree per thread, a chat timeline, a composer, a diff panel with review comments, a file tree and viewer, terminals, a branch and git toolbar (commit, push, PR, merge), a command palette, a preview, per-turn checkpoints with restore, supervised approvals, and parallel run groups compared side by side with the chosen result merged;
- the factory apps in Studio's sidebar and in context;
- all of it backed by `loams-agentd` through a Connect surface, `loams.agentd.v1`: the local daemon through Electron main on the desktop, and a remote daemon over TLS with OIDC in the browser;
- the vendored code with its provenance and attribution recorded, and a licence gate for npm.

**Architecture** (§53 §4–§9):
- **The shell.** `@loams/console-host` gains the `modes` manifest field and the `mode` service. `@loams/plugin-shell` renders the switcher and filters nav, pages and palette by mode.
- **The packages:**
  - `web/packages/studio-ui` (`@loams/studio-ui`) is a library of the vendored and adapted components, grouped as the inventory says, with `provenance.json` and `LICENSE.t3code`.
  - `web/plugins/studio` (`@loams/plugin-studio`, `modes: ["studio"]`) is the plugin. It holds the routes, the layout, the containers that wire `@loams/studio-ui` to connect-query, and the settings.
- **The daemon:**
  - `proto/loams/agentd/v1/*.proto` defines `loams.agentd.v1`.
  - `crates/loams-agentd-connect` serves it, through `connectrpc`, on a loopback listener (desktop) and, with `serve --remote`, on a TLS listener (web). Its handlers call the same `loams-agentd-sessions` functions as the ndjson dispatcher.
  - The ndjson wire, its roles and the renderer allowlist are unchanged (DD1 Ruling 2).
- **Transports:**
  - Desktop: renderer → `loams-app://console/loams.agentd.v1.*` → Electron main's protocol proxy, which adds the token and checks the allowlist → the loopback Connect listener.
  - Web: browser → the remote agentd's URL, carrying an Authentik access token whose audience is `loams-agentd`.
  - `@loams/plugin-rpc` provides `rpc.agentd.*` clients over the agentd transport. Pages read them with connect-query.
- **The daemon's gaps** (§53 §6.5, G1–G23) are new methods in `loams-agentd-sessions`, in the ndjson table (role and `renderer`) and in `loams.agentd.v1`.

**Tech Stack:**
- **Rust:** 1.97.1, edition 2024, workspace lints. `connectrpc` 0.9.1 and `buffa`, as `loams-agentd-link` already uses them; `jsonwebtoken` or `josekit` for JWT verification (Task 0 picks one already in the root lock if possible); `rustls` (workspace); the existing `portable-pty`, `ignore`, `nucleo-matcher` and `similar`; `git` through the daemon's existing process helpers (as `repos.rs` does).
- **TypeScript:** the workspace toolchain (TypeScript 5.9.3, Node ≥ 22, pnpm, Biome, Vitest 5, Playwright 1.63); React 19; `@loams/ui`, `@loams/slots`, `@loams/cordis`, `@loams/console-host`; `@connectrpc/connect`, `@connectrpc/connect-web`, `@connectrpc/connect-query`, `@tanstack/react-query`, `@tanstack/react-virtual`, `@base-ui/react` from the pnpm catalog (console-ui-stack ruling 7).
- **New npm dependencies, all through the catalog,** pinned exactly and at least 14 days old (licences checked on the npm registry on 2026-10-13; the inventory lists them):
  - `@pierre/diffs` and `@pierre/trees` (Apache-2.0);
  - `@tiptap/core`, `pm`, `react`, `starter-kit` and the listed extensions (MIT);
  - `@dnd-kit/*` (MIT);
  - `zustand` (MIT, Q819);
  - `react-markdown`, `remark-*`, `rehype-sanitize`, `unified`, `hast-util-to-jsx-runtime` (MIT);
  - `@xterm/xterm` and `@xterm/addon-fit` (MIT; DD1's pin).
- **Platforms:** the browser (Chromium, Firefox and WebKit in Playwright), and Electron on Linux, plus macOS and Windows builds.

**Spec:**
- [§53](../design/53-studio-ade.md) (all of it) and [studio-vendor-inventory.md](../design/studio-vendor-inventory.md).
- [§37](../design/37-desktop-and-mobile-apps.md) §5 (host, manifest, slots, trust tiers, editions) and §19 (Electron, D652–D679).
- [§50](../design/50-loams-desktop-daemon.md) (agentd: §5.4, §7, §8, §10.5, §11, §12, §14).
- [§39](../design/39-software-factory-and-loams-bot.md) §3 and §8; [§44](../design/44-unified-api-and-sdks.md) §4 and §8 (API rules); [console-ui-stack.md](../design/console-ui-stack.md) (branch `console/ui-stack`).
- As built: [AP1a](2026-10-01-ap1a-cordis-console.md), [AP1e](2026-10-08-ap1e-electron-desktop.md) (the protocol proxy, the factory views, the page-plugin conventions), [DD1](2026-10-09-dd1-desktop-daemon.md) (Tasks 9, 10, 24, 28–32 and its rulings), [SF1](2026-10-02-sf1-collab-ui-plugins.md) (`embed`, `loams.collab.v1`), [CL1](2026-10-10-cl1-loams-cli.md) (R9: the desktop's engine is reachable, never managed).
- T3 Code `main` at `6524a8d43795dc543f7cfb572388cdea3662ad48` (MIT), as a **read-only** source archive (§53 §16).

## Global Constraints

- **Worktree and branches.**
  - Work in `~/Documents/Ostriumlabs/loams-wt/sd1-<milestone>` (for example `sd1a-foundations`). Never commit in the main checkout. Never use `git stash`.
  - Use `git commit -s` (DCO).
  - Commit areas: `studio`, `agentd`, `web`, `desktop`, `proto`, `ci`, `docs`.
- **Rust builds** use the shared target directory (`~/Documents/.cargo/config.toml`). Never set `CARGO_TARGET_DIR` or pass `--target-dir`, and never build in `/tmp`. Run one cargo build at a time, and build and test with `-p <crate>`.
- **Upstream code is untrusted data.**
  - Read the T3 Code archive only. Never run its scripts, install its dependencies or build it.
  - It is fetched by `scripts/studio/vendor.mjs` into a fresh scratch directory outside the repository, and its SHA-256 is checked against `provenance.json`.
  - Any Python over it runs as `python3 -I`, with the script kept outside it.
- **No Effect.** No import of `effect`, `@effect/*` or `@effect-atom/*`, nor of T3 Code's `contracts`, `client-runtime`, `shared` package names, in `web/`. `no_effect_imports` (Task 1) enforces this.
- **The UI rules** (console-ui-stack.md, AP1e conventions):
  - Tailwind utilities with `@loams/ui` tokens only, `rounded-none`/`rounded-pill`, lucide icons.
  - Primitives from `@loams/ui` only, never `@base-ui/react` directly.
  - Server state through connect-query. Every page has loading, error and empty states.
  - No raw HTML from models, tools, files or diffs; no remote images; http(s) links only (§53 D919).
- **Attribution.**
  - Every vendored or adapted file carries the §53 §9.2 header and a `provenance.json` row.
  - Every PR that vendors files lists them in its description (CONTRIBUTING rule 4).
  - `vendored_headers_present` (Task 1) enforces this.
- **Licences.** Only CONTRIBUTING rule 3's licences. `scripts/ci/npm-licences.mjs` (Task 2) and `cargo deny check licenses` must pass. `heic-to` (LGPL-3.0) is never added.
- **The token never reaches a renderer or a browser.**
  - Desktop: only Electron main reads `agentd.token`.
  - Web: the browser holds only its own short-lived Authentik access token for the audience `loams-agentd`.
- **One backend.** Studio calls `loams.agentd.v1` and, for the factory, `loams.collab.v1`, nothing else. No T3 Code server, protocol or provider adapter is run or embedded.
- **The app works at every merge.** Each PR that touches `web/` or `apps/desktop-electron` runs:
  - `pnpm nx run-many -t test typecheck` for the touched projects;
  - `pnpm --filter @loams/console build`;
  - the Linux desktop smoke.
  
  Studio stays behind `flags.features.studio` in the browser edition until Task 32.
- **Wire changes.**
  - ndjson: through `crates/loams-agentd-proto`, followed by `cargo test -p loams-agentd-proto export_bindings`; `ts_bindings_are_fresh` passes.
  - Connect: through `proto/loams/agentd/v1`, followed by `buf generate` into `@loams/proto`; `buf lint` and `buf breaking` pass (the package is `unstable` until Task 32).
- **Pins.** Exact versions, at least 14 days old (`npm view <pkg> time`, `cargo info`), recorded in the commit that adds them.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The Connect handlers call the engine functions, not the ndjson dispatcher.** `loams-agentd-connect` maps proto messages to the same Rust types and calls `loams-agentd-sessions` directly | Behaviour stays identical; there is no JSON round trip through the dispatcher | Two thin mappings to keep (tested by Task 6's parity suite) |
| 2 | **One Connect listener per mode of reach**: the loopback one always runs (when the daemon runs); the remote one only with `serve --remote` | The loopback rules of §50 §7 stay simple; the remote listener has its own, stricter config | Two listeners in one process |
| 3 | **Studio routes are `/studio/…` in the shell's hash router**, not a second router | One router, one deep-link grammar | Long paths |
| 4 | **`@loams/studio-ui` is a library, not a cordis plugin.** Only `@loams/plugin-studio` (and `@loams/plugin-agent` for the dock) import it | Vendored code stays testable without cordis; plugins can't import each other's values (§37 §5.4) | None |
| 5 | **Run groups are a daemon record**, not a client composition of N chats | They survive the window closing; the daemon enforces concurrency limits | One more SQLite table and its migration |
| 6 | **Checkpoints use T3 Code's hidden-commit scheme**, under `refs/loams/checkpoints/<chatId>/turn/<n>/{pre,post}`, rewritten in Rust | A tested design; the user's index and HEAD stay untouched | Refs accumulate (Task 10 prunes them when a thread is deleted) |
| 7 | **Approval answers stay DD1's `ResolveApproval`**; SD1 adds the harness-driver half and the `studio` role, not a second method | One approval path (DD1 Ruling 8) | Task 13 coordinates with DD1 Task 24 |
| 8 | **xterm.js, not the ghostty wasm terminal** (Q818 default) | DD1 pins xterm; a prebuilt wasm needs a reproducible build | We lose ghostty's renderer quality |
| 9 | **The factory plugin keeps its pages and moves under `/studio/factory`**, with redirects from `/factory` | Bookmarks and deep links keep working | Two paths for one release |

## Execution order

| Milestone | Tasks | Scope | Runs |
|---|---|---|---|
| — | 0 | Reconcile with the code as built | First |
| **SD1a** — foundations | 1–8 (8) | The vendoring package and tool; the npm licence gate; the mode switcher; `loams.agentd.v1`; the Connect listener and the desktop transport; the TS client and the parity suite; the remote agentd and the web transport; the Mock harness edits | After Task 0. Tasks 1, 2, 3, 4 and 8 run in parallel; 5 needs 4; 6 needs 5; 7 needs 5 |
| **SD1b** — the daemon's gaps | 9–14 (6) | Worktrees and git writes; checkpoints; content search and review comments; run groups; approvals; notifications and usage | After Task 5 (each adds to both wires). 12 needs 9; 13 needs DD1 Task 24, or it lands Task 24's contract first by agreement (T0) |
| **SD1c** — Studio's UI | 15–26 (12) | The plugin skeleton; the `@loams/ui` primitives; then each component group | 15 after 3 and 6; 16 after 1 and the `console/ui-stack` merge; 17–24 after 15 and 16, in parallel, each after its gap task (20 after 11; 23 after 9; 18's approval cards after 13); 25 after 12 and 20; 26 after 15 |
| **SD1d** — factory and desktop | 27–28 (2) | The factory under Studio; desktop integration | 27 after 15 (its web half after SF1 Tasks 1 and 3); 28 after 17–26 |
| **SD1e** — end to end, review, release | 29–32 (4) | Playwright e2e on both editions; the security review and fuzzing; docs; the exit | Last, in order |

33 tasks in all (0–32).

## Review Focus

1. **Who can reach agentd.**
   - A browser never reaches the loopback listeners.
   - The remote listener accepts only the enrolled subject, the right audience and the configured origins.
   - The renderer reaches only `renderer: true` methods through the proxy.
   - A third-party plugin never gets `rpc.agentd.*`.
   - Tests:
     - Task 5: `connect_origin_is_403`, `connect_without_token_is_401`, `proxy_refuses_non_renderer_method`, `renderer_never_sees_token`.
     - Task 7: `remote_wrong_audience_is_401`, `remote_wrong_subject_is_403`, `remote_disallowed_origin_gets_no_cors`, `remote_studio_role_cannot_shutdown`, `remote_off_by_default`.
     - Task 15: `third_party_cannot_inject_agentd`.
2. **Approvals.**
   - In `supervised` mode, no harness runs a tool before an owner or studio client resolves it.
   - The `mcp` role can never resolve.
   - An approval survives a daemon restart.
   - Tests (Task 13): `claude_supervised_waits_for_resolve`, `codex_supervised_on_request`, `opencode_supervised_forwards_permission`, `acp_supervised_forwards_request_permission`, `mcp_role_cannot_resolve`, `pending_approval_listed_after_restart`.
3. **Destructive git actions are explicit and safe.**
   - Push never force-pushes without a lease.
   - Merge stops on conflicts with a clean tree.
   - Discard, restore and delete need the expected checksum.
   - The MCP shim can do none of them.
   - Tests:
     - Task 9: `push_never_plain_force`, `merge_conflict_leaves_clean_tree`, `commit_refuses_stale_checksum`, `git_writes_not_in_mcp_role`.
     - Task 10: `restore_refuses_stale_checksum`, `checkpoint_does_not_touch_index_or_head`.
4. **Untrusted content renders as text.** Markdown, diffs, highlighted code, file previews and terminal output. Tests: Task 18 `markdown_no_raw_html`, `markdown_no_remote_images`, `code_block_renders_text_not_html`; Task 20 `diff_renders_text_not_html`; Task 21 `file_viewer_renders_text`.
5. **Attribution and licences.** Every vendored file has a header and a provenance row; no disallowed licence enters. Tests: Task 1 `vendored_headers_present`, `provenance_hashes_match`, `no_effect_imports`; Task 2 `npm_licences_allowlist`, `npm_licences_rejects_lgpl`.
6. **Parity between the wires.** A Studio scenario gives the same result over ndjson and over Connect. Test: Task 6 `connect_and_ndjson_agree` (one case per Studio method).
7. **Mode switching never loses work.** Streams and drafts survive a switch, and the approval badge stays live. Tests: Task 3 `switch_keeps_fibers`; Task 19 `draft_survives_mode_switch`; Task 29 `badge_live_in_console_mode`.

## File structure

```
proto/loams/agentd/v1/{studio,projects,threads,queue,harness,git,diff,files,terminals,preview,review,groups,approvals,notifications,attachments}.proto   Task 4 (+ 9–14)
crates/loams-agentd-proto/src/{rpc.rs,studio.rs,git_write.rs,checkpoints.rs,review.rs,groups.rs,approvals.rs,notify.rs,usage.rs}   Tasks 9–14
crates/loams-agentd-proto/fixtures/*.json                                   Tasks 9–14
crates/loams-agentd-sessions/src/{git_write.rs,worktree_list.rs,checkpoints.rs,content_search.rs,review.rs,groups.rs,notify.rs,usage.rs}   Tasks 9–14
crates/loams-agentd-sessions/tests/{git_write.rs,checkpoints.rs,review.rs,groups.rs,content_search.rs}
crates/loams-agentd-harness/src/{claude,codex,opencode,acp}/approvals.rs, mock.rs (edit knob)   Tasks 8, 13
crates/loams-agentd-store/src/store.rs                                      (review, groups, usage tables; Tasks 11, 12, 14)
crates/loams-agentd-connect/                                                new: Tasks 5, 7
  Cargo.toml  src/{lib.rs,listener.rs,remote.rs,oidc.rs,cors.rs,roles.rs,map/*.rs,services/*.rs}
  tests/{loopback.rs,remote.rs,parity.rs}  fuzz/connect_decode
crates/loams-agentd/src/{server.rs,cli.rs,discovery.rs,remote.rs}            Tasks 5, 7 (connectPort; serve --remote)
crates/loams-agentd/README.md                                               Tasks 5, 7, 31
apps/desktop-electron/src/main/protocol/{route.ts,proxy.ts,handler.electron.ts}   Task 5 (/loams.agentd.v1.)
apps/desktop-electron/src/main/agentd/{allowlist.ts}                        Task 5
apps/desktop-electron/src/main/{shell/menu-model.ts,deeplink.ts}, src/shared/deeplink.ts   Task 28
apps/desktop-electron/NOTICE                                                Task 1
apps/desktop-electron/test/e2e/studio.spec.ts                               Task 29
web/packages/console-host/{plugin-manifest.schema.json,src/{manifest.ts,mode.ts,services.ts,permissions.ts,boot.ts}}   Tasks 3, 15
web/packages/slots/src/types.ts                                             Task 3 (meta.mode)
web/plugins/shell/src/{layout.tsx,router.ts,mode-switcher.tsx,palette.tsx}  Task 3
web/plugins/rpc/src/agentd.ts                                               Task 6
web/packages/agentd-client/src/{connect.ts,transport-desktop.ts,transport-remote.ts,hosts.ts}   Tasks 6, 7
web/packages/proto/src/gen/loams/agentd/v1/**                               Task 4 (generated)
web/packages/ui/src/{overlay.tsx,command.tsx,sidebar.tsx,…}                 Task 16
web/packages/studio-ui/                                                     new: Task 1, then 16–26
  package.json  LICENSE  LICENSE.t3code  NOTICE  provenance.json
  src/{thread-list,chat,markdown,composer,diff,files,terminal,git,palette,keybindings,approvals,groups,preview,hooks,lib}/**
  test/{headers.test.ts,provenance.test.ts,imports.test.ts,…}
web/plugins/studio/                                                         new: Task 15, then 17–28
  package.json (modes: ["studio"])  src/{index.tsx,layout/*,sidebar/*,thread/*,git/*,groups/*,preview/*,palette/*,settings/*,hosts/*}  test/**
web/plugins/factory/{package.json,src/index.tsx}                             Task 27
web/plugins/approvals/src/**                                                Task 13 (agent source)
web/plugins/agent/src/**                                                    Tasks 18, 22 (dock uses the shared store and components)
web/apps/console/{catalog/{base,web,desktop}.yml,src/cordis/{desktop.ts,start.ts},test/e2e/studio.spec.ts}   Tasks 3, 15, 29
scripts/studio/{vendor.mjs,upstream-report.mjs,vendor.test.mjs}             Task 1
scripts/ci/{npm-licences.mjs,npm-licences.test.mjs,npm-licences.exceptions.json}   Task 2
pnpm-workspace.yaml (catalog)                                               Tasks 2, 17–22
.github/workflows/{monorepo.yml,ci.yml,studio-e2e.yml,studio-upstream-reminder.yml}   Tasks 1, 2, 29
NOTICE                                                                      Task 1
docs/design/{53-studio-ade.md,studio-vendor-inventory.md,37-desktop-and-mobile-apps.md,50-loams-desktop-daemon.md,13-decision-log.md,README.md}   Tasks 0, 31
docs/guides/studio/{index.md,remote-agentd.md,parallel-runs.md}             Task 31
docs/security/studio-threat-model.md                                        Task 30
docs/plans/{README.md,sd1-exit-report.md}                                   Tasks 31, 32
```

## Shared contracts (all tasks use these names)

### Names

- **Product:** "Loams Studio".
- **Packages and crates:** `@loams/studio-ui`, `@loams/plugin-studio`; `loams-agentd-connect`.
- **Proto:** package `loams.agentd.v1` with the services `StudioService`, `ProjectService`, `ThreadService`, `QueueService`, `HarnessService`, `GitService`, `DiffService`, `FileService`, `TerminalService`, `PreviewService`, `ReviewService`, `RunGroupService`, `ApprovalService`, `NotificationService` and `AttachmentService`.
- **Feature flag:** `studio` (browser edition); the daemon's `features` from `Hello`/`GetHost` list `studio.v1`, `git.write`, `checkpoints`, `review`, `run-groups`, `approvals.supervised`, `notifications`, `usage` and `content-search` as each task lands.
- **Permissions** (console-host `permissions.ts`): `agentd:use`, `agentd:git-write`.
- **Daemon role** (remote only): `studio`.
- **Routes:**
  - `/studio`;
  - `/studio/p/:projectId`;
  - `/studio/t/:threadId` (query `panel=diff|files|terminal|preview|plan`);
  - `/studio/g/:groupId`;
  - `/studio/factory`, `/studio/factory/:app`, `/studio/factory/:app/app`, `/studio/factory/:app/configure`;
  - `/studio/settings`.
- **Deep links:** `loams://open/studio/t/<threadId>`, `loams://open/studio/g/<groupId>`; `loams://open/agent/<id>` maps to `/studio/t/<id>`. Web URLs use `?host=<hostId>`.
- **Storage keys:** `loams.shell.mode`, `loams.studio.hosts`, `loams.studio.keybindings`, `loams.studio.panel`. Every access is wrapped in try/catch.
- **Env (tests only):** `LOAMS_DESKTOP_HARNESS=mock`, `LOAMS_DESKTOP_MOCK_EDIT=<relative path>[,<path>…]`, `LOAMS_AGENTD_TEST_ISSUER=<jwks path>`.

### The manifest and the mode service (Task 3)

```ts
// web/packages/console-host/src/manifest.ts
export type Mode = 'console' | 'studio';
export interface PluginManifest { /* … existing … */ modes: Mode[] }   // default ['console']

// web/packages/console-host/src/mode.ts
export interface ModeService {
  readonly current: Mode;
  readonly available: readonly Mode[];
  set(mode: Mode, opts?: { navigate?: string }): void;
  onChange(cb: (mode: Mode) => void): () => void;
}
export const MODE_HOME: Record<Mode, string> = { console: '/', studio: '/studio' };
export function modeOfPath(path: string): Mode;  // '/studio' prefix → 'studio'
export function defaultMode(kind: 'web' | 'desktop'): Mode;  // web → console, desktop → studio
```

`SlotMeta` gains `mode?: Mode`.

### Provenance (Task 1)

```json
{
  "upstream": "https://github.com/pingdotgg/t3code",
  "license": "MIT",
  "copyright": "Copyright (c) 2026 T3 Tools Inc.",
  "archives": [{ "commit": "6524a8d43795dc543f7cfb572388cdea3662ad48",
                 "url": "https://github.com/pingdotgg/t3code/archive/6524a8d43795dc543f7cfb572388cdea3662ad48.zip",
                 "sha256": "<checked by Task 1>" }],
  "files": [{ "path": "src/composer/doc.ts", "upstreamPath": "apps/web/src/composer-rich-text-doc.ts",
              "commit": "6524a8d4…", "upstreamSha256": "…", "localSha256": "…",
              "verdict": "as-is" | "adapt" | "rewrite", "group": "composer", "note": "…" }]
}
```

The archive that §53 inventoried has SHA-256 `b9ca31efad706d6828f3d9fb6b411027d5837901b800790d241eb672a74b7cbd` (GitHub's `main` zip). Task 1 records the SHA-256 of the commit URL's archive, which may differ in byte layout.

### The header (Task 1; §53 §9.2)

```ts
// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// <upstreamPath> at commit <short sha>, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// <what changed>. Loams' changes are Apache-2.0.
```

### Discovery, CLI and remote config (Tasks 5, 7)

- `agentd.json` gains `"connectPort": <u16>`.
- `loams-agentd serve --remote --listen <addr:port> --tls-cert <pem> --tls-key <pem> --issuer <url> --audience loams-agentd --subject <sub> --allow-origin <origin> [--allow-origin …] --config <path>`. Every flag except the repeatable `--allow-origin` is required. A missing flag exits 4 and names the flag.
- `config.toml` gains:

  ```toml
  [remote]
  enabled = false
  listen = "0.0.0.0:8443"
  tls_cert = "…"   tls_key = "…"
  issuer = "https://auth.example.com/application/o/loams/"
  audience = "loams-agentd"
  subject = "…"
  allow_origins = ["https://console.example.com"]
  ```

### Error reasons (added to `loams-agentd-proto` and mapped to Connect codes)

| Reason | Connect code | When |
|---|---|---|
| `stale_diff` | `failed_precondition` | A checksum doesn't match (commit, discard, restore) |
| `merge_conflict` | `aborted` | `MergeBranch` stopped on conflicts (details list the paths) |
| `nothing_to_commit` | `failed_precondition` | |
| `push_rejected` | `failed_precondition` | A non-fast-forward or a lease failure |
| `provider_not_configured` | `failed_precondition` | No Forgejo or GitHub credential for `CreateChangeRequest` |
| `checkpoint_missing` | `not_found` | |
| `group_not_ready` | `failed_precondition` | Resolve while members are still running (unless `force`) |
| `approval_not_pending` | `failed_precondition` | |
| `forbidden_role` | `permission_denied` | Existing reason, now also for `studio` |
| `unauthenticated` | `unauthenticated` | Existing reason |
| `origin_not_allowed` | `permission_denied` | Remote listener |

### New ndjson methods (role, `renderer`)

All of these are `owner` with `renderer: true`; none is `mcp`:
- `ListWorktrees`, `CommitWorktree`, `PushBranch`, `CreateChangeRequest`, `MergeBranch`, `DiscardPaths`;
- `ListCheckpoints`, `RestoreCheckpoint`;
- `SearchWorkspaceContent`;
- `WatchReviewComments`, `UpsertReviewComment`, `DeleteReviewComment`, `ResolveReviewComment`, `SendReviewToThread`, `PostReviewToChangeRequest`;
- `CreateRunGroup`, `WatchRunGroups`, `GetRunGroupComparison`, `RunGroupCheck`, `ResolveRunGroup`;
- `WatchApprovals`, `SetThreadApprovalMode` (`ResolveApproval` is DD1's);
- `WatchNotifications`, `GetThreadUsage`.

Their shapes are §53 §6.5 G1–G23.

---

## SD1a — Foundations

### Task 0: Reconcile with the code as built

**Files:** this plan's "Rulings made during execution" only (`T0-*`), and §53 where a fact changed.

**Consumes:** §53 §3 and §17; the as-built code.

**Produces:** findings, each with file paths and the task that absorbs it:
1. **The UI stack.** Is `console/ui-stack` (Base UI in `@loams/ui`, `DataTable`, `VirtualList`, `ServerStateProvider`, the pnpm catalog) merged into `dev`? If not, SD1 rebases onto it, and Tasks 3, 15 and 16 wait.
2. **DD1's state.** Which of Tasks 10 (the Node client), 24 (approvals), 27 and 28–32 (the DSH UI) have merged. Agree the Q802 split with DD1's owner and record it. Name each DD1e component that SD1 replaces, and each one it keeps.
3. **agentd's method table.** Confirm the ~95 methods and the shapes §53 §6.4 relies on (`rpc.rs:267-401`, `sessions/src/rpc.rs` dispatch). For each Studio method, record whether it is `renderer: true` today.
4. **`@pierre/diffs` rendering.** Does it write HTML (`innerHTML`, `dangerouslySetInnerHTML`, a shadow root)? From what input? Read its published package from a scratch directory; do not execute it. Decide D919's path (the shadow root with escaped tokens, or React rendering).
5. **Licences.** Re-check the npm licences of the inventory's catalog table on the day. Record the npm `time` of each version to be pinned (at least 14 days old).
6. **JWT verification.** Which JWT/JWK crate is already in the root lock, and its licence. Authentik's JWKS endpoint path for the console's application (MT1's state).
7. **`connectrpc` server support.** How `connectrpc` 0.9.1 serves unary and server-streaming methods on a `hyper` listener (the link crate only uses the client). Any gap is a ruling.
8. **The Electron protocol handler.** Whether `protocol.handle` streams a response body without buffering it (`route.ts`, `proxy.ts`), as Task 5 needs.
9. **Decision numbers.** Confirm that D900–D922 and Q800–Q819 are still free in the decision log and in open PRs; renumber here and in §53 if not.
10. **The owner's answers** to Q800–Q819, if given. Otherwise §53's defaults stand.

**Tests:** none (a reading task).

**Steps:** read; build nothing beyond `cargo metadata` and `cargo tree`; write the `T0-*` rows; commit.

**Commit:** `docs(sd1): task 0 findings`.

### Task 1: The vendoring package, the import tool and attribution

**Files:**
- `web/packages/studio-ui/{package.json,tsconfig.json,vitest.config.ts,LICENSE,LICENSE.t3code,NOTICE,provenance.json,src/index.ts,test/{headers.test.ts,provenance.test.ts,imports.test.ts}}`;
- `scripts/studio/{vendor.mjs,upstream-report.mjs,vendor.test.mjs}`;
- root `NOTICE`, `apps/desktop-electron/NOTICE`;
- `.github/workflows/{monorepo.yml,studio-upstream-reminder.yml}`;
- `pnpm-workspace.yaml` (`zustand` in the catalog, Q819).

**Consumes:** §53 §9; the inventory; Shared contracts (provenance, header).

**Produces:**
- `@loams/studio-ui` (private, `"license": "Apache-2.0 AND MIT"`), empty apart from `src/index.ts`, with the files above. `LICENSE.t3code` is the MIT text of §53 §16, verbatim.
- `scripts/studio/vendor.mjs`:
  - `fetch --commit <sha> --out <scratch>`: downloads the archive into a new empty directory outside the repository; records its SHA-256; extracts it with a zip library that refuses absolute paths and `..`; executes nothing.
  - `import --from <scratch>/<top> --group <g> --map <upstream>=<local> … [--verdict as-is|adapt|rewrite]`: copies the files, writes the header, rewrites import specifiers only for `as-is` files (by an explicit map), and appends or updates provenance rows.
  - `check`: every file under `src/` that carries the header has a provenance row; every row's file exists; `as-is` rows' `localSha256` match.
- `scripts/studio/upstream-report.mjs --from <old scratch> --to <new scratch>`: per vendored file, `unchanged`, `changed upstream` (with the unified diff) or `gone upstream`.
- The root NOTICE entry and the desktop NOTICE entry of §53 §9.2.
- `studio-upstream-reminder.yml`: a monthly scheduled workflow that opens (or updates) one issue, "T3 Code upstream check due", with no network access to upstream.
- `monorepo.yml` runs `node scripts/studio/vendor.mjs check`.

**Tests:**
- `vendored_headers_present`: every file with a provenance row starts with the §53 §9.2 header and names its upstream path and commit.
- `provenance_hashes_match`: `as-is` rows' local hashes match the files.
- `no_effect_imports`: a source scan of `web/packages/studio-ui` and `web/plugins/studio` finds no `effect`, `@effect/*`, `@effect-atom/*`, `@t3tools/*`, `@legendapp/list`, `@tanstack/react-router` or `heic-to`.
- `vendor_refuses_path_traversal`: a crafted zip with `../x` and `/x` entries fails, and nothing is written outside the output directory.
- `vendor_never_executes`: the tool runs with a scratch `package.json` whose `scripts.postinstall` writes a marker; no marker appears.
- `upstream_report_classifies`: fixtures of two small trees give the three classes.

**Steps:** write the tests and fixtures (they fail); implement the tool; create the package; add the NOTICE entries; wire CI; commit. The first real import is Task 16's.

**Commit:** `studio: @loams/studio-ui, the vendoring tool, provenance and T3 Code attribution (D912, D913)`.

### Task 2: The npm licence gate

**Files:** `scripts/ci/{npm-licences.mjs,npm-licences.test.mjs,npm-licences.exceptions.json}`, `.github/workflows/monorepo.yml`, `CONTRIBUTING.md` (one line under rule 3).

**Consumes:** `deny.toml`'s allow list; CONTRIBUTING rule 3.

**Produces:**
- `npm-licences.mjs [--project <nx project>…]`: runs `pnpm licenses list --prod --json` for the console, the desktop and `@loams/studio-ui`.
  - It parses SPDX expressions and accepts an `OR` expression when one branch is allowed, recording the branch taken.
  - It fails on a licence outside the allow list, on `UNKNOWN`, and on any `@tiptap-pro/*` package.
  - It prints a table on failure.
- The exceptions file holds `{package, version, licence, reason, approvedBy}` entries; it starts empty.
- The job runs in `monorepo.yml` on every change to a `package.json`, `pnpm-lock.yaml` or `pnpm-workspace.yaml`.

**Tests:**
- `npm_licences_allowlist`: MIT, ISC, Apache-2.0, BSD-3-Clause, `MPL-2.0 OR Apache-2.0` and `MIT OR GPL-3.0-or-later` pass.
- `npm_licences_rejects_lgpl`: `LGPL-3.0` fails, naming `heic-to`.
- `npm_licences_rejects_unknown`.
- `npm_licences_rejects_tiptap_pro`.
- `npm_licences_current_tree_passes`: the real tree passes, or each failure gets a recorded exception before this task merges.

**Steps:** tests first with fixture JSON; implement; run on the real tree; fix or record findings; commit.

**Commit:** `ci: npm licence gate for the web workspace (D914)`.

### Task 3: The mode switcher

**Files:**
- `web/packages/console-host/{plugin-manifest.schema.json,src/{manifest.ts,mode.ts,services.ts,boot.ts,index.ts},test/{manifest.test.ts,mode.test.ts}}`;
- `web/packages/slots/src/types.ts`;
- `web/plugins/shell/src/{layout.tsx,router.ts,mode-switcher.tsx,palette.tsx}` and tests;
- `web/apps/console/src/cordis/{start.ts,desktop.ts}`;
- `apps/desktop-electron/src/shared/deeplink.ts` (allowlist entries).

**Consumes:** §53 §4; Shared contracts (manifest and mode service).

**Produces:**
- `modes` in the manifest. The schema, `KEYS` and the validator accept `["console"]`, `["studio"]` or both; it defaults to `["console"]`; unknown modes are refused.
- The `mode` service:
  - `available` is computed from the catalog's manifests.
  - The boot mode is chosen in this order: the path's mode (`modeOfPath`), then the stored `loams.shell.mode`, then `defaultMode(platform.kind)`.
  - `set` persists the mode and navigates to the mode's home or `opts.navigate`.
- **Lazy loading.** Plugins of a mode that has not been entered are not applied at boot. The first `set` into that mode applies them; later switches dispose nothing.
- **The shell:**
  - the switcher in the header, a segmented control shown only when two modes are available, with `Mod+Shift+M`;
  - `navSections` filtered by mode (a plugin's `modes` and an entry's `meta.mode`);
  - a route of the other mode switches the mode first;
  - the palette's mode scope.
- The desktop deep-link allowlist gains `studio/t/<id>` and `studio/g/<id>`.
- No plugin declares `studio` yet. The switcher is therefore hidden, and nothing visible changes in this task, except under the test catalog.

**Tests:**
- `manifest_modes_default_console`, `manifest_modes_unknown_refused`, `schema_and_validator_agree` (the existing schema test extended).
- `boot_mode_from_path_then_storage_then_edition`, `storage_failure_falls_back_to_edition_default`.
- `other_mode_plugins_load_on_first_entry`, `switch_keeps_fibers` (a stream started in Studio is still open after two switches).
- `nav_filtered_by_mode`, `meta_mode_overrides_plugin_modes`.
- `switcher_hidden_with_one_mode`, `switcher_keyboard_shortcut`.
- `studio_route_switches_mode`, `console_routes_unchanged` (every existing route of the parity suite still resolves).
- `deeplink_allows_studio_thread_and_group`.

**Steps:** tests first; implement the manifest, then the service, then the shell; run the console-host, slots, shell and console tests, the console build and the desktop smoke; commit.

**Commit:** `web: the Console | Studio mode switcher and the manifest's modes field (D900–D902)`.

### Task 4: `loams.agentd.v1`

**Files:** `proto/loams/agentd/v1/*.proto`, `buf.yaml` (the `unstable` ignore), `web/packages/proto` (generation config and output), `crates/loams-agentd-connect/build.rs` or the workspace's buffa generation (as `loams-agentd-link` does), `docs/api/reasons.md` (the agentd reasons, marked daemon-only).

**Consumes:** §53 §6.1 and §6.4; T0 items 3 and 7; T3 Code's `packages/contracts` as a schema reference only (no text copied).

**Produces:**
- The services and messages of Shared contracts for every method in §53 §6.4.
  - Streams are server streams that carry a reset frame and then deltas, as the ndjson `Watch*` methods do.
  - `int64` millisecond timestamps, as on the ndjson wire.
  - Each internally tagged ndjson enum becomes a message with a `oneof`.
- The G1–G23 messages are declared here too, so later tasks only implement them.
- `option (loams.api.module) = { unstable: true }` (§44 rules), and no idempotency-key fields: the daemon is single-user and its mutations are checked by checksum or id.
- TypeScript generation into `@loams/proto` (`loams/agentd/v1`), with connect-query method descriptors.
- **Not in the SDK catalogue** (Q816): a test checks that the SDK generation's `CATALOGUE` doesn't list it.

**Tests:**
- `buf lint` (STANDARD).
- `agentd_v1_not_in_sdk_catalogue`.
- `every_studio_method_has_rpc`: a table test lists the §53 §6.4 and §6.5 methods and finds each in the descriptors.
- `ts_descriptors_generated` (a `tsc` compile of a file importing every service).

**Steps:** write the table test (it fails); write the protos group by group; generate; commit.

**Commit:** `proto: loams.agentd.v1, the daemon's Connect surface for Studio (D907)`.

### Task 5: The loopback Connect listener and the desktop transport

**Files:**
- `crates/loams-agentd-connect/{Cargo.toml,src/{lib.rs,listener.rs,roles.rs,map/*.rs,services/*.rs},tests/loopback.rs}`;
- `crates/loams-agentd/src/{server.rs,discovery.rs}`;
- `crates/loams-agentd/README.md` (licence table row: `loams-agentd-connect`, new, Apache-2.0);
- `apps/desktop-electron/src/main/protocol/{route.ts,proxy.ts,handler.electron.ts}`, `src/main/agentd/allowlist.ts`, tests.

**Consumes:** Task 4; T0 items 7 and 8; §50 §7 (the same rules for the new listener).

**Produces:**
- `loams-agentd-connect` serves every existing method of §53 §6.4 on `127.0.0.1:0`. It uses the same token check before any handler, the same `Host` rule, and refuses any `Origin` with a 403. It writes `connectPort` into `agentd.json`. Handlers map the proto messages to the engine's Rust types and call `loams-agentd-sessions` directly (Ruling 1).
- Each Connect method is marked with its ndjson twin's `renderer` flag. A generated allowlist (from Task 4's descriptors joined with `METHODS`) is written to `apps/desktop-electron/src/main/agentd/allowlist.gen.ts`.
- **Electron main's route.** `/loams.agentd.v1.` forwards to `http://127.0.0.1:<connectPort>`. It adds `Authorization: Bearer <token>` and drops `Origin` and `Cookie`. Methods not on the allowlist get 403 `{code:"forbidden_method"}`. When the daemon is absent, the route answers 503 `{code:"agentd_unavailable"}`. Response bodies stream through unbuffered.

**Tests:**
- Rust:
  - `connect_without_token_is_401`;
  - `connect_origin_is_403`;
  - `connect_foreign_host_is_403`;
  - `connect_unary_and_stream_work` (`ListHarnesses`, `WatchChats`);
  - `discovery_has_connect_port`.
- TypeScript:
  - `proxy_adds_token_and_drops_origin`;
  - `proxy_refuses_non_renderer_method`;
  - `proxy_503_when_daemon_absent`;
  - `renderer_never_sees_token` (no response header or body echoes the token; the preload API has no getter);
  - `stream_passes_10000_frames_under_bound` (a fake listener streams 10 000 terminal frames; p99 inter-frame delay under 50 ms locally (target)).

**Steps:** tests first; implement the listener and the services in the order of §53 §6.4; then the route; run `cargo test -p loams-agentd-connect -p loams-agentd`, the desktop tests and the smoke; commit.

**Commit:** `agentd: the loopback Connect listener and the desktop proxy route (D907, D908)`.

### Task 6: The TypeScript client, `rpc.agentd.*` and the parity suite

**Files:**
- `web/packages/agentd-client/src/{connect.ts,transport-desktop.ts,index.ts}` and tests;
- `web/plugins/rpc/src/agentd.ts` and tests;
- `web/packages/console-host/src/{services.ts,permissions.ts}`;
- `crates/loams-agentd-connect/tests/parity.rs`.

**Consumes:** Tasks 4, 5; DD1 Task 10's client if merged (T0 item 2).

**Produces:**
- `createAgentdTransport({ kind: 'desktop' })`: a Connect web transport whose base URL is `loams-app://console/`, so it goes through Task 5's route.
- `@loams/plugin-rpc` provides `agentd.transport` and one `rpc.agentd.<service>` per service. They are provided only when the platform says an agentd is reachable (desktop: daemon status; web: a selected host, Task 7). The host gives them only to `core` and `first-party` plugins that hold `agentd:use` (§53 D918).
- **Query clients.** `ServerStateProvider` gets a second `TransportProvider` scope for agentd queries, so a page writes `useQuery(ThreadService.method.listThreads, {...}, { transport: agentd })` through a helper `useAgentdQuery`. One `QueryClient` is shared.
- The `@loams/agentd-client` reducer accepts the Connect `TranscriptUpdate` as well as the ndjson one, through one mapping function.
- **The parity suite.** For each Studio method, a scenario runs against a real daemon with the Mock harness over ndjson and over Connect. The results are normalised (ids, timestamps) and compared.

**Tests:**
- `transport_desktop_uses_protocol_base`.
- `rpc_agentd_absent_without_daemon`.
- `rpc_agentd_not_provided_to_third_party` (later asserted again by Task 15).
- `reducer_accepts_connect_updates` (every ndjson transcript fixture, converted, reduces to the same state).
- `use_agentd_query_shares_query_client`.
- `connect_and_ndjson_agree`: one case per method of §53 §6.4. Later tasks add their methods.

**Steps:** tests first; implement; run the client, rpc and console-host tests and `cargo test -p loams-agentd-connect --test parity`; commit.

**Commit:** `web: the agentd Connect client and rpc.agentd services (D907)`.

### Task 7: The remote agentd and the web transport

**Files:**
- `crates/loams-agentd-connect/src/{remote.rs,oidc.rs,cors.rs}`, `tests/remote.rs`;
- `crates/loams-agentd/src/{cli.rs,config.rs,remote.rs}`;
- `web/packages/agentd-client/src/{transport-remote.ts,hosts.ts}`;
- `web/plugins/studio/src/hosts/*` (the host registry UI; the plugin skeleton is Task 15, so this task adds a minimal settings section under `console.settings.section` that Task 15 moves);
- `docs/guides/studio/remote-agentd.md` (a first draft).

**Consumes:** Task 5; §53 §6.3, §8.2; MT1's Authentik setup if merged (otherwise a test issuer); T0 item 6.

**Produces:**
- **`loams-agentd serve --remote …`** (Shared contracts) adds a TLS listener (rustls) that serves `loams.agentd.v1` with the role `studio`. It is off unless `[remote] enabled = true` or the flag is given.
- **Token checks:**
  - a JWT verified against the issuer's JWKS (cached, refreshed on an unknown `kid` at most once a minute);
  - `aud` contains `loams-agentd`;
  - `iss` matches;
  - `exp` and `nbf` hold, with 60 s of leeway;
  - `sub` equals the configured subject.
- **The `studio` role** is the Studio method set: every Connect method of §53 §6.4 and §6.5 except the harness installs and account logins (Q808), `Shutdown` and any secret method.
- **CORS:**
  - The listener answers preflight only for the configured origins.
  - `Access-Control-Allow-Headers` is limited to the Connect headers plus `Authorization`.
  - It never sends `Access-Control-Allow-Credentials`.
  - Any other origin gets no CORS headers, and its non-preflight request gets a 403 `origin_not_allowed`.
- **The web side:**
  - `createAgentdTransport({ kind: 'remote', baseUrl, getToken })`. `getToken` comes from the identity plugin (an audience-specific token, MT1); in development it comes from a pasted token, with a warning banner.
  - The host registry: name, URL, an optional certificate fingerprint note; stored in the Loams server's user settings when served, else in `loams.studio.hosts`.
  - The `?host=` parameter selects a host by id.
- **The guide's draft:** a container recipe, `serve --remote` behind a tailnet (§43), the Authentik application settings, the supervised-mode default.

**Tests:**
- Rust:
  - `remote_off_by_default`;
  - `remote_missing_flag_exits_4`;
  - `remote_wrong_audience_is_401`, `remote_expired_is_401`;
  - `remote_wrong_subject_is_403`;
  - `remote_disallowed_origin_gets_no_cors`, `remote_allowed_origin_preflight_ok`;
  - `remote_studio_role_cannot_shutdown`, `remote_studio_role_cannot_install_harness`;
  - `remote_never_serves_ndjson`;
  - `jwks_refresh_rate_limited`.
- TypeScript:
  - `transport_remote_sends_bearer`;
  - `host_param_is_id_not_url` (`?host=https://evil` is ignored);
  - `hosts_storage_failure_tolerated`.

**Steps:** tests first, with a test issuer (a local JWKS file through `LOAMS_AGENTD_TEST_ISSUER`); implement; run `cargo test -p loams-agentd-connect -p loams-agentd` and the web tests; commit.

**Commit:** `agentd: serve --remote, a TLS Connect listener with OIDC for the web edition (D909, D921)`.

### Task 8: The Mock harness edits files

**Files:** `crates/loams-agentd-harness/src/mock.rs`, `crates/loams-agentd-sessions/src/registry.rs` (the default script), tests; `web/apps/console/test/e2e/fixtures/agentd.ts` (the Playwright fixture).

**Consumes:** the existing Mock harness and `crates/loams-agentd/tests/support/mod.rs`.

**Produces:**
- `LOAMS_DESKTOP_MOCK_EDIT=<path>[,<path>…]` makes the Mock harness write to each path relative to the run's cwd, creating or appending a line `mock edit <turn n>`. It emits matching `editFile` tool calls and results.
- `LOAMS_DESKTOP_MOCK_APPROVE=1` makes it request an approval before each edit (used by Task 13).
- **The Playwright fixture** `agentd`:
  1. starts `loams-agentd run --child --config <tmp>/config.toml` with `HOME=<tmp>` and `LOAMS_DESKTOP_HARNESS=mock`;
  2. waits for `agentd.json`;
  3. exposes `{port, connectPort, token}` to Node-side helpers only;
  4. creates a fixture git repository with two commits.

**Tests:** `mock_edit_writes_files_and_emits_tool_calls`, `mock_edit_produces_checkout_diff` (through `GetCheckoutDiff`), `mock_approve_requests_before_edit`, and a fixture self-test in Playwright (`agentd_fixture_starts_and_stops`).

**Steps:** tests first; implement; run `cargo test -p loams-agentd-harness -p loams-agentd-sessions` and the fixture test; commit.

**Commit:** `agentd: the Mock harness edits files and asks for approvals, for Studio tests`.

---

## SD1b — The daemon's gaps

Each task in this milestone:
- adds its methods to the ndjson table (`rpc.rs`, with role and `renderer`), to the ts-rs types and fixtures, and to `loams-agentd-connect`;
- adds its cases to Task 6's parity suite;
- lists its feature in `Hello.features` and `GetHost.features`.

Tests run against temporary git repositories (the `repos.rs` test pattern).

### Task 9: Worktrees and git writes (G1–G5, G21)

**Files:** `crates/loams-agentd-sessions/src/{worktree_list.rs,git_write.rs,change_requests.rs}`, `src/rpc.rs`; `crates/loams-agentd-proto/src/git_write.rs`; tests `tests/git_write.rs`; the Connect services `GitService` and `DiffService`.

**Consumes:** §53 §6.5 G1–G5, G21; the factory credentials in the keyring (DD1 Task 22; until then, a `FORGEJO_TOKEN` read from the keyring service `dev.loams.agentd` entry `factory/forgejo`, as T0 decides).

**Produces:**
- `ListWorktrees`: parses `git worktree list --porcelain`, joined with chats (`checkoutId`, `chatIds`), dirty state, and ahead and behind against the upstream.
- `CommitWorktree`:
  - stages `paths` or `all`;
  - refuses on a stale checksum (`stale_diff`) or an empty index (`nothing_to_commit`);
  - adds `Signed-off-by` when `signoff`;
  - uses the user's git identity, and never sets one.
- `PushBranch`: `git push [--set-upstream] [--force-with-lease=<ref>:<expected>]`. A plain `--force` is never constructed; the code has no path to it.
- `CreateChangeRequest`:
  - `forgejo` calls `POST /api/v1/repos/{owner}/{repo}/pulls` with the credential;
  - `github` runs `gh pr create`;
  - the owner and repository are parsed from the remote URL with T3 Code's `changeRequestUrl` rules, reimplemented in Rust.
- `MergeBranch`: in the target's worktree (or a temporary one when the target isn't checked out), runs `merge`, `squash`, `rebase` or `ff`.
  - On conflict it aborts (`merge --abort` or `rebase --abort`) and returns `merge_conflict` with the paths.
  - Optionally it deletes the source branch and removes the source worktree.
- `DiscardPaths`: restores the paths, refusing on a stale checksum.
- None of these is in the `mcp` role.

**Tests:**
- `list_worktrees_joins_chats`.
- `commit_stages_and_signs_off`, `commit_refuses_stale_checksum`, `commit_nothing_to_commit`, `commit_uses_user_identity`.
- `push_with_lease_rejected_when_remote_moved`, `push_never_plain_force` (a source scan plus a behaviour test).
- `forgejo_change_request_request_shape` (against a `wiremock` Forgejo), `github_change_request_uses_gh` (a fake `gh` on `PATH`), `change_request_without_credential_is_provider_not_configured`.
- `merge_each_strategy`, `merge_conflict_leaves_clean_tree`.
- `discard_paths_restores_only_those`.
- `git_writes_not_in_mcp_role`.

**Steps:** tests first; implement each method; wire both wires; run `cargo test -p loams-agentd-sessions -p loams-agentd-connect` and the parity suite; commit.

**Commit:** `agentd: list worktrees, commit, push, change requests and merge (G1–G5, G21)`.

### Task 10: Checkpoints and restore (G6, G7)

**Files:** `crates/loams-agentd-sessions/src/{checkpoints.rs,diff_sync.rs,sessions.rs}`, tests `tests/checkpoints.rs`; proto and Connect.

**Consumes:** Ruling 6; `diff_sync`'s turn snapshot.

**Produces:**
- At each turn's start and end, `refs/loams/checkpoints/<chatId>/turn/<n>/{pre,post}`. The commit is written through a temporary index in the git common directory (`GIT_INDEX_FILE`, `add -A`, `write-tree`, `commit-tree -p HEAD`, `update-ref`). Untracked files that are not ignored are included; ignored files are not.
- `ListCheckpoints` lists them per turn, with the files changed.
- `RestoreCheckpoint`:
  - `worktree` restores the tree to the `pre` commit (`git restore --source <c> --worktree --staged -- .` plus removing files absent from it), refusing on a stale checksum;
  - `worktree+transcript` also forks the thread at that turn (`ForkSideChat` at the turn) and never deletes history.
- When a thread is deleted, its refs are deleted.

**Tests:** `checkpoint_written_per_turn`, `checkpoint_does_not_touch_index_or_head`, `checkpoint_includes_untracked_not_ignored`, `restore_worktree_matches_pre`, `restore_refuses_stale_checksum`, `restore_with_transcript_forks_not_deletes`, `refs_deleted_with_thread`.

**Steps:** tests first; implement; run the crate tests and parity; commit.

**Commit:** `agentd: per-turn checkpoints and restore (G6, G7)`.

### Task 11: Content search and review comments (G8–G11, G23)

**Files:** `crates/loams-agentd-sessions/src/{content_search.rs,review.rs}`, `crates/loams-agentd-store/src/store.rs` (table `review_comments`), tests; proto and Connect.

**Consumes:** §53 §5.6, §6.5.

**Produces:**
- `SearchWorkspaceContent`: a streamed search with the `ignore` walker, honouring `.gitignore`. It supports regex or literal and case options. Caps: 2 000 results, 1 MiB per file, 10 s (target).
- Review comments are stored per checkout with the anchor of Shared contracts. `WatchReviewComments` streams a reset and then changes. A comment is marked `outdated` (computed, not stored) when the anchor's line no longer exists at the current diff checksum.
- `SendReviewToThread` queues one run whose prompt is a fixed template:

  ```
  Please address these review comments:
  - <path>:<line> — "<body>"
  ```

  The comment bodies are quoted as data.
- `PostReviewToChangeRequest` posts to Forgejo (`POST /repos/{o}/{r}/pulls/{n}/reviews` with inline comments) or GitHub (`gh api`).

**Tests:** `content_search_respects_gitignore`, `content_search_caps_results`, `review_comment_round_trip`, `review_comment_outdated_after_change`, `send_review_template_exact`, `post_review_forgejo_shape`, `post_review_github_uses_gh`.

**Steps:** tests first; implement; run the tests and parity; commit.

**Commit:** `agentd: content search and review comments (G8–G11, G23)`.

### Task 12: Run groups (G12–G16)

**Files:** `crates/loams-agentd-sessions/src/groups.rs`, `crates/loams-agentd-store/src/store.rs` (table `run_groups`), `crates/loams-agentd/src/config.rs` (`[groups] max_running_per_group = 4`, `max_running = 8`), tests `tests/groups.rs`; proto and Connect.

**Consumes:** Tasks 9 and 8; Ruling 5; §53 §5.5.

**Produces:**
- `CreateRunGroup` creates N chats with `parentChatId` unset and `groupId` set. Each gets its own worktree from `base` (`RunRequest.worktree`), and the same run is queued in each, subject to the limits; members over the limit wait in `queued`.
- `WatchRunGroups` streams group records, with members' status, diff stats (from `GetCheckoutDiff`), elapsed time, tokens (when the harness reports usage) and check state.
- `GetRunGroupComparison`:
  - the matrix is per path and per member (`+/−`);
  - `pairDiff` is `git diff <a-worktree-HEAD+worktree> <b…>`, computed by snapshotting each worktree to a temporary tree object, so nothing is committed;
  - `path` limits the output to one file.
- `RunGroupCheck` runs the group's check action in each member's worktree as a PTY (`RunProjectAction`) and records the exit code and the last 200 lines.
- `ResolveRunGroup`:
  - `merge` calls Task 9's `MergeBranch`; `pr` calls `PushBranch` and `CreateChangeRequest`; `keep` detaches the winner from the group.
  - It archives the others, and removes their worktrees only when `removeOtherWorktrees` is set.
  - It refuses while members are running (`group_not_ready`) unless `force`.
- A group survives a daemon restart: running members resume as their harness allows (§50 §11.5), and the group record reloads.

**Tests:** `group_creates_n_worktrees_from_base`, `group_respects_concurrency_limits`, `group_stats_follow_member_diffs`, `comparison_matrix_and_pair_diff`, `group_check_records_exit_codes`, `resolve_merge_archives_others`, `resolve_pr_pushes_and_opens`, `resolve_refuses_while_running`, `group_survives_restart`.

**Steps:** tests first with the Mock harness's edit knob (each member edits different files); implement; run the tests and parity; commit.

**Commit:** `agentd: parallel run groups with compare and resolve (G12–G16, D904)`.

### Task 13: Supervised approvals (G17, G18, G22)

**Files:** `crates/loams-agentd-harness/src/{claude,codex,opencode,acp}/approvals.rs` and the drivers, `crates/loams-agentd-sessions/src/{approvals.rs,sessions.rs}`, `crates/loams-agentd-proto/src/approvals.rs`, tests; proto and Connect; `web/plugins/approvals/src/**` (an `agent` source).

**Consumes:** DD1 Task 24's `ApprovalRequested`, `ApprovalResolved` and `ResolveApproval` and its durable approval promises (T0 item 2; if Task 24 hasn't merged, this task lands the contract part first, agreed with DD1's owner); §53 §8.1; Q803.

**Produces:**
- A per-thread `approvalMode` (`full-access` | `supervised`), set with `SetThreadApprovalMode` or a `setChatConfig` field (T0 decides).
  - The default is `supervised` on the remote listener.
  - On the desktop it is Settings › Studio's choice, pre-selected `supervised` (Q803).
- In `supervised` mode:
  - **Claude Code:** `can_use_tool` creates an approval and waits.
  - **Codex:** `approval_policy = "on-request"`, with the sandbox at the thread's level; forcing `danger-full-access` is removed for supervised threads.
  - **opencode:** its permission event becomes an approval.
  - **ACP:** `session/request_permission` becomes an approval, and the answer maps to the agent's offered options.
- `WatchApprovals` lists pending approvals with redacted arguments (the harness redaction rules).
- `ResolveApproval` accepts `owner` and `studio`, and never `mcp`.
- An "always" decision is remembered per thread and tool.
- `@loams/plugin-approvals` lists agent approvals beside the server's, labelled by source, when an agentd transport exists.

**Tests:**
- `claude_supervised_waits_for_resolve`, `codex_supervised_on_request`, `opencode_supervised_forwards_permission`, `acp_supervised_forwards_request_permission`: each with the fake CLIs in `harness/tests/fixtures`.
- `full_access_unchanged` (today's behaviour holds in that mode).
- `mcp_role_cannot_resolve`.
- `always_remembered_per_thread_and_tool`.
- `pending_approval_listed_after_restart`.
- `approvals_page_lists_agent_approvals`.

**Steps:** tests first, driver by driver; implement; run `cargo test -p loams-agentd-harness -p loams-agentd-sessions`, parity and the approvals plugin tests; commit.

**Commit:** `agentd: supervised approvals in every harness driver (G17, G18, G22, D911)`.

### Task 14: Notifications and usage (G19, G20)

**Files:** `crates/loams-agentd-sessions/src/{notify.rs,usage.rs}`, `crates/loams-agentd-store/src/store.rs` (table `turn_usage`), `crates/loams-agentd/src/notify.rs` (the OS notifications of §50 §5.5, if DD1 has not built them, reading this stream), tests; proto and Connect.

**Consumes:** §53 §6.5.

**Produces:**
- `WatchNotifications` emits `turn_done`, `approval`, `question`, `error`, `group_ready` and `check_failed`, with thread or group ids and a title. It keeps the last 200 in memory.
- `usage` events are persisted per turn: input, output and cache tokens, and duration. `costUsd` is filled only when the harness itself reports a cost; there is no price table, consistent with the no-metering rule. `GetThreadUsage` returns them.
- `scripts/ci/no-metering.sh` passes: the module is named `usage.rs` but holds no `meter`, `invoice`, `price` or `plan` identifiers (T0 checks the script's word list; rename if it flags `usage`).

**Tests:** `notifications_for_each_kind`, `notification_buffer_bounded`, `usage_persisted_per_turn`, `usage_cost_only_when_reported`, `no_metering_guard_passes`.

**Steps:** tests first; implement; run the tests, parity and the no-metering guard; commit.

**Commit:** `agentd: notifications and per-thread usage (G19, G20)`.

---

## SD1c — Studio's UI

Every component task:
- imports its upstream files with `scripts/studio/vendor.mjs import` (verdicts and targets from the inventory), then ports them in a second commit, so the review sees the upstream text and the port apart;
- writes containers in `@loams/plugin-studio` with `useAgentdQuery` and mutations;
- runs the colour scan, `no_effect_imports`, `vendored_headers_present`, its own tests, the console build and the desktop smoke.

### Task 15: The Studio plugin, its layout and projects

**Files:** `web/plugins/studio/{package.json,src/{index.tsx,layout/StudioLayout.tsx,home/*,projects/*,settings/*},test/**}`, `web/apps/console/catalog/{base,web,desktop}.yml`, `web/apps/console/src/cordis/{desktop.ts,modules.ts}`, `web/packages/console-host/src/guard.ts` (the agentd injection rule).

**Consumes:** Tasks 3, 6, 7; inventory (a) `AppSidebarLayout`; §53 §5.1–§5.2.

**Produces:**
- `@loams/plugin-studio`: `modes: ["studio"]`, `editions: ["oss", "desktop", "cloud"]`, `inject: ["router", "slots", "mode", "platform", "rpc.agentd.studio", "rpc.agentd.projects", …]`, `permissions: ["agentd:use", "agentd:git-write"]`, tier `first-party`.
- The layout of §53 §5.1: a resizable sidebar, the main area, the tabbed right panel and the bottom drawer. Its sizes are stored per device. It works at 360 px wide with overlays.
- **Routes** of Shared contracts, with empty states:
  - no agentd: "Start the daemon" on the desktop; "Connect an agentd host" on the web, linking to Task 7's registry, which moves here as `settings/Hosts.tsx`;
  - no projects;
  - no threads.
- **Projects:** the list (`WatchSpaces`); Add (desktop: the folder picker through `platform` and `AddRepo`; web: `CloneRepo` with a URL); rename; remove.
- **The header's Studio context** (project, branch, host), shown when the mode is `studio`.
- Settings › Studio: the default harness and model, the approval mode, parallel-run defaults, and notifications.
- **Catalogs.** `@loams/plugin-studio` is in `desktop.yml` and `web.yml`. The browser edition gates on `flags.features.studio` until Task 32.

**Tests:** `studio_plugin_manifest_valid`, `third_party_cannot_inject_agentd`, `empty_state_without_agentd_desktop`, `empty_state_without_host_web`, `add_project_desktop_uses_folder_picker`, `add_project_web_clones`, `layout_sizes_persist_and_tolerate_storage_failure`, `layout_narrow_uses_overlays`, `studio_hidden_on_web_without_flag`.

**Steps:** tests first; implement; run the plugin tests, the console build (both editions) and the desktop smoke; commit.

**Commit:** `web: @loams/plugin-studio, its layout and projects`.

### Task 16: `@loams/ui` primitives from T3 Code

**Files:** `web/packages/ui/src/{overlay.tsx,command.tsx,sidebar.tsx,kbd.tsx,toggle-group.tsx,input-group.tsx,middle-truncate.tsx,…}` (only those `@loams/ui` lacks after `console/ui-stack`), tests; `web/packages/ui/NOTICE` and provenance (the `@loams/ui` files that carry upstream code also get provenance rows; the tool takes `--package web/packages/ui`).

**Consumes:** inventory (k); console-ui-stack rulings 1–2; T0 item 1.

**Produces:** every primitive that the inventory's later groups need and `@loams/ui` lacks, ported to tokens, `rounded-none`/`rounded-pill` and `cx`, with Base UI underneath. None of them is a second copy of an existing primitive. `command.tsx` is the palette primitive (Base UI Dialog and Autocomplete with `VirtualList`).

**Tests:** for each primitive: `renders_with_tokens_only` (the colour scan), keyboard and ARIA behaviour (`command_arrow_keys_and_enter`, `sidebar_resize_keyboard`, `menu_typeahead`), and `no_direct_base_ui_import_outside_ui`.

**Steps:** list the needed primitives from the inventory against `@loams/ui`'s exports; import; port; test; commit.

**Commit:** `ui: primitives adapted from T3 Code (command, sidebar, kbd and others)`.

### Task 17: The thread list and sidebar

**Files:** `web/packages/studio-ui/src/thread-list/**`, `web/plugins/studio/src/sidebar/**`, tests; `pnpm-workspace.yaml` (`@dnd-kit/*`).

**Consumes:** inventory (a); `WatchChats`, `WatchSessions`, `WatchSpaces`, `WatchRunGroups` (Task 12, if merged; the group rows appear when the feature exists), `WatchSidebarPreferences`, `Mutate`, `ForkSideChat`, `ListWorktrees`, `DeleteWorktree`.

**Produces:**
- The sidebar of §53 §5.3:
  - projects with their threads and run groups;
  - status badges (running, awaiting approval, unread, errored, group member);
  - pin, reorder (dnd-kit) and search;
  - the context menu: rename, fork, archive, delete with the worktree prompt (`worktreeCleanup`), and copy path;
  - "Clean up worktrees".
- Keyboard navigation (↑/↓, Enter, `Mod+N` for a new thread).

**Tests:** `sidebar_groups_by_project`, `status_badges_follow_sessions`, `reorder_persists_pin_order`, `delete_offers_worktree_cleanup_only_when_unshared`, `search_filters_threads`, `keyboard_navigation`, `logic_matches_upstream_cases` (the upstream unit cases of `Sidebar.logic`, ported).

**Steps:** import (as-is then adapt) per the inventory; write the containers; tests; commit.

**Commit:** `studio: the thread list and sidebar, adapted from T3 Code`.

### Task 18: The chat view, markdown and inline approvals

**Files:** `web/packages/studio-ui/src/{chat,markdown,approvals}/**`, `web/plugins/studio/src/thread/ThreadPage.tsx`, tests; `web/plugins/agent/src/**` (the dock renders the same timeline in compact form, D917).

**Consumes:** inventory (b), (i), (j); `WatchDocMessages` through the shared reducer; `QueueCommand` (`interrupt`, `respondInput`), `ResolveApproval`, `WatchApprovals`, `ListCheckpoints`, `RestoreCheckpoint`, `GetCheckoutDiff mode:"turn"`; the Q802 split (T0).

**Produces:**
- **The virtualised timeline** (`VirtualList`). Rows: user and assistant messages, reasoning (collapsed), tool and command rows from the work-log presentation rules, per-turn changed files with "view diff" and "restore to before this turn", subagents, errors, and plan cards with the tasks badge.
- **Scrolling and find.** Stick-to-bottom with anchoring; find in thread.
- **Markdown** through the adapted pipeline without `rehype-raw`: code blocks highlighted with Shiki into React token spans, plus a copy button; file links that open the file viewer; http(s) links opened externally; no images except local blobs.
- **Inline cards.** Approval cards (Once, Always for this thread, Deny; `Enter`, `Shift+Enter`, `Esc`) and question cards.
- **The thread header:** title, harness and model, the effective approval mode and sandbox, the branch, and usage (G20).
- **The dock** (`@loams/plugin-agent`, in the daemon runtime) uses the same store and the compact variant.

**Tests:**
- `timeline_renders_every_part_fixture` (all `TranscriptUpdate` fixtures).
- `delta_rerenders_only_its_row`.
- `stick_to_bottom_and_anchor_on_prepend`.
- `markdown_no_raw_html`, `markdown_no_remote_images`, `markdown_links_http_only`, `code_block_renders_text_not_html`.
- `approval_card_shortcuts`, `approval_always_scoped_to_thread`, `question_card_answers`.
- `restore_turn_requires_confirmation`.
- `dock_and_page_share_store`.

**Steps:** import and port per the inventory; containers; tests; commit.

**Commit:** `studio: the chat timeline, markdown and inline approvals, adapted from T3 Code`.

### Task 19: The composer

**Files:** `web/packages/studio-ui/src/composer/**`, `web/plugins/studio/src/thread/{Composer.tsx,uploads.ts}`, tests; `pnpm-workspace.yaml` (`@tiptap/*`).

**Consumes:** inventory (c); `QueueCommand` (`run`, `steer`), `QueueMessage` and the queue methods, `WatchQueue`, `ListHarnesses`, `ListModels`, `ListSkills`, `ListCommands`, `SearchWorkspaceFiles`, `UploadChunk`, `UploadCommit`; Task 12's `CreateRunGroup` (the "Run in parallel" entry appears when the feature exists).

**Produces:**
- **The Tiptap editor** with file `@` mentions, `/` commands and skills, inline chips, paste handling, prompt history and stash.
- **Attachments** (PNG, JPEG, GIF, WebP; HEIC refused with a message) through the chunked upload.
- **Pickers.** The harness and model picker, plus reasoning and options.
- **Actions.** Send, queue while busy, steer, stop; the queued-messages editor; the context window meter.
- **Worktree choice.** "New worktree" (default) or "Local" for the first run; the base branch picker.
- **"Run in parallel…"**: the run-group dialog (variants, base, check, approval mode).
- Drafts are per thread, persisted by zod-validated storage, and survive a mode switch.

**Tests:** `mention_inserts_file_chip`, `slash_command_menu_filters`, `send_queues_when_busy`, `steer_while_running`, `stop_interrupts`, `heic_refused_with_message`, `upload_chunks_then_commits`, `draft_survives_mode_switch`, `draft_storage_failure_tolerated`, `parallel_dialog_builds_create_run_group`, `doc_model_round_trip` (the upstream `composer-rich-text-doc` cases, ported).

**Steps:** import and port; containers; tests; commit.

**Commit:** `studio: the composer, adapted from T3 Code's Tiptap editor`.

### Task 20: The diff panel and review comments

**Files:** `web/packages/studio-ui/src/diff/**`, `web/plugins/studio/src/thread/DiffTab.tsx`, tests; `pnpm-workspace.yaml` (`@pierre/diffs`, `@pierre/trees`).

**Consumes:** inventory (d); T0 item 4; `WatchCheckoutDiffs`, `GetCheckoutDiff` (`""`, `branch`, `commit`, `turn`), `GetCheckoutFileDiffText`, `DiscardWorkingTree`, `DiscardPaths`, `WatchReviewComments`, `UpsertReviewComment`, `DeleteReviewComment`, `ResolveReviewComment`, `SendReviewToThread`, `PostReviewToChangeRequest`, `WatchCheckoutChangeRequest`.

**Produces:**
- **The diff panel.** Sources: working tree against the base, branch, commit and turn. Unified or split view; the file tree; viewed marks; collapse; whitespace and word diff. Theming from tokens (`diffs.css`).
- **Inline comments.** Add, edit, resolve and delete; "outdated" badges.
- **Review actions.** "Send N comments to the agent"; "Post to PR" when a PR exists.
- **Discard.** Discard all or one file, with confirmation and the expected checksum.
- **No find-in-diff** in this task: T3 Code's patched `@pierre/diffs` API is not used (§53 §9.1). A follow-up decides between upstreaming it and a recorded local patch.

**Tests:** `diff_renders_text_not_html`, `diff_sources_switch`, `split_and_unified`, `comment_anchor_round_trip`, `outdated_comment_badge`, `send_review_to_thread_calls_rpc`, `post_review_only_with_change_request`, `discard_requires_confirmation_and_checksum`, `diff_theme_tokens_only`.

**Steps:** import and port; containers; tests; commit.

**Commit:** `studio: the diff panel with review comments, adapted from T3 Code (D905)`.

### Task 21: The file tree and viewer

**Files:** `web/packages/studio-ui/src/files/**`, `web/plugins/studio/src/thread/FilesTab.tsx`, tests.

**Consumes:** inventory (e); `ListWorkspaceDirectory`, `WatchWorkspaceFiles`, `ReadWorkspaceFile`, `ReadWorkspaceImage`, `SearchWorkspaceFiles`, `SearchWorkspaceContent` (Task 11).

**Produces:**
- **The tree.** `@pierre/trees`, or the `VirtualTree` fallback if T0 or review rejects the beta.
- **The viewer**, read-only: code highlighted as tokens, rendered markdown under the Task 18 rules, CSV and TSV tables, local images as blobs; breadcrumbs.
- **Quick open** and the content search dialog.
- "Mention in composer" from the tree.

**Tests:** `tree_lists_and_expands`, `tree_updates_on_watch`, `file_viewer_renders_text`, `markdown_preview_safe`, `image_preview_local_only`, `quick_open_ranks`, `content_search_streams_results`, `mention_from_tree_adds_chip`.

**Steps:** import and port; containers; tests; commit.

**Commit:** `studio: the file tree and viewer, adapted from T3 Code`.

### Task 22: Terminals

**Files:** `web/packages/studio-ui/src/terminal/**`, `web/plugins/studio/src/thread/TerminalDrawer.tsx`, tests; `web/plugins/agent` (it uses the shared component).

**Consumes:** inventory (f); Ruling 8; DD1's xterm pin; `OpenTerminal`, `SubscribeTerminal`, `WriteTerminal`, `ResizeTerminal`, `CloseTerminal`, `RunProjectAction`, `ListProjectActions`.

**Produces:**
- **The drawer.** Tabs and split per thread on xterm.js with the fit addon and a token theme; reattach with replay (`afterSeq`).
- **Input safety.** Multi-line paste needs confirmation; closing a terminal with a running process needs confirmation.
- **Links and context.** Link detection (http(s) links open externally; file paths open in the viewer); "send selection to composer".
- **Project actions** as named terminals.

**Tests:** `terminal_writes_stream_and_sends_input`, `terminal_reattach_replays`, `terminal_disposed_on_unmount`, `multiline_paste_confirms`, `close_running_confirms`, `file_link_opens_viewer`, `selection_to_composer`.

**Steps:** import and port; containers; tests; commit.

**Commit:** `studio: terminals on xterm.js, with T3 Code's drawer and links (D920)`.

### Task 23: The branch and git toolbar

**Files:** `web/packages/studio-ui/src/git/**`, `web/plugins/studio/src/git/**`, tests.

**Consumes:** inventory (g); Task 9; `ListBranches`, `ListRefs`, `ListWorktrees`, `SwitchRef`, `FetchAll`, `WatchWorkspaceGitStatus`, `WatchCheckoutChangeRequest`, `ListProjectActions`, `UpsertProjectAction`, `TakeProjectActionSetup`.

**Produces:**
- **The toolbar.** The branch and worktree with pickers; Local or New worktree; reuse of a previous worktree; the worktree setup card.
- **The git actions control.** Commit, with a message and a sign-off toggle on by default; push; open PR (Forgejo or GitHub, draft toggle, linked issue); merge into the base (strategy choice, conflict report); fetch.
- **Status.** Ahead and behind, and the PR status and checks.
- **Project actions** buttons and an editor.

**Tests:** `commit_flow_calls_commit_with_checksum`, `push_uses_lease`, `open_pr_requires_provider`, `merge_conflict_shown_with_paths`, `local_vs_worktree_choice`, `setup_card_shows_progress`, `project_action_editor_round_trip`, `git_actions_logic_matches_upstream_cases`.

**Steps:** import and port; containers; tests; commit.

**Commit:** `studio: the branch and git toolbar with commit, push, PR and merge, adapted from T3 Code`.

### Task 24: The command palette and keybindings

**Files:** `web/packages/studio-ui/src/{palette,keybindings}/**`, `web/plugins/studio/src/{palette,settings/Keybindings.tsx}`, `web/plugins/shell/src/palette.tsx` (the shared palette uses `@loams/ui` `command`), tests.

**Consumes:** inventory (h); Task 3's palette scope; Task 16's `command`.

**Produces:**
- **The palette** (`Mod+K`, shared by both modes). Studio contributes threads, projects, run groups, files (quick open), git actions, project actions, harness and model switches, and "Switch to Console". Console's commands appear under their mode heading.
- **Keybindings.** The model with `when` scopes; defaults; an editor in Settings › Studio; stored per device in `loams.studio.keybindings`.

**Tests:** `palette_scopes_by_mode`, `palette_ranks_like_upstream`, `palette_runs_git_action`, `keybinding_when_scopes`, `keybinding_conflict_detected`, `keybindings_storage_failure_tolerated`.

**Steps:** import and port; containers; tests; commit.

**Commit:** `studio: the command palette and keybindings, adapted from T3 Code`.

### Task 25: Parallel runs: the group view, compare and resolve

**Files:** `web/packages/studio-ui/src/groups/**`, `web/plugins/studio/src/groups/**`, tests.

**Consumes:** Tasks 12, 19, 20; inventory (l); §53 §5.5; Q811.

**Produces:**
- **`/studio/g/:id`**: the grid of members (status, last message, files, `+/−`, elapsed time, tokens, check).
- **Compare.** The diff matrix; the side-by-side pair diff per file, using the Task 20 components; check results with log tails.
- **Act.** "Run check on all"; "Continue as thread"; "Resolve…" (merge, PR or keep; archive the others; remove their worktrees, with confirmation).
- **Approvals.** "Approve for all variants" when the same tool and arguments are pending in several members.
- **The judge.** "Start judge thread" creates an ordinary read-only thread with the comparison in its prompt (Q811 default).
- The sidebar shows the group with its members nested.

**Tests:** `group_grid_follows_watch`, `matrix_marks_touched_files`, `pair_diff_side_by_side`, `resolve_merge_flow`, `resolve_pr_flow`, `resolve_refused_while_running_shows_reason`, `approve_for_all_variants_resolves_each`, `judge_thread_is_read_only`.

**Steps:** tests first; build the views; commit.

**Commit:** `studio: parallel run groups with compare and merge (D904)`.

### Task 26: The preview

**Files:** `web/packages/studio-ui/src/preview/**`, `web/plugins/studio/src/preview/**`, tests; `apps/desktop-electron/src/main/preview/{view.electron.ts,policy.ts}` (a `WebContentsView` placed by main, as D678), `crates/loams-agentd-connect/src/remote.rs` (`/preview/`, if Q806 is accepted).

**Consumes:** inventory (n); `WatchPreviews`, `RunProjectAction`; D678; Q806.

**Produces:**
- **The Preview tab.** Discovered servers with their URLs; start the dev action; a URL bar; empty and unreachable states.
- **Desktop.** A `WebContentsView` with partition `persist:studio-preview`, no preload, sandboxed, and the D678 frame, permission and download policy; "Open in browser".
- **Web.** With Q806 accepted, the remote listener proxies `/preview/<name>.<project>/…` to the daemon's preview proxy behind the same token check, with `X-Frame-Options: SAMEORIGIN`-equivalent CSP `frame-ancestors` limited to the allowed origins. Without it, the tab shows the port-forward command.

**Tests:** `preview_lists_discovered_servers`, `desktop_view_has_no_preload_and_own_partition`, `remote_preview_requires_token`, `remote_preview_frame_ancestors_restricted`, `web_without_preview_shows_forward_hint`.

**Steps:** tests first; implement; run the plugin tests, the desktop tests and the Rust tests; commit.

**Commit:** `studio: the preview panel on the desktop and through the remote listener`.

---

## SD1d — The factory and the desktop

### Task 27: The factory apps under Studio

**Files:** `web/plugins/factory/{package.json,src/index.tsx,src/*}` and tests, `web/plugins/studio/src/{sidebar/Factory.tsx,thread/FactoryLinks.tsx}`, `web/apps/console/catalog/*.yml`, `web/plugins/shell/src/layout.tsx` (the `Integrate` section loses the factory entry in Console).

**Consumes:** §53 §5.9, D916, D922; Q813, Q814; SF1 Tasks 1 and 3 for the web half.

**Produces:**
- **The manifest.** `@loams/plugin-factory` declares `modes: ["studio"]`, plus a Console overview card through `meta.mode: "console"` (the plugin lists both modes; its nav entry is Studio's).
- **Paths.** Its pages move to `/studio/factory/…`; `/factory/…` redirects for one release.
- **Studio's sidebar** gains the Factory group: Forgejo, Plane, Zulip, GlitchTip, OpenPanel, Langfuse, OpenObserve and Matomo (Q813), each with its configured state.
- **In-context links:**
  - "Start thread from issue" (Plane): the composer gets the issue's title, body and link, and the branch name gets the issue key.
  - "Fix this error" (GlitchTip): the stack trace and release go into the composer.
  - The thread's toolbar shows the Forgejo PR of its branch.
  - "Post result to Zulip" is a factory write, with an approval per §39 §8.
- **The web half.** When `@loams/plugin-embed` and `rpc.collab` exist (SF1), the browser's Factory group renders SF1's panels and embeds. Until then it shows the configured app links only (D922).

**Tests:** `factory_in_studio_mode_only`, `factory_overview_card_in_console`, `factory_redirects_old_paths`, `start_thread_from_plane_issue`, `fix_error_from_glitchtip`, `thread_toolbar_shows_forgejo_pr`, `web_factory_links_only_without_sf1`.

**Steps:** tests first; implement; run the factory and studio plugin tests, the console build and the desktop smoke (embedded views still work under the new paths); commit.

**Commit:** `web: the software factory apps move under Studio (D916)`.

### Task 28: Desktop integration

**Files:** `apps/desktop-electron/src/main/{deeplink.ts,shell/menu-model.ts,shell/tray-model.ts,settings.ts}`, `src/shared/{contracts.ts,deeplink.ts}`, `web/apps/console/src/cordis/desktop.ts`, `web/plugins/data-studio/**` (the rename, Q812), tests.

**Consumes:** Tasks 3, 15–27; §53 §4.3–§4.4, §7; Q801, Q810, Q812.

**Produces:**
- **Mode.** The desktop's default mode is Studio (Q801).
- **Menu.** "Studio" and "Console" entries switch modes (`Mod+Shift+M`); "New thread" is `Mod+N`.
- **Deep links.** `loams://open/studio/t/<id>` and `/g/<id>`; `loams://open/agent/<id>` maps to the thread.
- **The tray** shows running threads and pending approvals from `WatchNotifications` and `WatchApprovals`.
- **OS notifications** come from the daemon (§50 §5.5) and open Studio links.
- **"Open in editor"** (Q810): VS Code, Zed and Cursor URL schemes, or a configured command, run by main with the worktree path, never with a shell string built from untrusted input.
- **The folder picker** for Add project.
- **Q812.** The desktop's "Data Studio" page is renamed "Data explorer" (its route and plugin id stay, to keep links working).

**Tests:** `desktop_default_mode_studio`, `menu_switches_mode`, `deeplink_opens_thread_and_group`, `agent_deeplink_maps_to_thread`, `tray_counts_running_and_approvals`, `open_in_editor_uses_scheme_or_argv_not_shell`, `data_studio_renamed_label_only`.

**Steps:** tests first; implement; run the desktop tests, the console build and the smoke; commit.

**Commit:** `desktop: Loams Studio as the desktop's default mode, with deep links, tray and editor hand-off`.

---

## SD1e — End to end, review and release

### Task 29: Playwright end to end on both editions

**Files:** `web/apps/console/test/e2e/studio.spec.ts`, `apps/desktop-electron/test/e2e/studio.spec.ts`, `.github/workflows/studio-e2e.yml`.

**Consumes:** Task 8's fixture; Tasks 3–28.

**Produces:** the scenarios of §53 §10. They run on Linux in CI on every change to `crates/loams-agentd*`, `web/packages/{studio-ui,agentd-client,console-host}`, `web/plugins/{studio,factory,shell,rpc,approvals,agent}` and `apps/desktop-electron/src/main/{protocol,agentd}`:
- **web**: the console against `serve --remote` on loopback, with the test issuer;
- **electron**: `_electron.launch` with the bundled daemon.

**Tests:**
- `add_project_from_fixture_repo`.
- `thread_runs_and_shows_diff`.
- `comment_and_send_review_to_agent`.
- `commit_from_toolbar`.
- `restore_turn`.
- `run_group_of_three_compare_and_merge`.
- `terminal_round_trip`.
- `supervised_approval_blocks_then_runs`.
- `badge_live_in_console_mode` (a pending approval created while in Console shows on the badge, and resolving it in Studio clears it).
- `web_remote_requires_token`.
- `factory_group_visible_in_studio_only`.

**Steps:** write the specs; make them pass locally on Linux; add the workflow; run it twice in CI to check stability; commit.

**Commit:** `studio: end-to-end tests on the web and desktop editions against agentd`.

### Task 30: Security review and fuzzing

**Files:** `docs/security/studio-threat-model.md`, `crates/loams-agentd-connect/fuzz/{connect_decode,jwt}`, `.github/workflows/ci.yml` (nightly fuzz), findings fixed in their own commits.

**Consumes:** everything; §53 §8; §50 §18 (DD1 Task 36's model, if merged).

**Produces:**
- **The threat model.** It covers:
  - the remote listener (network attacker, a stolen token, a wrong origin, DNS rebinding);
  - the desktop proxy (renderer compromise);
  - vendored-code XSS paths;
  - supervised-mode bypasses (each harness);
  - git write misuse;
  - the preview proxy;
  - supply chain (vendored code, new npm packages, the import tool).
- **A review of the merged code against it.** It includes `cargo deny`, the npm licence gate, `pnpm audit` of the new packages, every `renderer: true` and `studio`-role method, and a manual XSS pass with the fixture corpus over every renderer.
- **Fuzz targets** that run nightly.
- **No open high or critical finding.**

**Tests:** the fuzz targets; a regression test per finding.

**Steps:** write the threat model; review; add fuzzing; fix with regression tests; commit.

**Commit:** `docs: Loams Studio threat model`; `studio: fixes from the security review` (as needed).

### Task 31: Documentation

**Files:**
- `docs/guides/studio/{index.md,remote-agentd.md,parallel-runs.md}`;
- `crates/loams-agentd/README.md`;
- `web/packages/studio-ui/README.md`;
- `docs/design/{53-studio-ade.md,studio-vendor-inventory.md,37-desktop-and-mobile-apps.md,50-loams-desktop-daemon.md,13-decision-log.md,README.md}`;
- `docs/plans/README.md`, `CHANGELOG.md`.

**Produces:**
- **The user guides:** modes; projects and threads; worktrees; review; git; parallel runs; approvals; the remote agentd setup and its security notes; the factory in Studio.
- **The maintainer guide** for vendoring and re-syncing (`studio-ui/README.md`: the tool, the header, provenance, the re-sync policy of D915).
- **Design updates.** §53 brought to the as-built state; the inventory's verdicts updated to what was actually done; the amendments recorded in §37 §19 (D666) and §50 (D788 for the remote listener; the DD1e split); D900–D922 statuses; release notes.

**Steps:** write; check every link; commit.

**Commit:** `docs: Loams Studio guides and the as-built design`.

### Task 32: Exit report

**Files:** `docs/plans/sd1-exit-report.md`, `docs/plans/README.md` (status), `proto/loams/agentd/v1` (`unstable` removed when the owner agrees), `web/apps/console/catalog/web.yml` (the `studio` flag on by default when the owner agrees).

**Produces:**
- Every exit criterion with its evidence: CI links, test names, the review.
- The rulings made during execution, summarised.
- The open questions with their current answers.

**Steps:** collect the evidence; write the report; ask the owner about `unstable` and the browser default; commit.

**Commit:** `docs: SD1 exit report`.

---

## Exit criteria

SD1 is done when every box is checked on a release candidate.

**Shell**
- [ ] One console, two modes, on the web and the desktop. Plugins declare `modes`. Switching keeps streams and drafts. Deep links select the mode (Tasks 3, 28, 29).
- [ ] Shared pieces (dock, approvals, notifications, palette) work in both modes (Tasks 3, 13, 18, 24, 29).

**Studio**
- [ ] Projects, threads with every installed harness, a worktree per thread, a timeline, a composer, a diff panel with review comments, a file tree and viewer, terminals, a git toolbar (commit, push, PR, merge), a palette and a preview (Tasks 15–24, 26).
- [ ] Per-turn checkpoints with restore (Tasks 10, 18).
- [ ] Parallel run groups with compare and resolve (Tasks 12, 25).
- [ ] Supervised approvals in Claude Code, Codex, opencode and ACP; `full-access` is labelled (Tasks 13, 18).
- [ ] The factory apps under Studio, with in-context links (Task 27).

**Backend and transports**
- [ ] `loams.agentd.v1` serves every Studio method on the loopback and remote listeners; the parity suite is green (Tasks 4–7).
- [ ] The desktop renderer reaches agentd only through main's proxy, without the token (Task 5).
- [ ] The remote listener is off by default and enforces TLS, audience, subject and origins (Task 7).

**Vendoring and licences**
- [ ] Every vendored file has its header and provenance row; NOTICE and `LICENSE.t3code` ship in the web and desktop bundles (Task 1).
- [ ] No Effect, T3 contracts, `@legendapp/list` or TanStack Router in `web/` (Task 1).
- [ ] The npm licence gate is green; `heic-to` is absent (Task 2).
- [ ] `upstream-report.mjs` runs against a newer T3 Code archive, and the re-sync policy is documented (Tasks 1, 31).

**Quality**
- [ ] The e2e suite is green on both editions in CI, twice in a row (Task 29).
- [ ] The threat model is complete and the review closed with no high or critical finding; the fuzz targets run nightly (Task 30).
- [ ] The guides are published; §53 is as built (Task 31).

## Self-review

- **Every §53 section maps to tasks:**

  | §53 | Tasks |
  |---|---|
  | §4 The mode switcher | 3, 24, 28 |
  | §5.1–§5.3 Layout, projects, threads | 15, 17, 18 |
  | §5.4 Worktrees | 9, 17, 23 |
  | §5.5 Parallel runs | 12, 19, 25 |
  | §5.6 Diff review | 10, 11, 20 |
  | §5.7 Terminals | 22 |
  | §5.8 Preview | 26 |
  | §5.9 Factory | 27 |
  | §5.10 Palette, keybindings, settings | 15, 24 |
  | §6.1–§6.3 Connect and transports | 4–7 |
  | §6.4 Existing methods | 4, 5, 6 |
  | §6.5 Gaps G1–G23 | 9–14 |
  | §7 Capability matrix | 15, 26, 27, 28, 29 |
  | §8 Security | 5, 7, 13, 15, 18, 20, 30 |
  | §9 Vendoring and licences | 1, 2, 16–26 |
  | §10 Testing | every task; 8, 29 |

- **The owner's direction:**
  - one shell, two modes → Task 3;
  - the factory under Studio → Task 27;
  - vendor T3 Code onto our stack → Tasks 1, 16–26;
  - no embedded T3 app or server → the Global Constraints and `no_effect_imports`;
  - the UI stack rulings → the Global Constraints and Tasks 6, 16;
  - the web on a remote agentd, the desktop on a local one → Tasks 5, 7, 28.
- **The owner's minimum task list:**
  - vendoring package and tooling: 1;
  - mode switcher: 3;
  - agentd Connect client: 4–7;
  - each component group: thread list 17, chat and markdown 18, composer 19, diff 20, file tree 21, terminal 22, git toolbar 23, palette 24;
  - parallel agents: 12, 25;
  - new agentd RPCs: 9–14;
  - factory under Studio: 27;
  - desktop: 28;
  - tests: every task, with e2e in 29;
  - docs: 31.
- **Every task names its tests**, and each test name states what it asserts.
- **The app works at every merge.** Studio is invisible until Task 15 and gated on the web until Task 32. Each gap task adds methods without changing old ones. The factory keeps redirects.
- **External dependencies:**
  - the `console/ui-stack` merge (Tasks 3, 15, 16);
  - DD1 Task 24 (Task 13);
  - DD1 Task 10 (Task 6, optional);
  - MT1 for real Authentik tokens (Task 7 uses a test issuer until then);
  - SF1 for the web factory (Task 27's web half).

## Open questions for the owner

These are §53 §15's questions, with the task that needs each answer. Every one has a default that the tasks follow until the owner answers.

| # | Question | Default | Needed by |
|---|---|---|---|
| Q800 | Connect facade `loams.agentd.v1`, or a typed ndjson client with plain TanStack Query? | Connect facade | Task 4 |
| Q801 | Desktop default mode: Studio from the first run, or Console until a project exists? | Studio | Task 3 |
| Q802 | DD1e's DSH-derived chat, sessions, terminal and diffs against SD1's T3 Code ports: which wins per component? | T3 Code for the thread list, timeline, composer, diff, files, terminal and git toolbar; DD1e keeps the store and reducer, the approval, question, subagent and outline cards, Settings › Plugins and Safe Mode | Task 0 |
| Q803 | Default approval mode on the desktop | `supervised`, pre-selected | Task 13 |
| Q804 | A remote agentd behind NAT: tailnet only, or an outbound tunnel through the Loams gateway? | Tailnet or a direct TLS port | Task 7 |
| Q805 | A hosted multi-user agentd (Studio in Loams Cloud)? | Out of scope | — |
| Q806 | Previews through the remote listener (`/preview/…`)? | Yes, with the same token and restricted framing | Task 26 |
| Q807 | Where to store a project's Loams binding | agentd space metadata | Task 15 |
| Q808 | Harness installs and account logins on a remote host from the browser? | No in SD1 | Task 7 |
| Q809 | Rename the `loams-desktop/` branch prefix and the worktree directory? | Keep in SD1 | Task 9 |
| Q810 | "Open in editor": which editors and how? | VS Code, Zed and Cursor URL schemes plus a configured command | Task 28 |
| Q811 | A "judge" agent in the compare view? | Yes, as an ordinary read-only thread, on demand | Task 25 |
| Q812 | Rename the desktop's "Data Studio"? | "Data explorer" (label only) | Task 28 |
| Q813 | Keep Matomo under Studio's Factory group? | Yes | Task 27 |
| Q814 | The factory loop's coding stage as an agentd thread? | Yes for local or remote-agentd runs; SF4 decides hosted | SF4 |
| Q815 | OS-level confinement in the daemon? | Not in SD1 | — |
| Q816 | `loams.agentd.v1` in the SDK catalogue? | No | Task 4 |
| Q817 | Vendor from `main` at `6524a8d4`, or from v0.0.45? | `main` at `6524a8d4` | Task 1 |
| Q818 | xterm.js, or T3 Code's libghostty-vt wasm terminal? | xterm.js | Task 22 |
| Q819 | zustand for client-only UI stores in `@loams/studio-ui`? | Allowed, through the catalog | Task 1 |

## Rulings made during execution

Task 0 was read on 2026-10-13 against `origin/dev` at `6c8481e8`. Nothing was built apart from `cargo metadata`. `@pierre/diffs` was read from its published tarball in a scratch directory and was not executed.

| # | Finding | Where | Absorbed by | Ruling |
|---|---|---|---|---|
| T0-1 | **The UI stack is not merged.** `console/ui-stack` is PR #513, open, with five commits not on dev: `82269206` (the pnpm catalog), `13180ecd` (tokens only), `9feea35a` (Base UI, `DataTable`, `VirtualList`), `670ea5c8` (`ServerStateProvider`, the Connect transport) and `dbdf37ab` (the design note). Its catalog has react 19.3.0, lucide-react 1.48.0, `@base-ui/react` 1.8.0, `@tanstack/react-query` 5.104.0, `@tanstack/react-table` 9.2.4, `@tanstack/react-virtual` 3.14.13, `@connectrpc/connect-query` 2.3.1, zod 3.25.76, echarts, `@xyflow/react` and tailwindcss 4.3.3. It has no zustand and no `@connectrpc/connect-web`. There is no `console/cordis-shell` branch: all the SHAs §53 §3 cites are on `console/ui-stack` | `pnpm-workspace.yaml` (no `catalog:` on dev); `web/packages/ui/src/{overlay.tsx,data-table.tsx}`, `web/packages/console-host/src/root.tsx` on the branch | 3, 15, 16 (and every task that adds a dependency) | Tasks 3, 15 and 16 wait for PR #513, or rebase onto it. **Until it merges, SD1 adds no catalog of its own:** a task that needs a new dependency before then waits, and Task 1 adds none (T1-3). §53 §3 is corrected |
| T0-2 | **DD1 has reached Task 9.** Task 10 (the Node client) has only the interface and a mock (`apps/desktop-electron/src/main/agentd/{client.ts,mock.ts}`; `client.ts` says the WebSocket client is pending), and there is no `./client` export in `@loams/agentd-client`. Task 24 (approvals) has not started: `ResolveApproval` and `ApprovalRequested` are not in `rpc.rs`. Tasks 27 and 28–32 have not started, and `web/plugins/agent/src` holds `index.tsx`, `panel.tsx`, `store.ts`, `providers.tsx` and `markdown.ts` only | `docs/plans/2026-10-09-dd1-desktop-daemon.md` (rulings T0-1…T9); `web/packages/agentd-client/src/reducer.ts` | 6, 13, 17–23, 28 | **The Q802 split is §53's default and has not been agreed with DD1's owner,** who could not be consulted during Task 0. The owner should confirm it. The T3 Code ports **replace** DD1e's `SessionList` (Task 30), its timeline (`Conversation`, `Transcript`, `NodeSeat`, `UserBubble`, `PendingEcho`, `RetryRow`, `FailureRow`, `Thinking`, `ToolCard`; Task 29), its `Composer` (Task 29) and its `DiffView` (Task 31), plus the file tree, terminal (xterm.js, Q818) and git toolbar. DD1e **keeps** the store and `reduceSession`, `ApprovalCard`, `QuestionCard`, `SubagentCard`, `TurnOutline`, and Settings › Plugins with Safe Mode (Task 32). Not covered by Q802 and needing a call: DD1 Task 30's `NewSession` and `HarnessSettings`, and Task 31's `ChangeRequests`. Task 13 lands DD1 Task 24's contract first by agreement, or waits |
| T0-3 | **The method table has 90 methods, not about 95:** 77 unary and 13 streams. 88 are `renderer: true`; the exceptions are `Hello` (role `mcp`, ndjson only, as §6.4 says) and `Shutdown`. Every other method §53 §6.4 names exists and is `renderer: true`. Small differences: the method is `CheckHarnessUpdates` (plural); `EngineReady` is not used by §6.4. `MutateParams` is tagged `op` (`params.rs:330`), `SessionCommandKind` has `Run`, `Steer`, `Interrupt` and `RespondInput` (`chat.rs:228-231`), `RunRequest.worktree` exists, and the handshake refuses any `Origin` (`loams-agentd-rpc/src/handshake.rs:80`). **Gap:** `reduceSession` folds `AgentEvent`, not `TranscriptUpdate` (`loams-agentd-proto/src/transcript.rs:24`), and no TypeScript code folds `TranscriptUpdate` | `crates/loams-agentd-proto/src/rpc.rs:267-401`; `crates/loams-agentd-sessions/src/rpc.rs:738-2153` | 4, 5, 6 | §53 §3 now says 90. Task 6's "the reducer accepts the Connect `TranscriptUpdate`" is a new reset-and-delta fold in `@loams/agentd-client`, not just a mapping. Task 6 sizes it |
| T0-4 | **`@pierre/diffs` 1.5.2 writes HTML into an open shadow root.** It builds a hast tree from Shiki's `codeToHast`, or from tokens as text nodes, serialises it with `hast-util-to-html` (which escapes text and attributes, with no `allowDangerousHtml`), and sets it with `innerHTML` or `insertAdjacentHTML` (`dist/components/FileDiff.js`, `File.js`, `renderers/DiffHunksRenderer.js`). The sinks that take HTML from the caller are `dist/editor/marker.js` (`message.html`) and the SSR `prerenderedHTML` path (`dist/ssr/FileDiffReact.js`, `react/utils/templateRender.js`) | the published tarball (Apache-2.0) | 20 | **D919 takes the shadow-root path with escaped tokens.** Studio never uses the marker `{html}` messages, SSR or `prerenderedHTML`, or `allowDangerousHtml`. Task 20 re-checks T3 Code's find-in-diff patch against these rules and keeps `diff_renders_text_not_html` |
| T0-5 | **Licences and ages on 2026-10-13** (npm `license` and `time`). Everything in the inventory's catalog table is allowed except `heic-to` (LGPL-3.0, not added). At least 14 days old: `@pierre/trees` 1.0.0-beta.6 (2026-07-25), shiki 4.2.0 (2026-06-03), `@tiptap/*` 3.31.3 (2026-09-04), `@dnd-kit/core` 6.3.1, `sortable` 10.0.0, `modifiers` 9.0.0, `utilities` 3.2.2 (2023–2024), zustand 5.0.14 (2026-05-28; 5.0.15 is from 2026-08-13), react-markdown 10.1.0, remark-gfm 4.0.1, remark-breaks 4.0.0, remark-parse 11.0.0, remark-rehype 11.1.2, rehype-sanitize 6.0.0, unified 11.0.5, hast-util-to-html 9.0.5, hast-util-to-jsx-runtime 2.3.6, `@xterm/xterm` 6.0.0 and `@xterm/addon-fit` 0.11.0 (2025-12-22; DD1 has no pin yet), diff 9.0.0, dompurify 3.4.16 (2026-09-23). **Too new:** `@pierre/diffs` 1.5.2 (2026-10-05, 8 days), shiki 4.5.0, `@tiptap/*` 3.31.4 and dompurify 3.4.17. The `@pierre/*` packages publish the licence id in lower case (`apache-2.0`) | `npm view` | 2, 16–22 | Pin the ages above. `@pierre/diffs`: pin 1.5.1 (2026-09-25), or wait until 2026-10-19 and pin 1.5.2 (Task 20 checks that T3 Code's patch applies). The licence gate matches SPDX ids case-insensitively (T2-1) |
| T0-6 | **JWT verification.** The only JWT crate in the root lock is `jsonwebtoken` 9.3.1 (MIT), pinned in the workspace at `=9.3.1` with `default-features = false` (`Cargo.toml:121`). `crates/loams/src/auth/oidc.rs` already verifies Authentik tokens: RS256 and EdDSA, JWKS from discovery or `LOAMS_OIDC_JWKS_URL`, a TTL cache and a rate-limited refetch on an unknown `kid`. Authentik's paths are `/application/o/<slug>/.well-known/openid-configuration` and `/application/o/<slug>/jwks/` (`crates/loams/tests/auth.rs:299-312`). MT1 is Planned and its `loams-auth` crate does not exist | `Cargo.lock`; `crates/loams/src/auth/{oidc.rs,mod.rs}` | 7 | Task 7 uses `jsonwebtoken` 9.3.1 and extracts or shares `oidc.rs`'s verifier instead of writing a second one. It finds the JWKS through discovery (`/application/o/loams/jwks/`), and uses a test issuer until MT1 lands |
| T0-7 | **`connectrpc` 0.9.1 serves unary and server-streaming methods.** It has `Server::bind`/`from_listener` with `serve_with_graceful_shutdown`, `Router::into_axum_service()`, TLS through `server-tls`, interceptors and limits. Streams are `ServiceStream<T>` over HTTP/1.1 and HTTP/2, and one registration serves Connect, gRPC and gRPC-Web. `loams-agentd-link` enables `client` and `server`, and uses the server only in its mock (`src/mock.rs`). **Gap: no CORS.** OPTIONS gets a 405 (`service.rs:1713`), and nothing handles `Origin` | `~/.cargo/registry/.../connectrpc-0.9.1/src/{server.rs,service.rs,response.rs}`; `crates/loams/src/api/connect.rs:546-585` (the axum mount pattern) | 5, 7 | Task 5 mounts the services through axum, as the engine does. The loopback listener checks `Host`, the token and "any `Origin` is a 403" in a layer in front. Task 7 adds its own CORS and origin-allowlist layer (tower-http's `cors` feature, which is not enabled today, or a hand-written layer) |
| T0-8 | **The Electron protocol handler streams.** The scheme is registered with `stream: true` (`handler.electron.ts:24-33`), and `proxy.ts` returns `new Response(res.body, …)` without reading it (`proxy.ts:87-103`), with request bodies sent as `duplex: "half"`. **But `proxyRequest` cannot be reused as it is:** it rewrites `Origin` to the destination's (agentd would answer 403), it does not drop `Cookie`, and `route.ts:66` sends every unmatched `/loams.` path to `server.url` | `apps/desktop-electron/src/main/protocol/{handler.electron.ts,proxy.ts,route.ts}` | 5 | Task 5 adds a `/loams.agentd.v1.` branch before the fallthrough. It **drops** `Origin` and `Cookie`, adds the token and the allowlist, and answers 503 `agentd_unavailable`. Its 10 000-frame test confirms that nothing buffers; no streaming test exists today |
| T0-9 | **D900–D922 and Q800–Q819 are free.** On all remote branches, and on all 18 open PRs' heads, they appear only in this plan, §53 and the inventory. dev's decision log ends at D855 and Q765 | `docs/design/13-decision-log.md` | 31 | No renumbering |
| T0-10 | **The owner has not answered any of Q800–Q819.** PR #516 has no review | PR #516 | all | §53 §15's defaults stand, including Q802's split, which is still to be agreed (T0-2) |

| # | Ruling | Why |
|---|---|---|
| T1-1 | **The source archive is the commit URL's.** `vendor.mjs fetch` downloaded `https://github.com/pingdotgg/t3code/archive/6524a8d43795dc543f7cfb572388cdea3662ad48.zip`, which has SHA-256 `4d13a8e39bee303cc33797742187abe15c70c70c3879d2d4e4bc8a51b3bb14a3` and 32 061 entries; its zip comment names the commit. `provenance.json` records that hash. The extracted tree is byte-identical to the `main` archive that §53 inventoried (`b9ca31ef…`), except for one symbolic link (`.claude/skills`), which the tool skips | The plan asks for the commit URL's archive, and its bytes differ from the `main` zip's |
| T1-2 | **Task 1 vendors five Effect-free files from inventory group (k)** (generic hooks and utilities, "SD1 Tasks 1 and 16"), all as-is: `lib/lruCache.ts`, `lib/observeResize.ts`, `hooks/useMediaQuery.ts`, `hooks/useResizeDrag.ts` and `components/AnimatedHeight.tsx`, the last as `src/lib/AnimatedHeight.tsx` with one import rewritten. The rest of group (k)'s row (`useLocalStorage`, `useResizableWidth`, `useCopyToClipboard`, `timestampFormat`) imports `effect/Schema` or `@t3tools/contracts`, so it waits for the task that first needs it and is ported there (zod, plain TypeScript). This deviates from "empty apart from `src/index.ts`" | It gives the header, provenance and hash tests real files to check, and these files need nothing beyond React |
| T1-3 | **zustand is not added yet.** dev has no pnpm catalog (T0-1), and nothing in Task 1 uses zustand. The first task that needs it adds it to the catalog after PR #513 merges, at 5.0.14 or 5.0.15 (both at least 14 days old, T0-5) | No second catalog; no unused dependency |
| T1-4 | **`@loams/studio-ui` keeps upstream's formatting.** `web/biome.json` sets double quotes for `packages/studio-ui/src/**`, so as-is files stay byte-identical to upstream apart from the header and the mapped imports. Two upstream idioms are allowed for the two files that use them: `??=` inside an expression (`observeResize.ts`) and a deliberate extra effect dependency (`AnimatedHeight.tsx`) | Re-syncs stay mechanical, and `localSha256` stays meaningful |
| T1-5 | **The zip reader is our own**, in `vendor.mjs` and about 100 lines on `node:zlib`. It reads the central directory, refuses zip64, encrypted entries, absolute paths, drive letters, backslashes and `..`, checks every entry before writing any, checks the CRC and sizes, writes with `wx` and mode `0644`, and skips symbolic links. `upstream-report.mjs` has its own Myers unified diff | No new root dependency; nothing from upstream is ever executed or loaded |
| T1-6 | **`vendor.mjs fetch` also takes `--archive <zip>`** (recorded as `local:<name>` unless `--url` is given) and `--expect-sha256`. It refuses an `--out` inside the repository. `import` reads the commit and archive hash from the `fetch.json` that `fetch` writes, and refuses to import from a tree with no recorded archive. Every command takes `--package` (Task 16 passes `web/packages/ui`) | Test fixtures, offline use, and Task 16's second package |
| T1-7 | **NOTICE files are not yet shipped in bundles.** Neither the console build (`/ui/NOTICE`) nor the desktop package copies a NOTICE today, and no bundle includes `@loams/studio-ui` until Task 15 imports it | Task 15 (console) and Task 28 (desktop) ship the NOTICE and `LICENSE.t3code` with the first bundle that contains vendored code |
| T2-1 | **The allow list is read from `deny.toml`**, so there is one list. SPDX ids are matched case-insensitively (`apache-2.0`, T0-5), and `X+` is matched as `X`. Some licences are refused outright, even through an `OR` branch's partner or an exception: GPL, LGPL, AGPL, SSPL, BUSL, Elastic and Commons Clause, and non-commercial or no-derivatives Creative Commons. So are `@tiptap-pro/*` and `@tiptap-cloud/*`, whatever licence they declare. An `OR` takes a permissive branch over MPL-2.0 when both are allowed, so dompurify is recorded under Apache-2.0, as §53 §9.3 says | One source for the rule; the owner's "no Tiptap Pro" holds even if a Pro package declares MIT |
| T2-2 | **No npm-only licences** (the owner, 2026-10-13). An earlier draft allowed `0BSD`, `BlueOak-1.0.0` and `Python-2.0` for npm; the owner rejected that. The allow list is exactly `deny.toml`'s, and `npm_licences_allow_list_matches_deny_toml` asserts that these three are refused | One rule for crates and npm |
| T2-3 | **Two exceptions, approved by the owner on 2026-10-13:** `@fontsource-variable/archivo` 5.3.0 and `@fontsource-variable/martian-mono` 5.3.0 (OFL-1.1). They are fonts, not code, and the root NOTICE already credits them | `npm_licences_current_tree_passes` needs a recorded exception for each failure |
| T2-4 | **The gate covers the workspace packages each project uses.** It runs `pnpm --filter <project>... licenses list --prod --json`; the trailing `...` matters, because without it pnpm lists 12 packages for the console instead of 130. It does not see `devDependencies` that a bundler inlines, nor Electron itself (MIT), which is a desktop dev dependency. Today the console's runtime libraries are all `dependencies` | Known limit, recorded |
| T2-5 | **Fixing what the rejected licences caught.** `sax` 1.6.1 (BlueOak-1.0.0) comes in transitively and in production, through `@loams/desktop` → `electron-updater` 6.8.9 → `builder-util-runtime` 9.7.0, which asks for `^1.2.4`. The last ISC release, 1.4.1, satisfies that range, so a scoped override pins `builder-util-runtime@9.7.0>sax` to 1.4.1. `argparse` 2.0.1 (Python-2.0) comes in through `js-yaml` 4.3.2, which the desktop uses directly and which `electron-updater` also requires. Only js-yaml's command-line tool uses it, never the library the desktop imports, so a scoped override removes it (`js-yaml@4.3.2>argparse: '-'`). argparse stays in the tree only as a development dependency of nx. Both overrides lapse when their parent's version changes, so the gate re-checks then. **`tslib` 2.3.0 (0BSD) cannot be avoided cleanly.** It comes in transitively and in production: `echarts` 6.1.0 pins it exactly, and `zrender` uses it too, through `@loams/charts` in the console. Every tslib from 1.11.2 on is 0BSD, and the Apache-2.0 releases (1.11.1 and earlier) lack the tslib 2 helpers that echarts' build calls. The gate fails on it, with no exception, until the owner decides | The owner's ruling on T2-2 |
| T3-1 | **`ModeService` also has `modesOf(pluginId)`.** The shell filters `shell.nav.section`, `console.nav` and `shell.header.server` entries by their plugin's modes, and a slot entry carries only the plugin id | The Shared contracts' interface has no way to ask a plugin's modes |
| T3-2 | **The boot mode.** A path names a mode only when it is not the shared root `/`: `/studio…` is Studio's, any other route Console's. At `/`, the stored `loams.shell.mode` decides, then the edition default (`chooseBootMode`). A boot mode whose home is not `/` (Studio) navigates to its home | Without this, the desktop's first run at `/` would boot Studio and the route `/` would switch it straight back to Console |
| T3-3 | **A plugin of a mode not yet entered has the status `deferred`**, not `pending`, so the stuck-plugin sweep (`pending()`) ignores it. Core plugins load in every mode. Third-party sandboxed rows ignore `modes` in SD1 | `pending` means "waiting for a service"; a deferred row is waiting for the person |
| T3-4 | **A mode can need an instance feature: `BootOptions.modeFlags`.** `startConsole` defaults it to `{ studio: 'studio' }`, so every browser edition gates Studio on `flags.features.studio` until Task 32; `startDesktop` passes `{}`. Only when a gate exists does boot wait for GetInstance. `available` is Console plus every mode an enabled catalog row declares, minus a gated mode whose feature is off | The plan's flag gate, without a second availability mechanism |
| T3-5 | **The header's context controls are `shell.header.server` entries filtered by mode**, with the nav's rule (`meta.mode`, else a `/studio` href, else the plugin's modes). No new slot. The environment switcher shows in Console only | Console's server switcher and Studio's project, branch and host share one place |
| T3-6 | **No palette exists in the shell yet.** `palette.tsx` and the palette's mode scope move to Task 24, which builds the palette; the shell exports `inMode()` for it | Nothing to scope |
| T3-7 | **The router matches the path without its query**, and the query's parameters join the page's `params` (route parameters win). `Location.path` is the pathname | `?panel=` and `?host=` (Shared contracts) |
| T3-8 | **The switcher is a new `@loams/ui` primitive, `Segmented`**, over Base UI's toggle group (one pressed option, never none). Below 640 px its labels are visually hidden, leaving the icons | `@loams/ui` had no segmented control; plugins never import Base UI directly |
| T3-9 | **Q801's default is applied: the desktop opens in Studio on the first run.** The desktop smoke now checks that, then switches to Console before it looks for the server switcher | The smoke would otherwise look for a Console control in Studio |
| T3-10 | **In Studio the breadcrumbs start at "Studio"** and do not repeat the section or the home page, and the nav marks only the most specific matching entry as current | `/studio/settings` showed "Studio / Studio / Studio" and two current entries |
| T3-11 | **The deep-link allowlist takes exactly `studio/t/<id>` and `studio/g/<id>`.** `loams://open/agent/<id>` → `/studio/t/<id>` stays Task 28's | Task 3's scope |
| T15-1 | **Where Studio's projects come from until Tasks 5–7.** On the desktop: agentd's existing ndjson methods `ListRepos`, `AddRepo` and `CloneRepo` (role `owner`, `renderer: true`), through a new, narrow desktop-bridge group `desktop.agentd.repos` (`list`, `add`, `clone`) that Electron main serves with the token only it holds. Main checks the folder path (absolute, no control characters) and the clone URL (https, ssh or `user@host:path`; never `file:`, git's `ext::` or a leading `-`). The folder picker is `desktop.shell.pickFolder`. Main's client is still DD1's mock until DD1 Task 10. In the browser: `webAgentd()`, a stub with no host, so Studio shows "Connect an agentd host" until Task 7; it takes an `AgentdProjectsClient` (tests). Both sit behind `StudioAgentd` (`web/plugins/studio/src/agentd.ts`), whose methods are named after the ndjson methods, so Task 6's `rpc.agentd.projects` replaces the two adapters and not the components. Server state goes through the console's TanStack Query cache; connect-query comes with the Connect client | The Connect client of Tasks 5–6 does not exist yet; the owner's direction for this task |
| T15-2 | **A project is a repository for now, and its id is its path** (`ListRepos` has no id). When projects become agentd spaces (`WatchSpaces`, `Mutate createSpace`) the id becomes the space id. So **rename and remove are not offered yet**: they are `Mutate renameSpace` and `deleteSpace`, which need spaces (Task 6 or 17) | No repository rename or remove exists on the ndjson wire |
| T15-3 | **The manifest injects `router`, `slots`, `mode`, `platform`, `flags` and `desktop`, and no `rpc.agentd.*` yet**: nothing provides those, and a plugin that injects a missing service stays pending. The base catalog row narrows `inject` to drop `desktop`; `desktop.yml` patches the row to add it | Task 6 adds `rpc.agentd.studio` and `rpc.agentd.projects` |
| T15-4 | **The `studio` row is in `base.yml`, not `web.yml`**, so every browser console (hosted, engine-served and the dev demo) has it, still behind `flags.features.studio` (T3-4); `desktop.yml` only patches the row. The dev server's demo turns `studio` on (`import.meta.env.DEV` only) | `web.yml` is applied only on the hosted path |
| T15-5 | **`agentd:use` and `agentd:git-write` join console-host's permissions.** A third-party catalog row that injects `rpc.agentd.*` or `desktop` fails at boot (`injectsAgentd`), and the sandbox bridge never bridges `rpc.agentd` or `desktop` (`NEVER_BRIDGED`) | §53 §8.3, D918; `third_party_cannot_inject_agentd` |
| T15-6 | **Storage.** The layout's sizes and open states are kept under `loams.studio.panel` (`sidebar`, `panel`, `drawer`, `panelOpen`, `drawerOpen`). Settings › Studio is a new key, `loams.studio.settings`: this device's defaults for new threads (harness, model, approval mode, runs per group) and its notification choices. The approval modes are §53 §8.1's two, `supervised` (the default, Q803) and `full-access` | Daemon-side defaults come with Tasks 13 and 14 |
| T15-7 | **The layout uses the vendored `useResizeDrag` and `useMediaQuery` from `@loams/studio-ui`**, so the console bundle now holds vendored code, and the console build emits `NOTICE` and `LICENSE.t3code` beside it (T1-7). The desktop package's NOTICE stays Task 28's | T1-7 |
| T15-8 | **Settings › Agentd hosts (`settings/Hosts.tsx`) is a stub**: it says the console cannot register a remote host yet and shows how one runs. Task 7's registry and transport move in here | Task 7 has not landed |
| T15-9 | **Q812 is applied here at the owner's direction**: data-studio's nav label and its `/data` page titles read "Data explorer"; its routes, page ids and plugin id stay. The factory plugin is not moved (Task 27), and its manifest gains no `modes`, because Task 3's interface does not need it | The owner's instruction for this task |
| T15-10 | **The thread and run-group routes exist with "not available" empty states**, the panel's tabs (`?panel=`) and the terminal drawer, so deep links and the layout work; Tasks 17–26 fill them | Shared contracts' routes |
| T15-11 | **In Studio the shell's sidebar shows Studio's nav** (Home and Settings, group "Studio"), and the Studio layout's own sidebar lists the projects | T3-1's nav filter |

Tasks 4 and 5 were run before Task 0 landed, on the defaults of Q800 (a Connect facade) and Q816 (not in the SDK catalogue).

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| T4-1 | **Fifteen files, one per Shared-contracts service**, `proto/loams/agentd/v1/{studio,projects,threads,queue,harness,git,diff,files,terminals,preview,review,groups,approvals,notifications,attachments}.proto`. `studio.proto` also holds the shared enums (`HarnessId`, `ReasoningLevel`, `SandboxLevel`, …) and `WorkspaceTarget`. Message names keep the engine's (`Chat`, `Space`, `chat_id`, `space_id`) so the mapping and §53 §6.5's shapes read alike; RPC names follow §53 §6.4 (`ThreadService.WatchTranscript`, `GitService.WatchStatus`, `DiffService.Discard`, `FileService.Search`; `FileService.SearchMentions` is `SearchFiles`). The ndjson `PreviewService` message is `PreviewServer` (the service owns the name); the sidebar `move`/`delete`/`import` variants are `reorder`/`remove`/`import_sections` (keywords in generated languages) | buf STANDARD needs a request and response per RPC; one file per service matches the plan's file list | Renames before Task 32 are free (`unstable`, breaking ignored) |
| T4-2 | **Every service carries `ModuleOptions{unstable: true}` and no method a `FacadeOptions`**, so the facade generator makes no module; and `loams-facade-gen` gains `NOT_SDK_PACKAGES = ["loams.agentd.v1"]`, which keeps the package out of `PROTO_PACKAGES` (the generator lists every package under `proto/`, so without it all thirteen SDKs would claim to speak agentd). `agentd_v1_not_in_sdk_catalogue` checks both over the real `proto/` tree; the SDK goldens are unchanged | Q816 | One constant to drop if the owner ever wants an agentd SDK |
| T4-3 | **`loams.agentd.v1` is a §44 §7.2 row** ("none: no SDK module (Q816)") | `crates/loams/tests/route_map.rs` fails on any `package` the design does not enumerate | None |
| T4-4 | **The agentd reasons in `docs/api/reasons.md` are a daemon-only table without backticks**, so neither `reasons_are_snake_case_and_unique` nor the SDK generator reads them as registry rows (several, `not_found` and `unauthenticated`, already are rows) | The daemon is not in any SDK; a duplicate row fails the registry test | None |
| T4-5 | **Timestamps**: an RFC 3339 instant on the ndjson wire (`DateTime<Utc>`) is `int64 *_ms`; ndjson's own millisecond fields keep their meaning with an `_ms` suffix. `usize` is `uint64`, `u16` is `uint32` (range-checked inward). JSON values are `google.protobuf.Value`. A list whose absence differs from empty (`ToolPart.diff_stats`, `FinishQueuedMessageEdit.attachments`) is wrapped in a message; `SkillList`'s `null` is `ListSkillsResponse.unavailable`. Paging `limit` is `optional`, unset = ndjson's default 100 | The plan's "int64 ms"; presence must survive | Sub-millisecond precision is lost on instants (no client uses it) |
| T5-1 | **Amends Ruling 1.** The handlers call the engine through its own `RpcService` dispatch, with the ndjson method's typed `Params` built from the proto and its typed `Result` mapped back (`loams_agentd_proto::rpc::Method`), not by calling `loams-agentd-sessions` functions one by one. The values pass the dispatcher as an in-process `serde_json::Value`, never over a wire | `EngineRpc`'s arms carry the path jails, checkout checks and validation (`catalog_root`, `change_request_root`, `featured_file_paths`, …); a second copy of 85 arms is exactly the drift Task 6's parity suite exists to catch. With one dispatch, Connect and ndjson run the same arm by construction, and the only mapping to test is proto ↔ Rust types (`every_fixture_survives_the_proto` round-trips every ndjson fixture through the proto and its binary encoding) | One `serde_json` conversion per call; if profiling shows it, a typed entry point on `EngineRpc` can replace it per method without touching the protos |
| T5-2 | **The twins table** (`loams-agentd-connect::twins::CONNECT_METHODS`) names each Connect method's ndjson twin: a method, a `Mutate` op, a `QueueCommand` kind, `GetHost`, or a gap (`G1`–`G23`, `unimplemented` + `not_implemented` until its task). The renderer allowlist is the table joined with `METHODS` (`renderer: true`, gaps never), generated to `apps/desktop-electron/src/main/agentd/allowlist.gen.ts`; `allowlist_gen_is_fresh` fails on drift (`LOAMS_UPDATE_ALLOWLIST=1` rewrites). `LocalDevice` is not a separate RPC: `GetHost` carries the same device id from `EngineInfo` | One table for the allowlist and Task 6's cases | None |
| T5-3 | **The Connect listener accepts the owner token only** (no MCP run tokens), checks peer loopback, then `Origin` (403), then `Host == 127.0.0.1:<connectPort>` (403), then the bearer token in constant time (401 after the ndjson listener's failure delay, serialized). Refusals are Connect JSON error bodies. `Shutdown` and `Hello` are not on it; `Hello` and `GetHost` list the feature `studio.v1` | §50 §7's rules; the MCP shim stays on ndjson | None |
| T5-4 | **`google.protobuf.Value` numbers**: a whole double within ±2⁵³ comes back as a JSON integer, so `{"limit": 10}` survives the round trip | `Value` stores every number as a double | `10.0` and `10` become the same value, which JavaScript cannot tell apart anyway |
| T5-5 | **`agentd.json`'s `connectPort` is optional on read** (`Option<u16>`, absent from an older daemon's file); this daemon always writes it. Electron main re-reads `agentd.json` and `agentd.token` per proxied request, so a restarted daemon (new port, rotated token) is picked up | Upgrades with a daemon still running | Two small file reads per request (a stream is one request) |
| T5-6 | **Electron**: `routeAgentd` (route.ts) decides 403 `forbidden_method` from the allowlist before the daemon or its token is looked up, then 503 `agentd_unavailable`; `proxyAgentdRequest` (proxy.ts) drops `Origin`, `Referer`, `Cookie` and the renderer's `Authorization`, adds the bearer token, streams the upstream body as the response body, and drops any response header that carries the token; `protocol/agentd.ts` composes them without importing electron, so the tests drive it with a real loopback server (`stream_passes_10000_frames_under_bound`) | Testability; the `/loams.` prefix would otherwise send agentd calls to the active server | None |
| T8-1 | **The Mock's turn number comes from the file.** `n` in `mock edit <n>` is one more than the `mock edit` lines the file already holds; tool ids are `mock-edit-t<n>-<i>` (`i` = the path's index in the knob) | The Mock keeps no state between runs and is shared by every chat; counting lines keeps it deterministic and restart-proof | A test that edits a file by hand with `mock edit` lines shifts `n` |
| T8-2 | **`LOAMS_DESKTOP_MOCK_APPROVE` asks through `RunControls::request_input`** (a question `mock-approve-t<n>-<i>`, header `Approval`, options `Allow`/`Deny`), not an `ApprovalRequested` event. A denial or a dropped question leaves the file untouched and the result is an error | DD1 Task 24's approval events and `ResolveApproval` haven't landed (agentd has only the role gate); `request_input` is the one blocking host bridge a harness has today | Task 13 moves the Mock onto the approval contract when it lands it; the knob and its order (call, ask, write, result) stay |
| T8-3 | **The Playwright fixture (`web/apps/console/test/e2e/fixtures/agentd.ts`) and `agentd_fixture_starts_and_stops` move to Task 29** | The console has no Playwright setup; adding `@playwright/test` needs a pinned, licence-gated dependency (Task 2) and a web build. Task 8 stays in the agentd crates; Task 29 builds the e2e harness and its fixture together | Task 29 grows by one fixture |
| T8-4 | **The knobs live in `mock.rs`, not in `registry.rs`'s default script.** They apply to any script: the edits are spliced in before the script's `Done`. `MockHarness::run_with(request, controls, MockEdits)` takes them explicitly for tests; `run` reads the environment; title runs never edit. Absolute paths, `..`, and symlinks out of the cwd are refused with an error result | Existing `MockHarness { script }` literals stay valid; harness tests need no process-global env | None |
| T9-1 | **A merge conflict is the `merge_conflict` error, not a reply.** Its text is `merge_conflict: <JSON array of paths>`, and the Connect error's `ErrorInfo.metadata["paths"]` carries the same array. `MergeBranchResponse.conflicts` (Task 4) stays empty; the ndjson `MergeBranchReply` has only `sha` | The Shared contracts' reasons table (`aborted`, "details list the paths") and Task 9's "returns `merge_conflict` with the paths" | Drop the proto field before Task 32 if no client wants it |
| T9-2 | **The lease is this checkout's remote-tracking ref.** `PushBranch.forceWithLease` is a bool (Task 4), so the expectation is `refs/remotes/<remote>/<branch>` as this checkout last fetched it, or empty (the branch must not exist) when there is none: `--force-with-lease=refs/heads/<b>:<sha>`. `remote` must be a configured remote's name, never a URL. `push_args` is the only argument list; no refspec starts with `+` | A UI-supplied expectation would need a proto field; the tracking ref is what the person's diff was computed against | A stale fetch lets a lease pass that the person did not see; an `expectedRemoteHead` field can be added (the package is unstable) |
| T9-3 | **The Forgejo credential, until DD1 Task 22.** Keyring service `dev.loams.agentd`, accounts `factory:forgejo:token` and `factory:forgejo:url` (§50 §12's `factory:<app>:<field>`; the URL sits beside the token until Task 22's store holds the non-secret part). The token is sent only when the URL's host is the repository remote's host, over `https` (plain `http` only on loopback), with redirects off. A Forgejo pull request has no draft flag, so a draft is titled `WIP: …`; `linkIssue` appends `Closes #N` (both providers) | No credential store exists yet; the token must never reach another host | Task 22 replaces `KeyringForgeCredentials` behind the `ForgeCredentials` trait |
| T9-4 | **No `wiremock`.** It is not in the root lock, so the fake Forgejo is a small axum server on `127.0.0.1:0` (axum is a workspace dependency; a dev-dependency of `loams-agentd-sessions`). The fake `gh` is a script in a temporary directory that `ChangeRequestCreator::with_gh_path` puts first on the child's `PATH`; no process-global environment changes | A new dev-dependency needs the pin and licence checks; the axum fake records the same request shape | None |
| T9-5 | **Module names.** `change_requests.rs` already exists (DD1's pull-request watcher), so creation is `change_request_create.rs`. `DiscardPaths` is `diff_sync::discard_paths`, beside `discard_working_tree`, reusing its safety rules and its per-checkout lock; like it, it needs a checkout the daemon tracks (one with a chat) | Reuse the audited discard helpers | None |
| T9-6 | **The jail.** A `cwd` must be a known checkout: a local chat's cwd, a local project's repository or one of its worktrees, or a registered repository (`ListRepos`, Studio's projects per T15-1) or one of its worktrees. A `repoPath` must be a registered repository or a known checkout. Paths are relative, without `..`, and passed as literal pathspecs. A write refuses while an agent runs in the checkout, and the engine serializes all git writes | §53 §8.2; Studio's projects are `ListRepos` paths until spaces | None |
| T9-7 | **Temporary worktrees.** A merge whose target is not checked out, and every rebase, runs in a worktree under `<git-common-dir>/loams-merge/`, removed afterwards. A rebase replays the source onto the target in a detached worktree and fast-forwards the target; the source branch itself is left as it was unless `deleteSource` | Neither the user's checkouts nor HEAD move; a conflict is aborted in the scratch tree | None |
| T9-8 | **`agentd:git-write` is the client's gate; the daemon's are the role and the checksums.** The daemon cannot see console permissions. `twins::GIT_WRITE_METHODS` names the methods that need the permission (Commit, Push, CreateChangeRequest, Merge, DeleteWorktree, Discard, DiscardPaths), generated as `AGENTD_GIT_WRITE_METHODS` in `allowlist.gen.ts` for Task 6's `rpc.agentd` to enforce. The person's confirmation reaches the daemon as `expectedChecksum` / `expectedSourceHead`. None of these is in the `mcp` role | §53 §8.1, §8.3 | Task 6 must wire the set |
| T9-9 | **`Hello` and `GetHost` list `git.write`.** `ListWorktrees` reuses `RepoPathParams`, lists the main worktree first, and names a detached worktree's branch `HEAD`. `DiscardPaths` answers `Empty`. Commit hooks run (no `--no-verify`); `summary` is the commit's subject | Shared contracts' features; the user's hooks are theirs | None |
| T9-10 | **Task 6's parity suite is not on `dev` yet**, so the Connect cases are in `loopback.rs` (`git_writes_over_connect`: `ListWorktrees` compared with ndjson, `stale_diff`, `merge_conflict`, a commit); Task 6 adds Task 9's parity cases | Task 6 has not landed | Task 6 grows by six cases |
| T27-1 | **The factory's manifest lists both modes, `["console", "studio"]`.** Its nav entry is Studio's (`meta.mode: "studio"`, group "Factory", `/studio/factory`) and its overview card is Console's (`meta.mode: "console"`). Studio-only would leave the plugin deferred in Console, so neither the overview card nor the `/factory` redirects could answer there. `editions` stays `["desktop"]` (D922) | The plan's "the plugin lists both modes" |
| T27-2 | **Redirects replace, they do not push.** `RouterService.navigate` takes `{ replace?: boolean }`, and the shell's `HashSource` gains an optional `replace` (`location.replace`). Each old path (`/factory`, `/factory/:app`, `/factory/:app/app`, `/factory/:app/configure`) is a page that replaces itself with its `/studio/factory…` route, query kept; a redirect still mounted after the route moved on does nothing. `loams://open/factory[/<app>]` now opens `/studio/factory[/<app>]` directly | Back must not bounce through the redirect |
| T27-3 | **Studio's sidebar has two Factory places.** The shell's Studio nav gains the "Factory" group (the hub), ordered after "Studio" (`STUDIO_SECTIONS`); the Studio layout's sidebar gains `sidebar/Factory.tsx` below the projects: the eight apps with their state, linking to panels, the full UI (OpenObserve) or configure. Studio reads `desktop.factory` itself (`StudioFactory`), never the factory plugin's values. On the web there is no source until SF1, so the group is absent; a `links` source shows the apps' own UIs in a new tab, http(s) only (D922) | §37 §5.4; D919 |
| T27-4 | **The in-context hand-off is a route, not a value**: `/studio?from=plane|glitchtip&title=…&prompt=…&branch=…&link=…`. Studio's `routeOf` checks it (known source, bounded text, git-safe branch, http(s) link) and the home shows a "New thread from …" card until the composer (Task 19) takes it. ItsAPlan's issue list has no description or link, so the Plane draft carries the key, title and priority and a branch named after the key. "Fix this error" reads a new read-only op, `glitchtip.latestEvent` (`latestIssueEvent`; numeric id only), for the stack (innermost frame first, 40 frames, 4000 chars) and release | No composer yet; plugins share routes |
| T27-5 | **`forgejo.issues` (`type: "pulls"`) now carries `head`**, and `thread/FactoryLinks.tsx`'s `BranchPr` finds a branch's open PR by it. Task 23 places it in the thread toolbar (there is no thread branch yet). **"Post result to Zulip" is not built**: the factory ops are read-only by test (`ops_are_read_only_names`), and a write needs §39 §8's approval; it follows with Task 13's approvals | Tasks 13, 19 and 23 have not landed |
| T27-6 | **Console-style pages keep their gutter in Studio**: `.lc-shell[data-mode="studio"] .lc-main .lc-page` restores the page padding Studio's full-bleed main removes. The factory's page titles are "Panels" and "App" so the breadcrumb reads Studio / Factory / Software Factory / Panels. The dev demo configures Plane with sample issues | Screenshots |
| T24-1 | **Plugins contribute palette rows through a `palette.command` slot, not a service.** An entry renders nothing; while the palette is open it is mounted with the query and calls `publish(commands)` (`PaletteCommand` in `@loams/slots`). The shell keeps the latest list per entry and re-renders only when a list's visible fields change | §53 §4.5 names the slot; a mounted entry can read server state with its own hooks (connect-query), and only while the palette is open | One render per entry per keystroke while open |
| T24-2 | **The mode scope** (T3-6): an entry's mode is its `meta.mode`, else its plugin's (the current mode when the plugin is in both). An entry whose `meta.mode` its plugin does not declare is dropped (§53 §4.1). The current mode's rows come first under their own headings ("Actions", "Git", "Threads", "Go to", "General"…), then each other available mode's under one heading, "Console" or "Studio". A plugin of a mode not yet entered is not loaded (T3-3), so its rows appear after that mode is first visited; the shell's own "Go to" rows cover both modes' nav from the start | §53 §4.2, §4.5 | — |
| T24-3 | **The ranking lives in `@loams/ui` (`command-rank.ts`), the row builders in `@loams/studio-ui` (`palette/logic.ts`)**, both adapted from `CommandPalette.logic.ts` with provenance rows. The shell cannot import `@loams/studio-ui` (Ruling 4) and ranks every row, so the ranking moved next to the `CommandPalette` primitive. It is upstream's rule for rule (`palette_ranks_like_upstream`); upstream's `>` prefix, submenus and directory browsing are not carried over | Ruling 4 | Two provenance rows for one upstream file |
| T24-4 | **The shell owns Mod+K, Mod+J and Mod+Shift+M in both modes; they are not Studio keybindings.** Studio's defaults move off them: upstream's `terminal.toggle` (Mod+J) is Ctrl+`, and upstream's Mod+Shift+M (model picker) is not bound. The keybinding editor flags a binding on a shell chord as a conflict ("Shell: Command Palette" and so on). "Switch to Console"/"Switch to Studio" is the shell's row, in both modes | One owner per chord across modes | The palette chord is not rebindable |
| T24-5 | **Studio's keybinding commands** are upstream's names where Studio has the command (`sidebar.toggle`, `navigation.back/forward`, `terminal.toggle`, `rightPanel.toggle`, `diff.toggle`, `preview.toggle`, `chat.new`) plus `files.toggle` (Mod+Shift+E) and `settings.open` (Mod+,). `when` flags: `terminalFocus` (`[data-terminal]`), `composerFocus` (`[data-composer]`), `editableFocus`, `threadOpen`, `isDesktop`, `isWeb`. Commands run through a bus (`StudioCommands`) where the mounted layout and view register handlers; a key press inside a modal (the palette, a dialog) is ignored. Terminal, composer, thread-jump and model-picker commands come with Tasks 17, 19 and 22 | Only what Studio can do today is bound | — |
| T24-6 | **Agentd rows go through `StudioPaletteSource`** (`threads`, `runGroups`, `searchFiles`, `gitActions`, `runGitAction`, `harnesses`); `pendingPaletteSource()` answers with no rows until Task 6's client fills it. Projects come from `useProjects` as elsewhere. Git actions only ask the source, which owns any confirmation (Task 23). Harness and model rows change `loams.studio.settings` (`writeSettings`) | Task 6 is in progress | The palette shows no threads, run groups, files or git rows until Task 6 |
| T24-7 | **`loams.studio.keybindings` stores only custom rules**, as upstream's `keybindings.json`: a command with custom rules loses its defaults, and Reset brings them back. Stored rows are checked (known command, parseable chord and `when`) and bad ones dropped; a blocked storage keeps the defaults on read and the change for the page on write (`keybindings_storage_failure_tolerated`). `shortcutModifierState.ts` is not vendored yet: its only use is the thread-jump hints of Task 17 | §53 §5.10 | — |
