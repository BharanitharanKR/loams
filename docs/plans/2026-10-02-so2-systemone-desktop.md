# SO2 — Loams SystemOne on the Desktop Implementation Plan

> **Re-targeted 2026-10-18: the desktop is in this repo.** This plan was written on 2026-10-02 for the zeron GPUI fork in a separate `ostrium-labs/loams-desktop` repository. That repository has been deleted. The desktop is now three things in `ostrium-labs/loams`: the Electron app `apps/desktop-electron`, the cordis shell `web/apps/console` with its Console and Studio modes, and the per-user daemon `loams-agentd` (design [§50](../design/50-loams-desktop-daemon.md), plan [DD1](2026-10-09-dd1-desktop-daemon.md); Studio: design [§53](../design/53-studio-ade.md), plan [SD1](2026-10-13-sd1-studio-ade.md)). Every task now targets this repository, with PRs into `dev`. The goals, the decisions D528–D530 and D538, and the intent of every test are kept. What changed is where the code goes, the interfaces it uses, the dependencies on other plans and the execution order. The choices made while re-targeting are in "Re-target rulings" below. Choices that change the text of a design decision are listed under "Owner questions" and are not applied to design 40 or the decision log; the PR that carries this re-target proposes the wording.

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task, **test first**. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, flags, field names, limits), use them verbatim. The code is not pre-written in this plan. Record every deviation in "Rulings made during execution" at the end of this file.

> **Status: Planned; re-targeted, awaiting the owner's approval** (written 2026-10-02, re-targeted 2026-10-18). **No SO2 code starts until the owner approves this re-target and answers the owner questions.** **Slot: track SO, second plan** (proposed; D528–D530, D538). Branches `so2-t<N>`, stacked, based on `dev`; PRs target `dev` of `ostrium-labs/loams`. Depends on [SO1](2026-10-02-so1-systemone.md) Tasks 1 to 10 (merged; SO1's Task 0 is being reconciled on branch `so1-t0`, and SO2's Task 0 reads its outcome), on DD1 and SD1 as built (see "Dependencies on other plans"), and on nothing in SF3 for Tasks 1 to 5.

**Goal:** Put SystemOne in Loams Desktop (the owner's request of 2026-10-02: "add this to our desktop app"):
- a **Swift Core ML sidecar** (`loams-coreml`) for Apple silicon, built on FluidInference's Apache-2.0 `FluidUse` library, shipped with the Electron app and **supervised by `loams-agentd`** (Re-target ruling R1);
- **SystemOne in the daemon** (`crates/loams-agentd-systemone`): host detection, the registry, model management and sidecar supervision, with the instance's `SystemOneService` as a second source;
- a **Decisions** settings section and a **Model Manager** in the cordis shell (download, verify, keep resident, delete, self-test, calibration status, the opt-in Cloud section), reading the daemon over `loams.agentd.v1` with connect-query;
- **Loams Bot** using decisions for routing, urgency, needs-person and an LLM pre-filter, shown on the thread's card in Studio and the Console agent dock;
- a read-only **`systemone_decide`** tool in agentd's MCP shim and tool registry, so every harness run and the native loop can use it;
- optionally, the **browser** plugin (a cordis plugin running `laya-ts` in a plain Web Worker), Linux and Windows CPU and NVIDIA paths, and the MLX adapter.

**Architecture:**
- **`apps/desktop-electron/sidecars/coreml/`** (SwiftPM executable `loams-coreml`, Swift 6, macOS 14+): depends on `FluidInference/FluidUse` pinned by revision (or the Laya files vendored, Task 0); serves `POST /v1/systemone` and `GET /health` in `laya-serve`'s shape over a Unix domain socket (mode 0600) with a per-launch bearer key. Built on the macOS arm64 job, placed in `resources/bin/` beside `loams` and `loams-agentd`, copied into the daemon's runtime directory by `loams-agentd install-runtime`, and started only by the daemon.
- **`crates/loams-agentd-systemone/`** (library, headless, Apache-2.0): depends on `loams-systemone` (SO1) as a library; owns `LocalSystemOne`, `CoremlSidecarBackend` (SO1's `LayaServeBackend` over a Unix socket), `RemoteBackend` (SO1's Connect client to the instance, through `loams-agentd-link`'s transport and token source), the supervisor wiring, residency, and a state model. The daemon (`crates/loams-agentd`) owns one `LocalSystemOne` for its lifetime.
- **Wire.** ndjson methods in `loams-agentd-proto` for the shims (`SystemOneDecide`, role `mcp` allowed) and for Electron main; a Connect service `loams.agentd.v1.SystemOneLocalService` in `proto/loams/agentd/v1/systemone.proto`, served by `loams-agentd-connect` beside the Studio services and reached from the renderer through Electron main's `loams-app://console/loams.agentd.v1.*` proxy (D908). The Connect messages import `loams.systemone.v1`'s types; nothing is redefined.
- **UI.** A cordis plugin `web/plugins/systemone` (`@loams/plugin-systemone`, editions `["desktop"]`, modes `["console", "studio"]`) contributes the Settings › Decisions section (`console.settings.section`) with the Model Manager, through connect-query hooks on the agentd transport. The decision chip on Loams Bot's cards is a presentational component in `@loams/studio-ui` (`chat/tools/`), used by Studio's thread view and the Console agent dock.
- **Loams Bot:** the ACP shim `loams-agentd loams bot-acp` (crate `loams-agentd-link`, `src/acp.rs` and `src/a2a/`) asks the daemon for decisions over ndjson before each A2A `SendMessage`; decisions travel as A2A message metadata and as ACP tool-call `_meta` for the card.
- **Agents:** `systemone_decide` is a tool of the injected MCP shim (`loams-agentd mcp`, crate `loams-agentd-mcp`) and, once DD1 Task 19 lands, a `read` tool in the native loop's `ToolRegistry`.
- **`web/plugins/systemone-local/`** (`@loams/plugin-systemone-local`): optional, unchanged in intent (D530).

**Tech Stack:**
- **Rust:** 1.97.1, edition 2024, workspace lints, the root workspace; `loams-systemone` (SO1); the daemon's crates as built; `connectrpc` and buffa as `loams-agentd-connect` uses them; `keyring` through DD1's `SecretStore`.
- **Swift:** Swift 6 with SwiftPM and Xcode, on the macOS arm64 runner only; `FluidUse` (Apache-2.0) and `FluidAudio` pulled transitively.
- **TypeScript:** the workspace toolchain (TypeScript 5.9.3, Node ≥ 22, pnpm, Biome, Vitest, Playwright), React 19, Electron as pinned by `apps/desktop-electron/package.json`. The owner's UI rulings hold: **Tailwind 4 with `@loams/ui` tokens only; Base UI primitives only as wrapped in `@loams/ui`; no Effect; `@connectrpc/connect-query` with TanStack Query for server state.** Lists use `@loams/ui`'s DataTable and VirtualList (TanStack Table and Virtual).
- **Packaging:** electron-builder with the hardened runtime and notarisation as `electron-builder.config.cjs` and `desktop-sign.yml` set up.
- **Browser plugin (optional):** cordis, `laya-ts` (Apache-2.0) and `onnxruntime-web` (MIT).

**Spec:**
- [`docs/design/40-loams-systemone.md`](../design/40-loams-systemone.md): §7.3, §7.4, §7.5, §8, §9.5, §12; [decision log](../design/13-decision-log.md) (D528–D530, D538, D537).
- [`docs/design/50-loams-desktop-daemon.md`](../design/50-loams-desktop-daemon.md): §3 (architecture), §4.1 (crates, `loams-agentd-link` kept for Loams Bot), §4.3 (headless guard), §5.2 (runtime directory), §7 (RPC security, roles, scoped run tokens), §10.4 to §10.6 (tools, approvals, harnesses), §12 (secrets), §15 (packaging).
- [`docs/design/53-studio-ade.md`](../design/53-studio-ade.md): §4.1 (manifest `modes`), §4.5 (what both modes share: settings, dock, notifications), §6.1 to §6.3 (`loams.agentd.v1`, the desktop proxy, the remote `studio` role), §8.3 (plugin trust tiers).
- [`docs/design/39-software-factory-and-loams-bot.md`](../design/39-software-factory-and-loams-bot.md) §5 (A2A), §8 (approvals, D-SF-9), §9 (D-SF-11), §13 (Loams Bot in the apps; its "zeron fork" paragraph is superseded by §50 and this plan's Ruling R6).
- [DD1](2026-10-09-dd1-desktop-daemon.md) (shared contracts, Tasks 9, 11, 14, 19, 22 and rulings T1-2, T3-1), [SD1](2026-10-13-sd1-studio-ade.md) (shared contracts, Tasks 4 to 6, 13, 14, 18), [SO1](2026-10-02-so1-systemone.md).
- Upstream, read in Task 0: `FluidInference/FluidUse` (`README.md`, `Package.swift`, `Sources/FluidUse`), the `FluidInference/laya-coreml` model card and `config.json`, `FluidInference/mobius` `models/computer-use/laya/coreml`.

## Global Constraints

- **Worktree.** One worktree per task under `loams-wt/` (for example `loams-wt/so2-t2`). Never commit in the main checkout. `git commit -s` (DCO). Never `git stash`.
- **Local first; cloud only by explicit opt-in.** The Cloud section is off by default; the Jev key lives in the OS keyring through the daemon's `SecretStore`, never in a file, the config, a reply or the renderer; a decision shows where it ran.
- **No TCP port for the Core ML sidecar.** A Unix socket in a 0700 directory under the daemon's data directory at mode 0600, and a per-launch bearer key passed on an inherited file descriptor. A non-loopback address is refused for every other sidecar (D111).
- **The sidecar never downloads.** Models arrive through the model manager (verified, pinned, consented). `LayaManager.load()`'s own download path is not used.
- **A decision is advisory (D537).** Loams Bot's use never approves, merges, deploys or answers an approval; a SystemOne route is a hint that a person or a gated agent can override. Nothing that holds a scoped run token can call `ResolveApproval` (DD1 Ruling 8).
- **No telemetry from the app.** The app and the daemon send no usage data to Loams or anyone else; the decision log is local, opt-in and off by default. When signed in, A2A trace metadata (`systemone.backend`, `systemone.latency_ms`, no content) goes to the operator's own collector like every other span (§39 D-SF-11), and the instance's own `io.loams.dev.systemone.decided.v1` event is the operator's.
- **Credentials:** no Loams token, provider key or Jev key is passed to the sidecar, the browser plugin or an agent subprocess (§37 D489, §50 §12). The injected MCP tool and the Loams Bot shim carry only the daemon's scoped run token (role `mcp`).
- **The renderer never sees the daemon token** (§50 §7). The renderer reaches SystemOne only through `loams.agentd.v1` methods whose ndjson twin is `renderer: true`. The remote `studio` role (§53 §6.3) never reaches the Cloud key methods or model downloads.
- **Headless.** `loams-agentd-systemone` adds no GUI crate; `scripts/ci/agentd-deps.sh` must still pass.
- **Wire changes** go through `crates/loams-agentd-proto` (and `cargo test -p loams-agentd-proto export_bindings`, `ts_bindings_are_fresh`) and through `proto/loams/agentd/v1/` with `buf lint` and `buf breaking`, then the regenerated Electron allowlist (`allowlist.gen.ts`).
- **The app works at every merge.** Each PR that touches `apps/desktop-electron` or `web/` runs `pnpm nx run-many -t test typecheck -p loams-desktop-electron @loams/console plugins`, `pnpm --filter @loams/console build`, and the Linux Playwright smoke.
- **The build machine.** One cargo build at a time, the shared target directory, `-p <crate>` only. Swift builds, Core ML tests and notarisation only on the macOS arm64 runner. Model downloads only in Task 0, by hand, or in the labelled runner, never in unit tests.
- **Names:** `loams-*` crates, `LOAMS_*` env vars, `loams.systemone.v1`, `loams.agentd.v1`.
- **Commit areas:** `systemone`, `coreml`, `agentd`, `desktop`, `web`, `bot`, `plugin`, `ci`, `docs`.

## Rulings made while writing this plan (2026-10-02, kept)

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The Core ML sidecar speaks `laya-serve`'s JSON on a Unix socket**, so the Rust side is one `LayaServeBackend` with two transports | One client, one fixture set, one self-test | A little HTTP framing overhead (microseconds) |
| 2 | **Models are loaded from paths we manage; the sidecar never fetches** | Integrity and consent belong to one place (SO1 Task 9) | Needs `LayaManager` to accept paths (Task 0 checks; vendoring is the fallback) |
| 3 | **On Apple silicon, English text uses the multilingual checkpoint in v1** | Core ML ships only that; MLX is optional (Q522) | English accuracy 0.657 against 0.783 (upstream MASSIVE) until fine-tuned |
| 4 | **Default Core ML buckets 128 and 512**, others on demand | What `LayaManager.load()` loads; about 614 MB per fp16 bucket (int8-embedding variants smaller if accuracy matches) | A 700-token state truncates on the 512 bucket until 1024 is pulled; the UI says so |
| 5 | **Lazy residency with idle stop is the default; "keep resident" is a setting** | Memory on 8 GB laptops; the daemon outlives the window, so a resident sidecar would hold memory with no window open | First decision after idle is slower (bucket load) |
| 6 | **Loams Bot's default `min_confidence` is 0.6** until a calibration table exists | Raw confidences are over-confident, so 0.6 is conservative, not calibrated | Some obvious routes fall through to the LLM; harmless |
| 7 | **The browser plugin is optional and not in the first release** | The server route covers the browser; COOP/COEP conflicts with framed apps (Q530) | None |

## Re-target rulings (2026-10-18)

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| R1 | **The Swift Core ML sidecar stays a separate binary, `loams-coreml`, supervised by `loams-agentd` on Apple silicon.** Source in `apps/desktop-electron/sidecars/coreml/`; built only by the macOS arm64 job; shipped in `resources/bin/` (so in `Contents/Resources/bin/`) on macOS arm64 builds only; copied with the other binaries into `<userData>/agentd/runtime/<version>/` by `install-runtime`, hash-checked against `SHA256SUMS`; started, health-gated, restarted and stopped only by the daemon. FluidUse (Apache-2.0) is pinned by revision or its Laya files vendored with the notice, as D528 says | Core ML needs Swift and the daemon is Rust; a separate process keeps the ANE runtime's memory and crashes out of the daemon, and the daemon already owns every other child (§50 §3). One supervisor, one restart policy, one runtime directory and one upgrade path | A second notarised Mach-O to sign; Task 0 checks that a binary copied out of the bundle into the runtime directory still passes Gatekeeper and loads Core ML |
| R2 | **SystemOne lives in a new daemon crate `crates/loams-agentd-systemone`, not in `loams-agentd-link`.** `loams-agentd-link` stays the Connect client, Authentik and Loams Bot crate (§50 §4.1); the new crate depends on it only for the `RemoteBackend`'s transport and token source | The daemon is the one long-lived process on the desktop; the shims and the UI are its clients. Keeping it out of the link crate keeps the bot shim binary path small and the link crate's purpose single | One more `loams-agentd*` crate (sixteen) |
| R3 | **Two wires, one implementation.** ndjson methods for the shims and Electron main (`SystemOneDecide`, `SystemOneState`, `WatchSystemOne`, and the management methods), and `loams.agentd.v1.SystemOneLocalService` on the Connect listener for the renderer, both calling `LocalSystemOne` | DD1 Ruling 2 (ndjson for main, the MCP shim and the CLI) and D907 (Connect for pages, connect-query); the Electron allowlist requires an ndjson twin per Connect method | Two method lists to keep equal; SD1's parity suite pattern covers it |
| R4 | **The daemon does not serve `loams.systemone.v1.SystemOneService` itself.** The local Connect surface is `loams.agentd.v1.SystemOneLocalService`, whose messages import `loams.systemone.v1`'s | Electron's proxy forwards only the `/loams.agentd.v1.` prefix (D908); a second prefix would widen the renderer's reach | A client that wants the instance's exact service on the desktop uses the active server's transport, not the daemon |
| R5 | **The UI is a cordis plugin, `@loams/plugin-systemone`**, editions `["desktop"]`, modes `["console", "studio"]`, contributing Settings › Decisions through `console.settings.section` (Model Manager, backends, residency, self-test, calibration, Cloud). Server state through connect-query on the agentd transport; components from `@loams/ui` only; Tailwind 4 token utilities; no Effect | The GPUI `loams-panels` crate is gone; §53 §4.5 makes Settings shared by both modes; the owner's UI rulings | A web edition gets no Decisions page in SO2 (the instance's route has its own CLI and status) |
| R6 | **Loams Bot's hooks move to where Loams Bot runs on the desktop today:** the ACP shim `loams-agentd loams bot-acp` (`crates/loams-agentd-link/src/{acp.rs,bot/decisions.rs,a2a/}`) for decisions and A2A metadata; the card chip in `@loams/studio-ui` (`chat/tools/`), rendered by Studio's thread and the Console agent dock; `needs_person` raises the priority of the daemon's OS notification and Studio's toast (SD1 Task 14). `loams-harness-bot` and `loams-ui-bot` (§39 §13) do not exist and are not created | §50 §4.1 and DD1 rulings T1-2 and T3-1 keep Loams Bot as the ACP harness inside the daemon's binary; §53 §4.5 puts thread cards and notifications in the shared shell | If SF3 moves the desktop to `loams.bot.v1` directly, `decisions.rs` moves with the dispatch point; its tests move unchanged |
| R7 | **The Loams Bot shim calls the daemon with a scoped run token.** The daemon gives the Loams Bot harness process the same per-run token lease it gives the injected MCP shim (`LOAMS_AGENTD_RUN_TOKEN`, `LOAMS_AGENTD_PORT`, `LOAMS_AGENTD_SESSION_ID`), role `mcp`, bound to the session. Task 0 checks whether it already does | The shim is the daemon's own binary; role `mcp` reaches `SystemOneDecide` and never `ResolveApproval`, which is also the runtime half of `decision_cannot_approve` | If the token leaks from the shim it grants only the `mcp` method set for one session, as for the MCP shim today |
| R8 | **`systemone_decide` goes into `loams-agentd-mcp`'s tool catalogue now and into DD1's `ToolRegistry` when DD1 Task 19 lands**, as a `read` tool. It calls `SystemOneDecide` over ndjson and never reaches the network itself | "Beside `zeron mcp`" now means agentd's injected MCP shim (§50 §10.6); the native loop gets the same tool through its registry | Two registrations; one shared definition file keeps the name, description and schema equal |
| R9 | **The Jev key is a `SecretStore` entry, service `dev.loams.agentd`, account `systemone:jev`** (DD1 Task 22), set through a write-only `ConfigureSystemOneCloud` (role `owner`, `renderer: true`, never in the `studio` role) that returns `hasKey` and `persistent` | One keyring service for the daemon (§50 §12), the same memory fallback and the same canary test | Without a keyring the key is kept in memory only and must be re-entered after a daemon restart; the UI says so |
| R10 | **The model cache is the daemon's:** SO1's §9.5 layout rooted at `<userData>/agentd/models/systemone/<backend>/<checkpoint>/<revision>/`. The socket directory is `<userData>/agentd/run/systemone/` (0700); if the full socket path exceeds 100 bytes (macOS `sun_path` is 104), it falls back to a 0700 directory `loams-<uid>` under the user's `$TMPDIR` | The daemon owns its data directory (DD1 shared contracts); `~/Library/Application Support/…` paths can exceed `sun_path` | Two possible socket locations; both are tested |
| R11 | **The daemon's `config.toml` gains `[systemone]`** (`enabled = true`, `residency = "lazy"`, `idle_minutes = 10`, `prefer = "local"`, `log = "off"`, `log_state = false`, `quick_replies = false`); the Cloud switches are not in the file (they live with the key's consent record in the store) | Settings in one place the daemon reads at start and `SetConfig` updates | None |
| R12 | **UI tests replace GPUI snapshot tests**: Vitest and Testing Library for components and states, and Playwright screenshots of each section in both themes in the desktop e2e | GPUI is gone; same intent (every status renders) | None |

## Owner questions

Each changes the text of a design decision. Nothing here is applied to design 40 or the decision log; the PR body proposes the wording.

| # | Question | Recommendation |
|---|---|---|
| SO2-Q1 | **D528 (where the sidecar lives and who launches it).** D528 puts `loams-coreml` in `loams-desktop/sidecars/coreml/`, launched by `loams-link`, in `Contents/MacOS/` or `Contents/Helpers/`. Accept R1: source in `apps/desktop-electron/sidecars/coreml/`, shipped in `Contents/Resources/bin/`, copied to the daemon's runtime directory and supervised by `loams-agentd`; socket under the daemon's data directory instead of `$LOAMS_HOME/run/`? | **Yes.** It is the default the owner named, it keeps D528's substance (Swift, FluidUse pinned, Unix socket, per-launch key, no TCP, notarised) and changes only the location and the supervisor |
| SO2-Q2 | **D538 (the host of the desktop integration).** D538 says the zeron fork links `loams-systemone` through `loams-link`, the Decisions panel is GPUI, and `systemone_decide` is injected like `zeron mcp`. Accept R2, R5, R6 and R8: the daemon hosts the registry in `loams-agentd-systemone`, the panel is a cordis Settings section, Loams Bot's hooks are in the ACP shim and the shared card, and the tool is in agentd's MCP shim and registry? | **Yes.** Every behaviour in D538 is kept; only the hosts change. §37 §18.3's "do not embed the engine" still holds: the daemon links the client crate, not the engine |
| SO2-Q3 | **One registry per desktop.** The daemon also supervises `loams dev`, whose SystemOne route SO1 Task 10 turns on by default. Two registries would mean two model caches and possibly two sidecars on one laptop. Should the daemon start the supervised engine with its SystemOne route off, so the daemon is the desktop's only SystemOne host? | **Yes**, with a supervisor flag set by `loams-agentd-supervisor` (DD1 Task 14) and documented in design 40 §8. `loams dev` run by hand keeps SO1's default |
| SO2-Q4 | **The instance as the second source needs a daemon sign-in.** D538's `RemoteBackend` needs a Loams token in the daemon; that is `loams-agentd-link`'s Authentik sign-in, still open as Q703. Ship local-only first and add the `RemoteBackend` when Q703 is answered? | **Yes.** Task 2 builds the backend and its tests against a fake token source; it is registered only when the link reports a signed-in principal, so it is inert until Q703 lands |
| SO2-Q5 | **D538's "notification priority on phones"** has no desktop path. On the desktop `needs_person` raises the OS notification and Studio toast priority only (R6). Leave the phone policy to the mobile and SF3 plans? | **Yes**; record it as out of SO2's scope in design 40 §8 |

## Dependencies on other plans

| Plan and task | Needed by | State on 2026-10-18 | If not ready |
|---|---|---|---|
| SO1 Tasks 1–10 (`loams-systemone`, `loams.systemone.v1`, `LayaServeBackend`, `ModelManager`, self-test, calibration, MCP tool shape) | 0 (reads), 2–11 | Planned; SO1 Task 0 being reconciled on `so1-t0` | SO2 waits; Task 0 may start with SO1's as-planned API and is redone if `so1-t0` changes it |
| SO1 Task 12 (in-process backend) | 9 (Linux CPU) | Spike-gated | Task 9 uses the `laya-serve` installer on CPU |
| DD1 Tasks 6–9 (discovery, token, roles, method table, ts-rs) | 2, 3 | Built | — |
| DD1 Task 11 (binaries ship in `resources/bin/`, `install-runtime`) | 1 | `install-runtime` and the runtime directory built (`runtime_install.rs`); Task 0 confirms the bundle step | Task 1 adds the macOS arm64 copy step itself |
| DD1 Task 14 (`loams-agentd-supervisor`) | 2 (restart policy), SO2-Q3 | Not built | Task 2 uses SO1's `sidecar.rs` supervisor with the same policy and moves onto DD1's when it lands |
| DD1 Task 19 (`ToolRegistry`) | 7 (native loop half) | Not built | Task 7 lands the MCP half; the registry half is a follow-up commit in DD1 Task 19's PR or after it |
| DD1 Task 22 (`SecretStore`) | 5 (Cloud) | Not built | Task 5 lands everything but the Cloud section, which follows DD1 Task 22 |
| SD1 Tasks 4–6 (`loams.agentd.v1`, the Connect listener, the desktop proxy, the TS client and parity suite) | 3 | Merged | — |
| SD1 Tasks 14, 18 (notifications; chat and tool cards in `@loams/studio-ui`) | 6 | Merged | — |
| CF1 (Settings region, shell regions) | 4, 5 | Planned | The section uses `console.settings.section` as today and follows CF1's Settings when it lands |
| SF3 (server-side Loams Bot), SF2/SF5 (the A2A agents) | 11 (live routing demo only) | Planned | Tasks 6 and 11 use a fake A2A server, as the shim's tests already do |
| Q703 (daemon sign-in) | 2 (`RemoteBackend` registration) | Open | SO2-Q4 |

## Execution order

| Step | Tasks | Notes |
|---|---|---|
| 1 | 0 | First. Gates Task 1's FluidUse choice and records every check |
| 2 | 1 and 2 in parallel | 1 is Swift on the macOS runner; 2 is Rust on Linux with a fake sidecar |
| 3 | 3 | The wire for the UI and the shims |
| 4 | 4, 5, 6, 7 in parallel | 4 and 5 are UI; 6 is the bot shim and card; 7 is the MCP tool. 5's Cloud section waits for DD1 Task 22 |
| 5 | 8, 9, 10 | Optional or conditional: 8 on Q530, 9 on host demand, 10 on Q522 |
| 6 | 11 | Last: the exit gate and the docs |

## Carried in

SO1 as built (the crate API, the wire, the lock, the self-test); DD1 as built (the daemon, its roles and scoped run tokens, the runtime directory, the MCP shim, the Loams Bot ACP harness and shim); SD1 as built (`loams.agentd.v1`, the desktop proxy and its allowlist, `@loams/studio-ui`, notifications).

## Review Focus

1. **The sidecar is not reachable by anyone else.** Tests: Task 1 (`socket_mode_0600`, `wrong_key_is_401`, `no_tcp_listener`), Task 2 (`socket_path_fits_sun_path`).
2. **Nothing downloads behind the user's back.** Tests: Task 4 (`no_network_without_consent`), Task 3 (`studio_role_cannot_pull_models`).
3. **Cloud is opt-in everywhere.** Tests: Task 5 (`cloud_off_by_default`, `key_only_in_keychain`), Task 3 (`configure_cloud_reply_has_no_key`, `studio_role_cannot_configure_cloud`).
4. **Decisions never gate approvals.** Tests: Task 6 (`decision_cannot_approve`), Task 3 (`mcp_role_can_decide_but_not_resolve_approval`).
5. **Memory is bounded and visible.** Tests: Task 2 (`idle_stop_releases_sidecar`).
6. **The daemon stays headless and the renderer stays token-free.** Tests: Task 2 (`agentd-deps.sh` passes), Task 3 (`renderer_allowlist_matches_twins`).

## File structure

```
apps/desktop-electron/sidecars/coreml/{Package.swift,Package.resolved,Sources/loams-coreml/*.swift,Tests/*,NOTICE}
apps/desktop-electron/scripts/build-coreml.mjs          # macOS arm64: swift build, copy to resources/bin, SHA256SUMS
apps/desktop-electron/{electron-builder.config.cjs,build/entitlements.mac.plist}
crates/loams-agentd-systemone/{Cargo.toml,src/{lib,local,coreml,remote,state,residency,config,questions}.rs,tests/*}
crates/loams-agentd/src/{server.rs,config.rs}           # owns LocalSystemOne; [systemone] config
crates/loams-agentd-proto/src/systemone.rs  fixtures/systemone_*.json
crates/loams-agentd-connect/src/systemone.rs
proto/loams/agentd/v1/systemone.proto
apps/desktop-electron/src/main/agentd/allowlist.gen.ts  # regenerated
crates/loams-agentd-link/src/{acp.rs,bot/{mod,decisions}.rs,a2a/types.rs}
crates/loams-agentd-sessions/src/sessions.rs            # run token for the Loams Bot harness (R7), if Task 0 finds it missing
crates/loams-agentd-mcp/src/{tools.rs,systemone.rs}
web/plugins/systemone/{package.json,src/{index.tsx,decisions/*,models/*,cloud/*,selftest/*},test/*}
web/packages/studio-ui/src/chat/tools/{decision-chip.tsx,presentation.ts}
web/plugins/systemone-local/{package.json,src/*,worker/*}   # optional, Task 8
.github/workflows/desktop-electron.yml                  # the coreml job (macOS arm64)
docs/plans/so2-systemone-spike.md  docs/design/40-loams-systemone.md (Task 11, as built)  docs/guides/desktop/decisions.md
THIRD_PARTY_NOTICES.md  apps/desktop-electron/NOTICE
```

### Task 0: Reconcile with the code as built and check the Swift side

**Files:** read SO1 as merged (and the outcome of `so1-t0`); `crates/loams-agentd*` as on `dev`, in particular `runtime_install.rs`, the scoped run token in `loams-agentd-sessions/src/sessions.rs`, the Loams Bot harness in `loams-agentd-harness/src/acp/loams_bot.rs`, `loams-agentd-link/src/{acp.rs,a2a/}`, `loams-agentd-mcp/src/tools.rs`, `loams-agentd-connect`; `apps/desktop-electron/{electron-builder.config.cjs,scripts/,src/main/agentd/}`; `web/plugins/{studio,desktop-settings,desktop-agentd}`; `web/packages/studio-ui/src/chat/tools/`; FluidUse's `Package.swift` and `Sources/FluidUse`; the `laya-coreml` card and `config.json`. Write `docs/plans/so2-systemone-spike.md`.

**Checks** (record commands, dates and the `dev` commit):
- **Code as built:** does the Loams Bot harness process receive a scoped run token (R7)? does `a2a::Message` carry `metadata`? how does ACP `_meta` on a tool call reach `@loams/studio-ui`'s tool presentation? which DD1 and SD1 tasks have landed since 2026-10-18 (update the dependency table)? does `loams-systemone` build inside the daemon's tree without duplicate versions (`cargo tree -d -p loams-agentd`), and does `scripts/ci/agentd-deps.sh` still pass with it?
- **Swift** (on a Mac): which FluidUse revision to pin; its Swift tools version (6.0) against the runner's Xcode; **can the `FluidUse` library product be consumed alone** (does it drag FluidAudio and resources unacceptably); whether `LayaManager.Configuration` can load a bucket from a directory we manage; if not, vendor only the Laya files (Apache-2.0, notice kept) and record the list.
- **On Apple silicon:** latency for 1, 5 and 10 questions at buckets 128, 512 and 1024 (fp16 and int8-embedding), peak and idle memory, first-load time, compute-unit settings, and the 32-option error behaviour. Compare answers with SO1 Task 0's (`laya-serve` on the same inputs) on the 16-case self-test and 200 decisions; the max abs probability difference.
- **Signing:** does a SwiftPM executable in `Contents/Resources/bin/` sign and notarise with electron-builder's hardened runtime and `build/entitlements.mac.plist`, with no extra entitlements; does the copy in `<userData>/agentd/runtime/<version>/` still run and load `.mlmodelc` bundles when launched by the daemon (Gatekeeper, quarantine, library validation)?
- **Socket path:** the length of `<userData>/agentd/run/systemone/coreml.sock` on a default macOS profile against `sun_path` (R10).

**Output:** the spike doc; the pinned FluidUse revision or the vendored file list; the R7 and metadata findings; an updated dependency table in this plan.

**Commit:** `docs: SO2 spike results`.

### Task 1: The Core ML sidecar

**Files:** `apps/desktop-electron/sidecars/coreml/*`, `apps/desktop-electron/scripts/build-coreml.mjs`, `electron-builder.config.cjs` (the binary on macOS arm64 only), `.github/workflows/desktop-electron.yml` (a `coreml` job on the macOS arm64 runner), `THIRD_PARTY_NOTICES.md` and `apps/desktop-electron/NOTICE` (FluidUse, FluidAudio, Laya).

**Produces:** the `loams-coreml` executable with `--socket <path> --key-fd <fd> --models <dir>`; `POST /v1/systemone` and `GET /health` per design §7.3; `resources/bin/loams-coreml` and its line in `SHA256SUMS` on macOS arm64 builds.

**Semantics:**
- The key arrives on an inherited file descriptor (never argv or the environment); the socket is created at mode 0600 in a 0700 directory and removed on exit; no TCP.
- Loads the buckets present in `--models` (128 and 512 by default), picks the smallest that fits and truncates on the right in the largest, and reports `usage.truncated`, `state_tokens`, `state_tokens_dropped`.
- A `choice` over 32 options answers `422` with detail `options_over_32`; more than 64 questions, 32 levels, or a body over 2 MiB follow `laya-serve`'s codes (`413`, `422`).
- `noul` native with a `choice_ab` mode on request (a hidden `noul_mode` field); `score` and `choice` as in the fixtures.
- Answers carry `confidence` and `answer_confidence` computed as `laya-serve` does; `action.act_probability` is **not** emitted.
- `/health` answers `{"status":"ok"}` to everyone and the full payload (loaded buckets, compute units, revisions) only with the key.
- Exits on a closed key fd (the daemon died).
- Linux, Windows and macOS x64 builds do not contain the binary, and `install-runtime` does not expect it there.

**Tests:** XCTest and a script: `choice_matches_golden_fixture` (SO1 Task 2's fixtures where applicable, probabilities within 0.02 of the PyTorch reference rows recorded in Task 0); `score_and_noul_shapes`; `over_32_options_is_422`; `truncation_reported`; `bucket_selection_smallest_fit`; `wrong_key_is_401`; `socket_mode_0600`; `no_tcp_listener` (`lsof`-style check); `exits_when_parent_dies`; `act_probability_absent`; `health_without_key_is_status_only`. Packaging: `coreml_only_in_macos_arm64_bundle` (a Vitest over the builder config), `sha256sums_lists_coreml`. CI on a macOS 14+ Apple silicon runner; skipped (not failed) with a notice where the runner is unavailable.

**Commit:** `coreml: Core ML sidecar for Laya on Apple silicon`.

### Task 2: `loams-agentd-systemone`

**Files:** `crates/loams-agentd-systemone/*`; `crates/loams-agentd/src/{server.rs,config.rs}` (owns one `LocalSystemOne`; the `[systemone]` table, R11).

**Produces:** `LocalSystemOne` (owns SO1's `Registry`; registers `CoremlSidecarBackend` on macOS aarch64 when `loams-coreml` is in the runtime directory, `LayaServeBackend` supervisors on NVIDIA and CPU hosts when installed, `RemoteBackend` when `loams-agentd-link` reports a signed-in principal (SO2-Q4), `JevBackend` only when enabled) with a `watch::Receiver<SystemOneState>` (backends, statuses, active `route_reason`, residency, memory); `decide()`, the one entry point for the daemon, which builds the `DecideRequest`, calls the registry and returns the response with `loams.*` provenance.

**Semantics:**
- `CoremlSidecarBackend` = SO1's `LayaServeBackend` over a Unix socket: spawn `loams-coreml` from `runtime/current/`, pass the key by fd, health-gate, restart with backoff 1 s to 30 s and at most five tries in ten minutes, then fail closed with a status the UI shows; kill on daemon drain and exit; the child is in the daemon's process group or job object.
- Socket directory per R10.
- `RemoteBackend`: SO1's Connect `SystemOneService` client using `loams-agentd-link`'s transport and token source; locality is the instance's (reported as `remote`); used when no local backend is ready or when `prefer = "instance"`.
- Residency: `lazy` (default; start on first use, stop after `idle_minutes`, default 10) or `resident`.
- No GUI dependency; `scripts/ci/agentd-deps.sh` passes.

**Tests:** `macos_arm64_registers_coreml`; `coreml_absent_from_runtime_is_not_installed`; `nvidia_registers_torch_when_installed`; `signed_out_has_no_remote_backend`; `remote_used_when_no_local_ready`; `local_preferred_when_ready`; `lazy_start_and_idle_stop` (`idle_stop_releases_sidecar`, fake sidecar, paused clock); `resident_mode_keeps_running`; `sidecar_crash_restarts_then_fails_closed`; `drain_stops_sidecar_and_removes_socket`; `socket_path_fits_sun_path` (the fallback is taken above 100 bytes); `state_stream_updates_on_status_change`; `decide_returns_provenance`; `config_table_defaults`. Platform-specific tests are `cfg`-gated.

**Commit:** `agentd: SystemOne in the daemon with the Core ML sidecar and the instance`.

### Task 3: The wire: ndjson methods and `loams.agentd.v1.SystemOneLocalService`

**Files:** `crates/loams-agentd-proto/src/systemone.rs` and fixtures, the method table; `proto/loams/agentd/v1/systemone.proto`; `crates/loams-agentd-connect/src/systemone.rs`; `crates/loams-agentd/src/server.rs` (dispatch); `apps/desktop-electron/src/main/agentd/allowlist.gen.ts` (regenerated); `web/packages/agentd-client/src/gen/` (regenerated).

**Produces** (ndjson name, role, `renderer`; each with a Connect twin of the same name in `SystemOneLocalService` unless marked):
- `SystemOneDecide` (owner and mcp, yes): a `loams.systemone.v1.DecideRequest` with `locality` forced to `LOCAL_ONLY` for role `mcp`;
- `SystemOneState`, `WatchSystemOne` (owner, yes);
- `SystemOneModels`, `WatchSystemOneModels`, `SystemOnePullModel`, `SystemOneVerifyModel`, `SystemOneRemoveModel` (owner, yes);
- `SystemOneSelfTest`, `SystemOneCalibrate`, `SystemOneCalibrationStatus` (owner, yes);
- `SystemOneSetSettings` (owner, yes): residency, idle minutes, prefer, log settings, quick replies;
- `ConfigureSystemOneCloud` (owner, yes; write-only, R9), `SystemOneCloudStatus` (owner, yes).
- The remote `studio` role (§53 §6.3) gets `SystemOneDecide`, `SystemOneState`, `WatchSystemOne`, `SystemOneModels` and `SystemOneCalibrationStatus` only.

**Semantics:** both wires call `LocalSystemOne`; errors use the daemon's reasons plus `systemone_unavailable`, `model_consent_required`, `cloud_not_allowed`, mapped to Connect codes as SD1 maps the others; no state, question or answer text appears in an error, a log line or a span.

**Tests:** `ts_bindings_are_fresh`; `method_table_roles` (the list above); `renderer_allowlist_matches_twins`; `connect_equals_ndjson` (SD1's parity pattern over a fake registry); `mcp_role_can_decide_but_not_resolve_approval`; `mcp_role_decide_is_local_only`; `studio_role_cannot_pull_models`; `studio_role_cannot_configure_cloud`; `configure_cloud_reply_has_no_key`; `no_state_in_spans_or_errors`; `buf lint` and `buf breaking`.

**Commit:** `agentd: SystemOne methods on ndjson and loams.agentd.v1`.

### Task 4: The Model Manager

**Files:** `web/plugins/systemone/src/{index.tsx,models/*}`, `web/plugins/systemone/test/*`; `crates/loams-agentd-systemone/src/models.rs` glue.

**Produces:** the `@loams/plugin-systemone` plugin (editions `["desktop"]`, modes `["console", "studio"]`) and, inside Settings › Decisions, a Model Manager listing every artefact in SO1's lock for this host (Core ML buckets, `laya-serve` checkpoints), with size, licence, status (not installed, downloading with progress and resume, verifying, installed, corrupt), actions (download, verify, delete), total disk used; and a first-run prompt on the first decision attempt that needs a model ("Download 1.2 GB of models for local decisions?"), raised by the daemon as `model_consent_required` and shown by the plugin as a `@loams/ui` dialog.

**Semantics:** uses SO1 Task 9's `ModelManager` in the daemon (the consent callback is answered by `SystemOnePullModel`); a download runs in the daemon and survives closing the window; stopping the daemon pauses it and it resumes at the next start; errors name the file and the fix, not the URL's query; `LOAMS_OFFLINE=1` in the daemon's environment hides the download actions. Server state through connect-query hooks with TanStack Query; the list is `@loams/ui`'s DataTable; Tailwind 4 token utilities only; loading, error and empty states.

**Tests:** `no_network_without_consent` (Rust); `progress_and_resume_reflect_manager_state`; `download_survives_window_close` (the daemon keeps pulling with no client); `corrupt_file_shown_and_redownloadable`; `delete_removes_revision_dir`; `first_run_prompt_shows_total_size`; `offline_hides_downloads`; Vitest component tests of the list for each status; the manifest validator accepts `modes`.

**Commit:** `web: Model Manager for local decisions`.

### Task 5: The Decisions settings and the Cloud section

**Files:** `web/plugins/systemone/src/{decisions,cloud,selftest}/*`; `crates/loams-agentd-systemone/src/cloud.rs` over DD1's `SecretStore`.

**Produces:** Settings › Decisions: the active backend and its `route_reason`; a backend table with status and the one-line fix; residency and idle minutes; log decisions (off/local) and `log_state`; **Run self-test** (result per case id, no state shown); calibration status per type with the table id and "calibrate from my corrections" (starts SO1's `calibrate` in the daemon); **Cloud decisions**: off by default, a Jev key field sent once through `ConfigureSystemOneCloud` and stored in the keyring (R9), the consent text (design §10: what is sent, TypeSafe's published no-training statement, that no API terms were found, that the model is closed), a separate switch "allow cloud fallback", and the instance policy line when the org forbids it. A "not persistent" note when the keyring is unavailable.

**Semantics:** enabling Cloud requires typing the key and ticking the consent; the key is validated with a single minimal request (a one-question `noul` on a fixed neutral string) only after consent; disabling deletes nothing but turns the backend inert; org policy from `GetInstance` (when signed in, SO2-Q4) can force-disable. The key never returns to the renderer.

**Tests:** `cloud_off_by_default`; `key_only_in_keychain` (nothing in `config.toml`, the store, logs, replies or the UI state dump; the daemon's canary test covers the Jev key); `consent_required_before_validation_request`; `org_policy_disables_cloud`; `self_test_result_has_no_state`; `route_reason_displayed`; `calibration_status_per_type`; `memory_fallback_shown_when_no_keyring`; Vitest component tests of each section.

**Commit:** `web: Decisions settings, self-test and opt-in Cloud section`.

### Task 6: Loams Bot uses decisions

**Files:** `crates/loams-agentd-link/src/{bot/mod.rs,bot/decisions.rs,acp.rs,a2a/types.rs}` (the A2A message metadata builder); `crates/loams-agentd-sessions/src/sessions.rs` (the run token for the Loams Bot harness, R7, only if Task 0 found it missing); `web/packages/studio-ui/src/chat/tools/{decision-chip.tsx,presentation.ts}`; the notification priority hook of SD1 Task 14.

**Produces:** four decisions available to the shim, with fixed question definitions in code (versioned, so fine-tunes bind to them):
- `route.agent` (`choice`: `zulip`, `plane`, `forgejo`, `glitchtip`, `analytics`, `none`; descriptions from each agent's Agent Card);
- `urgency` (`score`: `routine`, `soon`, `now`, `critical`);
- `needs_person` (`noul`: does this need a person's attention before any action);
- `needs_llm` (`noul`: does this message need a language model at all; false for acknowledgements and thanks).

**Semantics:** before each A2A `SendMessage`, the shim calls the daemon's `SystemOneDecide` over ndjson with its scoped run token, `min_confidence = 0.6` (Ruling 6) and `locality = LOCAL_ONLY`. A confident route sets the A2A `to` hint, and the ACP tool call for the task carries `_meta.systemone` so the card shows "routed to forgejo, 0.91, local" in Studio's thread and the Console agent dock; a `low_confidence` or absent decision sends the message unrouted for Loams Bot to decide, as today. `needs_person` raises the priority of the daemon's OS notification and Studio's toast only (never auto-answers). `needs_llm = false` with confidence above 0.9 may short-circuit to a canned acknowledgement only if the user enabled "quick replies" (`[systemone] quick_replies`, off by default) **and** an independent rule-based check agrees (the message is under 40 characters, is not a question and has no `?`, and matches a per-language allowlist of thanks and acknowledgements); otherwise the message goes on to the LLM. The decision, backend, calibrated confidence and latency go into A2A metadata (`systemone.*`, no content). **No decision can create, approve or answer an approval, a question or a destructive skill.** If the daemon is unreachable or SystemOne is unavailable, the shim behaves exactly as before.

**Tests:** `confident_route_sets_hint_and_card` (Rust, plus a Vitest of the chip from `_meta`); `low_confidence_sends_unrouted`; `unavailable_behaves_as_today`; `decision_cannot_approve` (the approval path is unreachable from `bot/decisions.rs`, checked by a compile-time module-visibility test, plus the runtime test that the shim's scoped token is refused by `ResolveApproval`); `needs_person_only_raises_priority`; `quick_replies_off_by_default`; `canned_reply_needs_independent_check` (a model-confident `needs_llm = false` on a question still goes to the LLM); `metadata_has_no_content`; `questions_are_versioned` (a snapshot of the four definitions); `run_token_is_session_bound` (if R7 needed a change).

**Commit:** `bot: route, urgency, needs-person and pre-filter decisions`.

### Task 7: The `systemone_decide` tool for agents

**Files:** `crates/loams-agentd-mcp/src/{tools.rs,systemone.rs}`; a shared definition (`name`, description, JSON Schema) in `crates/loams-agentd-systemone/src/tool.rs`; after DD1 Task 19, its registration in `loams-agentd-loop`'s `ToolRegistry` as a `read` tool.

**Semantics:** read-only; local-only (refuses `ALLOW_CLOUD`); listed only when a local backend is ready (`SystemOneState`); question limits as SO1's; the description tells the agent that it is a fast classifier that can be wrong and must not authorise actions; the MCP shim reaches it only through `SystemOneDecide` with its scoped token, so it has no network path of its own. Mirrors `loams mcp install` behaviour for external agents (SO1 Task 10).

**Tests:** `tool_listed_only_when_local_ready`; `cloud_request_refused`; `limits_enforced`; `no_network_in_tool_path`; `tool_description_states_advisory`; `definition_shared_by_mcp_and_registry` (after DD1 Task 19).

**Commit:** `agentd: read-only decision tool for coding agents`.

### Task 8: The browser plugin (optional; owner go-ahead, Q530)

**Files:** `web/plugins/systemone-local/*`.

**Semantics:** a cordis plugin `@loams/plugin-systemone-local` that provides the `systemone` service with a `LayaWasmBackend` in a plain Web Worker running `laya-ts` (Apache-2.0) on `onnxruntime-web` (WebGPU, then single-threaded WASM, about 6x slower); weights from the pinned mirror with SHA-256 checks in the worker before use; **no cross-origin isolation is requested** (an isolated iframe cannot live in a non-isolated console, and `require-corp` would block the framed app UIs), so threaded WASM is not offered; this task also measures the real size of the pinned `laya-ts` export; the plugin is `trusted-first-party` (§53 §8.3), editions `["oss", "cloud"]`, off by default, with a size warning and a consent prompt; falls back to the instance route. The owner's UI rulings apply to its settings card.

**Tests:** `worker_never_runs_on_main_thread`; `console_headers_unchanged` (no COEP or COOP on the console document); `no_threaded_wasm_requested`; `weights_verified_before_use`; `consent_before_download`; `falls_back_to_server_when_declined`; `answers_match_server_within_0_02` (Playwright, needs the weights; skipped in CI without them).

**Commit:** `plugin: opt-in local decisions in the browser`.

### Task 9: Linux and Windows CPU and NVIDIA paths

**Semantics:** on Linux with NVIDIA the Decisions section offers to install `laya-serve` (SO1 Task 5's installer, run by the daemon after consent) and shows the size; the daemon supervises it as SO1 Task 5 says; on CPU-only hosts the in-process backend if SO1 Task 12 landed, else the same installer on CPU; Windows stays remote-only unless Q533 says otherwise. The section shows the fix line per host.

**Tests:** `linux_nvidia_offers_laya_serve`; `linux_cpu_prefers_native_when_present`; `windows_is_remote_only`; `installer_requires_consent`; `installer_runs_in_daemon_not_renderer`.

**Commit:** `agentd: Linux and Windows decision paths`.

### Task 10: Optional MLX adapter (only if Q522 says the English gap matters)

**Semantics:** a wrapper script we own (about 150 lines) that serves `mizorewww/laya-mlx` (Apache-2.0, independent, Python 3.11+, macOS 14+) in `laya-serve`'s shape on a Unix socket, installed by the same `uv` flow, supervised by the daemon like `loams-coreml`, labelled "unofficial port", off by default; selected for English text on Apple silicon when enabled.

**Tests:** `mlx_selected_only_for_english_when_enabled`; `mlx_off_by_default`; `mlx_passes_selftest` (`#[ignore]`, needs the install); `licence_notice_present`.

**Commit:** `agentd: optional MLX English checkpoint on Apple silicon`.

### Task 11: Exit gate and docs

**Exit gate:** Tasks 1 to 7 tests; on the macOS arm64 runner, from a clean profile with the packaged app: the app starts, the daemon starts with no models, the first decision prompts for the download in the shell, the models install and verify, a Loams Bot message (against a fake A2A server) is routed locally in under 100 ms end to end (p50, excluding the first load), the chip shows on the card in Studio, the self-test passes, Cloud stays off, `LOAMS_OFFLINE=1` makes no network calls, closing the window leaves decisions working through the daemon, and stopping the daemon leaves no `loams-coreml` process or socket. A Linux CPU run covers the same through `laya-serve`.

**Files:** `docs/design/40-loams-systemone.md` (as built, with the owner's answers to SO2-Q1 to Q5), `docs/design/13-decision-log.md` (D528 and D538 as amended by the owner), `docs/design/50-loams-desktop-daemon.md` (a pointer in §4.1 for the new crate), `docs/guides/desktop/decisions.md`, `THIRD_PARTY_NOTICES.md` (FluidUse, FluidAudio, `laya-coreml`, Laya), `docs/plans/README.md`.

**Commit:** `docs: SystemOne on the desktop, SO2 closed`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
