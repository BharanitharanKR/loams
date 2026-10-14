# SB1 — Loams Sandbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task, test first. Each task lists:
> - its files;
> - the interfaces it must produce;
> - the tests that must exist and pass before it is done (exact function names);
> - its steps;
> - its pull-request size.
>
> Where this plan gives exact names, paths, flags, exit codes or defaults, use them verbatim. Where it gives a contract and named tests, write the code to that contract, and record any deviation in "Rulings made during execution" at the end of this file. The code is not pre-written in this plan.
>
> **Status: Planned: starts after v0.1.0-preview.3** (owner, 2026-10-15). This is track SB, design [§56](../design/56-loams-sandbox.md) (D990–D1009, Q880–Q894). The owner's direction of 2026-10-15 fixes:
> - one crate, `loams-sandbox`, with a declarative `SandboxSpec` and a `Sandbox` trait;
> - the adapter ladder `linux`, `wasm`, `gvisor`, `microvm`, `seatbelt`, `appcontainer`;
> - the threat model (`linux` for a single tenant; stacked in `gvisor` or `microvm` for multi-tenant hostile code);
> - the build order;
> - limits open, metering private.
>
> This plan builds that direction and does not redesign it. No task starts before the v0.1.0-preview.3 tag.

**Goal:** every process Loams starts to run code it does not trust runs under one tested crate. This covers House workers, Live workers, agent harness runs, function tiers and workspaces. One conformance kit proves, for each adapter, that the escapes in §56 §12 fail.

**Architecture** (§56 §1.3, §7, §8):
- **`crates/loams-sandbox`** (new, root workspace, `unsafe_code = "forbid"`) holds:
  - `SandboxSpec` and its presets, `Sandbox`, `SandboxedChild`, `Capabilities`, `SandboxStats`, `SandboxReport` and `SandboxError`;
  - the `linux` adapter (`seal_self`, and `spawn` through the `__loams-sandbox-exec` trampoline);
  - `cgroup`;
  - `net` (the forwarder moved from House, plus the allowlisting proxy);
  - behind features: `wasm`, `gvisor`, `microvm`, `seatbelt` (macOS) and `appcontainer` (Windows).
- **`crates/loams-sandbox-sys`** (new) holds the macOS and Windows FFI only, with `unsafe_code = "deny"` and `SAFETY` comments.
- **`crates/loams-sandbox-conformance`** (new) is the escape kit, with the probe binary `loams-sandbox-probe`.
- **Consumers:**
  - House (`fabric/crates/loams-house-worker`, `fabric/crates/loams-house`), through a path dependency;
  - Live (`crates/loams-live-js`);
  - agentd (`crates/loams-agentd-harness`, `crates/loams-agentd`), and Studio's thread header (`web/plugins/studio`).

**Tech Stack:**
- Rust 1.97 (workspace `rust-version`), edition 2024, workspace lints.
- `landlock` 0.4.7, `seccompiler` 0.5.0 and `rustix` 1 (features `process`, `thread`, `mount`, `net`, `fs`): the root workspace's pins.
- `libc` (syscall numbers), `serde`, `postcard` and `serde_json` from the workspace.
- Behind features, each new pin at least 14 days old: `wasmtime` (`wasm`), `windows-sys` 0.61 (the harness's pin, `appcontainer`).
- External, pinned by version and checksum in `release/companions.toml`: `runsc` (gVisor), and `firecracker` with its `jailer`.
- CI: `ubuntu-24.04` (with `kernel.apparmor_restrict_unprivileged_userns=0`, as `fabric.yml` does), `macos-latest`, `windows-2025`, and a KVM-capable runner for `microvm` (nightly).

**Spec:**
- [§56](../design/56-loams-sandbox.md) (all of it).
- [§49](../design/49-loams-house-production.md) §12–§13 and [HS1](2026-10-08-hs1-house-production.md) R1.10, R1.11, R6.1, R6.6, R-ACL.1.
- [§45](../design/45-loams-live-production.md) §3.1–§3.2 and [LV1](2026-10-08-lv1-live-production.md) Task 5.
- [§50](../design/50-loams-desktop-daemon.md) §7, [§53](../design/53-studio-ade.md) §8, [DD1](2026-10-09-dd1-desktop-daemon.md), [SD1](2026-10-13-sd1-studio-ade.md) Task 13.
- [§24](../design/24-cpu-time-runtime.md) §4, §10, §16, and [§15](../design/15-agent-workspaces.md) §5, §8.
- [open-core](../open-core.md).

## Global Constraints

- **Not before preview.3.** Task 0 starts only after the `v0.1.0-preview.3` tag exists. Nothing in this plan changes preview.3.
- **Worktree and branches.** Work in `~/Documents/Ostriumlabs/loams-wt/sb1`, with one branch per task, `sb1-t<N>`, cut from a fresh `origin/dev`.
  - **Each task is its own pull request into `dev`.** It gets one CodeRabbit round (with the 20-minute rate-limit rule) and is merged with `gh pr merge <n> -R ostrium-labs/loams --admin --merge` once `CI required` is green.
  - Sign every commit (`git commit -s`, DCO). Stage only your own paths. Never `git stash`. Never commit `.superpowers/`.
  - Commit areas: `sandbox`, `house`, `live`, `agentd`, `studio`, `ci`, `docs`.
- **Rust builds** use the shared target directory (`~/Documents/.cargo/config.toml`).
  - Never set `CARGO_TARGET_DIR`, and never build in `/tmp`. Run one cargo build at a time.
  - Build and test only the touched crates: `cargo test -p loams-sandbox`, and `cd fabric && cargo test -p loams-house-worker --test sandbox`.
  - Never run `cargo build --workspace --all-features` locally.
  - Two worktrees hash alike in the shared target (HS1 R-ACL.1). Touch the crate root, or build with `--config profile.dev.package.<crate>.codegen-units=255`, when a stale artifact is suspected.
- **No `unsafe` in `loams-sandbox`.** No `pre_exec` and no `fork` from Rust: the `linux` adapter spawns through the trampoline (D996). Only `loams-sandbox-sys` may contain `unsafe`, each block with a `SAFETY` comment, and only for macOS and Windows APIs. One exception: the Linux ambient-capability clear, if Task 0 finds no safe API.
- **Fail closed.** No code path may run a payload with a weaker sandbox than its spec asks for, unless the field is in `best_effort` and the `SandboxReport` names it. A reviewer rejects any "warn and continue".
- **House's behaviour is frozen through Tasks 1–2.** The Landlock rules and seccomp programs a House worker installs are byte-identical before and after the move (`house_worker_preset_matches_legacy_rules`). Every House sandbox test passes unchanged.
- **Live's behaviour is frozen through Task 3**, except `RLIMIT_CPU` and the optional cgroup (D1003).
- **No metering.** Nothing in these crates names `loams.meter`, `meter.sock`, `HostReport` or `x-loams-usage`. Nothing aggregates or signs `stats()` (D995).
- **Hermetic tests.** Probes run against planted files, sockets and listeners that the test creates, under a temporary directory and on random high ports. No test touches the real home directory, the real `/run`, or a real network host. Proxy tests use a planted local HTTP and TLS server.
- **CI hosts.** Every Linux job that starts a sandbox first runs `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0`. Tests that need a delegated cgroup, KVM or `runsc` skip with a printed reason when it is absent, and the job that is meant to have it fails if any of them skipped (`SB1_REQUIRE=cgroup,kvm,runsc`).
- **Pins.** Exact versions at least 14 days old. Record each pin in the commit that adds it.

## Rulings (from §56 and the owner's direction; binding for execution)

| # | Ruling | Source |
|---|---|---|
| R1 | One crate `loams-sandbox`; a declarative `SandboxSpec` (fs, net, syscalls, resources, identity) compiled by each adapter; an unenforceable field refuses the spawn unless it is best-effort | D990 |
| R2 | The `Sandbox` trait is `kind`, `capabilities`, `spawn`, `kill`, `stats`; `linux::seal_self` serves processes that load code first | D991 |
| R3 | The adapters are `linux` (default), `wasm`, `gvisor`, `microvm`, `seatbelt`, `appcontainer` | D992 |
| R4 | Single tenant: `linux`. Multi-tenant hostile code: the linux spec inside `gvisor` or `microvm`; cloud refuses bare `linux` for multi-tenant products | D993 |
| R5 | The build order is House → Live → kit → agentd → gvisor (functions) → microvm (workspaces) | D994 |
| R6 | `stats()` is ops-grade; no metering in this repository | D995 |
| R7 | `linux` spawns through `__loams-sandbox-exec` (stage 1) and `__loams-sandbox-init` (stage 2, PID 1); exit 77 on any sealing failure | D996 |
| R8 | The syscall profiles are `worker` (House's, `EPERM`), `interpreter` (Live's, `KILL_PROCESS`) and `shell` (agents); every profile returns `ENOSYS` for io_uring and `clone3`; `shell` refuses `TIOCSTI` and `TIOCLINUX` | D997 |
| R9 | The network is `None`, `Forward(ForwardTarget)` or `HostPorts`; the proxy resolves on the host and refuses private, link-local and metadata addresses unless listed | D998 |
| R10 | Resources come from a delegated cgroup v2 child; `kill` is `cgroup.kill`; deadlines are a host timer and a `cpu.stat` watchdog | D999 |
| R11 | The payload is never root; its own uid and gid are mapped; capabilities are cleared; `no_new_privs` is always set | D1000 |
| R12 | One kit, `loams-sandbox-conformance`; every probe passes or is declared not applicable with a reason | D1001 |
| R13 | House's `--sandbox=netns\|pods\|none` is unchanged and maps to presets | D1002 |
| R14 | Live adds `RLIMIT_CPU` at the CPU limit + 1 s and an optional cgroup | D1003 |
| R15 | agentd enforces `read-only` and `workspace-write` with the platform adapter; `danger-full-access` runs unsandboxed and labelled; harness sandboxes go to full access when Loams enforces | D1004 |
| R16 | Functions: T0 is `gvisor(linux)`, with a `linux` fallback that is reported; T1 is `wasm`; T2 is `gvisor` | D1005 |
| R17 | Workspaces run in `microvm` with snapshot, restore and fork; §15's in-sandbox agent is `loams-workspace-agent` | D1006 |
| R18 | Every sandboxed process logs a `SandboxReport`, which each product shows | D1007 |
| R19 | Where the code lives, and the unsafe rule | D1008 |
| R20 | When a host falls short: House and Live refuse; agentd follows Q885's default (run labelled "not enforced by Loams") | D1009 |

## Review Focus

1. **A sealing step that silently does less.** Expected: refusal, or a named entry in `report.skipped`. Tests: Task 1 `unenforceable_field_refuses_spawn`; Task 6 `missing_userns_refuses_with_step`; Task 4 `escape_report_matches`.
2. **House or Live behave differently after the move.** Tests: Task 2 `house_worker_preset_matches_legacy_rules`, and every House test of §56 §3.1; Task 3 `live_worker_preset_matches_legacy_filter`, and Live's three sandbox tests.
3. **A mount-namespace escape** (a symlink, hardlink, bind or `..` path). Tests: Task 4 `probe_symlink_escape`, `probe_hardlink_escape`, `probe_write_read_only_bind`; Task 1 `spec_rejects_relative_and_dotdot_paths`.
4. **The proxy reaches what it must not**: metadata, the LAN, loopback after DNS rebinding. Tests: Task 7 `proxy_refuses_metadata_by_name_and_address`, `proxy_refuses_rebinding_to_private`.
5. **A process escapes the cgroup or survives `kill`.** Tests: Task 5 `probe_fork_bomb`, `probe_other_tenant_cgroup`; Task 8 `kill_reaps_the_whole_tree`.
6. **The person's terminal is reachable.** Test: Task 4 `probe_tiocsti`.
7. **Credentials are visible to an agent run.** Test: Task 9 `agent_preset_masks_credentials`.
8. **A run gets a wider sandbox than its session.** `roles.rs` already refuses this. The test is Task 9 `enforced_level_never_wider_than_requested`.

---

## File structure

```
crates/loams-sandbox/
  Cargo.toml                      (features: wasm, gvisor, microvm; seatbelt/appcontainer by target)
  src/lib.rs                      Sandbox, SandboxedChild, ChildRef, Capabilities, SandboxStats, SandboxError, AdapterKind (Task 1)
  src/spec.rs                     SandboxSpec, FsSpec, NetSpec, ForwardSpec, Resources, Rlimits, Identity, EnvSpec, StdioSpec, BestEffort, validate() (Task 1)
  src/presets.rs                  house_worker, live_worker, agent, function_t0 (Tasks 2, 3, 9, 14)
  src/report.rs                   SandboxReport (Task 1)
  src/cgroup.rs                   Delegated, CgroupChild, CgroupLimits, join, stats, kill (Task 1; io.max Task 8)
  src/linux/{mod.rs,seal.rs,ns.rs,landlock.rs,seccomp.rs,caps.rs,rlimits.rs}        (Task 1)
  src/linux/{trampoline.rs,init.rs,mounts.rs,spawn.rs}                                (Task 6)
  src/net/{mod.rs,forwarder.rs,serve.rs}                                              (Task 1)
  src/net/{proxy.rs,allowlist.rs,expose.rs}                                           (Task 7)
  src/watchdog.rs                 wall and CPU deadlines (Task 8)
  src/seatbelt.rs                 (Task 11)
  src/appcontainer.rs             (Task 12)
  src/gvisor.rs                   (Task 14)
  src/wasm.rs                     (Task 15)
  src/microvm/{mod.rs,firecracker.rs,snapshot.rs}                                     (Tasks 16, 17)
  tests/{spec.rs,seal.rs,cgroup.rs,forwarder.rs,spawn.rs,proxy.rs,watchdog.rs}
  tests/golden/{house_landlock_rules.txt,house_seccomp_x86_64.bpf,house_seccomp_aarch64.bpf,live_seccomp_x86_64.bpf,live_seccomp_aarch64.bpf}
crates/loams-sandbox-sys/{Cargo.toml,src/lib.rs,src/macos.rs,src/windows.rs}           (Tasks 11, 12)
crates/loams-sandbox-conformance/
  Cargo.toml, src/lib.rs (Probe, Expectation, run_kit), src/bin/loams-sandbox-probe.rs
  tests/kit_linux.rs, tests/kit_seatbelt.rs, tests/kit_appcontainer.rs, tests/kit_gvisor.rs, tests/kit_microvm.rs, tests/kit_wasm.rs
fabric/crates/loams-house-worker/src/{main.rs, sandbox/ (removed), forwarder.rs (removed)}   (Task 2)
fabric/crates/loams-house/src/sandbox.rs, src/watchdog.rs                                  (Task 2)
fabric/crates/loams-chdb-sys/src/lib.rs   (capabilities::drop_all removed if unused; Task 2)
crates/loams-live-js/src/{sandbox_linux.rs (removed), child.rs, worker.rs}                 (Task 3)
crates/loams-agentd-harness/src/process/sandboxed.rs, the spawn sites of §56 §3.3          (Tasks 9, 10)
crates/loams-agentd/src/{main.rs, service/systemd.rs, config.rs}                           (Task 9)
crates/loams-agentd-proto/src/agent.rs, proto/loams/agentd/v1/studio.proto                  (Task 13)
web/plugins/studio/src/thread/ThreadPage.tsx, web/plugins/studio/src/header.tsx             (Task 13)
.github/workflows/{ci.yml,nightly.yml,fabric.yml}                                          (Tasks 2, 4, 5, 11, 12, 14, 16)
docs/guides/sandbox.md, docs/guides/studio/settings.md, deploy notes                       (Task 19)
```

## Shared contracts (all tasks use these names)

```rust
pub trait Sandbox: Send + Sync + fmt::Debug + 'static {
    fn kind(&self) -> AdapterKind;
    fn capabilities(&self) -> &Capabilities;
    fn spawn(&self, spec: &SandboxSpec, cmd: SandboxCommand) -> Result<SandboxedChild, SandboxError>;
    fn kill(&self, child: &ChildRef) -> Result<(), SandboxError>;
    fn stats(&self, child: &ChildRef) -> Result<SandboxStats, SandboxError>;
}
pub enum AdapterKind { Linux, Wasm, Gvisor, Microvm, Seatbelt, AppContainer }
pub enum SyscallProfile { Worker, Interpreter, Shell, None }
pub enum NetSpec { None, Forward(ForwardSpec), HostPorts(Vec<u16>) }
pub struct SandboxError { pub step: String, pub detail: String }       // House's shape, kept
pub enum ExitReport { Exited(i32), Signalled(i32), WallDeadline, CpuDeadline, OomKilled, SealFailed(SandboxError) }
pub mod linux {
    pub fn seal_self(spec: &SandboxSpec, inherited: Inherited) -> Result<SandboxReport, SandboxError>;
    pub fn trampoline() -> Option<std::process::ExitCode>;   // Some(..) when argv[1] is __loams-sandbox-exec or __loams-sandbox-init
    pub struct LinuxSandbox;                                  // impl Sandbox
}
pub trait ForwardTarget: Send + Sync + fmt::Debug + 'static {
    fn serve(&self, who: &SandboxId, listener: u16, stream: std::os::unix::net::UnixStream);
}
pub trait Microvm: Sandbox {                                  // Task 17
    fn snapshot(&self, child: &ChildRef) -> Result<SnapshotRef, SandboxError>;
    fn restore(&self, spec: &SandboxSpec, snap: &SnapshotRef) -> Result<SandboxedChild, SandboxError>;
    fn fork(&self, snap: &SnapshotRef, n: usize) -> Result<Vec<SandboxedChild>, SandboxError>;
}
```

- **Trampoline argv:** `<self> __loams-sandbox-exec -- <program> <args…>`.
- **Trampoline file descriptors:**
  - fd 3: the postcard-encoded `SandboxSpec` (a pipe);
  - fd 4: the forwarder socket, when the spec is `Forward`;
  - fd 5: the status pipe, which carries a JSON `SandboxReport` or `SandboxError`.
- **Exit 77** means a sealing failure, as for House's worker.

## Execution order

1. **Before anything:** the `v0.1.0-preview.3` tag. Then Task 0.
2. **SB1a** (Tasks 1–3) in order: the crate, then House, then Live. **Build steps 1 and 2.**
3. **SB1b** (Tasks 4–5): the kit. Task 4 can start beside Task 2. **Build step 3.**
4. **SB1c** (Tasks 6–8): `spawn`, the proxy, deadlines. Tasks 6 and 7 can run in parallel.
5. **SB1d** (Tasks 9–13): agentd. Task 9 after Tasks 6–8. Tasks 11 and 12 (macOS, Windows) after Task 1, in parallel with SB1c. Task 13 after Task 9. **Build step 4.**
6. **SB1e** (Tasks 14–15): `gvisor` and `wasm`, after Task 8. The function supervisor that uses them is F1's. **Build step 5.**
7. **SB1f** (Tasks 16–17): `microvm`, after Task 8, beside §15 W2. **Build step 6.**
8. **SB1g** (Tasks 18–20): review, docs, exit.

---

## SB1a — The crate, House and Live (Tasks 0–3)

### Task 0: Reconcile with the code as built, and verify the third-party facts

**Files:**
- this plan's "Rulings made during execution";
- §56 §3 and §19 where a fact there is wrong;
- scratch programs under the worktree's `scratch/` directory, never committed.

Steps:
1. **Record, as rulings with file paths and line numbers, the current sandbox code and its tests, re-checked on the fresh `origin/dev`.** If any of this changed after `c6551a69`, update §56 §3.
   - House:
     - `fabric/crates/loams-house-worker/src/sandbox/{mod.rs,cgroup.rs,netns.rs,landlock.rs,seccomp.rs}`;
     - `fabric/crates/loams-house-worker/src/forwarder.rs`, `src/main.rs` (`seal`, exit 77);
     - `fabric/crates/loams-house/src/sandbox.rs` (`serve_forwarder`, `CgroupLimits`, `WorkerCgroups`, `default_mode`, `unsandboxed_warning`);
     - `fabric/crates/loams-house/src/watchdog.rs` (`with_sandbox`, `unsandboxed`, `with_cgroups`);
     - `fabric/crates/loams-house-ipc/src/lib.rs` (`SandboxMode`, `FORWARDER_PORT`);
     - `fabric/crates/loams-chdb-sys/src/lib.rs` (`capabilities::drop_all`);
     - `deploy/house/kind/worker-pods.yaml`.
   - House tests:
     - `tests/sandbox.rs`: `sandbox_blocks_with_deny_list_off`, `worker_reaches_only_its_forwarder`, `worker_cannot_exec`, `worker_cannot_write_outside_tmp`, `cgroup_limits_applied`, `pods_fallback_network_policy`, `sandbox_tests_are_linux_only`;
     - the unit tests `rules_read_the_code_and_write_only_the_private_dir`, `the_request_is_one_new_link_with_up`, `an_error_answer_is_its_errno`, `filters_compile_for_this_machine`;
     - in `loams-house`: `limits_name_their_controllers`, `a_directory_that_is_not_a_cgroup_is_refused`, `the_default_is_sealed_on_linux`, `the_forwarder_answers_a_request_with_one_socket`.
   - Live:
     - `crates/loams-live-js/src/sandbox_linux.rs`, `src/child.rs` (`sandbox`), `src/worker.rs` (spawn, `env_clear`, the wall-clock kill);
     - tests: `worker_cannot_open_files`, `worker_cannot_open_sockets`, `worker_cannot_exec`, `the_filter_compiles_and_the_address_space_is_read`.
   - agentd:
     - `crates/loams-agentd-proto/src/agent.rs` (`SandboxLevel`, `RunRequest`);
     - `crates/loams-agentd/src/roles.rs` (`rank`, `ceiling`, `check_command`, `check_new_chat`);
     - `crates/loams-agentd-harness/src/codex/{mod.rs,catalog.rs}`, `claude/mod.rs`, `acp/mod.rs`, `opencode/mod.rs`, `pi/mod.rs` (each spawn site and how it sets the harness's own sandbox);
     - `src/process/owned.rs`, `src/windows_process.rs`;
     - `crates/loams-agentd/src/service/systemd.rs`;
     - `web/plugins/studio/src/thread/ThreadPage.tsx` (`SANDBOX_LABEL`);
     - `proto/loams/agentd/v1/studio.proto` (`SandboxLevel`).
   - CI: the `fabric.yml` sysctl step, and whether `ci.yml` runs `loams-live-js`'s sandbox tests (`cargo test -p … loams-live-js`).
   - Whether F1 (the function supervisor), `loams-runner` or §15 W2 have started. If they have, Tasks 14–17 attach to them. If not, they deliver the adapters and their kits only.
2. **Verify against primary sources and record the versions and commands used:**
   - rustix 1's capability calls: does `rustix::thread` cover clearing the bounding and ambient sets, or is a raw `prctl` still needed (which then stays in `loams-sandbox-sys`)? Also rustix's netlink socket address for `bind`, and its `mount`/`pivot_root` calls.
   - The kernel release for Landlock ABI 4 (TCP rules) and ABI 6 (scoping).
   - Ubuntu 24.04's default for `kernel.apparmor_restrict_unprivileged_userns`.
   - Docker's and podman's default seccomp handling of `unshare(CLONE_NEWUSER)`.
   - Kubernetes `RuntimeDefault` on kind (HS1 R1.11 left this unmeasured).
   - `runsc` flags for rootless runs, `--network=none`, fd donation and `--host-uds`, and whether the Sentry implements Landlock.
   - `sandbox-exec`'s deprecation status on the current macOS.
   - **Windows (Q884):** on `windows-2025`, whether an AppContainer process can connect to a loopback listener of the parent (loopback exemption or a broker), against a restricted token with a dedicated account and DACLs. Write both as scratch programs, then pick one. This rules Q884.
   - Firecracker's current snapshot API, and copy-on-write memory restore with `MAP_PRIVATE` file-backed guest memory.
3. Confirm that D990–D1009 and Q880–Q894 are still free in the decision log, and renumber in §56 and here if not.
4. Commit `docs(sb1): task 0 rulings`.

**PR size:** S (docs only).

### Task 1: The crate, the spec, the report, cgroups, and `linux::seal_self`

**Files:**
- create `crates/loams-sandbox/{Cargo.toml,src/lib.rs,src/spec.rs,src/report.rs,src/cgroup.rs,src/linux/{mod.rs,seal.rs,ns.rs,landlock.rs,seccomp.rs,caps.rs,rlimits.rs},src/net/{mod.rs,forwarder.rs,serve.rs},tests/{spec.rs,seal.rs,cgroup.rs,forwarder.rs}}`;
- the root `Cargo.toml` (member, `loams-sandbox` in `[workspace.dependencies]`, rustix features).

**Interfaces:**
- the shared contracts (`spawn` returns `SandboxError { step: "spawn", detail: "not yet" }` until Task 6);
- `SandboxSpec::validate()`;
- `linux::seal_self` supporting `NetSpec::{None, Forward, HostPorts}`, `CodeSource::MappedAtSeal` and the `worker` and `interpreter` profiles;
- `cgroup::{Delegated::prepare, Delegated::create, CgroupChild::{join_self, stats, kill, remove}, CgroupLimits}` (`memory.max`, `memory.swap.max`, `cpu.max`, `pids.max`);
- `net::{forwarder::Forwarder, serve::serve_forwarder, ForwardTarget, Relay}`.

**The moved code.** House's modules move with their logic, comments and step order intact. Generalise only:
- names: `Rules::new` takes the spec's paths and ports;
- `seccomp::filters(profile, scoped_signals)`;
- the Live allowlist, which becomes `SyscallProfile::Interpreter`.

Tests (in the crate):
- `spec_round_trips_postcard_and_json`
- `spec_rejects_relative_and_dotdot_paths`
- `spec_rejects_overlapping_read_only_and_read_write`
- `spec_rejects_host_ports_with_shell_profile`
- `unenforceable_field_refuses_spawn` (a fake adapter with `cpu_max: false`)
- `best_effort_field_is_reported_as_skipped`
- `rules_read_the_code_and_write_only_the_private_dir` (moved)
- `the_request_is_one_new_link_with_up` (moved)
- `an_error_answer_is_its_errno` (moved)
- `worker_filters_compile_for_this_machine` (moved from `filters_compile_for_this_machine`)
- `interpreter_filter_allows_only_the_thirteen_calls`
- `limits_name_their_controllers` (moved)
- `a_directory_that_is_not_a_cgroup_is_refused` (moved)
- `the_forwarder_answers_a_request_with_one_socket` (moved)
- `seal_self_refuses_a_second_thread`
- `seal_self_reports_landlock_and_profile` (a child process seals itself and prints its report)
- `capabilities_are_cleared_after_seal` (`CapEff`, `CapAmb` 0 in `/proc/self/status`)

Steps:
1. Write the tests first, copying House's unit tests verbatim where they move.
2. Create the crate.
3. Move the code without deleting the originals yet: House switches in Task 2.
4. Run `cargo test -p loams-sandbox` and `cargo clippy -p loams-sandbox --all-targets -- -D warnings`.

Commit `feat(sandbox): loams-sandbox with the spec, cgroups and linux::seal_self`. **PR size:** L (~1500 lines, most of it moved).

### Task 2: House onto `loams-sandbox` (build step 1)

**Files:**
- `fabric/crates/loams-house-worker/{Cargo.toml,src/main.rs,src/lib.rs}`, deleting `src/sandbox/` and `src/forwarder.rs`;
- `fabric/crates/loams-house/{Cargo.toml,src/sandbox.rs,src/watchdog.rs}`;
- `fabric/crates/loams-chdb-sys/src/lib.rs` (remove `capabilities` if nothing else uses it);
- `fabric/Cargo.toml`, `fabric/Cargo.lock`;
- `crates/loams-sandbox/src/presets.rs` (`house_worker`);
- `crates/loams-sandbox/tests/golden/house_*`.

**Interfaces:**
- `presets::house_worker(tmp: &Path, net: NetSpec, cgroup: Option<PathBuf>) -> SandboxSpec`;
- House's `SandboxMode` maps as in D1002;
- `loams-house` re-exports `loams_sandbox::cgroup::{CgroupLimits, Delegated as WorkerCgroups}` and `loams_sandbox::net::serve_forwarder`, so its public names do not change for its callers.

Tests:
- `house_worker_preset_matches_legacy_rules`. The golden files are captured **before** the switch from the old `landlock::Rules::new` and `seccomp::filters(false)` and `filters(true)` output (x86_64 and aarch64 BPF bytes), and compared after it.
- Every House test listed in Task 0, step 1, passes unchanged, in its current file.
- `the_default_is_sealed_on_linux` (unchanged).

Steps:
1. Capture the golden files on the current code and commit them first.
2. Switch the worker's `seal` to `linux::seal_self`, and the front to the moved forwarder and cgroups.
3. Delete the old modules.
4. Run `cd fabric && cargo test -p loams-house-worker -p loams-house --locked` with the sysctl set, and `cargo deny check` (fabric).
5. Record the boot time p95 before and after in the rulings (target: no regression beyond noise).

Commit `refactor(house): seal workers with loams-sandbox`. **PR size:** M.

### Task 3: Live onto `loams-sandbox` (build step 2)

**Files:**
- `crates/loams-live-js/{Cargo.toml,src/lib.rs,src/child.rs,src/worker.rs}`, deleting `src/sandbox_linux.rs`;
- `crates/loams-sandbox/src/presets.rs` (`live_worker`);
- `crates/loams-sandbox/tests/golden/live_seccomp_*`;
- `docs/design/45-loams-live-production.md` §3.1 (the amendment of §56 §16).

**Interfaces:**
- `presets::live_worker(memory_limit: usize, cpu_limit: Duration, cgroup: Option<PathBuf>) -> SandboxSpec`: `interpreter` profile, Landlock with no rights, `RLIMIT_NOFILE` 3, `RLIMIT_AS` = mapped + memory + 64 MiB, and **`RLIMIT_CPU` = `cpu_limit` + 1 s**, rounded up to whole seconds (soft = hard);
- `[live] cgroup_root` (optional): when set, the host creates a child per worker.

Tests:
- `live_worker_preset_matches_legacy_filter` (golden BPF from the old `filter()`).
- `worker_cannot_open_files`, `worker_cannot_open_sockets`, `worker_cannot_exec` (unchanged).
- `rlimit_cpu_kills_a_spinning_native_loop`, using a test-only probe that spins in Rust past `RLIMIT_CPU`. Expect `FUNCTION_ERROR`, reason `live_worker_crashed`.
- `worker_joins_its_cgroup_when_configured` (skips without a delegated cgroup; required in the kit job).

Steps:
1. Capture the golden filter.
2. Switch.
3. Run `cargo test -p loams-live-js --locked`.

Commit `refactor(live): isolated workers on loams-sandbox, with RLIMIT_CPU`. **PR size:** M.

## SB1b — The conformance kit (Tasks 4–5; build step 3)

### Task 4: The kit and the probe binary; filesystem, syscall and network probes

**Files:**
- create `crates/loams-sandbox-conformance/{Cargo.toml,src/lib.rs,src/bin/loams-sandbox-probe.rs,tests/kit_linux.rs}`;
- `.github/workflows/ci.yml` (a new job `Sandbox: conformance (linux)`).

**Interfaces:**
- `Probe` (an enum with one variant per §56 §12.2 row);
- `Expectation::{Denied, Allowed, Killed, Enosys, NotApplicable(&'static str)}`;
- `run_kit(adapter: &dyn Sandbox, preset: &SandboxSpec, table: &[(Probe, Expectation)]) -> KitResult`;
- the probe binary prints one JSON line;
- planted fixtures (files, symlinks, a setuid copy where the CI user may make one, a listening TCP port, a pathname socket, an abstract socket, a host process with a known `environ`) under a temporary directory.

Until Task 6, the presets are run through a `seal_self` harness: the probe seals itself with the preset, then probes.

Tests (each is `probe_<name>` run for `worker` and `interpreter`, and for `shell` from Task 6):
- **Filesystem:** `probe_read_etc_shadow`, `probe_read_home_ssh_key`, `probe_read_other_proc_environ`, `probe_write_outside_scratch`, `probe_write_read_only_bind`, `probe_symlink_escape`, `probe_hardlink_escape`.
- **Processes:** `probe_execve`, `probe_fork`.
- **Network:** `probe_connect_host_port`, `probe_connect_metadata`, `probe_dns_direct`, `probe_unix_socket_host`, `probe_abstract_socket_host`.
- **Other processes and the kernel:** `probe_ptrace_parent`, `probe_process_vm_readv`, `probe_kill_outside`, `probe_unshare_and_mount`, `probe_kernel_surfaces`, `probe_io_uring`, `probe_tiocsti`, `probe_setuid_binary`.
- **Self-checks:** `probe_capabilities_and_nnp`, `escape_report_matches`.
- **Plus:** `kit_fails_on_an_undeclared_skip`, a kit self-test with a planted adapter that skips a probe without declaring it.

Steps:
1. Write the probe binary.
2. Write the expectation tables from §56 §12.2.
3. Add the CI job: `ubuntu-24.04`, the sysctl, `cargo test -p loams-sandbox-conformance --test kit_linux`. It runs on pull requests that touch `crates/loams-sandbox*`, `fabric/crates/loams-house*/src/**` or `crates/loams-live-js/src/**`.

Commit `test(sandbox): the conformance kit and the linux escape probes`. **PR size:** L.

### Task 5: Resource and cross-tenant probes; the nightly matrix

**Files:**
- `crates/loams-sandbox-conformance/{src/lib.rs,tests/kit_linux.rs}`;
- `.github/workflows/nightly.yml`;
- `.github/workflows/ci.yml`.

Tests:
- `probe_fork_bomb` (stopped at `pids.max`; a host-side check that a fresh `ps` returns within 1 s)
- `probe_memory_balloon` (`oom_kills` ≥ 1)
- `probe_disk_fill` (tmpfs scratch, stopped at its size)
- `probe_other_tenant_cgroup` (two sandboxes; each fails to join or read the other's cgroup)
- `probe_other_tenant_scratch`
- `kit_runs_inside_rootless_podman` (nightly: the `linux` kit inside a rootless podman 6.1 container, as R1.11 measured)

The CI job delegates a cgroup to the runner user: `systemd-run --user --scope -p Delegate=yes` around the test, or a sibling cgroup as House's `delegated_cgroup()` does. With `SB1_REQUIRE=cgroup`, the job fails if the probes skipped.

Commit `test(sandbox): resource and cross-tenant probes, nightly matrix`. **PR size:** M.

## SB1c — `spawn`, the proxy and deadlines (Tasks 6–8)

### Task 6: The trampoline, the mount namespace, the PID namespace and the `shell` profile

**Files:**
- `crates/loams-sandbox/src/linux/{trampoline.rs,init.rs,mounts.rs,spawn.rs,seccomp.rs}`;
- `tests/spawn.rs`;
- `crates/loams/src/main.rs` and `crates/loams-agentd/src/main.rs` (call `loams_sandbox::linux::trampoline()` first).

**Interfaces:**
- `LinuxSandbox::spawn`, following §56 §8.1.2 steps 1–12 exactly, in that order;
- `SyscallProfile::Shell` (§56 §6.3);
- the mount layout of §56 §6.4: a tmpfs root, read-only binds remounted with `MS_RDONLY|MS_NOSUID|MS_NODEV` (keeping locked flags), masks, a minimal `/dev`, a new `devpts`, a new `proc` with `hidepid=invisible`, then `pivot_root` and a detach of the old root;
- stage 2 as PID 1, reaping orphans and relaying SIGTERM, SIGINT and SIGHUP to the payload;
- stdio as `Pipes` or `NewPty` (a fresh pty pair; the payload's session has no controlling terminal of the caller's).

Tests:
- `spawn_runs_true_and_reports`
- `spawn_payload_is_pid_two_and_sees_no_host_pids`
- `spawn_root_contains_only_listed_paths`
- `spawn_read_only_bind_is_read_only`
- `spawn_mask_hides_credential_files`
- `spawn_exits_77_and_names_the_step` (a planted unbindable path)
- `missing_userns_refuses_with_step` (simulated by a spec the host's capabilities lack)
- `trampoline_entry_is_wired` (in `loams` and in `loams-agentd`)
- `shell_profile_allows_exec_and_fork`
- `shell_profile_refuses_tiocsti`
- `toolchains_run_under_shell_profile`: under the agent-like preset, `git status`, `node -e`, `npm ci` (offline, a planted cache), `cargo build` (a tiny crate, offline), `go build`, `python3 -c`, and `bun --version` where installed. Each one that is absent prints a skip line.

Kit additions: the `shell` column of Task 4's probes now runs through `spawn`.

Commit `feat(sandbox): linux spawn through the trampoline, with mount and PID namespaces`. **PR size:** L.

### Task 7: The allowlisting proxy and reverse forwards

**Files:** `crates/loams-sandbox/src/net/{proxy.rs,allowlist.rs,expose.rs}`, `tests/proxy.rs`.

**Interfaces:**
- `Proxy::new(Allowlist, AuditSink) -> impl ForwardTarget`;
- `Allowlist` entries: `host`, `*.domain`, `host:port`, CIDR;
- HTTP `CONNECT`, absolute-form HTTP proxying and SOCKS5 with domain names;
- resolution on the host, then a refusal of loopback, link-local, private, CGNAT, multicast and the metadata addresses unless the entry names the address;
- audit events `{sandbox, host, port, allowed, bytes_up, bytes_down}`;
- `ForwardSpec.expose: Vec<u16>`, where the host side listens on an ephemeral loopback port and connects into the sandbox's `127.0.0.1:<port>`;
- for `Forward` specs, the trampoline sets `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, the lowercase forms, and `NO_PROXY=localhost,127.0.0.1`.

Tests:
- `proxy_connects_an_allowed_host`
- `proxy_refuses_a_host_not_listed`
- `proxy_refuses_metadata_by_name_and_address`
- `proxy_refuses_rebinding_to_private` (a planted resolver answers a public name with `10.0.0.5`)
- `proxy_allows_an_explicitly_listed_private_cidr`
- `proxy_socks5_with_domain_names`
- `proxy_emits_one_audit_event_per_connection`
- `proxy_caps_connections_per_sandbox` (the forwarder's 64)
- `expose_reaches_a_server_inside`
- `connect_allowed_via_proxy` and `connect_denied_via_proxy` (kit probes, `shell`)

Commit `feat(sandbox): the allowlisting proxy and reverse forwards`. **PR size:** M.

### Task 8: `kill`, `stats`, deadlines and `io.max`

**Files:** `crates/loams-sandbox/src/{watchdog.rs,cgroup.rs,lib.rs}`, `tests/watchdog.rs`.

**Interfaces:**
- `kill` writes `cgroup.kill`; without a cgroup, it SIGKILLs stage 2 (the PID 1);
- `stats` reads `cpu.stat`, `memory.current`, `memory.peak`, `pids.current`, `pids.peak`, `io.stat` and `memory.events`;
- `Watchdog` covers the wall deadline (a timer) and the CPU deadline (`cpu.stat usage_usec` polled at 10 Hz);
- `ExitReport::{WallDeadline, CpuDeadline, OomKilled}`;
- `CgroupLimits.io_max: Vec<IoMax>`.

Tests:
- `kill_reaps_the_whole_tree` (a payload that double-forks and `setsid`s)
- `stats_reads_cpu_and_memory`
- `stats_costs_under_100_microseconds` (a timing check that allows CI noise: 10× the target fails)
- `cpu_deadline_kills_a_spinner`
- `wall_deadline_kills_a_sleeper`
- `oom_is_reported`
- `io_max_is_written_when_the_controller_is_delegated`
- the kit probes `probe_cpu_spin` and `probe_wall_sleep`

Commit `feat(sandbox): kill, stats, deadlines and io.max`. **PR size:** M.

## SB1d — Agent sessions (Tasks 9–13; build step 4)

### Task 9: agentd enforces `SandboxLevel` on Linux

**Files:**
- `crates/loams-agentd-harness/{Cargo.toml,src/process/sandboxed.rs,src/process.rs}`;
- each spawn site of §56 §3.3 in `claude/mod.rs`, `codex/mod.rs`, `opencode/mod.rs`, `pi/mod.rs`, `acp/mod.rs`;
- `crates/loams-sandbox/src/presets.rs` (`agent`);
- `crates/loams-agentd/src/{config.rs,service/systemd.rs}`;
- `crates/loams-agentd/README.md`.

**Interfaces:**
- `presets::agent(level: SandboxLevel, cwd: &Path, worktree: Option<&Path>, home: &HomePolicy, allow: &Allowlist) -> Option<SandboxSpec>` (`None` for `DangerFullAccess`), following §56 §13.3's table;
- `HomePolicy { toolchains_ro, caches_rw, harness_state_rw }` with defaults per harness;
- `process::sandboxed(cmd: Command, run: &RunSandbox) -> Result<Command, SandboxUnavailable>`;
- `[sandbox]` in agentd's `config.toml`:
  - `enforce = "auto" | "require" | "off"` (Q885's default `auto`);
  - `memory_max` (Q892's default 8 GiB);
  - `allow` (extra hosts);
  - `home.toolchains`;
  - `home.caches`;
- the systemd unit gains `Delegate=cpu memory pids io`.

Tests:
- `agent_preset_read_only_has_no_writable_project`
- `agent_preset_workspace_write_writes_only_the_worktree_and_state`
- `agent_preset_masks_credentials` (planted `~/.ssh/id_ed25519`, `~/.aws/credentials`, `~/.kube/config` unreadable from a payload)
- `enforced_level_never_wider_than_requested` (`roles.rs`'s ceiling plus the preset: a `read-only` session's side chat cannot get a writable spec)
- `danger_full_access_runs_unsandboxed_and_says_so`
- `unavailable_adapter_runs_labelled_when_auto` and `unavailable_adapter_refuses_when_require`
- `systemd_unit_delegates_controllers`
- `mock_harness_runs_inside_the_sandbox` (the Mock harness's file edit lands in the worktree; a write outside fails)

Commit `feat(agentd): enforce SandboxLevel with loams-sandbox on Linux`. **PR size:** L.

### Task 10: Harness sandboxes, the MCP shim and real harnesses

**Files:** `crates/loams-agentd-harness/src/{codex/mod.rs,codex/catalog.rs,claude/mod.rs,acp/mod.rs,opencode/mod.rs,pi/mod.rs}`, and their tests.

**Interfaces:**
- When Loams enforces, each driver sets its harness's own sandbox to full access:
  - Codex: `danger-full-access`;
  - Claude: `bypassPermissions` for file tools;
  - ACP: the no-prompts mode.
- Supervised approvals (D911) are unchanged. When Loams does not enforce, today's behaviour holds.
- The MCP shim's agentd port is added to the run's allowlist as `127.0.0.1:<port>`.

Tests:
- `codex_full_access_only_when_loams_enforces`
- `claude_permission_mode_follows_enforcement`
- `supervised_still_asks_inside_the_sandbox`
- `mcp_shim_reaches_agentd_through_the_forwarder`
- `real_harness_turn_in_sandbox` (`#[ignore]`; runs Claude Code, Codex and opencode where installed, one edit-and-test turn each, in the nightly agentd job)

Commit `feat(agentd): harness sandboxes defer to Loams when it enforces`. **PR size:** M.

### Task 11: `seatbelt` (macOS)

**Files:**
- `crates/loams-sandbox/src/seatbelt.rs`;
- `crates/loams-sandbox-sys/src/macos.rs` (only if Task 0 rules the `sandbox_init` fallback in);
- `crates/loams-sandbox-conformance/tests/kit_seatbelt.rs`;
- `.github/workflows/ci.yml` (`Sandbox: conformance (macOS)` on `macos-latest`).

**Interfaces:**
- `SeatbeltSandbox` compiles a spec to SBPL (§56 §8.5) and runs `/usr/bin/sandbox-exec -f <profile> -- <payload>`;
- rlimits;
- a host timer;
- `stats` from `proc_pid_rusage` (through `libc`), or `wait4` at exit;
- `capabilities()` declares the cgroup fields not applicable.

Tests:
- `sbpl_profile_is_deny_default`
- `sbpl_profile_matches_golden_for_agent_preset`
- `kit_seatbelt` (the §56 §12.2 probes that apply, with the rest declared)

Commit `feat(sandbox): the seatbelt adapter`. **PR size:** M.

### Task 12: `appcontainer` (Windows)

**Files:**
- `crates/loams-sandbox/src/appcontainer.rs`;
- `crates/loams-sandbox-sys/{Cargo.toml,src/windows.rs}`;
- `crates/loams-sandbox-conformance/tests/kit_appcontainer.rs`;
- `.github/workflows/ci.yml` (on `windows-2025`).

**Interfaces:** the mechanism Task 0 ruled for Q884:
- **(a) AppContainer:** a profile per run, ACEs for the container SID on the spec's paths (removed at exit; a startup sweep removes leftovers from a crash), and `SECURITY_CAPABILITIES` in the attribute list;
- **(b) a restricted token** with a dedicated sandbox account and DACLs.

Both use a job object with memory, active-process and CPU-rate limits and kill-on-close, reusing `windows_process.rs`'s job plumbing. The network is the loopback proxy, or none.

Tests:
- `appcontainer_denies_reads_outside_the_spec`
- `appcontainer_acl_grants_are_removed_at_exit`
- `appcontainer_job_limits_apply`
- `kit_appcontainer`

Commit `feat(sandbox): the appcontainer adapter`. **PR size:** L.

### Task 13: Studio shows who enforces

**Files:**
- `proto/loams/agentd/v1/studio.proto` (a `SandboxEnforcement` message on the thread: `adapter`, `enforced`, `reason`, `skipped`);
- `crates/loams-agentd-proto/src/agent.rs`;
- the agentd Connect handlers;
- `web/plugins/studio/src/thread/ThreadPage.tsx`, `web/plugins/studio/src/header.tsx`;
- the `docs/guides/studio/settings.md` sandbox section;
- the allowlist editor in Studio's settings (Q883).

Tests:
- `thread_reports_enforcement` (Rust)
- the Studio component tests `header shows enforced by Loams`, `header shows not enforced and why`, and `audit event offers allow this host`
- `buf breaking` passes (an additive field)

Commit `feat(studio): show sandbox enforcement and edit the allowlist`. **PR size:** M.

## SB1e — Functions (Tasks 14–15; build step 5)

### Task 14: `gvisor`

**Files:**
- `crates/loams-sandbox/src/gvisor.rs`;
- `crates/loams-sandbox/src/presets.rs` (`function_t0`);
- `crates/loams-sandbox-conformance/tests/kit_gvisor.rs`;
- `release/companions.toml` (the `runsc` pin);
- `.github/workflows/nightly.yml`.

**Interfaces:**
- `GvisorSandbox` writes an OCI bundle from the spec (§56 §8.3) whose process is the `linux` trampoline (stacking, §56 §5.4);
- `runsc` with the flags Task 0 recorded;
- `stats` from the sandbox's cgroup;
- `report.outer`.

Tests:
- `oci_bundle_matches_golden_for_function_t0`
- `gvisor_stacks_the_linux_spec_and_reports_both`
- `kit_gvisor`
- `bare_linux_refused_for_multi_tenant` (the D993 refusal: a `Tenancy::Multi` spawn on `LinuxSandbox` alone is refused)

Commit `feat(sandbox): the gvisor adapter`. **PR size:** M.

### Task 15: `wasm`

**Files:** `crates/loams-sandbox/src/wasm.rs`, `crates/loams-sandbox-conformance/tests/kit_wasm.rs`, and the root `Cargo.toml` (the `wasmtime` pin, feature `wasm`).

**Interfaces:**
- `WasmSandbox`: preopens from `FsSpec`, `wasi:http` through the proxy for `Forward`, fuel for `cpu_deadline`, epochs for `wall_deadline`, and `StoreLimits` for memory;
- `stats` from fuel and wall time.

Tests:
- `wasm_preopens_only_the_spec`
- `wasm_fuel_stops_a_loop`
- `wasm_epoch_stops_a_blocked_guest`
- `wasm_http_goes_through_the_proxy`
- `kit_wasm` (the applicable probes)

Commit `feat(sandbox): the wasm adapter`. **PR size:** M.

## SB1f — Workspaces (Tasks 16–17; build step 6)

### Task 16: `microvm` (Firecracker)

**Files:**
- `crates/loams-sandbox/src/microvm/{mod.rs,firecracker.rs}`;
- `crates/loams-sandbox-conformance/tests/kit_microvm.rs`;
- `release/companions.toml` (the Firecracker and `jailer` pins, and a guest kernel and rootfs for tests);
- `.github/workflows/nightly.yml` (the KVM runner).

**Interfaces:**
- `MicrovmSandbox` uses the `jailer`, a read-only rootfs drive, a read-write workspace drive, a tap device into a host namespace routed only to the proxy (`Forward`) or no device (`None`), vCPU and memory sizing, and rate limiters;
- the guest's init runs the payload through the `linux` adapter;
- `kill` stops the VMM, and `stats` reads the VMM's cgroup.

Tests:
- `microvm_boots_and_runs_true`
- `microvm_network_reaches_only_the_proxy`
- `kit_microvm`

Commit `feat(sandbox): the microvm adapter on Firecracker`. **PR size:** L.

### Task 17: Snapshot, restore and fork

**Files:** `crates/loams-sandbox/src/microvm/snapshot.rs`, `tests/microvm_snapshot.rs` (nightly), and §15 W2's workspace agent if it exists (Task 0).

**Interfaces:**
- `Microvm::{snapshot, restore, fork}`;
- `SnapshotRef { host_class, vmm_version, memory, state, disk_diff }`;
- restore refuses a mismatched `host_class` or `vmm_version`;
- storage is local files. The bucket CAS upload is §15 W2's.

Tests:
- `snapshot_restore_keeps_a_file_and_a_process`
- `fork_three_copies_share_memory_until_written`
- `restore_refuses_another_host_class`
- `fork_of_one_gib_guest_under_one_second` (nightly, target)

Commit `feat(sandbox): microvm snapshot, restore and fork`. **PR size:** M.

## SB1g — Review, docs and exit (Tasks 18–20)

### Task 18: Security review

**Files:** `docs/security/sandbox-review.md`; fuzz targets `crates/loams-sandbox/fuzz/{spec_decode.rs,proxy_request.rs,socks5.rs}`.

Steps:
1. Run an external review of the `linux` adapter's mount and trampoline code against bubblewrap's steps, the proxy, and the agent preset.
2. Fuzz the spec decoder and the proxy parsers for 1 h each (target).
3. Fix every finding, or record it with its owner.

Tests:
- `spec_decode_fuzz_corpus_replays`
- `proxy_request_fuzz_corpus_replays`

**PR size:** M.

### Task 19: Docs and the design amendments

**Files:**
- `docs/guides/sandbox.md` (what each level does on each OS; the AppArmor sysctl or profile for Ubuntu; Kubernetes notes);
- `crates/loams-sandbox/README.md`;
- the amendments §56 §16 lists:
  - §24 §4.1 and §10;
  - §49 §13.2's sentence;
  - §15 §8 and its W2 row;
  - §11's buy-versus-build row;
  - §16's fleet demo rows;
  - §53 §8.2's Q815 line;
- the decision log's status column for D990–D1009 as built;
- `docs/design/README.md` and `docs/plans/README.md` statuses.

**PR size:** S.

### Task 20: Exit

Steps:
1. Every exit criterion below is checked, with the run that shows it.
2. Record the open items in "Rulings made during execution".

**PR size:** S.

## Exit criteria (§56, with the owning tasks)

1. House and Live run on `loams-sandbox` with identical rules (Tasks 2, 3).
2. The `linux` kit passes on `ubuntu-24.04` and inside rootless podman. The `seatbelt` and `appcontainer` kits pass on their runners. The `gvisor`, `wasm` and `microvm` kits pass nightly (Tasks 4, 5, 11, 12, 14, 15, 16).
3. A Studio thread at `workspace-write` on Linux, macOS and Windows cannot read a planted `~/.ssh` key, cannot write outside its worktree, and cannot reach a host not on its allowlist, and its header says who enforces (Tasks 9–13).
4. `linux` spawn overhead ≤ 15 ms p95 (target), measured in Task 6. Proxy latency ≤ 2 ms p95 (target), measured in Task 7.
5. Multi-tenant spawns on bare `linux` are refused (Task 14).
6. The security review is closed (Task 18).

## Self-review

- **Every owner direction item maps to a task:**
  - the spec and trait: Tasks 1, 6, 8;
  - the ladder: Tasks 6, 11, 12, 14, 15, 16;
  - the threat model: Task 14's refusal;
  - build steps 1–6: Tasks 2, 3, 4–5, 9–13, 14, 16–17;
  - limits open and metering private: the Global Constraint and Task 8.
- **Every test name is unique in the plan.** Every task has files, interfaces, named tests, steps and a size.
- **Frozen behaviour.** House and Live are frozen by golden files before any change.

## Open questions for the owner

These are the §56 §19 questions that change behaviour people see. Each has a default that SB1 follows unless the owner rules otherwise.
- **Q883:** `workspace-write`'s network becomes an allowlist; today Codex sets `networkAccess: true`.
- **Q885:** when Loams cannot enforce, run the thread labelled, or refuse.
- **Q884:** AppContainer or a restricted token on Windows. Task 0 measures both.
- **Q881:** drop microsandbox as §15's default.
- **Q889:** the `linux` kit joins `CI required`.

## Rulings made during execution

(none yet)
