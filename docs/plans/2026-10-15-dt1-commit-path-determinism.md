# DT1 — Commit-Path Determinism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Work task by task, test first. Each task lists its files, the interfaces it must produce, the tests that must exist and pass before it is done (exact function names), its steps and its pull-request size. Where this plan gives exact names, paths, flags or defaults, use them verbatim. Where it gives a contract and named tests, write the code to that contract, and record any deviation in "Rulings made during execution" at the end of this file. The code is not pre-written in this plan.
>
> **Status: Planned: starts after v0.1.0-preview.3** (written 2026-10-15). Track DT, design [§55](../design/55-commit-path-determinism.md) (D946–D962, Q835–Q842). Owner direction of 2026-10-15: "Loam Io" is not adopted as written; five parts are adopted narrowly; the pilot is `loams-log`'s WAL and manifest commit path and runs after the upgrades release (v0.1.0-preview.3, [UP1](2026-10-14-up1-upgrades.md)); a measured gate decides the rollout. The pilot is Tasks 0–7, the gate Task 8; the rollout (Tasks 10–16) runs only on a "go". Tasks 9, 17, 18 and 19 run either way.

**Goal:** the decisions on Loams' commit paths (when to flush, retry, classify an outcome, fail a fenced append, delete a written object) run as sans-I/O machines that read time and randomness only through an injected `Clock` and `Entropy`; they are simulated bit-exactly with `loams-detsim`; every async function on those paths states its cancel-safety and the WAL and manifest commits are swept at every suspension point; the store models can show stale listings, real races and lost unsynced writes; liveness loops cannot die silently; and the optimizer can see CPU time apart from wall time. First on `loams-log`, and on the other commit-path crates only if the pilot earns it.

**Architecture** (§55 §1.3, §5, §6):
- **`loams_common::det`** (new module, std only): `Clock`, `Mono`, `Entropy`, `SystemClock`, `ManualClock`. **`loams_common::machine`**: `Machine`, `Ctx`, `Millis`, `TraceSink`, `SpecEvent`, moved from `loams-sqlrouter` (re-exported there unchanged).
- **`crates/loams-task`** (new): `TokioClock`, `OsEntropy`; later `TaskSet`, `hedge`, `spawn_concurrent` (Task 18) and `cpu::{Accounted, accounted_blocking}` (Task 19).
- **`crates/loams-detsim`** (RT1 Tasks 1–2's crate; built here if RT1 has not): the scheduler, network model, fault points and shrinking, plus `cancel::{CancelAfter, sweep}` and `SeededEntropy`.
- **`loams-log`**: `LogIo { clock, entropy }`; the machines `writer::policy::Buffer`, `commit::CommitRetry`, `fence::partition`, `segmenter::swap::Decision`; scenarios in `crates/loams-log/tests/sim/`; sweeps in `crates/loams-log/tests/it/cancel.rs`; a crate-level `clippy.toml`.
- **`loams-store`**: list views, per-key history and races in `FaultyStore`; `disk::TornDisk` (feature `torn-disk`). **`loams-meta`**: a `test-util` backend hook for `meta.redb`.
- **CI:** `scripts/ci/{ambient-inventory,ambient-ratchet,cancel-tags}.py` and `scripts/ci/commit-path.toml` in `Repo: checks`; the scenario and torn-disk budgets in `Engine: simulation`.

**Tech Stack:**
- Rust 1.97 (workspace `rust-version`), edition 2024, workspace lints (`unsafe_code = "forbid"`).
- Workspace crates: `loams-common`, `loams-sqlrouter`, `loams-log`, `loams-store`, `loams-meta`, `loams-meta-conformance`, `loams-sim`, `loams-worker`; in the rollout `loams-collection`, `loams-link`, `loams-hot`, `loams-graph`, `loams-git`, `loams-live`, `loams-safekeeper`; `loams-query` for Task 19.
- New dependencies, only those already named: `rand_core` 0.10 in `loams-common` (as `loams-sqlrouter` has it); RT1's `rand` 0.10 (`chacha`), `rand_core` 0.10, `sha2`, `serde_json` for `loams-detsim`; `redb` (workspace) as an optional dependency of `loams-store`; `rustix` (workspace) gains its `time` feature (and `process` for `getrusage`, verify) in Task 19. No other new third-party crate; any addition follows the 14-day rule.
- Python 3.12 for the CI scripts, standard library only, each with `--self-test`.

**Spec:**
- [§55](../design/55-commit-path-determinism.md) (all of it).
- [§31](../design/31-loams-router-and-verification.md) §7.1, §13; [§12](../design/12-roadmap-testing-risks.md) §2 items 1–3; [§13](../design/13-decision-log.md) D28, D313, D314, D744.
- [RT1](2026-10-01-rt1-postgres-slice-and-sim.md) Tasks 1–2 (`loams-detsim`), Global Constraints ("Bit-exact simulation") and Rulings 1 and 6.
- [M0.4](2026-09-24-m0.4-workers-links-gates.md) (the seeded simulation and the crash gate), [M0.1](2026-09-23-m0.1-storage-primitives.md) (`FaultyStore`).
- D541 and D552 for Task 19.

## Global Constraints

- **Worktree and branches.** Work in `~/Documents/Ostriumlabs/loams-wt/dt1`. One branch per task, `dt1-t<N>`, from fresh `origin/dev`. **Each task is its own pull request into `dev`**, reviewed by one CodeRabbit round (with the 20-minute rate-limit rule), then merged with `gh pr merge <n> -R ostrium-labs/loams --admin --merge` once `CI required` is green. `git commit -s` (DCO). Stage only your own paths; never `git stash`; never commit `.superpowers/`. Commit areas: `det`, `task`, `sim`, `log`, `store`, `meta`, `query`, `ci`, `docs`, and the rollout crates' own.
- **Not before preview.3.** Task 0 checks that `v0.1.0-preview.3` is tagged; until it is, no DT1 task starts.
- **Rust builds** use the shared target directory (`~/Documents/.cargo/config.toml`). Never set `CARGO_TARGET_DIR`, never build in `/tmp`. One cargo build at a time. Build and test the touched crates (`cargo test -p loams-log`, `cargo clippy -p loams-log --all-targets -- -D warnings`); never `cargo build --workspace --all-features` locally.
- **No behaviour change outside what a task names.** Extracting a machine must leave every existing test of the crate passing unchanged; a test edited to pass is a deviation and goes in the rulings.
- **Machines are pure.** A machine module has no `async`, no tokio, no `std::time`, no `std::thread`, no I/O and no ambient randomness; it iterates only `BTreeMap`/`BTreeSet`/`Vec`; its time and randomness come from `Ctx`. A reviewer rejects a machine that calls out.
- **Drivers decide nothing.** The driver executes a machine's outputs and feeds results back. Any `if` in a driver that is not about executing an output is a finding.
- **Bit-exact means bit-exact.** No test under `crates/*/tests/sim/` reads the wall clock, spawns threads or uses an unseeded RNG (RT1's rule); `loams-detsim` asserts it runs on the thread that created the scheduler.
- **The sweeps use real code.** Cancel sweeps and torn-disk tests run the production driver over a single-node metastore (`loams-meta`, openraft and redb in a temporary directory) and a `FaultyStore` over the in-memory store. They are seeded, not bit-exact, and report the suspension points they reached.
- **Wall time the metastore checks stays the metastore's** (`MetaStore::now_ms`: ULID times, `created_at_ms`, freshness). `Clock::now_ms` is for everything else (D954).
- **Nothing billing-grade** (Task 19): no namespace or tenant label, and no new export path.
- **Hermetic tests.** Crate tests use the in-memory or `file://` store and in-process metastores. TiKV-backed sweeps (Task 15) are `#[ignore]` and run in the TiKV job.

## Rulings (from §55 and the owner's direction; binding for execution)

| # | Ruling | Source |
|---|---|---|
| R1 | "Loam Io" is not adopted as written: no `&Io` through functions, no engine-wide simulator, no Workers backend, no io_uring work, no engine-wide `HashMap` ban | D946, D952 |
| R2 | Commit-path code reads time and randomness only through injected `Clock` and `Entropy`; its decisions are machines simulated bit-exactly with `loams-detsim`; D28's seeded simulation stays for the rest | D947 |
| R3 | Every async function on a scoped commit or recovery path carries one of the three cancel tags; the WAL commit and the segment swap get cancel-at-every-await sweeps | D948, D956 |
| R4 | `FaultyStore` gains stale and moving listings, per-key history and real races; `TornDisk` models lost unsynced writes | D949, D957 |
| R5 | `TaskSet` (fail-fast, first-ok), `hedge` and `spawn_concurrent` in `loams-task` | D950, D960 |
| R6 | CPU against wall accounting is observability, never billing | D951, D961 |
| R7 | The pilot is `loams-log`, after preview.3; the gate decides the rollout, and the owner rules on the gate report | D953, D958 |
| R8 | The seam lives in `loams_common::{det, machine}` and `loams-task` (Q836's default) | D954 |
| R9 | If RT1 Tasks 1–2 have not merged, Task 2 builds them to RT1's spec (Q837's default) | D954, RT1 |
| R10 | Lint bans: crate-level `clippy.toml` plus module-level `deny`/`allow`; driver sites carry `#[expect(…, reason)]`, ratcheted | D959 |
| R11 | Rollout order: collection/link/hot manifests, metastore client and leases, worker leases, Graph ownership, Git ref log, Live journal, safekeeper journal | D962 |

## Review Focus

1. **A machine that is not pure.** It reads a clock, an RNG, a `HashMap` iteration order, or does I/O. Expected: never. Tests: Task 4 `wal_commit_trace_hash_stable` (two runs per seed); Task 3's `clippy.toml`.
2. **A driver that decides.** Logic left in the tokio code that the simulation never runs. Expected: none. Tests: Task 4's scenarios plus Task 7's sweeps over the real driver.
3. **An outcome reported as definite when it is unknown.** Expected: never. Tests: Task 4 `commit_outcome_classification_exhaustive`, `wal_commit_unknown_never_reported_definite`.
4. **A commit split by a cancel.** Some partitions of one WAL object visible, or a segment and its chunks both indexed. Expected: never. Tests: Task 7 `sweep_write_and_commit`, `sweep_segment_swap`.
5. **A false success.** `flush()` returning `Ok` for a flush that did not happen. Expected: never. Test: Task 7 `flush_task_panic_fails_waiters_not_ok` (H1).
6. **An acknowledged append lost to a power cut.** Expected: never. Test: Task 6 `torn_meta_disk_loses_no_acked_append`.
7. **A model that lies.** A `Race` that does not reach the inner store, a listing view that hides an object forever. Tests: Task 5 `race_create_makes_put_if_absent_fail_for_real`, `stale_view_hides_recent_puts`, `fresh_view_is_unchanged`.
8. **Billing creep.** A CPU metric with a tenant label. Test: Task 19 `metrics_have_no_namespace_label`.

---

## File structure

```
crates/loams-common/src/det.rs                       (Clock, Mono, Entropy, SystemClock, ManualClock; Task 1)
crates/loams-common/src/machine.rs                   (moved from loams-sqlrouter; Task 1)
crates/loams-common/tests/{det.rs,machine.rs}
crates/loams-sqlrouter/src/{machine.rs,trace.rs}     (re-exports only; Task 1)
crates/loams-task/{Cargo.toml,src/lib.rs,src/clock.rs,src/entropy.rs}               (Task 1)
crates/loams-task/src/{set.rs,hedge.rs,concurrent.rs}                                (Task 18)
crates/loams-task/src/cpu.rs                                                         (Task 19)
crates/loams-task/tests/{clock.rs,set.rs,concurrent.rs,cpu.rs}
crates/loams-detsim/…                                (RT1 Tasks 1–2's files, if built here; Task 2)
crates/loams-detsim/src/{cancel.rs,entropy.rs}       (Task 2)
crates/loams-detsim/tests/it/{cancel.rs,entropy.rs}
crates/loams-log/clippy.toml                         (Task 3)
crates/loams-log/src/io.rs                           (LogIo; Task 3)
crates/loams-log/src/writer/{mod.rs,policy.rs,driver.rs}  (writer.rs split; Task 4)
crates/loams-log/src/commit.rs                       (CommitRetry; Task 4)
crates/loams-log/src/fence.rs                        (partition; Task 4)
crates/loams-log/src/segmenter/{mod.rs,swap.rs}      (segmenter.rs split; Task 4)
crates/loams-log/src/testing.rs                      (feature test-util: flush_once, swap_once; Task 7)
crates/loams-log/tests/it/{io.rs,machines.rs,cancel.rs,torn.rs,baseline.rs}
crates/loams-log/tests/sim/{main.rs,model.rs,wal_commit.rs,swap.rs,corpus/*.seed}
crates/loams-store/src/fault.rs                      (ListView, Race, history; Task 5)
crates/loams-store/src/disk.rs                       (TornDisk, feature torn-disk; Task 6)
crates/loams-store/tests/it/{fault.rs,disk.rs}
crates/loams-meta/src/{node.rs,db.rs}                (MetaConfig::db_backend, test-util; Task 6)
scripts/ci/{ambient-inventory.py,ambient-ratchet.py,cancel-tags.py,commit-path.toml}
scripts/ci/testdata/{ambient,cancel-tags}/…
.github/workflows/ci.yml                             (Repo: checks; Engine: simulation; Task 9)
docs/design/55-commit-path-determinism.md            (as built; Tasks 8, 20)
docs/plans/README.md, docs/ci/README.md, CHANGELOG.md
```

## Shared contracts (all tasks use these names)

```rust
// loams_common::det
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mono(pub u64);                                   // ns since the clock's origin
impl Mono { pub fn saturating_add(self, d: Duration) -> Mono; pub fn saturating_duration_since(self, earlier: Mono) -> Duration; }
pub trait Clock: Send + Sync + fmt::Debug { fn now_ms(&self) -> u64; fn mono(&self) -> Mono; }
pub trait Entropy: Send + Sync + fmt::Debug { fn fill(&self, dest: &mut [u8]); fn u128(&self) -> u128; }
pub struct SystemClock;                                     // std::time; origin captured on first use
pub struct ManualClock;                                     // new(wall_ms), set(wall_ms), advance(Duration): both clocks move

// loams_common::machine (moved verbatim from loams_sqlrouter::{machine, trace})
pub struct Millis(pub u64);  pub struct Ctx<'a> { pub now: Millis, pub rng: &'a mut dyn rand_core::Rng, pub trace: &'a mut dyn TraceSink }
pub trait Machine { type Input; type Output; fn on(&mut self, ctx: &mut Ctx<'_>, input: Self::Input) -> Vec<Self::Output>; }
pub trait TraceSink { fn emit(&mut self, event: SpecEvent); }   pub struct SpecEvent { /* as in loams-sqlrouter */ }

// loams_task
pub struct TokioClock;  pub struct OsEntropy;

// loams_log
pub struct LogIo { pub clock: Arc<dyn Clock>, pub entropy: Arc<dyn Entropy> }   // LogIo::system() = TokioClock + OsEntropy
impl LogWriter { pub fn start_with(meta, store, config, io: LogIo) -> Result<Self, LogError>; }   // start() = start_with(…, LogIo::system())

// loams_detsim::cancel
pub struct CancelAfter<F>;  pub enum Cancelled<T> { Completed(T), DroppedAt(u32) }
pub struct SweepReport { pub points_uncancelled: u32, pub points_reached: BTreeSet<u32>, pub failures: Vec<(u32, String)> }
pub fn sweep<S>(setup: impl FnMut() -> S, run: impl FnMut(S, Option<u32>) -> Result<(), String>) -> SweepReport;

// loams_detsim::entropy
pub struct SeededEntropy;   // new(seed); repeating(seed, period): repeats its output every `period` draws, to force collisions
```

Cancel tags (doc section `# Cancel safety`, first word): `cancel-safe:`, `cancel-unsafe, use <method>().await:`, `atomic-commit:`.

`scripts/ci/commit-path.toml`:

```toml
[[scope]]
crate = "loams-log"
whole_crate = true
ratchet = { "src/writer/driver.rs" = 2, "src/reader.rs" = 1 }   # #[expect(clippy::disallowed_methods)] sites; Task 3 sets the numbers
cancel_tags = ["src/writer/mod.rs", "src/writer/driver.rs", "src/segmenter/mod.rs", "src/reader.rs", "src/retention.rs", "src/gc.rs"]
```

Environment knobs: `DT_SIM_SEEDS` (scenario seeds; default 200), `TORN_SEEDS` (torn-disk crashes; default 50), `CANCEL_SWEEP_REPEAT` (default 1).

## Execution order

1. Task 0, after `v0.1.0-preview.3` is tagged.
2. **Pilot:** Task 1; then Task 2 (needs Task 1's move); Task 3 after Task 1; Task 4 after Tasks 2 and 3; Task 5 beside Task 4; Task 6 after Task 5's crate changes merge; Task 7 after Tasks 4–6.
3. **Gate:** Task 8 after Task 7. The owner rules (Q835).
4. **CI scoping:** Task 9 after Task 8, either way (for `loams-log` alone on a "no go").
5. **Rollout, on a "go" only:** Tasks 10–16 in order; Tasks 13 and 14 may run in parallel.
6. **Either way:** Task 17 after Tasks 5 and 6; Task 18 any time after Task 1 (Task 7's H1 fix moves onto it when it lands); Task 19 any time after Task 1.
7. Task 20 last.

---

## DT1a — The pilot (Tasks 0–7)

### Task 0: Inventory, baseline and reconciliation

**Files:** `scripts/ci/ambient-inventory.py`, `scripts/ci/testdata/ambient/` (planted files: a site in a `#[cfg(test)] mod`, one in a comment, one in `tests/`, one real), `crates/loams-log/tests/it/baseline.rs`, this plan's "Rulings made during execution".

**Produces:** `ambient-inventory.py [--crate NAME] [--list] [--json]`: counts, per crate and per file, the production sites of `Instant::now`, `.elapsed()`, `SystemTime::now`, `tokio::time::{sleep, sleep_until, timeout, interval}`, `tokio::spawn`, `spawn_blocking`, `thread::spawn`, `thread::Builder`, `Ulid::{new, generate}`, `Uuid::{new_v4, now_v7}`, `rand::{rng, thread_rng, random}` and unbiased `tokio::select!`, excluding `tests/`, `benches/`, `examples/`, `#[cfg(test)]` modules and comments (§55 §3.2's method); `--list` prints `file:line call`.

**Tests:**
- `ambient_inventory_self_test` (`python3 scripts/ci/ambient-inventory.py --self-test`: the planted real site counts, the others do not)
- `writer_baseline` (`#[ignore]`; 10 000 single-record appends to 4 partitions on a single-node metastore and the in-memory store; prints appends/s and p50/p99 latency; median of five runs)

**Steps:**
1. Check `v0.1.0-preview.3` is tagged (`git tag -l v0.1.0-preview.3`); stop if not.
2. Write and self-test the script; run it on `loams-log` and on every crate of §55 §11.5; record the table (expect §55 §12's sixteen `loams-log` sites at `f1283fec`, and say what moved).
3. Reconcile: is `crates/loams-detsim` on `dev` (RT1 Tasks 1–2)? Does `loams-sqlrouter` still hold `machine.rs` and `trace.rs` unchanged? Do `writer.rs` and `segmenter.rs` still match §55 §6.2's line ranges? Re-read H1–H3 (§55 §11.2) against the code and record whether each still applies.
4. Run `writer_baseline` and record it (gate C2). Record the `Engine: simulation` job's current duration on a pull request (gate C3).
5. Record everything under "Rulings made during execution", rows T0-1….

Commit `ci: ambient call-site inventory and the writer baseline`. **PR size:** S (~250 lines).

### Task 1: The seam: `loams_common::{det, machine}` and `loams-task`'s clock and entropy

**Files:** `crates/loams-common/{Cargo.toml,src/lib.rs,src/det.rs,src/machine.rs,tests/det.rs,tests/machine.rs}`; `crates/loams-sqlrouter/src/{machine.rs,trace.rs,lib.rs}` (re-exports), `crates/loams-sqlrouter/Cargo.toml`; `crates/loams-task/{Cargo.toml,src/lib.rs,src/clock.rs,src/entropy.rs,tests/clock.rs}`; workspace `Cargo.toml`.

**Interfaces:** the shared contracts above. `loams-common` gains only `rand_core` 0.10. `loams-sqlrouter`'s `machine` and `trace` modules become `pub use loams_common::machine::…`, so every path RT0 and RT1 use still resolves. `loams-task` depends on `loams-common`, `tokio` (`time`, `rt`), `rand` (workspace).

**Tests:**
- `system_clock_mono_is_monotonic`
- `manual_clock_moves_wall_and_mono_together`
- `mono_saturating_arithmetic`
- `entropy_u128_uses_sixteen_bytes`
- `machine_seam_has_no_io_dependencies` (reads `loams-common`'s `Cargo.toml`: no tokio, sockets, files or OS randomness crates)
- `machine_types_are_reexported_from_common` (in `loams-sqlrouter`; plus its existing suite unchanged, including `kernel_crate_has_no_io_dependencies`)
- `tokio_clock_follows_paused_time` (`#[tokio::test(start_paused = true)]`: `advance(5 s)` moves `mono()` by 5 s)
- `os_entropy_values_differ`

**Steps:** tests first; add `det.rs`; move `machine.rs` and `trace.rs` into `loams-common` with `git mv` history where possible and leave re-exports; create `loams-task`; run `cargo test -p loams-common -p loams-sqlrouter -p loams-task` and `cargo clippy` on the three.

Commit `det: shared Clock, Entropy and machine seam`. **PR size:** M (~450 lines, half of it moved).

### Task 2: `loams-detsim`: the scheduler (RT1 Tasks 1–2 if needed), the cancel sweep and seeded entropy

**Files:** if Task 0 found no `loams-detsim`: RT1 Tasks 1–2's files exactly (`crates/loams-detsim/{Cargo.toml,src/{lib,sched,net,fault,trace,shrink,report}.rs,tests/it/{main,sched,net,shrink}.rs}`), with `NodeCx` naming `loams_common::machine::TraceSink` instead of `loams_sqlrouter::TraceSink`. In every case: `crates/loams-detsim/src/{cancel.rs,entropy.rs}`, `crates/loams-detsim/tests/it/{cancel.rs,entropy.rs}`.

**Interfaces:** RT1 Tasks 1–2's `Sim`, `SimConfig`, `Node`, `NodeCx`, `SimReport`, `LinkModel`, `NetFault`, `FaultSelection`, `fault_point!`, `shrink` verbatim; `cancel` and `entropy` as in the shared contracts. `cancel` uses only `std::future` and `std::task` (a no-op waker is not used: `CancelAfter` wraps the future inside whatever runtime polls it).

**Tests:**
- RT1's, if built here: `same_seed_same_trace_hash`, `different_seeds_differ`, `timers_fire_in_time_order`, `crash_drops_volatile_state_and_restart_runs_hook`, `report_describes_seed_and_schedule`, `partition_blocks_both_ways_when_symmetric`, `asymmetric_partition_blocks_one_way`, `drop_and_duplicate_rates_hold`, `reset_makes_in_flight_requests_indeterminate`, `swarm_selection_is_a_function_of_the_seed`, `shrink_finds_the_single_fault_that_matters`
- `cancel_after_drops_at_kth_pending`
- `cancel_after_completes_when_k_exceeds_points`
- `sweep_covers_every_point_of_the_uncancelled_run`
- `sweep_reports_missed_points`
- `seeded_entropy_is_reproducible`
- `seeded_entropy_repeating_forces_a_collision`

**Steps:** if building RT1's tasks, follow RT1's Task 1 and Task 2 text and tests exactly and update RT1's status line ("Tasks 1–2 built by DT1 Task 2, PR #…"); then add `cancel` and `entropy`; `cargo test -p loams-detsim`.

Commit `sim: loams-detsim cancel sweep and seeded entropy` (plus RT1's two commits if built here). **PR size:** M with RT1 merged (~350 lines); L otherwise (~1 500 lines; split into RT1 Task 1, RT1 Task 2, and this task's additions as three pull requests).

### Task 3: `loams-log`: inject `Clock` and `Entropy`, and ban the ambient calls

**Files:** `crates/loams-log/{Cargo.toml,clippy.toml,src/io.rs,src/lib.rs,src/writer.rs,src/segmenter.rs,src/reader.rs,src/retention.rs,src/gc.rs}`, `crates/loams-log/tests/it/{main.rs,io.rs}`, `scripts/ci/{ambient-ratchet.py,commit-path.toml}`, `scripts/ci/testdata/ambient/ratchet/`.

**Interfaces:** `LogIo` and `LogWriter::start_with` (shared contracts); `SegmenterSource`, `Segmenter`, `RetentionSource`, `Retention`, `GcSource`/`Gc` and `LogReader` gain `with_io(LogIo)` builders (defaults unchanged). Every site of §55 §12's table reads `io.clock` or `io.entropy`, except the driver's `sleep_until`/`sleep` and the flush task's spawn, which carry `#[expect(clippy::disallowed_methods, reason = "…")]`. Both `tokio::select!`s become `biased;`. `crates/loams-log/clippy.toml` is §55 §6.4's list plus the workspace settings. `ambient-ratchet.py` counts `#[expect(clippy::disallowed_methods` per file listed in `commit-path.toml` and fails above the recorded number.

**Tests:**
- `wal_ulid_random_part_comes_from_entropy`
- `segment_ulid_random_part_comes_from_entropy`
- `flush_interval_follows_injected_clock` (paused time)
- `commit_deadline_follows_injected_clock` (a metastore that always times out; paused time; `CommitUnknown` exactly at the deadline)
- `long_poll_deadline_follows_injected_clock`
- `retention_interval_follows_injected_clock`
- `gc_interval_follows_injected_clock`
- `start_keeps_its_signature_and_defaults`
- `ambient_ratchet_self_test` (`python3 scripts/ci/ambient-ratchet.py --self-test`)
- the existing `loams-log` suite, with and without `--features failpoints`, unchanged

**Steps:**
1. Tests first (they fail: the clock is ambient).
2. Add `io.rs`; thread `LogIo` through the constructors; convert the sixteen sites.
3. Add `clippy.toml`; check whether `tokio::spawn` must be listed as `tokio::task::spawn` (record which, R-T3-1); add `#![allow(clippy::disallowed_methods)]` to `tests/it/main.rs`.
4. `cargo clippy -p loams-log --all-targets -- -D warnings`, `cargo test -p loams-log`, `cargo test -p loams-log --features failpoints`.

Commit `log: inject Clock and Entropy; ban ambient calls in loams-log`. **PR size:** M (~500 lines).

### Task 4: `loams-log`: the four machines and their bit-exact scenarios

**Files:** `crates/loams-log/src/writer/{mod.rs,policy.rs,driver.rs}` (from `writer.rs`), `src/commit.rs`, `src/fence.rs`, `src/segmenter/{mod.rs,swap.rs}` (from `segmenter.rs`), `crates/loams-log/tests/it/machines.rs`, `crates/loams-log/tests/sim/{main.rs,model.rs,wal_commit.rs,swap.rs,corpus/}`, `crates/loams-log/Cargo.toml` (`[[test]] name = "sim"`, dev-dependency `loams-detsim`).

**Interfaces** (all `Machine`s over `loams_common::machine::Ctx`, or pure functions):
- `policy::Buffer`: inputs `Appended { bytes }`, `FlushRequested`, `FlushDone { requeued_bytes }`, `Close`, `Tick`; outputs `Flush`, `WaitUntil(Millis)`, `Wait`, `Exit`. It owns the counters (`pending_bytes`, `inflight_bytes`, `opened_at`, `flushing`, `closed`, `stopped`); the driver owns the `Pending` values and channels.
- `commit::CommitRetry::new(deadline: Duration, backoff: (Duration, Duration))`: inputs `Start`, `Attempted(AttemptResult, earlier_unknown: bool)`, `Timer`; outputs `CommitWal`, `SleepUntil(Millis)`, `Done(CommitOutcome)` where `CommitOutcome = Committed(Vec<u64>) | Rejected(ApplyError) | ClockSkew { stamped_ms, leader_ms } | Unknown(String)`.
- `fence::partition(appends: &[FenceRef], lease: &LeaseId, current: Option<u64>) -> Partition { fail: Vec<usize>, requeue: Vec<usize>, fail_all: bool }`.
- `swap::Decision`: `after_put(written_at_ms, now_ms, swap_deadline, cancelled) -> AfterPut { Swap | SkipAndDelete }`; `after_swap(result, earlier_unknown) -> AfterSwap { Swapped | Skip { delete } | Fenced { delete } | Unknown }`.
- `tests/sim/model.rs`: `WalCommitModel` (a `loams-detsim` `Node`): assigns dense offsets per partition, remembers commit records for `WAL_COMMIT_WINDOW_MS`, answers a retry of a known object with its offsets, refuses a pruned or stale object with `StaleCommit`, checks fences; `StoreModel` for create-only PUTs. Network drops and resets (RT1 Task 2) produce unknown outcomes.
- Spec events: `wal.flush`, `wal.attempt`, `wal.outcome`, `seg.swap` (the trace hash covers them).

**Tests:**
- `buffer_flushes_within_interval` (proptest over input sequences)
- `buffer_bytes_return_to_zero`
- `buffer_requeued_appends_go_first`
- `buffer_answers_each_waiter_once`
- `commit_outcome_classification_exhaustive` (P2: every sequence of up to six results, five kinds × `earlier_unknown`, every deadline position)
- `commit_never_attempts_after_deadline`
- `commit_one_attempt_in_flight`
- `fence_partition_fails_only_stale_epochs`
- `fence_partition_unreadable_lease_fails_all_under_it`
- `fence_partition_keeps_order`
- `swap_decision_deletes_only_when_definitely_unreferenced`
- `wal_commit_trace_hash_stable` (P1: `DT_SIM_SEEDS` seeds, two runs each, equal hashes)
- `wal_commit_acked_exactly_once` (every acknowledged append is at its offsets once in the model; offsets dense)
- `wal_commit_unknown_never_reported_definite`
- `swap_scenario_trace_hash_stable`
- `corpus_replays` (every `corpus/*.seed` twice, equal hashes)
- the existing `loams-log` suite unchanged

**Steps:** write the machine tests first against the interfaces; extract each machine from the existing code without changing behaviour (the existing suite must pass after each extraction, one commit per machine); write the model and scenarios; run `DT_SIM_SEEDS=10000 cargo test -p loams-log --release --test sim` once locally and record the time (Q842's budget); add any failing seed to the corpus with its fix.

Commit `log: commit-path machines and bit-exact scenarios`. **PR size:** L (~1 400 lines; split "machines" and "scenarios" if review asks).

### Task 5: `FaultyStore`: listing views, per-key history and real races

**Files:** `crates/loams-store/src/{fault.rs,lib.rs}`, `crates/loams-store/tests/it/fault.rs`; `crates/loams-log/tests/it/{gc.rs,writer.rs}`.

**Interfaces:** `pub enum ListView { Fresh, Stale { lag: u32 }, Moving }`, `FaultyStore::set_list_view`; `pub enum RaceWrite { Create(Bytes), Overwrite(Bytes) }`, `Fault::Race(RaceWrite)` (for `PutCreate` and `PutIfMatch`; other ops treat it as `Error`); `FaultyStore::history(path) -> Vec<VersionRecord { seq: u64, etag: Option<String>, len: u64, writer: Writer::{Caller, Race}, deleted: bool }>`. `FaultyStore` records every mutation it forwards (sequence number, path, kind), which the views and the history use. `Stale` both hides creates and shows deletes (Q841's default). `Moving` re-reads the inner store between pages.

**Tests:**
- `stale_view_hides_recent_puts`
- `stale_view_shows_recent_deletes`
- `moving_view_pages_see_different_moments`
- `fresh_view_is_unchanged` (the existing listing tests, run under `Fresh`)
- `history_records_every_version_and_writer`
- `race_create_makes_put_if_absent_fail_for_real`
- `race_overwrite_makes_put_if_match_fail_for_real`
- `race_is_visible_in_history`
- `gc_never_deletes_referenced_object_under_stale_list` (`loams-log`)
- `gc_reclaims_late_object_under_retired_prefix_with_stale_list` (`loams-log`; H2: expected to fail if H2 holds; then fix GC, or record the leak as accepted with the owner)
- `wal_path_collision_never_commits_foreign_object` (`loams-log`: two writers with the same `node_id` and `SeededEntropy::repeating`, plus `Race`; the second writer's appends fail with a store error and nothing of the first writer's is acknowledged to it)

**Steps:** model tests first; implement the mutation log, views, race and history; then the `loams-log` tests; record H2's result.

Commit `store: listing views, per-key history and real races`. **PR size:** M (~700 lines).

### Task 6: `TornDisk` and the metastore's backend hook

**Files:** `crates/loams-store/{Cargo.toml,src/disk.rs,src/lib.rs,tests/it/disk.rs}` (feature `torn-disk`, optional `redb`); `crates/loams-meta/{Cargo.toml,src/node.rs,src/db.rs}` (feature `test-util`); `crates/loams-log/tests/it/{torn.rs,common/mod.rs}`.

**Interfaces:** `pub struct TornDisk` implementing `redb::StorageBackend`: `new()`, `durable_image() -> Vec<u8>`, `crash(seed) -> TornDisk` (a new backend whose content is the durable image with each unsynced write dropped, kept or torn at a 512-byte boundary, by the seed), `unsynced_writes() -> usize`. `MetaConfig::db_backend: Option<Arc<dyn Fn() -> Box<dyn redb::StorageBackend> + Send + Sync>>` under `test-util` (Q838); `LocalDb::open` uses `Database::builder().create_with_backend` when it is set.

**Tests:**
- `torn_disk_keeps_synced_writes`
- `torn_disk_drops_or_tears_unsynced_writes`
- `torn_disk_crash_is_a_function_of_the_seed`
- `torn_disk_tears_on_512_byte_boundaries`
- `db_backend_hook_is_used_when_set` (`loams-meta`)
- `meta_survives_torn_crash_after_acked_write` (`loams-meta`: a lease acquired before the crash is there after reopen)
- `torn_meta_disk_loses_no_acked_append` (`loams-log`, P4: `TORN_SEEDS` crashes at seeded points during a workload; reopen; every acknowledged append is in the partition index at its offsets)
- `torn_meta_disk_never_serves_shorter_log` (reopen succeeds with every acknowledged append, or fails with a corruption error; never opens without one)

**Steps:** tests first; write `TornDisk` against `redb`'s `StorageBackend` contract (`len`, `read`, `set_len`, `sync_data`, `write`; verify the contract and record it); add the hook; run 1 000 seeds once locally and record the time. A loss is first reproduced with a minimal redb program (Risk 4 of §55 §14) before any Loams fix.

Commit `store: TornDisk; meta: test-only storage backend hook`. **PR size:** M (~650 lines).

### Task 7: Cancel tags, the tag checker and the sweeps

**Files:** `crates/loams-log/src/**` (doc sections), `crates/loams-log/src/testing.rs` and `Cargo.toml` (feature `test-util`), `crates/loams-log/src/writer/driver.rs` (H1 failpoint `wal.flush_task`, and the fix if H1 holds), `crates/loams-log/tests/it/cancel.rs`, `scripts/ci/{cancel-tags.py,commit-path.toml}`, `scripts/ci/testdata/cancel-tags/`.

**Interfaces:** §55 §7.4's tag on every `async fn` of the `cancel_tags` files; `loams_log::testing::{flush_once(meta, store, config, io, appends) -> Result<Vec<AppendAck>, LogError>, swap_once(…)}` (feature `test-util`), which run one `write_and_commit` and one segment attempt through the real driver code; the failpoint `wal.flush_task` (feature `failpoints`, action `panic`) at the top of the flush loop.

**Tests:**
- `cancel_tags_self_test` (`python3 scripts/ci/cancel-tags.py --self-test`)
- `sweep_write_and_commit` (P3)
- `sweep_commit_retry` (P3; a metastore client that times out once, so the retry path is reached)
- `sweep_segment_swap` (P3)
- `sweep_append_many_caller` (P3; a multi-partition append dropped at each point: all partitions or none)
- `drop_shutdown_neither_loses_nor_hangs`
- `flush_task_panic_fails_waiters_not_ok` (H1)
- `append_after_flush_task_panic_fails_fast` (H1)

**Steps:**
1. Write the sweeps and the H1 tests; run them on the unchanged driver and record what fails (this is the gate's evidence; do not fix before recording).
2. Add the tags; the checker runs in `Repo: checks` for `loams-log`.
3. If H1 holds: the minimal fix (when its task ends unexpectedly, the driver marks the writer stopped, answers every waiter with `LogError::Closed` and refuses new appends with it); Task 18 later moves it onto `spawn_concurrent`.
4. `cargo test -p loams-log --features failpoints,test-util`.

Commit `log: cancel-safety contracts and cancel sweeps`. **PR size:** M (~800 lines).

---

## DT1b — The gate (Task 8)

### Task 8: Measure and report

**Files:** this plan ("Gate report" section), [§55](../design/55-commit-path-determinism.md) (§11 results), [decision log](../design/13-decision-log.md) (D958's status; D947's scope note on a "no go").

**Measure:**
- **Defects found:** each confirmed hypothesis (H1, H2) and any other failure from Tasks 3–7, with the test, the commit it failed on, and whether `loams-sim` (PR and the last 30 nightly runs), the crash gate or `loams-log`'s suites had caught it.
- **Properties:** P1 (seeds and runs at equal hashes), P2 (cases enumerated), P3 (points covered against points in the uncancelled run, per sweep), P4 (crashes run, losses).
- **Cost:** C1 (`git diff --stat` of `crates/loams-log/src` from Task 0's base, tests excluded), C2 (`writer_baseline` now against Task 0), C3 (the PR's added CI minutes), C4 (new third-party crates).

**Decide:** go if (a defect found or two properties proved) and C1–C4 hold (D958). Write the report; ask the owner to rule (Q835); record the ruling as row G-1 in "Rulings made during execution".

Commit `docs: DT1 gate report`. **PR size:** S (docs only).

---

## DT1c — CI scoping (Task 9)

### Task 9: The scoping manifest, the checks and the budgets in CI

**Files:** `scripts/ci/commit-path.toml`, `scripts/ci/{ambient-ratchet.py,cancel-tags.py}` (read the manifest's `crate`, `whole_crate`, `modules`, `ratchet`, `cancel_tags`), `.github/workflows/ci.yml` (`Repo: checks`; `Engine: simulation`; the `changes` filter adds `crates/loams-log/**`, `crates/loams-detsim/**`, `crates/loams-store/**`), `docs/ci/README.md`.

**Interfaces:** for a crate with `whole_crate = false`, `modules = [...]`: the script checks the crate root has `#![allow(clippy::disallowed_methods)]` and each listed module `#![deny(clippy::disallowed_methods)]`. `Engine: simulation` adds `cargo test -p loams-log --release --test sim` (`DT_SIM_SEEDS` 200 on pull requests, 100 000 nightly) and `cargo test -p loams-log --release --features test-util --test it torn::` (`TORN_SEEDS` 50 and 1 000), per Q842's default.

**Tests:**
- `commit_path_manifest_lists_existing_files` (in the scripts' self-tests)
- `every_scoped_module_denies_disallowed_methods`
- `ratchet_fails_on_increase`
- `cancel_tags_fail_on_missing_section`

**Steps:** generalise the two scripts; wire the job steps; measure the PR job time against Task 0 (C3 holds after the change too).

Commit `ci: commit-path scoping manifest and simulation budgets`. **PR size:** S (~300 lines).

---

## DT1d — The rollout (Tasks 10–16; only on a "go")

Every rollout task has the same shape: **Step 1** runs the inventory on its files and lists the commit points; **Step 2** extracts the decisions into machines (reusing `CommitRetry`/`Retry` and `ManifestCommit` where they fit) with exhaustive or proptest tests; **Step 3** injects `Clock` and `Entropy`; **Step 4** adds the tags and the sweeps; **Step 5** adds the crate to `commit-path.toml` with its `clippy.toml`. A rollout task that finds its crate has no decision worth extracting stops after Steps 1, 4 and 5 and says so in the rulings.

### Task 10: Collection, link and hot-tier manifest commits

**Files:** `crates/loams-common/src/machine/manifest.rs` (`ManifestCommit`: after the manifest PUT, decide CAS, skip and delete, or leave to GC, from the CAS result and `earlier_unknown`, the same rule as `swap::Decision`); `crates/loams-collection/src/{target.rs,index_build.rs,maintenance/merge.rs,maintenance/compaction.rs}`, `crates/loams-link/src/counter.rs`, `crates/loams-hot/src/build.rs`, their `clippy.toml`, tests in each crate's `tests/it/cancel.rs`.

**Tests:** `manifest_commit_decision_exhaustive`; `sweep_collection_commit`; `sweep_merge_commit`; `sweep_compaction_commit`; `sweep_index_build_commit`; `sweep_link_counter_commit`; `sweep_hot_build_commit`; `manifest_cas_race_is_real` (`Fault::Race` on `PutIfMatch` of a pointer-adjacent object); `collection_ulid_from_entropy`.

Commit `collection, link, hot: manifest commit machine, tags and sweeps`. **PR size:** L (~1 200 lines; one pull request per crate if review asks).

### Task 11: The metastore client's retries and the lease commands

**Files:** `crates/loams-common/src/machine/retry.rs` (`Retry`, generalised from `CommitRetry`; `loams-log` moves onto it), `crates/loams-meta/src/{client.rs,clock.rs,state_machine.rs}`, `crates/loams-meta/clippy.toml` (module scope: `client`, `clock`), tests in `crates/loams-meta/tests/`.

**Tests:** `retry_machine_exhaustive`; `client_deadline_follows_injected_clock`; `meta_clock_adapts_common_clock` (`loams_meta::Clock` is implemented for every `loams_common::det::Clock`); `sweep_lease_acquire`; `sweep_cas_pointer`; `sweep_commit_wal_client`; `torn_three_node_cluster_loses_no_committed_entry` (nightly; `TornDisk` under each node); the metastore conformance suite unchanged.

Commit `meta: Retry machine, injected clock, lease sweeps`. **PR size:** L (~900 lines).

### Task 12: Worker leases and the scheduler

**Files:** `crates/loams-worker/src/{lib.rs,worker.rs,runner.rs}`, `crates/loams-worker/clippy.toml`, `crates/loams-worker/tests/`.

**Tests:** `renew_interval_follows_injected_clock`; `lease_loss_machine_exhaustive`; `sweep_task_run_cancel` (a task dropped at each point, then `ctx.cancel` semantics: the lease is released or expires, never held by two); `worker_scheduler_exit_is_loud` (with Task 18 merged: `spawn_concurrent`; before it: the scheduler's `JoinHandle` is watched).

Commit `worker: injected clock, lease machine, sweeps`. **PR size:** M (~600 lines).

### Task 13: Graph ownership

**Files:** `crates/loams-graph/src/{owner.rs,durable/append.rs,durable/lane.rs,durable/mod.rs}`, `crates/loams-graph/clippy.toml` (module scope), `crates/loams-graph/tests/cancel.rs`.

**Tests:** `owner_lease_timing_follows_injected_clock`; `ownership_loss_machine_exhaustive` (renewal refused, deadline passed, metastore unreachable: the engine is dropped exactly once, D744's I5); `sweep_graph_fenced_append`; `sweep_owner_takeover`; `graph_ids_from_entropy`; `cargo test -p loams-graph --features failpoints --test failpoints --test durability` unchanged.

Commit `graph: ownership machine, injected clock, sweeps`. **PR size:** M (~700 lines).

### Task 14: Git's ref log

**Files:** `crates/loams-git/src/{clock.rs,sequencer.rs,owner.rs,retry.rs,wal.rs,reflog.rs}`, `crates/loams-git/clippy.toml` (module scope), `crates/loams-git/tests/cancel.rs`.

**Tests:** `git_clock_adapts_common_clock`; `sequencer_retry_uses_retry_machine`; `sweep_ref_log_segment_put`; `ref_log_create_race_is_real` (`Fault::Race(Create)` on the next segment: exactly one writer wins, the loser reloads); `stale_list_never_skips_committed_segment`.

Commit `git: ref-log machines, injected clock, sweeps`. **PR size:** M (~700 lines).

### Task 15: Live's commit journal

**Files:** `crates/loams-live/src/journal.rs` (tags; the `Tailer` and `Janitor` decisions as machines if Step 1 finds any), `crates/loams-live/clippy.toml` (module scope: `journal`), `crates/loams-live/tests/journal_cancel.rs` (`#[ignore]`, `LOAMS_TEST_PD`).

**Tests:** `journal_tags_present` (the checker); `sweep_journal_append` (TiKV); `sweep_journal_trim` (TiKV); `tailer_ack_is_cancel_safe` (positions advance only after an acknowledged batch, whatever point the tick is dropped at).

Commit `live: commit-journal cancel contracts and sweeps`. **PR size:** S–M (~400 lines).

### Task 16: The safekeeper's NVMe journal

**Files:** `crates/loams-safekeeper/src/journal/{mod.rs,segments.rs,format.rs}`, `crates/loams-safekeeper/clippy.toml` (module scope: `journal`), `crates/loams-safekeeper/tests/journal_torn.rs`.

**Step 1 decides the shape:** if the journal's engines write through a seam a test can replace (a file trait, or `Tier::Buffered`'s `pwrite`/`fdatasync` calls in one place), add a `TornFile` model next to `TornDisk` and run the torn tests; if not, add only the tags and the cancel sweep, and record the seam as a follow-up.

**Tests:** `journal_timing_follows_injected_clock`; `sweep_journal_flush_unit`; `torn_journal_loses_no_acked_unit` (if the seam exists); `durable_prefix_never_moves_backwards_after_crash`.

Commit `safekeeper: journal cancel contracts and torn-write tests`. **PR size:** M (~600 lines).

---

## DT1e — Independent of the gate (Tasks 17–19)

### Task 17: `loams-sim` uses the new models

**Files:** `crates/loams-sim/src/sim.rs`, `crates/loams-sim/tests/it/sim.rs`.

**Interfaces:** the seeded simulation (D28) draws `ListView` changes and `Fault::Race` among its store fault bursts, and its writers and segmenters use `LogIo` with `SeededEntropy` from the run's seed. Budgets unchanged.

**Tests:** `sim_draws_list_views_and_races`; `sim_writers_use_seeded_entropy`; the existing checks over 32 seeds, unchanged.

Commit `sim: listing views, races and seeded entropy in the engine simulation`. **PR size:** S (~250 lines).

### Task 18: `TaskSet`, `hedge` and `spawn_concurrent`

**Files:** `crates/loams-task/src/{set.rs,hedge.rs,concurrent.rs,lib.rs}`, `crates/loams-task/Cargo.toml` (`tokio-util` for `CancellationToken`), `crates/loams-task/tests/{set.rs,concurrent.rs}`; `crates/loams-log/src/writer/driver.rs` (the flush task under `spawn_concurrent`).

**Interfaces:** §55 §9 verbatim. `Exit<E> = Returned | Failed(E) | Panicked(String)`; `on_exit` runs on the runtime, once. The unexpected-exit counter is a `metrics`/OTel counter named `loams_task_unexpected_exit_total` with the label `task` only. With `debug_assertions`, after `on_exit` it panics with the task's name (Q840's default: release builds do not abort).

**Tests:**
- `fail_fast_returns_in_spawn_order`
- `fail_fast_aborts_rest_on_first_error`
- `panic_becomes_task_set_error`
- `first_ok_returns_first_success_and_aborts_rest`
- `first_ok_reports_all_errors`
- `hedge_starts_backup_after_delay` (paused time)
- `hedge_skips_backup_when_primary_answers`
- `spawn_concurrent_without_runtime_is_error`
- `unexpected_exit_calls_on_exit_and_counts`
- `unexpected_exit_panics_in_debug`
- `stop_is_not_unexpected`
- `drop_without_stop_aborts_and_warns`
- `writer_flush_task_under_spawn_concurrent` (`loams-log`; Task 7's H1 tests pass on the new code)

Commit `task: TaskSet, hedge and spawn_concurrent; the log writer's flush task under it`. **PR size:** M (~700 lines).

### Task 19: CPU time against wall time

**Files:** `crates/loams-task/src/cpu.rs`, `crates/loams-task/tests/cpu.rs`, workspace `Cargo.toml` (`rustix` features `time`, and `process` if `getrusage` needs it, verify); `crates/loams-query/src/exec/{ann,sparse,tantivy_search,filter_bitmap}.rs` (the `cpu_compute` metric beside `elapsed_compute`), the slow log and EXPLAIN ANALYZE output in `loams-query`, `crates/loams-query/tests/it/accounting.rs`; `docs/design/55-commit-path-determinism.md` §10 (as built).

**Interfaces:** `Accounted<F>` (`cpu() -> Duration`, `wall() -> Duration`), `accounted_blocking(f) -> (T, Duration)`; `cpu_compute` through DataFusion's `MetricBuilder` (a custom `Time` metric; verify the API on DataFusion 54.1); slow-log fields `cpu_ms`, `wall_ms`; OTel histograms `loams_query_operator_cpu_seconds`, `loams_query_operator_wall_seconds` with the label `operator` only. Whether accounting is always on follows Q839 and this task's measurement.

**Tests:**
- `accounted_cpu_le_wall`
- `accounted_counts_busy_loop_cpu`
- `accounted_sleep_is_wall_not_cpu`
- `accounted_blocking_measures_closure`
- `operators_report_cpu_compute`
- `explain_analyze_shows_cpu_and_wall`
- `slow_log_has_cpu_ms_and_wall_ms`
- `metrics_have_no_namespace_label`
- `accounting_overhead` (`#[ignore]`; the M1 query benchmark with and without accounting; records the overhead for Q839)
- `ann_strategy_calibration_report` (`#[ignore]`; writes `bench/results/ann-calibration-<date>.md`: measured CPU and wall per strategy against §05's static thresholds; no default changes in this task)

Commit `query, task: CPU and wall time per operator`. **PR size:** M (~700 lines).

### Task 20: Docs and exit

**Files:** [§55](../design/55-commit-path-determinism.md) (an "As built" section: what was extracted, the gate's numbers, the rollout's results), the [decision log](../design/13-decision-log.md) (statuses of D954–D962), [plans README](README.md) (DT1's status), [§12](../design/12-roadmap-testing-risks.md) §2 item 1 (the result), `docs/ci/README.md`, `CHANGELOG.md`.

**Tests:** `scripts/docs/check-decision-ids.sh`.

Commit `docs: DT1 as built`. **PR size:** S.

---

## Exit criteria (§55, with the owning tasks)

- [ ] `loams-log` reads no ambient clock or randomness outside its driver's ratcheted sites; its crate-level ban is in CI: Tasks 3, 9.
- [ ] The four machines exist, are pure, and their scenarios replay bit-exactly: Task 4.
- [ ] Every async function on `loams-log`'s commit and recovery paths has a tag; the sweeps cover every suspension point: Task 7.
- [ ] Listing views, per-key history, races and `TornDisk` exist and are used by the pilot: Tasks 5, 6.
- [ ] The gate report is written and the owner has ruled: Task 8.
- [ ] On a "go": the rollout's crates meet the first three items: Tasks 10–16.
- [ ] `loams-sim` uses the new models: Task 17.
- [ ] `TaskSet`, `hedge`, `spawn_concurrent`; the writer's flush task cannot die silently: Tasks 7, 18.
- [ ] CPU and wall time per operator in EXPLAIN ANALYZE, the slow log and OTel, with no tenant label; a calibration report: Task 19.
- [ ] Docs: Task 20.

## Self-review

- **Spec coverage.**

  | §55 section | Task(s) |
  |---|---|
  | §5 The amendment of D28; tiers | 2, 4, 7, 20 |
  | §6.1 The seam | 1, 3, 11, 14 |
  | §6.2 The pilot's machines | 4 |
  | §6.4 Lint bans | 3, 9, 10–16 |
  | §7 Cancel-safety contracts and sweeps | 2, 7, 10–16 |
  | §8 Store and disk models | 5, 6, 17 |
  | §9 `TaskSet`, `spawn_concurrent` | 18 (and 7 for H1) |
  | §10 CPU and wall time | 19 |
  | §11 The pilot, the gate, the rollout | 0–8, 10–16 |

- **Owner's order** (pilot, gate, rollout, `TaskSet`, accounting, CI scoping): kept, except that CI scoping (Task 9) runs right after the gate so each rollout task adds its crate to the manifest, and Tasks 18–19 may start early because the gate does not govern them.
- **Names.** `loams_common::{det, machine}`, `loams-task`, `LogIo`, the machines, `CancelAfter`, `sweep`, `SeededEntropy`, `ListView`, `Fault::Race`, `TornDisk`, `commit-path.toml` and the environment knobs are defined once, in the shared contracts.
- **Review Focus.** Items 1–8 each name owning tests (Tasks 3–7, 19).

## Open questions for the owner

These are §55 §15's questions. Every one has a default the tasks follow until the owner answers.

| # | Question | Default | Needed by |
|---|---|---|---|
| Q835 | The gate's thresholds | One defect, or two of P1–P4, inside the cost budget | Task 8 |
| Q836 | Move the machine seam to `loams-common`? | Yes, re-exported unchanged | Task 1 |
| Q837 | Build RT1 Tasks 1–2 here if they have not merged? | Yes | Task 2 |
| Q838 | A `test-util` redb backend hook in `loams-meta`? | Yes | Task 6 |
| Q839 | CPU accounting always on? | If under 1 % overhead; else opt-in | Task 19 |
| Q840 | Release builds abort on a liveness loop's unexpected exit? | No; fail the owner visibly | Task 18 |
| Q841 | Stale listings also show deleted objects? | Yes | Task 5 |
| Q842 | Seed budgets | PR 200 and 50; nightly 100 000 and 1 000 | Tasks 4, 9 |

## Gate report

(Task 8 writes this section.)

## Rulings made during execution

(Task 0 starts this section.)
