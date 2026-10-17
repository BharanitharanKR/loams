# 61 — Loams Jailer: a Hypervisor Agent that Oversees the Sandbox

Status: **Proposed** · 2026-10-17. Decisions **D1094–D1105**, questions **Q942–Q948** ([decision log](13-decision-log.md)).

Builds on:
- [§56](56-loams-sandbox.md) — Loams Sandbox; plan SB1.
- [§53](53-studio-ade.md) — Studio and agentd approvals.
- [§60](60-places-and-harness-composition.md) — Places.
- [§58](58-data-access-and-htap.md) §5.8 — the agent safety profile.
- [§57](57-loams-gateway.md) — the gateway's `llm` and `mcp` modules.

Owner direction, 2026-10-17: "the Loams harness should be managed by a jailer agent, an agent that oversees the sandbox", and "KVM and syscall interception, eBPF, all tools for a hypervisor agent". The name is by analogy with Firecracker's `jailer`, the process that sets up and confines each microVM.

## 1. The recommendation in one paragraph

The Jailer is one supervisor built from three layers.

1. **A deterministic jailer process.** It is the parent of the sandbox and the policy decision point (PDP). It is the layer that enforces.
2. **A sensor and actuator layer.** It covers the kernel, the VMM and the network, streams structured events to the jailer, and carries out its decisions.
3. **An LLM overseer.** A trusted monitor that sees only the cases the policy marks ambiguous, and may only make outcomes **stricter**.

The research agrees on one point. Every LLM monitor tested so far is beaten by an adaptive attacker, and the only defences with hard guarantees are deterministic and sit outside the model. So the overseer is a filter that saves people clicks. The kernel, the VMM and the Place policy are the real boundary.

## 2. What the research shows (2024–2026)

### 2.1 AI control: monitoring, editing, deferring, resampling

**Greenblatt et al. 2023** ([arXiv 2312.06942](https://arxiv.org/abs/2312.06942)). Setup: GPT-4 untrusted, GPT-3.5 trusted, a 2% human audit budget.

| Approach | Safety | Usefulness |
|---|---|---|
| Upfront auditing | 15% | — |
| Trusted monitoring | 62% | 97% |
| Trusted monitoring plus defer-to-trusted at 20% | 92% | 83% |
| Trusted editing at 20% | 92% | 94% |
| Naive untrusted monitoring | 0% (collusion) | — |

**Other results:**
- **Ctrl-Z resampling** ([arXiv 2504.10374](https://arxiv.org/abs/2504.10374)) on BashBench cut attack success from 58% to 7%, at about 5% usefulness cost.
- **Adaptive attacks on monitors** ([arXiv 2510.09462](https://arxiv.org/abs/2510.09462), ICLR 2026). The untrusted model plants a prompt injection aimed at the monitor, and the monitor then scored 0 on all 375 backdoors. Picking the *least*-suspicious resample acts as best-of-n for the attacker; pick the *most* suspicious.
- **SHADE-Arena** ([Anthropic](https://www.anthropic.com/research/shade-arena-sabotage-monitoring)). Monitors catch sabotage poorly: catching 80% of attacks needed a false-positive rate above 20% (from a search snippet, unverified).
- **Chain-of-thought monitoring** helps but is "fragile" ([arXiv 2507.11473](https://arxiv.org/pdf/2507.11473)).
- **Frontier labs in 2026** mostly monitor offline, according to a [secondary overview](https://www.lesswrong.com/posts/E4Jpmo3igPnGnXtL4/an-overview-of-frontier-ai-company-monitoring-practices-in) (unverified).

### 2.2 Guard frameworks

- **LlamaFirewall** ([arXiv 2505.03574](https://arxiv.org/abs/2505.03574)), on AgentDojo. Attack success falls from 17.6% to 1.75% with PromptGuard plus AlignmentCheck, while utility falls from 47.7% to 42.7%. Small checkers over-refuse.
- **CaMeL** ([arXiv 2503.18813](https://arxiv.org/abs/2503.18813)): a privileged/quarantined dual-LLM design that tracks capability-tagged data flow. It is provably secure on 77% of AgentDojo tasks (84% undefended), at about 2.8× the tokens. Side channels remain.
- **Progent** ([arXiv 2504.11703](https://arxiv.org/html/2504.11703v2)): a default-deny JSON-Schema proxy in front of tools. Attack success falls from 39.9% to 0%, at about 0.8 ms per call. Attacks that stay within the task's own permissions are out of scope.
- **MELON** ([arXiv 2502.05174](https://arxiv.org/html/2502.05174v2)): a masked re-run. Attack success falls from 16% to 0.24%, at 2× the calls and a 9.3% false-positive rate.
- **Research-grade only:** AgentSpec, ShieldAgent, GuardAgent, FIDES.
- **Design-patterns paper** ([arXiv 2506.08837](https://arxiv.org/abs/2506.08837)): reliable guarantees are not possible for a general-purpose agent.
- **The lethal trifecta** ([Willison](https://simonwillison.net/2025/Jun/16/the-lethal-trifecta/)): private data, untrusted content and external communication. "95% detection is a failing grade."
- **Meta's Agents Rule of Two** ([Meta](https://ai.meta.com/blog/practical-ai-agent-security/)): hold at most two of {untrusted input, sensitive access, state change or egress}.
- **"Guardian agents"** ([Gartner](https://www.gartner.com/en/newsroom/press-releases/2025-06-11-gartner-predicts-that-guardian-agents-will-capture-10-15-percent-of-the-agentic-ai-market-by-2030)): no independent efficacy data.

### 2.3 How shipping harnesses do it

Each pairs an LLM reviewer with an OS boundary, and each says the reviewer is not the boundary.

- **Claude Code auto mode** ([Anthropic](https://anthropic.com/engineering/claude-code-auto-mode)):
  - **Review:** a two-stage, reasoning-blind classifier, meaning the assistant's prose and tool outputs are removed before review.
  - **Measured:** 0.4% false positives on real traffic and 5.7% false negatives on synthetic exfiltration.
  - **Escalation:** to a person after 3 consecutive or 20 total denials.
  - **Hooks:** they can allow, deny, ask or rewrite.
  - **Boundary:** bubblewrap or Seatbelt plus an allow-list proxy.
- **Codex auto-review** ([docs](https://learn.chatgpt.com/docs/sandboxing/auto-review)):
  - **Reviewer:** a separate agent that sees only boundary-crossing actions.
  - **Denies:** exfiltration, credential probing, security weakening and irreversible destruction.
  - **Circuit breaker:** 3 consecutive or 10 of the last 50 denials.
  - **Stated limit:** "not a deterministic security guarantee".
- **Cursor** says its reviewer is "not a security boundary".
- **The Copilot coding agent** pushes only to `copilot/*`, needs a person to approve Actions runs, and has a firewall that covers only its Bash tool.
- **Hosted sandboxes** (Vercel, Cloudflare, E2B, Codex and Claude cloud, Docker Sandboxes) inject credentials through a proxy outside the VM, and several can change egress policy on a running sandbox.

### 2.4 The low-level layer: KVM, syscalls, eBPF

**Firecracker's jailer** ([jailer.md](https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md)):
- **Isolation it sets up:** a chroot and `pivot_root`, mount and PID namespaces, a network namespace, cgroups and rlimits.
- **Process hardening:** device nodes, then drop to an unprivileged uid and gid, close fds, clear the environment, and exec the VMM.
- **Privilege:** it runs as root. Seccomp is per-thread inside Firecracker.
- **No network filtering:** Firecracker does none; that belongs on the host tap.

**Control through the VMM** ([snapshot support](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md)):
- **Firecracker:**
  - pause and resume (`PATCH /vm`);
  - full snapshots (diff snapshots are a developer preview);
  - token-bucket rate limiters on network and block;
  - a balloon whose stats come from the guest and are untrusted;
  - vsock to host Unix sockets;
  - MMDS.
- **Cloud Hypervisor** adds copy, on-demand and copy-on-write restore, and Landlock on the VMM.
- **Kata** adds an agent policy in Rego, which is strong only with attested guests.

**VM introspection** (LibVMI, KVM-VMI, DRAKVUF) needs out-of-tree kernels, targets Xen first, and is GPL or LGPL. **Not practical.**

**Syscall interception:**
- **seccomp user notify:** its [man page](https://man7.org/linux/man-pages/man2/seccomp_unotify.2.html) says it "can not be used to implement a security policy" (TOCTOU). About 4 µs per round trip (estimate).
- **gVisor:** systrap is its default platform. gVisor *runtime monitoring* ([seccheck](https://raw.githubusercontent.com/google/gvisor/master/pkg/sentry/seccheck/README.md)) streams about 1,000 trace points as protobuf to an external sink, with no host eBPF.
- **Landlock ABI 7** (kernel 6.15) adds audit logging of denials.

**eBPF:**
- **Tetragon** enforces: it can send a signal or override a return value (needs `CONFIG_BPF_KPROBE_OVERRIDE`).
- **Falco** detects only.
- **cgroup BPF** (`connect4`/`connect6`, `cgroup_skb/egress`) enforces egress per cgroup.
- **BPF-LSM** can deny, but it is inert on Ubuntu (built in, not active; [SRU 2026-09](https://lists.ubuntu.com/archives/kernel-team/2026-September/171489.html)).
- **Privileges:** eBPF needs CAP_BPF and CAP_PERFMON, so **an unprivileged desktop daemon cannot use it**.
- **Agent-specific work:**
  - [ActPlane](https://arxiv.org/abs/2606.25189): eBPF information flow for harnesses, at 1.9–8.4% overhead.
  - [AgentSight](https://arxiv.org/abs/2508.02736v1): observability only.
  - Meta's BpfJailer ([LPC 2025](https://lpc.events/event/19/contributions/2159/)): BPF-LSM for microVM workloads. Not public.

**eBPF licensing** ([kernel doc](https://www.kernel.org/doc/html/latest/bpf/bpf_licensing.html)):
- LSM programs, kfuncs and GPL-only helpers need a GPL-compatible program licence.
- The usual split, used by Cilium, Tetragon and aya-template: BPF source under `GPL-2.0-only OR BSD-2-Clause/MIT`, userspace under Apache-2.0 or MIT.
- Aya is MIT OR Apache-2.0.

## 3. Where Loams is today, and the gaps

| Piece | State | Gap for a jailer |
|---|---|---|
| §56/SB1: `SandboxSpec`, adapters `linux`/`gvisor`/`microvm`/`seatbelt`/`appcontainer`; kill via `cgroup.kill`; `stats()`; deadline watchdog (D999); proxy audit events (D998); `SandboxReport` (D1007); microVM snapshot, restore and fork (SB1 Task 17) | Designed; SB1 starts after preview.3 | No pause or resume (`cgroup.freeze`, VM pause). No live policy change (tighten the allow-list, throttle). No event stream beyond proxy audit. No syscall or exec telemetry |
| agentd approvals (`crates/loams-agentd-harness/src/approvals.rs`): `ToolRisk` Read/Write/Destructive from tool names and shell fragments; supervised by default (SD1 T30-19); `ResolveApproval` by the owner only, never the `mcp` role (D788) | Built | Risk comes from substring matching, so `find -exec rm`, base64 and scripts get past it. `webfetch`/`fetch` count as Read, but a GET with data in the URL is exfiltration. No taint, no egress class, no circuit breaker, no slot for an automated reviewer |
| Harness credentials sit inside the sandbox (§56 §13.3, "certain by design") | Accepted | An injected agent can read its own token. The trifecta stays open through the model-provider hosts |
| §60 Places: effective capability = agent ∩ place (D1087); refusal wins (D1088); left behind (D1089) | Proposed (merged as #722) | No trifecta check on entry. No one-way tightening during a session. No gate on leaving a Place |
| §58 §5.8: writes on `agent/<session>`, budgets, named tools, results marked untrusted (D1049) | Proposed | Covers data only; shell and network need the same rules |
| §57 `llm` and `mcp` modules (D1022): provider keys held in the gateway, rate limits | Designed | No policy hook, no hard spend cap tied to a run, no taint labels |
| D-SF-14: kill switch by run, agent or org; budgets; no auto-merge | Proposed (factory) | Not wired to agentd or Studio threads |
| SystemOne (D520): typed-question classifier with a confidence, running locally | Proposed | The natural cheap first stage for the overseer |
| §36 §19 and §15 §9: workspaces, `agent/<id>` branches, durable approval promises, checkpoints | Designed | Rollback exists in pieces (branch, checkpoint, snapshot) with nothing coordinating them |

## 4. The proposed design

### 4.1 What it is

`loams-jailer` is a library plus a per-host supervisor role. For each agent session it owns five things:
- the sandbox lifecycle, through `loams-sandbox`;
- the PDP, which every enforcement point consults;
- the sensor bus;
- the actuators;
- the overseer client.

It is not a new boundary; the boundary stays §56's adapters and the D993 ladder.

```
 PEPs (ask the PDP)                PDP (deterministic first)        Overseer (trusted LLM)
 ─ agentd approval hook ─┐        ┌ L1 Place capability (OpenFGA) ┐  only on "ambiguous";
 ─ MCP gateway call_tool ┤        │ L2 rules: risk, taint,        │  may only tighten:
 ─ LLM gateway request  ─┼──────▶ │    trifecta, budgets, breaker ├─▶ deny / ask human /
 ─ egress proxy CONNECT ─┤        └──────────┬────────────────────┘  allow-within-capability
 ─ Data API / Git push  ─┘                   │ verdict
 Sensors (async) ───────▶ event bus ─────────┴──▶ actuators: freeze · snapshot · throttle ·
  proxy audit, cgroup PSI/events, Landlock audit,      cut egress · revoke token · kill ·
  gVisor seccheck, vsock guest agent, opt. eBPF        discard branch · restore snapshot
```

### 4.2 Where it runs

- **Desktop and remote agentd:** inside the agentd process. agentd already parents the harness (D1004), holds the approval stream and owns the cgroup subtree (`Delegate=`). It runs unprivileged, so no eBPF.
- **Cloud workspaces (§15) and the functions supervisor (F1):** in the node supervisor that drives `runsc` or Firecracker. It holds the VMM API socket, the tap or proxy, and the cgroup, with an optional privileged eBPF sensor.
- **Gateways (§57):** the `llm`, `mcp` and Data API modules are enforcement points that call the PDP. Run, Place and taint labels travel in the token and in `traceparent`.
- **The overseer:** never inside the sandbox. It is a stateless call made by the jailer, through the `llm` gateway or to SystemOne locally. It has no tools; its only output is a verdict.

### 4.3 What it sees

Structured events, not raw transcripts:
- tool calls: name, normalised arguments, and the harness's `ToolRisk`;
- diffs on `agent/<session>`;
- egress (host, port, bytes, allowed or not) from the D998 proxy;
- cgroup `memory.events`, PSI and `cpu.stat`;
- Landlock audit denials (kernel 6.15+);
- exec, open and connect events, from gVisor seccheck (class C) or from the guest agent over vsock (microVM);
- spend and tokens from the LLM gateway, and Data API budgets (D1045);
- Place entry and exit, and the run's taint state.

The overseer gets a **reasoning-blind, paraphrased view**: the user's intent, the Place, the action, the diff and a summary of the taint. It never sees raw text written by the agent or by tools. This is the Anthropic and Codex design, and Loams' defence against the adaptive-attack result.

### 4.4 Decision types

| Decision | Mechanism |
|---|---|
| allow | Inside the effective capability and the rules |
| deny, with a reason | Returned to the harness. Agents usually retry differently, which works as a cheap form of resampling |
| require approval | A durable `ApprovalRequested` (D911; §15 §9.2 promise) |
| rewrite | Deterministic narrowing only: point a push at `agent/<session>`, turn a write into `Explain` (D1045), add `--dry-run` |
| defer to trusted | Hand the turn to another harness or model (D1093 switch), or fork a run-group member (D904). Choose by **maximum** suspicion |
| pause | `cgroup.freeze`, `runsc pause`, or Firecracker `PATCH /vm Paused` |
| throttle | `cpu.max`, Firecracker rate limiters, proxy bandwidth |
| cut egress | Swap the run's allow-list to "model API only" or "none" while it runs |
| quarantine | Freeze, snapshot, revoke the run's scoped tokens, and mark its left-behind artifacts unpromotable |
| kill | `cgroup.kill`, or stop the VMM |
| roll back | Discard `agent/<session>` across Git, Postgres and collections; restore the checkpoint or microVM snapshot |

### 4.5 The deterministic rules that matter most

1. **Trifecta check on Place entry.** Evaluate the effective capability (D1087) on three axes: private data, untrusted input, and egress or state change. If all three hold, the run cannot start. The person must drop one axis (for example, limit egress to the model API and Loams) or approve with a typed confirmation (D1082).
2. **One-way taint.** Reading untrusted content marks the run tainted. Untrusted content means web fetches, issue or PR text, rows the Data API marks untrusted (D1049), and files from other agents. From then on, any new egress host, any write outside the branch, or any unnamed tool needs approval for the rest of the session. This is CaMeL-style tracking at session granularity, which needs no changes to harnesses.
3. **Egress is its own risk class.** `webfetch`, `curl`, and `git push` to remotes outside Loams count as egress, not Read or Write.
4. **Circuit breaker.** After 3 consecutive or 20 total denials, pause and ask a person.
5. **Hard budget stops.** LLM gateway spend, CPU and wall time (D999), Data API budgets, and maximum turns. A breach freezes the run; it doesn't just warn.

### 4.6 Per adapter and per OS

| Adapter or OS | Already there (§56) | Jailer sensors | Jailer actuators |
|---|---|---|---|
| `linux`, unprivileged (desktop or server) | Namespaces, cgroup v2, Landlock, seccomp `shell`, proxy | Proxy audit; cgroup events and PSI; Landlock audit (6.15+); optional seccomp user notify, for **audit and asking only, never as the boundary**; process tree from `/proc` | Freeze/thaw, kill, `cpu.max`/`io.max`, live allow-list change, token revocation, branch discard |
| `linux` on a privileged node | Same | Optional **eBPF sensor** (Aya: exec, open, connect per cgroup); `cgroup/connect4` egress pinning as a second check | Tetragon-style kill. BPF-LSM deny only where it is enabled (off on Ubuntu) |
| `gvisor` (class C) | runsc with the linux spec inside | **runsc seccheck remote sink**: syscall-level events with no host eBPF | `runsc pause`, kill; checkpoint (verify) |
| `microvm` (workspaces) | Firecracker and its jailer, tap to the proxy, snapshot and fork | **vsock channel to `loams-workspace-agent`** (Landlock audit, and optional in-guest eBPF since we own the guest kernel); VMM metrics; balloon stats (untrusted) | VM pause, full snapshot, rate limiters (throttle or cut), restore or fork from a snapshot, stop the VMM. No VM introspection |
| `seatbelt` (macOS) | SBPL profile, proxy, rlimits | Proxy audit, rusage, the approval stream. No kernel sensor (Endpoint Security needs an Apple entitlement) | SIGSTOP/SIGCONT on the process group, kill, allow-list change, branch discard |
| `appcontainer` (Windows) | Job object, ACLs | Proxy audit, job accounting | Suspend the job, kill-on-close, allow-list change |

### 4.7 Model roles

- **The untrusted model** is the harness model.
- **The trusted monitor** is weaker and ideally from a different model family, to reduce collusion. It has no tools and sees the paraphrased view.
- **Stage 1** is SystemOne (D520): typed questions with a confidence (exfiltration? destructive? off-task?). It runs locally and errs towards flagging.
- **Stage 2** is a larger model reached through the `llm` gateway, and runs only on what stage 1 flagged.
- **The monotone rule:**
  - The overseer may deny or escalate.
  - It may **auto-allow only calls the deterministic layer marked "ask" that sit inside the effective capability**. That is where the utility gain comes from: fewer clicks for people.
  - It never allows anything the policy denies.
  - Destructive git actions (§53 §8.1) and D1082 typed confirmations always go to a person.

### 4.8 Audit

- Every verdict is a CloudEvent `io.loams.dev.jailer.verdict.v1` on the thread's stream, with these fields:
  - agent, role, Place and run;
  - a digest of the input;
  - which layer decided, and the decision;
  - the model and its confidence;
  - the actuator used.
- Verdicts link to what the agent left behind (D1089) and to OTel spans (D510).
- A local copy is kept as a hash-chained JSONL log.
- Quarantined artifacts carry the verdict id.

### 4.9 Evaluation

- **Jailer probes in the conformance kit (D1001):**
  - exfiltration over DNS, HTTP GET and the model-provider host;
  - reading masked credentials;
  - pushing to a protected branch;
  - `DROP` on main;
  - a fork bomb;
  - a runaway loop.
- **A Loams control eval in the ControlArena style:** main tasks with hidden side tasks inside Places. It reports:
  - attack success and usefulness;
  - the false-positive rate on real Studio traffic;
  - p95 added latency, with targets under 5 ms for the deterministic layer and under 300 ms for stage 1, and stage 2 running only on flagged calls.
- **An adaptive red team** that injects text aimed at the overseer.
- **Shadow mode first.** The overseer only logs until its false-positive rate is under 1% and adaptive attacks do not raise attack success above the deterministic-only baseline.

## 5. Decisions (all Proposed)

| ID | Decision |
|---|---|
| D1094 | The Loams Jailer is a layered supervisor: a deterministic PDP plus sensors and actuators, plus an LLM overseer. **The deterministic layer and the §56 sandbox are the only boundary; the overseer may only tighten**, except auto-allowing "ask" calls inside the effective capability |
| D1095 | It runs where the sandbox's parent runs: agentd on desktop and remote daemons, the node supervisor for workspaces and functions. The gateways (`llm`, `mcp`, Data API, Git push) are enforcement points that call it. Crate `crates/loams-jailer`, Apache-2.0, `unsafe` forbidden |
| D1096 | `loams-sandbox` gains `pause`, `resume`, `set_net_policy`, `set_limits` and a `SandboxEvent` stream, added to SB1 Tasks 7, 8 and 16 so they aren't retrofitted later |
| D1097 | Place entry checks the trifecta, and taint is one-way (§4.5 rules 1–2). Extends D1087/D1088 |
| D1098 | `ToolRisk` gains `Egress`, and fetch tools move out of `Read`. A small fix to `approvals.rs`, possible now |
| D1099 | A circuit breaker (3 consecutive or 20 total denials → pause and ask a person) and hard budget stops. D-SF-14's kill switch by run, agent or org applies to Studio threads |
| D1100 | The overseer's view is reasoning-blind and paraphrased. Stage 1 is SystemOne; stage 2 goes through the `llm` gateway, using a model from a different family than the harness where one is available. It ships in shadow mode |
| D1101 | Sensor order: proxy and cgroup events, then Landlock audit, then gVisor seccheck (class C), then vsock to the guest agent (microVM), then an **optional privileged eBPF sensor (Aya), on servers only**. No VM introspection. seccomp user notify is never the boundary |
| D1102 | eBPF kernel programs live in a separate crate licensed `GPL-2.0-only OR MIT`; userspace stays Apache-2.0. Confirm `cargo deny` accepts the OR expression. GPL or LGPL VM-introspection libraries are never linked. Tetragon, if used, runs as a separate unmodified process |
| D1103 | Rollback is one operation, `Jailer::rollback(run)`: discard `agent/<session>` across Git, Postgres and collections, and restore the checkpoint or snapshot. Recorded as left behind |
| D1104 | Verdict CloudEvents and a hash-chained log (§4.8) |
| D1105 | Plan **JL1**, after SB1d and alongside Part 2: JL1a, the deterministic jailer in agentd; JL1b, the overseer in shadow mode; JL1c, gateway enforcement points (with §57's LLM and MCP plans); JL1d, gVisor and microVM sensors (with SB1e and SB1f); JL1e, eBPF |

## 6. Questions

| ID | Question | Default |
|---|---|---|
| Q942 | The name collides with Firecracker's `jailer` (also used by SB1 Task 16) | Keep "Loams Jailer" for the product and crate; always write "Firecracker's jailer" in full |
| Q943 | Harness credentials inside the sandbox: keep accepting this, or add TLS termination that injects credentials for the model-provider hosts only? | Accept it in SB1. JL1c routes API-key harnesses through the `llm` gateway with scoped Loams tokens. Provider-only TLS termination later, opt-in (D998 currently says no TLS interception) |
| Q944 | May the overseer auto-allow without a person? | Yes, only for "ask" calls inside the effective capability, and only after the shadow-mode gate |
| Q945 | Overseer model: local only, or cloud? | SystemOne locally; a cloud stage 2 only with the instance's consent (D525: local only by default) |
| Q946 | Is the eBPF sensor open core or private? | Open, optional, off by default (enforcement is open, D995) |
| Q947 | Can Loams become the harness's own reviewer (Codex `approvals_reviewer`, Claude hooks)? | Use agentd's existing `can_use_tool` and approval streams; check whether Codex accepts an external reviewer (unverified) |
| Q948 | Overseer cost | Stage 2 on at most 5% of calls (target), counted in the run's budget |

## 7. What not to do

- **Don't make the LLM overseer the boundary.** Adaptive attacks reduce LLM monitors to upfront-auditing levels, and every vendor says the same of its own reviewer.
- **Don't use seccomp user notify as policy.** Its man page warns against it because of TOCTOU.
- **Don't rely on BPF-LSM denials on desktops or Ubuntu.** It is inert there, and eBPF needs privileges agentd lacks.
- **Don't build VM introspection.**
- **When resampling, don't pick the least-suspicious sample.**
- **Don't show the overseer raw text written by the agent or by tools.**
- **Don't let run-group agents and the overseer share free-text channels.** That is a collusion risk.
- **Don't expect monitors to catch sabotage and sandbagging.** Most of it goes uncaught (SHADE-Arena). The real mitigation is branches, tests and promotion approved by a person.
- **Watch click fatigue.** A high false-positive rate pushes people to full access. Ship in shadow mode and measure first.
- **Firecracker's jailer runs as root,** so the `microvm` path needs a privileged helper, which means cloud and servers only. Desktops stay on `linux`, `seatbelt` and `appcontainer`.
