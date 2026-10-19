# 60 — Places and Harness Composition

Status: **Proposed** · 2026-10-17. Decisions **D1087–D1093** and questions **Q936–Q941** are in the [decision log](13-decision-log.md). This is a research note with proposals; nothing here is built yet. It feeds Part 2 of the console redesign (the agent home, [§59](59-console-foundation.md) §9). It also feeds [§53](53-studio-ade.md) (Studio and agentd), [§57](57-loams-gateway.md) (the LLM module) and [§58](58-data-access-and-htap.md) §5 (the Data API's agent safety profile).

## 1. Where this comes from

On 2026-10-17 the owner asked for two sources to be studied, to see "what fits Loams":

- **Endo** ([longsurf-ai/endo](https://github.com/longsurf-ai/endo), MIT, version 0.1.x, about 88 stars; Show HN at <https://news.ycombinator.com/item?id=50110614>). It calls itself a *Higher Order Harness*: one chat interface over Codex, Claude Code, Antigravity and Grok, each driven through its own CLI and the subscription the user already has.
  - Agents can be switched in the middle of a thread, and one agent can continue another's session.
  - Workflows are TypeScript files in the workspace: `agent()` calls a model, `parallel()` fans out, `phase()` groups calls, and a session id continues an earlier conversation.
  - "Dig In" opens a side conversation from selected text.
  - Prompts and workflows can be scheduled, by cron or once.
  - It is built on Effect. Loams's console rules exclude Effect ([console UI stack](console-ui-stack.md), decision 3), so Endo is a source of ideas, not code.
- **"The Harness Is an Object: Notes toward composing intelligence"** by Jason Duncan, an X article published 2026-10-16. The owner pasted the text, because the article is behind an X login. Its argument, summarised:
  - **"Harness" is splitting into four things**, the way "object" once split into class, instance, interface and module:
    - **Cognitive harness:** how a mind is assembled from models.
    - **Suit:** what the agent carries everywhere. That is its identity, memory and values.
    - **Place:** the environment the agent works in. That is its context, tools, rules and the other people and agents present. A repository, a support queue and a kitchen are all places.
    - **Composition:** what happens when these meet.
  - **Inward composition** is "engineered intelligence":
    - a small model answers a narrow, typed triage question with a confidence;
    - a threshold escalates to a large model, as a priced choice;
    - a second model verifies the result;
    - specialists handle code or images;
    - arithmetic, dates and lookups go to deterministic code.
  - **Outward composition:** entering a place composes your harness with the place's harness into a temporary combined harness. Leaving decomposes it. Whatever was made stays behind, in the event log. A person who enters a kitchen becomes a chef, and leaves holding a sandwich.
  - **One operator at every scale:** model into harness, agent into place, team into company. Roles behave like polymorphism, and places add traits, much like mixins.
  - **The proposed algebra:**
    - **tools intersect:** the place sets the limits;
    - **context accumulates:** for the duration of the visit;
    - **identity stays with the suit:** a place can assign a role but cannot replace who you are.
    
    The article claims it holds both inward and outward.
  - **Proposed language:** three tiers. Natural-language intent sits on top, an exact "harness IR" in the middle that can be checked before anything runs (LLVM is the stated precedent), and deterministic machinery at the bottom. The verbs are *enter, leave, wear, carry, leave behind*. The prior work it cites is DSPy, LMQL, BAML and LangGraph.
  - **Open questions in the article:** suit-vs-place conflicts, whether visitors change places, harnesses that rewrite themselves, and whether the algebra holds at every scale.
- **Context:** in 2026 the wider industry started calling this layer a "meta-harness", meaning a harness of harnesses. See, for example, the [meta-harness pattern](https://codex.danielvaughan.com/2026/07/27/meta-harness-pattern-orchestrating-codex-claude-code-pi-sub-agents/) and a [2026 buyer's guide](https://codepick.dev/en/guides/meta-harness-2026/). Those are secondary sources and not verified in depth.

## 2. What Loams already has

| Concept | Loams today |
|---|---|
| One UI over many harnesses, on the user's own subscriptions | agentd's `HarnessService` (`proto/loams/agentd/v1/harness.proto`): `ListHarnesses`, `InstallHarness`, `ListModels`, and the agent-account flow (`ListAgentAccounts`, `StartAgentLogin`, `CompleteAgentLogin`, `ActivateAgentAccount`). This is Endo's core pitch, and it already ships in Studio (§53). |
| `parallel()` fan-out | Studio **run groups** (D904): one prompt runs as N threads, each with its own worktree and harness or model, followed by compare and merge. Merging always needs a person's click. |
| Continuing from another session; "Dig In" | `ThreadService.ForkThread` exists. A fork-from-selection gesture does not. |
| Place | A namespace, project and environment, plus a Postgres or Git **branch**. Authority comes from OpenFGA (D750–D751, §45 §5), scopes and the environment fence (LV1 T15-*), and a workspace in Loams Git (§36 §19). |
| Tools intersect | An agent token's scopes are already narrowed by the namespace's grants. §58 §5.8's agent profile requires writes to go to a branch (`agent/<session>`, Q920), within budgets, with named tools only. |
| What stays behind | Loams is built on logs: streams and CloudEvents (D364), Git commits, the audit log, branch snapshots and Durable's history. |
| Suit memory | Loams Memory (agent memory on Graph), parked until preview.4. |
| Inward composition | The LLM module planned for `loams-gateway` (§57, Q899). Nothing is built yet. |
| Scheduling | Jobs and Durable timers (Resonate, D1 / §21). |

## 3. Proposals

### D1087 — Places are a first-class object (Proposed)

A **Place** is a record that bundles:
- the **data scope:** namespace, project and environment, plus a branch for Postgres, Loams SQL, Git or a collection;
- an **allow-list of tools:** MCP tools, Data API named operations (§58 D1042–D1050) and engine APIs;
- the **roles** offered there (reviewer, operator, analyst, …);
- the **approval policy** (§53 approvals, D1082 typed confirmation);
- **budgets:** time, rows, cost units and model spend.

Places nest: an organisation is a place made of places. Studio projects, review rooms and support queues are places.

**Entering** a place is an explicit act that is recorded. It computes the **effective capability**: the agent's own grants (its "suit") intersected with the place's grants. Loams computes this with OpenFGA plus the place record, **before the run starts**, and shows it in the composer and the inspector. Nothing the agent does in that place can exceed it.

This is the essay's "check it before it runs", delivered as data, not a new language (see D1092). Part 2's composer scope chips (project, branch, access mode) **select the Place** rather than being separate settings.

### D1088 — The composition rules (Proposed)

Loams adopts the essay's algebra as its rules, applied the same way at every level: model into harness, agent into place, place into place.

- **Tools and authority intersect.** The enclosing scope always sets the limit.
- **Context accumulates for the duration of the visit.** On entering, the agent gets the place's context (schema, docs, recent activity) and keeps its own memory. When it leaves, place-only context is dropped.
- **Identity stays with the agent.** A place assigns a *role*, and the audit trail records (agent, role, place). Roles never replace identity.
- **Conflict rule.** When the agent's own values or policy refuse something a place permits, **refusal wins**: the most restrictive side decides. When a place requires something the agent's policy forbids, the agent cannot enter that place, and it is told why.

### D1089 — "Left behind" is recorded and shown (Proposed)

Every action an agent takes in a place produces attributed artifacts: commits, rows (with branch and LSN), branches, files, events and approvals. Each carries (agent, role, place, thread, run).

- The thread and the inspector show **"what this agent left behind"**, with links.
- Because writes land on a branch first (§58 §5.8), what an agent left behind can be **promoted or discarded** as a unit.

This is the place where Loams being built on logs is a real advantage.

### D1090 — Durable agent workflows (Proposed, later design)

Workflows built from `agent()`, `parallel()` and `phase()`, like Endo's, run on Loams Durable (Resonate, §21), with agentd's harnesses as the executors.

- They are **durable** (they survive restarts), **resumable** and **auditable**, and they run next to the data, inside a Place (D1087).
- A scheduled prompt is a Durable timer.

This needs its own design. Q938 asks where workflow files live.

### D1091 — Cascades in the LLM gateway (Proposed, with §57's LLM module)

The planned LLM module of `loams-gateway` offers inward composition as configuration:
- a small model answers a **typed triage question** with a confidence;
- a **threshold escalates** to a larger model, and the cost of that is reported;
- an optional **verifier** model checks the answer;
- **deterministic routes** send arithmetic, dates and lookups to SQL, Data API operations or code.

Each step is logged with its model, price and confidence.

### D1092 — No new harness language now (Proposed)

Loams does **not** invent a harness IR or DSL. The same lesson applies as with query languages (§58: FQL, EdgeQL, PRQL and XTQL fell back): languages that try to replace the incumbent fail. Places, roles and composition are expressed as **data**, in proto and YAML records, checked by OpenFGA plus a policy check.

We watch DSPy, BAML and any emerging standard. If one wins, Loams compiles Places to it instead of competing.

### D1093 — Studio UX items for Part 2 (Proposed)

Part 2 (the agent home) takes three small items from Endo:
- **switching harness or model in the middle of a thread**, with the transcript carried over and the switch recorded in it;
- **"Dig In"**: select text in a message and fork a side thread from it (`ForkThread` with a quoted anchor);
- **scheduled prompts**: run a prompt or a workflow later, once or by cron, on Durable.

## 4. How this lands

| Item | Where |
|---|---|
| D1087, D1088, D1089, D1093 | Part 2 of the console redesign: the agent-home spec (§59 §9). The scope chips select a Place; the inspector shows the effective capability and what the agent left behind. |
| D1090 | A later design for agent workflows on Durable |
| D1091 | §57's LLM module plan |
| D1092 | A standing rule |

## 5. Questions

| ID | Question | Default |
|---|---|---|
| Q936 | Where does the Place record live: a new `loams.places.v1` API, or extended project and environment records? | Extend projects and environments with a place section first; a separate API only if nesting needs it |
| Q937 | How is the effective capability shown before a run, so people understand it? | A compact "can / can't" list in the composer's scope chip and in the inspector's Place tab |
| Q938 | Where do Durable agent workflows live: files in a Git workspace (like Endo) or records in Durable? | Files in the project's Loams Git repository, registered with Durable when committed |
| Q939 | Do places learn from visitors (the essay's "does the visitor change the place?")? | Not automatically. A suggested change to a place is itself left behind and needs approval |
| Q940 | Can an agent rewrite its own harness or Place? | Only inside the enclosing place's limits, and only as a proposal a person approves (D1082) |
| Q941 | Borrow anything from Endo's code? | No. It is MIT, but built on Effect, which the console stack excludes; ideas only |

## 6. Sources

- Endo: <https://github.com/longsurf-ai/endo> (MIT; README read 2026-10-17); Show HN: <https://news.ycombinator.com/item?id=50110614>.
- Jason Duncan, "The Harness Is an Object: Notes toward composing intelligence", X article, 2026-10-16, text supplied by the owner. It cites Josh Rosen, "Composable Models".
- Meta-harness context (secondary sources): <https://codex.danielvaughan.com/2026/07/27/meta-harness-pattern-orchestrating-codex-claude-code-pi-sub-agents/>, <https://codepick.dev/en/guides/meta-harness-2026/>.
- Prior work cited by the essay: DSPy, LMQL, BAML, LangGraph; object-capability security; context-oriented programming.


> **See also (2026-10-17):** [§61](61-loams-jailer.md) D1097 adds a trifecta check on Place entry and one-way taint.
