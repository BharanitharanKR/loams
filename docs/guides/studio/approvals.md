# Approvals

Each thread has an approval mode (design [§53](../../design/53-studio-ade.md) §8.1, D911; SD1 Task 13, rulings T13-1 to T13-16):

- **Supervised:** the harness stops before each tool call and waits for you.
- **Full access:** tool calls run without asking. Studio labels it as such.

Destructive git actions (commit, push, merge, discard, restore) always ask in Studio, whatever the mode ([Git actions](git.md)).

## Which mode a thread gets

- **New thread** (the **+** on a project's row) **or Mod+N** sets the mode from **Settings › Approvals** right after creating the thread, before its first run. **Supervised** is the default (Q803).
- **A thread started from a project page's composer** is not given a mode by Studio yet, so it takes the daemon's default.
- **The daemon's default** is `[approvals] default_mode` in its `config.toml`. Unset, it is `full-access`, or `supervised` when the remote listener is on (`serve --remote` or `[remote] enabled = true`).
- **A side chat takes its parent's mode**, up the chain, so an agent can't leave supervision by spawning one. A fork that is not a side chat takes the default.
- **A [run group](parallel-runs.md)** sets every variant to the group's mode before any variant runs.

The thread header shows the thread's effective mode as the daemon reports it. There is no switch in the header yet: the mode is set when the thread is created. The daemon method is `ApprovalService.SetThreadApprovalMode`; only the owner (or the remote `studio` role) can call it, never an agent's MCP tools.

**Pi can't run a supervised thread.** It has no way to ask, so the daemon refuses the run with "set this thread to full access to run it".

## Answering

A waiting tool call shows as a card at the end of the timeline. It names the tool, its arguments (secrets redacted, cut to 4 096 characters) and a risk label: `read`, `write` or `destructive` (`rm -r`, forced pushes, `reset --hard` and the like). The label only informs; it never allows anything.

With the card focused:
- **Once** (Enter) allows this call.
- **Always for this thread** (Shift+Enter) allows it and remembers the tool for this thread. The daemon keeps that memory, never the agent: opencode is told `once`, and an ACP agent gets its allow-once option, so an agent's own "always" can't reach further than the thread.
- **Deny** (Esc) refuses it.

Elsewhere:
- **The thread list** shows **Pending Approval**.
- **The shell** counts pending approvals in its badge in both modes.
- **The Approvals page** (Console) lists agent approvals beside the server's, labelled "Agent" and "Server", with the same three answers. In a browser it shows agent approvals only inside Studio for now: the console's shared agentd connection has no host until Studio's host list is shared (T13-15).
- **On the desktop**, an approval also posts an `approval` [notification](notifications.md).
- **In a run group**, **Approve for all variants** answers the same call once in every variant that waits for it.

Only you can answer. The approval methods are role `owner` on the local daemon, and the `studio` role on the remote listener; an agent's MCP token can't watch, answer or switch a thread to full access.

## How each harness asks

| Harness | In a supervised thread |
|---|---|
| Claude Code | Runs with `--permission-mode default`, never bypass; each permission request waits. A denial is sent as a deny |
| Codex | Runs with the approval policy `on-request` and the thread's own sandbox; command and file-change approvals wait (`accept` / `decline`) |
| opencode | Each `permission.asked` waits (`once` / `reject`) |
| ACP agents | Each tool permission waits; allow maps to the agent's allow-once option, deny to its reject option |
| Pi | Can't ask, so a supervised thread doesn't run |

Questions the agent asks you stay questions, answered on their own card. In a full-access thread, the drivers answer their agents' permission requests themselves.

## Timeouts and restarts

- **Unanswered approvals are denied** after `[approvals] timeout_secs` (1800 seconds by default; 0 is refused). No answer is never an allow.
- **When a turn ends** (done, errored or interrupted), whatever it still waits for is denied and forgotten.
- **Pending approvals survive a daemon restart.** They are kept in the daemon's store, with arguments redacted, and listed again. Answering one clears it; an "always" is still remembered. When the resumed turn asks again for the same tool with the same redacted arguments, the new request takes the old one's place.

## Not built yet

- A per-thread mode switch in the thread header.
- A mode for threads started from the project page's composer.
- ACP permission requests made while a session is being created or loaded, before any prompt, are still accepted automatically.
- The daemon does no OS-level confinement (Q815): supervision is the guard, so keep full access for work you would leave unattended.
