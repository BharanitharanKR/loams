# Parallel runs

A **run group** sends one prompt to several agents at once, each in its own worktree. You compare what they did side by side, pick one, and merge it or open a pull request from it (design [§53](../../design/53-studio-ade.md) §5.5, D904; SD1 Tasks 12 and 25). The daemon owns the group, so it keeps running when you close the window.

## Starting a group

In the [composer](composer.md), write the prompt and choose **Run in parallel…** (it shows when the daemon lists the `run-groups` feature). The dialog asks for:
- **2 to 4 variants**, each a harness and a model;
- **Base branch**, which every variant starts from;
- **Check** (optional): a project action, such as your tests, to run in each worktree when its run ends;
- **Approvals**: **Supervised** (the default) or **Full access**, for the whole group. The daemon sets every variant to this mode before any of them runs ([Approvals](approvals.md)).

Studio then opens the group's page, `/studio/g/<groupId>`.

What the daemon does:
- **One worktree per variant.** It resolves the base to one commit when the group is created, so every variant and the comparison start from the same commit even if the base branch moves.
- **Ordinary threads.** Each variant is a top-level thread in the project, titled `<prompt start> · <harness> <model>`. Its branch is never renamed.
- **Limits.** At most 4 variants run at once per group and 8 across all groups (`[groups] max_running_per_group` and `max_running` in the daemon's `config.toml`). Your other threads are never held back. Variants beyond the limit wait as **queued**.
- **Restarts.** After a daemon restart, queued variants are sent by the new daemon. A variant whose run was not resumed is marked done, and a check that was running is marked failed.

## The group page

- **Variants:** a card per variant with its status (queued, running, awaiting input, done, errored, cancelled), its elapsed time, files changed, lines added and removed, and tokens ("—" until the harness reports usage). **Continue as thread** opens the variant's thread, where you can keep working with it before you decide; it resolves nothing.
- **Compare:** a matrix of which variant changed which file (**Files changed by each variant**). Pick a cell, or the **Left** and **Right** variants (or the base) and a **File**, to see the two side by side. The comparison reads each worktree's current state without committing or staging anything.
- **Check results:** each variant's check, with its exit status and the last 200 lines of output.
- **Approve for all variants** appears when two or more variants wait for the same tool call with the same arguments. It approves that call once in each of them. "Always" and "Deny" stay on each thread's own card.
- **Start judge thread** (Q811) starts an ordinary, read-only thread whose prompt holds the task, each variant's status and check, the matrix and each variant's diff (cut to 60 000 characters in all). The judge is never a member and cannot resolve the group.

## Resolving

**Resolve…** opens one dialog that names every write it will make, in order (SD1 ruling T25-4):

1. **Winner:** the variant to keep.
2. **What to do with it:**
   - **Merge into the base**, with a strategy (**Merge commit**, **Squash**, **Rebase** or **Fast-forward only**);
   - **Open a pull request**, on Forgejo or GitHub, with a title, a description and **Draft**;
   - **Keep as a thread**, which merges nothing.
3. The winner's uncommitted changes are committed first, with your identity, against the diff checksum read when the dialog opened.
4. **Archive the other runs** is on by default.
5. **Remove the other worktrees** is off by default, and asks again when you turn it on.
6. **Stop the runs still going** shows only while variants are running, and is off. Without it, the daemon refuses to resolve a group that is still running (`group_not_ready`).

A merge or pull request goes through the same paths as the [git toolbar](git.md), with the same checks: `stale_diff` keeps the dialog open and reads the winner again, conflicts stop the merge with a clean tree, and nothing is ever force-pushed. Resolving needs `agentd:git-write`. A merge always needs your click; nothing merges on its own (§53 §2.2).

## Notifications

A group posts **group_ready** when its variants have finished, and **check_failed** when a check exits non-zero. Both lead to the group's page ([Notifications](notifications.md)).
