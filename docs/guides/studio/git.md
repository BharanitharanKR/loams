# Git actions

Studio's git toolbar sits in the header on a project or a thread (SD1 Tasks 9 and 23). Every write goes through `loams-agentd`, which runs git with your own identity, hooks and credentials on its machine.

## The toolbar

- **On a project:** the workspace picker (the project's checkout, a **New worktree**, or the **Previous worktree**) and the branch picker. Switching a branch and creating a worktree are confirmed first.
- **On a thread:** the thread's own checkout and branch, fixed, then the actions.
- **The main button** offers the next sensible step: **Commit**, **Commit & push**, **Commit, push & PR**, **Push** or **Create PR**, depending on the checkout's state. The menu beside it has **Commit**, **Push**, **Create PR** and **Merge into base**.
- **Fetch**, at the end of the menu, updates the remote-tracking refs. It moves nothing else, so it isn't confirmed.
- **The pull request chip** shows the branch's pull request and its state, when it has one. On the desktop, a branch's open Forgejo pull request is linked through the factory.
- **Project actions** (Build, Test, Lint, Run, Debug, Configure, or your own commands) are edited from the toolbar and run in the [terminal drawer](terminal.md). They belong to an agentd space, and for now only appear for a thread that has one.

## One confirmation, naming every write

Each action opens one dialog that lists every write it will make, in order (SD1 ruling T23-1):

- **Commit:** a message (required; the daemon doesn't write one for you), **Sign off (Signed-off-by)**, on by default, and the files, any of which you can leave out. Your commit hooks run.
- **Push:** the remote, and **Force with lease**, off by default. Studio never force-pushes without a lease. The lease is this checkout's remote-tracking ref for the branch as it was last fetched, or "the branch must not exist yet" when there is none.
- **Pull request:** Forgejo or GitHub (chosen for you when the checkout's provider is known), the base branch, title, body, draft, and a linked issue (`Closes #N`). A Forgejo pull request has no draft flag, so a draft is titled `WIP: …`.
- **Merge into base:** **Merge commit**, **Squash**, **Rebase** or **Fast-forward only**. The merge or rebase runs in a temporary worktree under the repository's git directory, so neither your checkouts nor HEAD move. If it stops on conflicts, the daemon aborts it, leaves your tree clean, and the dialog lists the conflicting paths.
- Committing on the default branch shows a warning in the same dialog.

The writes run in order and stop at the first failure. A retry runs only the steps not yet done.

**Checksums.** A dialog reads the checkout's diff checksum (and, for a merge, the source branch's head) when it opens, and sends them with the write. If the checkout changed since, the daemon refuses with `stale_diff`; the dialog stays open and reads the checkout again.

**Who can write.** The daemon refuses a git write while an agent is running in that checkout, and runs one git write at a time. In the console, every git write needs the permission `agentd:git-write`; Studio has it. A call without it shows "Not allowed" and never reaches the daemon. The agents' own MCP tools can't call any of these.

## Credentials

- **GitHub** pull requests and reviews go through the `gh` CLI on the daemon's machine, with its login.
- **Forgejo** uses the token and URL the daemon keeps in the OS keyring (service `dev.loams.agentd`, accounts `factory:forgejo:token` and `factory:forgejo:url`). The token is sent only when the URL's host is the repository remote's host, over `https` (plain `http` only on loopback), with redirects off.

## Not built yet

- **Pull** is shown disabled: the daemon has no pull method yet.
- **Publish repository** (adding a first remote) is not built.
- Pull request checks are not shown.
