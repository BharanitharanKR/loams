# Checkpoints

The daemon takes a checkpoint of a thread's checkout at the start and at the end of every turn, so you can put the files back to how they were before any turn (design [§53](../../design/53-studio-ade.md) §6.5 G6–G7, SD1 Task 10).

## What a checkpoint is

A checkpoint is a hidden git commit of the checkout, under `refs/loams/checkpoints/<threadId>/turn/<n>/pre` (the start of turn `n`) and `…/post` (its end). The scheme is T3 Code's, rewritten in Rust (SD1 Ruling 6):

- **Your index, HEAD and branches are never touched.** The commit is written through a temporary index.
- **It includes untracked files that are not ignored.** Ignored files are not included.
- **The refs live in the repository's common git directory**, so a worktree's checkpoints sit beside the main checkout's.
- **Deleting a thread deletes its checkpoint refs.**
- **The checkpoints don't show up** in `git log` or `git branch`.

`git for-each-ref refs/loams/checkpoints/` in the repository lists them.

## Restoring a turn

In the timeline, a turn with a checkpoint has **Restore to before this turn**:

1. Studio asks first: the checkout goes back to how it was before the turn started, and changes made since, by the agent or by you, are discarded. The thread's messages stay.
2. On **Restore**, Studio reads the checkout's current diff checksum and sends it with the request.
3. The daemon compares the checksum with the checkout as it is now. If anything changed since Studio read it, the daemon refuses with `stale_diff` and changes nothing.
4. Otherwise it writes the turn's `pre` commit into the working tree, removes untracked files that the checkpoint doesn't have (ignored files stay), and resets the staging area to HEAD.

**Limits:**
- Restore works only in the thread's own checkout; the daemon finds it from the thread, never from a path you give it.
- Restore isn't offered in the docked compact view.
- Restore puts files back, not commits. A commit made since the turn stays in the branch's history.
