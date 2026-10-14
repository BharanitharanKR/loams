# Diffs and review

The **Diff** tab of a thread's right panel shows what changed in the thread's checkout, and lets you comment on it the way you would on a pull request (design [§53](../../design/53-studio-ade.md) §5.6, D905; SD1 Tasks 11 and 20). **Mod+D** opens and closes it.

## What it shows

**Diff source** picks what to compare:
- **Working tree** (the default): uncommitted changes, kept up to date as the agent writes;
- **Branch**: the thread's branch against a base you pick (`main`, `master`, `dev` or `trunk` are offered first);
- **Commit**: one of the last 30 commits;
- **Latest turn**: what the agent's last turn changed.

A turn's **View diff** in the timeline opens this tab on that turn.

**View options:**
- **Split** or **Unified** layout;
- **Word diff**, **Wrap lines** and **Hide whitespace changes**;
- a **File tree** of the changed files;
- **Viewed** marks per file, to tick off what you have read.

These choices last for the page's life. Each file's header has its counts, a collapse toggle, **Open in Files** and, on the working tree, a discard button.

Diffs are rendered by `@pierre/diffs` inside a shadow root, from escaped text only: nothing in a diff is ever treated as HTML (D919). There is no find-in-diff yet.

## Discarding changes

On the **Working tree** source only, you can discard one file's changes or all of them. Studio asks first, then sends the diff's checksum with the request. If the checkout changed since the diff was shown, the daemon refuses (`stale_diff`) and Studio says so; nothing is discarded. Discarding needs the console permission `agentd:git-write`.

## Review comments

Click a line, or drag over a range, to comment on it. A comment is anchored to the checkout, the file, the side (old or new), the line or lines, and the checksum of the diff it was written on. The daemon stores the comments, so they survive reloads and are shared by every client of that daemon. You can edit, resolve and delete them.

**Outdated comments.** When the lines a comment points at are no longer in the diff, the comment is marked as outdated ("The lines changed since this comment") and moves above its file, so it never disappears.

**Sending a review:**
- **Send N comments to the agent** starts one new turn in the thread with every open comment, quoted with its file and lines. A comment can't break out of its quotes to add instructions of its own.
- **Post to PR #n** shows when the thread's branch has an open pull request on Forgejo or GitHub. It posts the open comments that are not outdated as one review, with each comment on its line. The daemon uses `gh` for GitHub and the factory's Forgejo token for Forgejo, sending the token only to the repository's own host over `https`. Posting needs `agentd:git-write`.

**Limits:** 1 000 comments per checkout, 64 KiB per comment.
