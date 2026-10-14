# The composer

The composer is where you write to the agent (SD1 Task 19). Its editor is T3 Code's Tiptap editor, vendored into `@loams/studio-ui` (D912); the rest is written for Studio.

## Choosing the agent

Under the editor:
- **Harness** (Claude Code, Codex, opencode, Pi, the ACP agents, Loams' own agent: whatever the daemon reports as installed);
- **Model**, empty for the harness's own default;
- **Reasoning**, where the harness has levels.

On a project's page, the composer starts a new thread: its harness picker starts on the first installed harness, not on the [Settings](settings.md) default (that default applies to **New thread** and Mod+N; see [Starting a thread](projects-and-threads.md#starting-a-thread)). On a thread, the pickers start on the thread's own harness.

**Where the first run works** (the project page only): **New worktree**, from a **Base branch** you pick, or **Local**, in the project's own checkout. See [Worktrees](projects-and-threads.md#worktrees).

## Writing

- **`@`** opens a file search over the thread's checkout and inserts a file chip. Clicking a chip opens the file in the Files tab.
- **`/`** opens the harness's commands, and **`$`** its skills.
- **Lists and code blocks** continue as you type, and undo groups your typing as an editor would.
- **↑** in an empty composer recalls your earlier prompts (the last 30); **↓** goes forward again.
- **The stash** keeps a prompt for later (up to 10): stash it, and bring it back from **Prompt stash**.

**Context from elsewhere in Studio** joins the prompt as text when you send:
- **Terminal selections.** Select text in a terminal and press **Add to composer**. The selection waits as a chip above the editor (up to 8, in memory only) and is sent with the prompt as a reference.
- **Mentions from the file tree.** **Mention in composer** in the [Files](files.md) tab becomes a file chip here.

## Attachments

Attach images with the **Attach images** button, by pasting or by dropping them: PNG, JPEG, GIF or WebP, up to 20 MB each and 8 per message. HEIC images are refused, because the only HEIC decoder is LGPL. Images are uploaded to the daemon in chunks before the message is sent, and they are not downscaled first.

## Sending, queueing, steering and stopping

- **Idle:** Enter sends. A new thread's first message creates the thread (and its worktree).
- **While a turn runs:**
  - Enter and the main button **queue** the message for when the turn ends. The queue shows above the composer, with **Send now** and **Remove** for each message. To change a queued message, remove it and queue it again.
  - **Steer** sends the text into the running turn. It is text only, so it is disabled while images are attached.
  - **Stop** interrupts the turn.

## Run in parallel

**Run in parallel…** sends the prompt to several agents at once, each in its own worktree. It shows when the daemon lists the `run-groups` feature. See [Parallel runs](parallel-runs.md).

## Drafts

Each thread keeps its own draft: the prompt, attachments already uploaded, the harness, model and reasoning, the worktree choice and base, the ↑ history and the stash. A new thread's draft is kept per project. Drafts are stored in this browser under `loams.studio.drafts` (at most 50), so they survive a reload and a mode switch. If the browser's storage is unavailable they are kept for the page's life only.

## Not built yet

- A context-window meter.
- Mentioning another thread.
- The factory's "Start thread" hand-off filling the composer; the home shows it as a card instead ([The factory](factory.md)).
- Keybindings for the stash and the model picker.
