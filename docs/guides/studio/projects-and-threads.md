# Projects and threads

Studio's model is **project → thread → worktree** (D903). A thread is one conversation with one coding agent, which agentd calls a *chat*. By default each thread works in its own git worktree, on its own branch.

## The layout

- **The header** is the shell's: the Console | Studio switcher, the breadcrumbs (starting at "Studio"), the [notification bell](notifications.md) and, on a project or thread, its [git toolbar](git.md).
- **The left sidebar** lists the projects with their threads and run groups. On the desktop, the [factory apps](factory.md) are listed below the projects. **Mod+B** hides and shows it.
- **The thread** fills the middle: the timeline above and the [composer](composer.md) below.
- **The right panel** has the tabs **Diff**, **Files**, **Terminal**, **Preview** and **Plan**. Each tab is part of the address (`?panel=diff`), so a link can open one. **Mod+Alt+B** hides and shows the panel. The **Terminal** and **Plan** tabs show nothing yet: terminals live in the bottom drawer ([Terminals](terminal.md)), and an agent's plan shows as a card in the timeline.
- **The bottom drawer** holds the [terminals](terminal.md). **Ctrl+`** opens and closes it.

You can drag the edges of the sidebar, the panel and the drawer. Studio remembers their sizes and open states on this device, under `loams.studio.panel`.

## Projects

A project is a git repository the daemon knows about. For now its id is its path on the daemon's machine (the plan's ruling T15-2).

- **Add one** with **Add** beside the **Projects** heading. On the desktop this opens a folder picker. In a browser it asks for a git URL and clones the repository on the remote machine.
- **Rename and remove are not offered yet.** They need projects to become agentd *spaces*; until then a project is just a registered repository.

## The thread list

Each project lists its threads in two parts:
- **Pinned threads first**, in the order you give them. Drag a pinned thread to move it, or use **Move up** and **Move down** in its menu. Dragging a thread out of the pinned block unpins it. Pins belong to the daemon, so every client of that daemon sees the same order.
- **The rest, newest first**, by their last message.

Each row shows badges:
- **Pending Approval** when one of the thread's tool calls waits for you ([Approvals](approvals.md)).
- **Awaiting Input** when the agent asked a question.
- **Working**, with the time since the turn began.
- **Errored**.
- **Completed** when a turn finished after you last opened the thread.
- The thread's **worktree**, and a badge when the thread is a member of a [run group](parallel-runs.md).

Run groups show as rows with their member threads nested under them.

A thread's menu (the "…" button, or a right-click) offers:
- **Pin thread** / **Unpin thread**, **Move up**, **Move down**;
- **Rename thread** and **Fork thread**;
- **Copy path**, **Copy branch**, **Copy thread ID**;
- **Archive thread** / **Unarchive thread**;
- **Delete**. The confirmation offers to remove the thread's worktree too, but only when no other thread uses it.

**Search threads** filters the list. Archived threads are hidden unless you show them. **Clean up worktrees** lists the linked worktrees that no thread uses any more and removes the clean ones; it lists ones with uncommitted changes and keeps them.

**Keyboard:**
- **Mod+N** (or **Mod+Shift+O**) creates a thread in the project in view ([below](#starting-a-thread)).
- **Mod+Shift+[** and **Mod+Shift+]** go to the previous and next thread.
- **Mod+1** to **Mod+9** jump to the first nine threads, on the desktop only (in a browser those keys switch tabs). Hold the modifier to see the numbers.

## Starting a thread

There are two ways, and they differ:
- **The project's page.** Click a project's name: its page has a **New thread** composer. Your first message creates the thread, and the composer asks where its first run works (below). The harness and model are the composer's own pickers, and the thread takes **Settings › Approvals**' [approval mode](approvals.md#which-mode-a-thread-gets), as New thread does.
- **New thread** (the **+** on the project's row) **or Mod+N** creates an empty thread at once and opens it. It works in the project's **own checkout**, not a new worktree, and starts with the defaults from [Settings](settings.md): the harness, the model, and the approval mode (supervised by default) with its sandbox.

A thread created outside Studio without a mode is supervised, because the daemon's own default is `supervised`. Full access has to be asked for ([Approvals](approvals.md#which-mode-a-thread-gets)).

## Worktrees

On the project page's composer, **Where the first run works** offers:
- **New worktree** (the default) creates a git worktree from the base branch you pick, on a new branch. The agent's edits stay in it, apart from your own checkout and from other threads.
- **Local** runs in the project's own checkout.

The daemon keeps worktrees under `~/.loams-desktop/worktrees` on its machine, on branches named `loams-desktop/<name>` (Q809 keeps these names in SD1). `LOAMS_DESKTOP_WORKTREES_DIR` moves the root.

## The thread page

**The header** shows:
- the thread's title, harness and model;
- its sandbox and branch;
- token totals for the thread, as the harness reports them (counts only; Studio shows no prices);
- the thread's approval mode, as the daemon reports it ([Approvals](approvals.md));
- on the desktop, a folder button that opens the thread's folder in your editor ([Settings](settings.md#editor-desktop-only)).

**The timeline** shows your messages and the agent's replies, rendered as sanitised markdown. Tool calls show as rows, with their diffs, and a subagent as a row of its own. An agent's to-do list shows as a plan card. Questions and approval requests show as cards at the end.

- **Scrolling.** The timeline follows new output while you are at the bottom. Scroll up and it stays where you are.
- **Find.** **Mod+F** (with focus in the thread) finds text. Enter and Shift+Enter move between matching rows, and Esc closes the find bar.
- **Links.** http(s) links open in your browser, and a workspace path opens the file in the [Files](files.md) tab. Raw HTML in a message shows as text, and a remote image shows as its alt text; Studio never fetches it (D919).
- **Turns.** A turn that changed files has **View diff**, which opens the Diff tab on that turn, and, when the turn has a [checkpoint](checkpoints.md), **Restore to before this turn**.

The thread page and the desktop's docked agent panel are built to share one transcript store (D917). The dock doesn't use it yet: it still runs the desktop's own agent loop (DD1 Task 29).
