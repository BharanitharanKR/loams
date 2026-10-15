# Studio settings

Open Studio's settings with **Settings** in Studio's nav, **Mod+,**, or the address `#/studio/settings`. Everything here is this device's, kept in the browser's local storage. Every read and write tolerates blocked storage, falling back to the defaults.

## Agentd hosts (browser only)

The remote `loams-agentd` hosts this browser knows, under `loams.studio.hosts`: a name, an `https://` URL (`http://` only for `localhost`), an optional certificate fingerprint as a note, and a pasted access token, which is kept in memory only. **Use** picks the host Studio works with. See [Remote agentd](remote-agentd.md#in-studio).

## New threads

Kept under `loams.studio.settings`:

| Setting | Default | Notes |
|---|---|---|
| **Harness** | Claude Code | Offers Claude Code, Codex, OpenCode and Loams agent |
| **Model** | empty | Empty uses the harness's own default |
| **Approvals** | Supervised | Supervised or Full access: the new thread's approval mode, set on the daemon, and its sandbox (`workspace-write` or `danger-full-access`) ([Approvals](approvals.md#which-mode-a-thread-gets)) |
| **Parallel runs** | 2 | 2 to 6. **Not used yet:** the Run in parallel dialog always starts with two variants and allows up to four |

These defaults apply to threads created with **New thread** or Mod+N. A thread started from a project page's composer takes the composer's own harness and model pickers and this **Approvals** mode ([Starting a thread](projects-and-threads.md#starting-a-thread)). **Full access** here is the explicit request for it: the daemon itself supervises every thread nobody chose a mode for. Destructive git actions always ask, whatever **Approvals** says.

The palette's **Harness and model** rows change the same settings.

## Editor (desktop only)

**Open threads in** picks the editor the thread header's folder button opens (Q810):
- **VS Code**, **Cursor** or **Zed**, through their `vscode://file/…`, `cursor://file/…` and `zed://file/…` links;
- **Another program…**, which you choose in the OS file dialog. Electron main runs it with the folder as its one argument, never through a shell (`open -a <app> <folder>` for a macOS app bundle).

The page can choose a scheme, but never which program runs: the program's path stays in Electron main (`studio.editor` in the app's `settings.json`), and the page sees only its file name. The header opens the thread's folder, not a file at a line.

## Notifications

**When a turn finishes** and **When an approval is waiting**, both on by default. On the desktop they decide which OS notifications show while the window is in the background ([Notifications](notifications.md#on-the-desktop)).

## Keyboard shortcuts

Every Studio binding, with rebinding, conflicts and reset ([Keybindings](keybindings-and-palette.md#rebinding)).

## Other storage keys

Studio also keeps, in this browser:
- `loams.shell.mode`, the last mode used;
- `loams.studio.panel`, the layout's sizes and open states;
- `loams.studio.drafts`, the composer's drafts;
- `loams.studio.keybindings`, your shortcut changes;
- `loams.studio.terminals` (session storage, per browser tab), the open terminal tabs.
