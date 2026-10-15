# Keybindings and the palette

"Mod" is Ctrl on Linux and Windows and Cmd on macOS.

## The command palette

**Mod+K** opens the palette in both modes (SD1 Task 24). Its ranking is T3 Code's, moved into `@loams/ui`. The current mode's rows come first, under their own headings; the other mode's rows follow under one heading, "Console" or "Studio".

In Studio the palette offers:
- **Actions:** the Studio commands that something on screen handles right now (new thread, the panels, the terminal, settings), each with its shortcut, and **Copy project path**.
- **Git:** the open thread's git actions, the same as its [toolbar](git.md)'s, with the same confirmation.
- **Threads:** the 12 most recent for an empty query, all of them when you type; **Projects** and **Run groups**.
- **Harness and model:** set the harness and model for new threads on this device.
- **Go to** rows for both modes' pages, and **Switch to Console** / **Switch to Studio**. These are the shell's rows.

**Not built yet:** quick-opening files from the palette (use **Go to file** in the [Files](files.md) tab), and models in the harness rows.

A mode's plugins load the first time you enter that mode, so their own rows appear after that; the shell's "Go to" rows cover both modes from the start.

## The shell's shortcuts

These belong to the shell, work in both modes and can't be rebound:

| Keys | Does |
|---|---|
| Mod+K | The command palette |
| Mod+Shift+M | Switch between Console and Studio |
| Mod+J | The docked agent panel |

## Studio's default shortcuts

| Keys | Command | When |
|---|---|---|
| Mod+B | Toggle the sidebar (`sidebar.toggle`) | not while typing in a field |
| Mod+[ / Mod+] | Back / forward (`navigation.back`, `navigation.forward`) | not in a terminal |
| Ctrl+` | Toggle the terminal drawer (`terminal.toggle`) | always |
| Mod+Alt+B | Toggle the right panel (`rightPanel.toggle`) | always |
| Mod+D | The Diff tab (`diff.toggle`) | a thread is open, not in a terminal |
| Mod+Shift+E | The Files tab (`files.toggle`) | a thread is open, not in a terminal |
| Mod+Shift+J | The Preview tab (`preview.toggle`) | a thread is open |
| Mod+N, Mod+Shift+O | New thread (`chat.new`) | not in a terminal |
| Mod+, | Studio settings (`settings.open`) | always |
| Mod+Shift+[ / Mod+Shift+] | Previous / next thread (`thread.previous`, `thread.next`) | always |
| Mod+1 … Mod+9 | Jump to the sidebar's first nine threads (`thread.jump.1` … `9`) | the desktop only (in a browser these keys switch tabs) |

**Differences from T3 Code:**
- **Ctrl+`** toggles the terminal, as in VS Code; T3 Code's Mod+J is the shell's agent panel.
- **No model-picker key**: T3 Code's Mod+Shift+M is the shell's mode switch.

**When keys are ignored.** A key press inside a dialog or the palette is not a Studio shortcut. Inside a terminal only the commands allowed there reach Studio ([Terminals](terminal.md#using-them)).

Inside the thread: **Mod+F** finds text in the thread. On an approval or question card with focus, **Enter** answers once, **Shift+Enter** answers always and **Esc** denies.

## Rebinding

**Studio settings › Keyboard shortcuts** lists every binding with its command, keys, `when` clause and source. **Filter shortcuts** searches the list.
- **Change keys:** click a binding's keys, then press the new ones ("Press keys…"); Escape cancels.
- **Change the `when` clause:** it is checked as you type, and saved on Enter or when the field loses focus.
- **Conflicts** are shown on the row ("Conflicts with …"), including the shell's three chords.
- **Reset** puts one command back to its defaults; **Reset all** puts them all back.

Only your changes are stored, under `loams.studio.keybindings` in this browser, in the shape of T3 Code's `keybindings.json`. A command you rebind loses its default keys until you reset it. Stored rules that no longer parse are dropped. If the browser's storage is unavailable, Studio keeps the defaults, and a change lasts for the page's life.
