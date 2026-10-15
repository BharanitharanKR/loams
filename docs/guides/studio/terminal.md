# Terminals

Studio's terminals live in the drawer at the bottom of a thread (design [§53](../../design/53-studio-ade.md) §5.7, D920; SD1 Task 22). They are shells that `loams-agentd` runs on its machine, in the thread's checkout, drawn with xterm.js. T3 Code's drawer, tabs and link detection are vendored on top of it; its own wasm terminal is not (Q818).

## Using them

- **Ctrl+`** opens and closes the drawer. Drag its top edge to resize it.
- **Each tab is one shell**, labelled "Terminal N", or with the project action's name when an action started it. Open more from the drawer's toolbar.
- **Shells keep running** when you hide the drawer, switch threads or switch to Console mode.
- **After a reload** Studio finds its terminals again and replays their recent output (the daemon keeps about 1 MB per terminal). The open tabs are kept per browser tab, under `loams.studio.terminals` in session storage. The daemon has no way to list terminals, so if that storage is cleared, a shell keeps running on the daemon until it exits.
- **When a shell exits** its tab stays, with "[terminal] Process exited with code N", until you close it.
- **Closing a tab whose shell is still running** asks first.

**Pasting.** Studio removes control characters from a paste first (escape sequences, ^C, ^D; tabs and line breaks stay), so a copied snippet can't end a bracketed paste early and run the rest. Pasting more than one line then asks first. The paste goes through as a bracketed paste when the shell asks for one.

**Links.** Ctrl+click (Cmd+click on macOS) opens a link:
- an http(s) URL in your browser;
- a file path in the [Files](files.md) tab, at its line and column when the path names them. A path inside the terminal's directory is taken relative to the checkout; a path outside the checkout opens nothing.

**Selection to composer.** With text selected, **Add to composer** puts the selection in the thread's [composer](composer.md) as a context chip. It is kept in memory only, since it is terminal output.

**Keys.** Inside a terminal every key goes to the shell (Ctrl+C, Ctrl+D…), except:
- the shell's own chords, **Mod+K** (the palette), **Mod+J** (the agent panel) and **Mod+Shift+M** (the mode switch);
- Studio's shortcuts that are allowed while a terminal has focus, such as **Ctrl+`**.

So on Linux, Ctrl+K and Ctrl+J don't reach the shell.

**Project actions** run as named terminals in the drawer. They belong to an agentd space, so the drawer offers them only on a thread that has one; for now Studio's projects are plain repositories, and the menu stays hidden.

## Limits

- At most 32 terminals per daemon.
- Input is sent base64-encoded, in chunks of up to 32 KiB.
- There is no right-click menu, no split view and no terminal in the right panel (the **Terminal** tab there is empty).
- The theme follows `@loams/ui`'s tokens (`--op-term-*`), light and dark.
