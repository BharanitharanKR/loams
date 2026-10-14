# Files

The **Files** tab of a thread's right panel shows the thread's checkout: a file tree, a read-only viewer and a content search (SD1 Tasks 11 and 21). **Mod+Shift+E** opens and closes it. Studio is not an editor: to change a file, ask the agent or open the folder in your own editor ([Settings](settings.md#editor-desktop-only)).

## The tree

- Folders are read when you open them. **Search files** filters by path and shows only matching files with their folders.
- **Refresh workspace files** reads the tree again. You can expand and collapse everything at once, and the tree reveals the open file.
- **Mention in composer** (on a row) queues the file for the thread's [composer](composer.md), shown as removable chips under the tab until the composer takes them.
- **Go to file** opens a file by name.

## The viewer

- **Text** is syntax highlighted with Shiki, rendered from escaped text inside a shadow root (D919). Highlighting loads on first use, so plain text shows first.
- **Markdown** can be shown rendered, with the chat's sanitised renderer: no raw HTML, no remote images, http(s) links only. A relative link opens the linked file in the viewer and never leaves the checkout.
- **Images** (the types T3 Code shows) are read as bytes through the daemon, up to 16 MB, and shown from a `blob:` URL; they are never loaded from a URL. Other binary files, video, audio, PDF and HTML are not previewed; HTML shows as source text.
- **Word wrap**, rendered markdown and the tree beside the file are view choices that last for the page's life.

The daemon caps a read at 1 MB, and refuses a path outside the checkout.

**Links into the viewer.** `?panel=files&path=<path>[&line=<n>][&column=<n>]` on a thread's address opens a file at a line, as a terminal's file links do. The path must be relative to the checkout: an absolute path, `.` or `..` segments, a backslash or a control character drops the link.

## Search in files

**Search in files** searches the contents of the checkout:
- with **Match case** and **Use regular expression** options (the daemon also takes path globs, which the dialog doesn't offer yet);
- honouring `.gitignore`, including hidden files, skipping `.git`, and never following symbolic links;
- reading the first 1 MiB of each file and skipping binary files;
- stopping at 2 000 matches or after 10 seconds.

A query is at most 1 024 characters. Regular expressions use Rust's `regex` crate, which runs in linear time. Results stream in as they are found; clicking one opens the file at that line. Result lines are plain text, not highlighted.
