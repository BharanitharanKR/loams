# 59 — Console and Desktop Redesign, Part 1: the Foundation

Status: **Approved direction** (owner, 2026-10-17) · details **Proposed** · 2026-10-17. Decisions **D1073–D1086**, questions **Q928–Q935** in the [decision log](13-decision-log.md). It amends the [console UI stack](console-ui-stack.md) (tokens and primitives) and the shell layout of [§19](19-console-identity-and-agents.md) and [§37](37-desktop-and-mobile-apps.md). It reuses [§53](53-studio-ade.md)'s T3 Code vendoring.

## 1. Owner direction (2026-10-17, binding)

- "Current UI/UX is not good." All four problems apply: the navigation and structure, the visual style, the core workflows, and the desktop feeling like a web page.
- "The square UI is bad. I want a T3 Code, Codex, DSH Desktop-like UI." The DSH Desktop reference is DataElement's [dsh-desktop](https://github.com/dataelement/dsh-desktop), which is MIT-licensed and also built on cordis.
- **Agent-first.** The home is a composer and threads. You ask Loams to create databases, query or deploy. Results, tables and diffs appear inline or in a side inspector. Classic pages stay reachable from the sidebar and the palette.
- **The work comes in four parts, in order:**
  1. the foundation (this document);
  2. the agent home;
  3. the core workflows;
  4. porting the remaining plugin pages.
- **Visual direction:** direction B (focused, dense, T3 Code-like) as the **dark** theme, and direction A (calm, warm, Codex-like) as the **light** theme. The mockups are §12.
- **"Copy as much code from T3 Code and dsh-desktop."** Both are MIT-licensed. §10 sets the limits.

## 2. What this part covers, and what it doesn't

This part covers:
- the visual language, as tokens in `@loams/ui`;
- the primitives, completing the T3 Code `components/ui` port into `@loams/ui`;
- the app shell: sidebar, top bar, main area, right inspector, bottom drawer and command palette;
- the desktop window chrome, menus, splash and shortcuts.

The agent home's content (the composer, threads and starters as a product flow) is Part 2. This part gives the shell a **home slot** that Part 2 fills; until then, the home shows the existing Studio composer centred, as in mockup 2. Existing plugin pages keep working unchanged inside the new shell. They pick up the new look through the tokens, and Part 4 reworks them one by one.

## 3. Visual language (D1073–D1076)

**D1073. Tokens keep their names and get new values; a few are added.**
- `--op-*` stays the only colour, radius, shadow and spacing source. Plugins don't change for this part.
- The values come from T3 Code's `apps/web/src/index.css` (commit pinned in `provenance.json`), adapted:
  - **Radius:**
    - `--op-radius` moves from 0 to **10px**;
    - new `--op-radius-xs` (4), `--op-radius-sm` (6, today 4), `--op-radius-md` (8, controls), `--op-radius-lg` (14, cards and composer), `--op-radius-xl` (22, dialogs);
    - `--op-radius-pill` stays 999px.
  - **Dark (direction B, T3 Code's dark):**
    - canvas neutral-950 `#0a0a0a`;
    - surface (card, popover, sidebar) = canvas 97% + white 3%;
    - borders white at 6% alpha, inputs white at 8%;
    - muted fill white at 3–4%;
    - text neutral-100 with a muted text tone;
    - accent `oklch(0.571 0.21 264)` (indigo);
    - status emerald, amber, red and blue for state only.
  - **Light (direction A, warm neutral):**
    - window `#f4f2ee`;
    - the work area a white card on it;
    - borders `#efece7`/`#ece8e2`;
    - text `#1d1b18`, muted `#8a857c`;
    - the same indigo accent at `oklch(0.488 0.217 264)`;
    - the selected sidebar row is a white chip with a 1px shadow.
  - **Elevation:** layers come from lightness steps and hairlines. Shadows are only for floating things:
    - `--op-shadow-composer`: `0 12px 28px -18px` (new);
    - `--op-shadow-pop` and `--op-shadow-modal`: kept, values softened;
    - dialogs get a 12–16px backdrop blur at 80% opacity, with a 4% white inset highlight in dark.
  - **Contrast:** every new colour is checked for AA contrast against the surfaces it sits on (the console UI stack's rule). `colours.test.ts` stays: literals live only in `tokens.css`.

**D1074. Theme selection.** The default follows the OS (`prefers-color-scheme`). With no OS preference, dark is the default. A user choice (System, Dark, Light) is saved per device; the desktop app saves it in its settings, the web console in local storage. `.dark` on `<html>` stays the switch. The desktop syncs its native chrome to the page theme (§6).

**D1075. Type.**
- **UI font:** the system stack (`-apple-system, BlinkMacSystemFont, "Segoe UI", Inter, sans-serif`), replacing Archivo, so the app looks native on each OS.
- **Code font:** the system monospace stack, replacing Martian Mono, which today labels much of the UI and makes it read as a terminal.
- **Monospace is for code, ids and SQL only**, never for labels.
- **Scale:**

  | Use | Size | Weight |
  |---|---|---|
  | micro | 11px | 500 |
  | small (sidebar rows, tables) | 12px | 500 |
  | body | 13px | 400 |
  | emphasis | 14px | 600 |
  | section titles | 16px | 600 |
  | page titles | 20px | 600, tracking −0.02em |
  | home hero | 24px | 600 |

- **Data cells stay at 12–13px.** They never use the micro sizes, because readability of wide result tables matters more than density (§3 of the research note in §12).
- Q928 asks whether to keep a brand font for headings only.

**D1076. Density and motion.**
- **Control heights:** 28 (compact), 32 (default) and 36 (input). Top bar 44, sidebar rows 28, gutters 12–20.
- **Motion is off by default:** panels snap open. Colour and opacity transitions are 100–150ms. An optional setting allows panel easing up to 400ms on `cubic-bezier(.32,.72,0,1)`, which is T3 Code's drawer curve. `prefers-reduced-motion` always wins.

## 4. Primitives (D1077)

**D1077. `@loams/ui` completes the port of T3 Code's `components/ui`, on Base UI.**
- 12 primitives are already vendored (`web/packages/ui/provenance.json`). The rest of T3 Code's set comes over with the same per-file provenance rows (verdict `as-is`, `adapt` or `rewrite`):
  - alert / alert-dialog, autocomplete, badge, button, checkbox, collapsible-section-header;
  - combobox, command (the palette), dialog and its styles, empty, group, input / input-group, kbd;
  - menu, middle-truncate, number-field, popover, preview-card, radio-group, scroll-area, select;
  - separator, sheet, sidebar (+ `sidebarState`), skeleton, spinner, switch, table, textarea;
  - toast (+ `toastHelpers`), toggle / toggle-group, tooltip.
- Each is restyled onto the `--op-*` tokens. Plugins still import UI only from `@loams/ui`.
- Primitives that `@loams/ui` has today and T3 Code lacks keep their API, restyled: `DataTable`, `VirtualList`, `Segmented`, `status`, `card`, `field`, `grain`, `logo`, `coming-soon`.
- `grain` (the paper-grain texture) is removed from the shell; Q929.

## 5. The app shell (D1078–D1082)

**D1078. Regions.** The shell (`web/plugins/shell`) lays out six regions:

| Region | What it holds | Slot |
|---|---|---|
| Sidebar (232px, collapsible with ⌘B, resizable 200–320) | brand and the server indicator; **New thread** (⌘N); projects, each with its threads; **Pages** (§5 D1080); Settings at the foot | `shell.sidebar.*` |
| Top bar (44px, the window's drag region) | project pill, branch pill, breadcrumbs for classic pages, ⌘K hint, and the inspector and drawer toggles | `shell.topbar.{left,center,right}` |
| Main | home (Part 2), a thread, or a classic page | `shell.main` (router) |
| Inspector (right, 280px default, resizable 240–640, ⌘I) | tabs contributed per context, e.g. Branch, Diff, Plan, SQL, Details | `shell.inspector.tab` (list) |
| Drawer (bottom, resizable, ⌘J) | tabs, e.g. SQL console, Logs, Jobs, Terminal (desktop) | `shell.drawer.tab` (list) |
| Palette (⌘K, also ⌘⇧P) | projects, branches, threads, pages, commands, table names and thread text | `shell.palette.source` (list) |

- The slots use the existing `@loams/slots` registry. A plugin contributes inspector tabs, drawer tabs and palette sources the way it contributes pages today.
- A result card anywhere (a thread, a page) can call `inspector.open({tab, props})`, which is how "Open ↗" works.

**D1079. Sidebar rows carry live status.**
- A thread row shows a status dot (running, needs approval, done, failed, idle) and, while running, an elapsed time.
- Rows can be pinned, renamed, drag-reordered and archived ("settled", from T3 Code). Archived threads live in Settings.
- Thread ordering and status come from Studio's existing thread store (`@loams/studio-ui` `chat/store`) through `rpc.agentd.*`. Projects come from the console's existing projects API.

**D1080. Navigation moves from many sections to three places.**
- **The sidebar's Pages list** holds the few pages a project uses daily. By default these are Data, Deployments, Git, Agents and Connectors; each project can pin or unpin pages.
- **The palette** reaches every page and every command.
- **Settings** holds organisation admin: Teams, Members, Access, Audit log, Approvals settings and Billing. These are real pages (D1084), not inspectors.
- The sidebar's current section headers ("Data", "Compute", "Organisation", "Operate") go away. Every plugin page keeps its route, so links, bookmarks and the hash router keep working.

**D1081. Keyboard.**

| Keys | Action |
|---|---|
| ⌘K / ⌘⇧P | palette |
| ⌘N | new thread |
| ⌘B | sidebar |
| ⌘I | inspector |
| ⌘J | drawer |
| ⌘1–9 | the first nine threads |
| ⌃Tab | next thread |
| ⌘[ / ⌘] | back / forward |
| ⌘⇧T | reopen a closed thread |
| ⌘, | settings |
| Esc | close the topmost overlay |

- On Windows and Linux, ⌘ is Ctrl.
- Bindings live in one table in the shell, so Part 2 and later parts add to it. A user-editable keymap (T3 Code's JSON with `when` clauses) is Q930.

**D1082. Toasts and confirmations.**
- **Toasts** use the vendored T3 Code toast: stacked, bottom right, auto-dismissing, with actions.
- **Destructive confirmations** use the alert dialog and name the object and its impact.
- **Typed confirmation** is required for production data, e.g. typing the branch name to drop a table on production. The rule is set here; Part 3 applies it in each flow.

## 6. Desktop chrome (D1083–D1085)

**D1083. One window style per OS.** This follows T3 Code's `apps/desktop/src/window/DesktopWindow.ts` and dsh-desktop's `src/main/index.ts` and `macos-window-backdrop.ts` (both MIT):
- **macOS:** `titleBarStyle: 'hiddenInset'`, traffic lights at x 16, vertically centred in the 44px top bar.
  - Vibrancy `sidebar` behind the sidebar only, in dark and light.
  - When the window is hidden or minimised, it switches to a solid background to avoid flashes (dsh-desktop's backdrop swap).
- **Windows and Linux:** `titleBarStyle: 'hidden'` with `titleBarOverlay` 44px high. Its `color` and `symbolColor` follow the page theme and are updated live when the theme changes.
- **The top bar is the drag region** (`-webkit-app-region: drag`). Every control in it opts out with `no-drag`.
  - The preload sets `html[data-platform]` and `data-fullscreen`, so CSS reserves the traffic-light space only when it is there.
- **Window size:** minimum 900×640. Window state is saved and validated against the current displays, as today.

**D1084. Native menus and the splash.**
- **Menus:** an application menu with App, File, Edit, View, Window and Help. It carries the D1081 accelerators and items such as Check for updates, Show logs, Restart local server and Report an issue. A context menu offers cut, copy, paste, and look-up where the OS supports it.
- **Splash:** a frameless splash window shows while the local stack starts. It uses dsh-desktop's light and dark loader generator (`scripts/generate-loader-gifs.mjs`) with Loams' mark, and stays up until the console reports ready.
- **Pages, not inspectors:** organisation admin pages (D1080) and the desktop's own pages (Servers, Agents runtime, Desktop settings) open as full pages in the main region.

**D1085. Native notifications and the update card.**
- **Notifications:** a thread that needs approval, or a long run that has finished, posts an OS notification when the window isn't focused. Clicking it focuses the window and opens the thread. This uses dsh-desktop's notification shim pattern, re-implemented on the existing preload bridge.
- **Updates:** the update card moves to the vendored toast. It follows dsh-desktop's consent-before-download flow (skip this version, download, restart) and respects the existing opt-out (#688).

## 7. Accessibility and quality

- **Contrast:** every token pair used for text meets WCAG AA. A token test checks the documented pairs.
- **Focus:** focus rings use the accent at 2px, offset 2px. Every region and palette result is keyboard reachable.
- **Testing:**
  - **Screenshot tests.** Playwright runs `@loams/console` in `?demo` mode and captures the shell in dark and light:
    - home;
    - a thread with the inspector open;
    - a classic page;
    - the palette;
    - the drawer.
    
    They run in CI's web job; Q931 asks where the baseline images live.
  - **Accessibility checks:** axe on the same screens.
  - **Desktop:** the desktop e2e test checks window chrome per OS on CI's Linux runner, plus a screenshot of the title-bar area.

## 8. Rollout (D1086)

**D1086. One switch, no flag.**
- The new tokens, primitives and shell land on `dev` behind no feature flag. Pages restyle through the tokens.
- Each step is a PR that keeps the console working:
  1. tokens;
  2. primitives;
  3. shell regions;
  4. desktop chrome.
- Plugin pages that look wrong after the token change get fixed in the step that broke them, not deferred to Part 4.
- The release notes call it out as a visual change, not a breaking API change.

## 9. Relation to Parts 2–4

- **Part 2, the agent home,** fills `shell.main`'s home route. It brings:
  - the composer with scope chips (project, branch, access mode: read-only / propose / apply, and model);
  - the starters;
  - the thread view with inline result cards and approvals;
  - the flows that open the inspector.
- **Part 3** builds onboarding and connection, database and branch creation, querying, deploying and Git in that home. It also applies the typed-confirmation rule (D1082).
- **Part 4** moves the remaining pages onto inspector and drawer patterns where they fit, and keeps admin as pages.

## 10. Licensing and provenance

- **T3 Code** (MIT, © 2026 T3 Tools Inc.) files keep using `web/packages/ui/provenance.json` and `web/packages/studio-ui/provenance.json`. Each row has the upstream path, commit, both SHA-256 sums and a verdict. `LICENSE.t3code` and NOTICE stay.
- **dsh-desktop** (MIT, © DataElement) files get a new `apps/desktop-electron/provenance.json` with the same row shape, a `LICENSE.dsh-desktop` and a NOTICE entry.
- **Only the dsh-desktop repository** (`dataelement/dsh-desktop`) is a source. The prebuilt `@deepseek-ai/dsh-client-ui-*` npm packages it loads are **not** copied, read or bundled, because their licence is not established.
- New third-party dependencies follow CONTRIBUTING rule 3 and `deny.toml`, and need versions at least 14 days old.

## 11. Decisions and questions

| ID | Decision | Status |
|---|---|---|
| D1073 | Tokens keep their names, take T3 Code-derived values (dark = B, light = A), and add the radius scale and the composer shadow | Approved direction (owner, 2026-10-17); values Proposed |
| D1074 | Theme follows the OS, dark when there is no preference; a per-device choice; `.dark` on `<html>` | Proposed |
| D1075 | System UI font and system monospace; monospace only for code, ids and SQL; the type scale | Proposed |
| D1076 | Density (28/32/36 controls, 44 top bar) and motion off by default | Proposed |
| D1077 | `@loams/ui` completes the T3 Code `components/ui` port with per-file provenance | Approved direction (owner, 2026-10-17: "copy as much code") |
| D1078 | Six shell regions with slots: sidebar, top bar, main, inspector, drawer, palette | Approved direction (owner, mockups 2026-10-17) |
| D1079 | Sidebar thread rows with live status, pin, reorder and archive | Approved direction |
| D1080 | Navigation moves to sidebar Pages, the palette and Settings; routes kept | Proposed |
| D1081 | The keyboard table; Ctrl on Windows and Linux | Proposed |
| D1082 | Toasts; destructive confirmations; typed confirmation for production data | Proposed |
| D1083 | Window style per OS, the top bar as the drag region, native chrome synced to the theme | Approved direction |
| D1084 | Native menus, the splash, admin as pages | Proposed |
| D1085 | OS notifications for approvals and finished runs; the update card in a toast with consent before download | Proposed |
| D1086 | Rollout without a flag, in four PR steps | Proposed |

| ID | Question | Default |
|---|---|---|
| Q928 | A brand font for headings only? | No; system fonts throughout |
| Q929 | Keep the paper-grain texture anywhere? | No |
| Q930 | A user-editable keymap (JSON with `when` clauses, T3 Code style)? | Later, in Part 2 or after |
| Q931 | Where screenshot baselines live (repository or CI artifact) | In the repository under `web/apps/console/test/screens/`, updated by a script |
| Q932 | Default pinned pages per project | Data, Deployments, Git, Agents, Connectors |
| Q933 | Sidebar vibrancy on macOS in light mode too? | Yes, behind the sidebar only |
| Q934 | Should the hosted web console show the same sidebar thread list? (Threads need an agentd.) | Yes, when the server offers agentd; otherwise Pages only |
| Q935 | Mobile (§37) | Out of scope for Part 1; the tokens apply when the mobile app is restyled |

## 12. References

- **Mockups** (2026-10-17, local, not committed):
  - `.superpowers/brainstorm/2210344-1792201038/content/shell-directions.html` (A/B/C);
  - `foundation-v1.html` (dark home, light thread with inspector).
- **Research:**
  - T3 Code `apps/web/src/index.css`, `components/ui/*` and `apps/desktop/src/window/DesktopWindow.ts` at `pingdotgg/t3code` commit `33aa4614`. It is MIT.
  - dsh-desktop `src/main/index.ts`, `src/main/macos-window-backdrop.ts`, `src/preload/desktop-update-ui.ts`, `build/splash.html` and `scripts/generate-loader-gifs.mjs` at `dataelement/dsh-desktop` commit `bc732f5`. It is MIT.
  - Codex app: <https://learn.chatgpt.com/docs/app>, <https://learn.chatgpt.com/docs/code-review.md>, <https://learn.chatgpt.com/docs/reference/commands.md>.
- **What does not translate from coding tools to a database console:**
  - data needs a real grid beside the thread, not inside chat;
  - review needs schema, row and migration diffs;
  - destructive operations need branch-first defaults and typed confirmation;
  - organisation admin stays as pages.
