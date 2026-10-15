# Console UI stack

Status: decided (owner ruling, October 2026). Applies to the cordis console, which is
both the web console (`web/apps/console`) and the Electron desktop's UI
(`apps/desktop-electron` loads it at `/ui/`), and to everything under `web/plugins/*`
and `web/packages/*`.

These rulings settle how console UI code is written, so the page-porting and
Connect-porting work that follows does not re-open them.

## Decisions

1. **Tailwind 4 stays, tokens only.** Colours, spacing, corner radii and shadows come
   only from `@loams/ui`'s `tokens.css`, one token set with a light and a dark value
   (`.dark` on `<html>`). Tailwind's default palette, radii and shadows are cleared in
   `web/apps/console/src/tailwind.css`; utilities such as `bg-surface`, `text-muted`,
   `border-rule`, `shadow-pop` and `rounded-pill` map onto the tokens. Plugins never
   hard-code a colour. `web/packages/ui/test/colours.test.ts` fails on any raw hex,
   `rgb()`, `hsl()`, `oklch()` (and similar) literal in `web/plugins/**` or
   `web/packages/**` outside `tokens.css`; tests and generated code are exempt. Chart
   series colours are tokens too (`--op-chart-1` .. `--op-chart-8`).

2. **Headless primitives, owned in `@loams/ui`.** Dialog, menu, popover, select,
   combobox, tabs, tooltip and toast are Base UI (`@base-ui/react`), wrapped once in
   `web/packages/ui/src/overlay.tsx` and styled with Tailwind token utilities, the
   shadcn way: we own the code. Plugins import UI only from `@loams/ui`, never from
   `@base-ui/react` directly. No styled kits (MUI, Mantine, Chakra and the like).
   The native `Select` field stays for plain forms; `SelectMenu` is the Base UI select.

3. **No Effect.** Plain TypeScript; cordis for dependency injection and plugin
   lifecycle; Connect for typed RPC; zod at trust boundaries (IPC, config, manifests).

4. **Server state.** `@tanstack/react-query` with `@connectrpc/connect-query`. The
   console host provides one `QueryClient` and the console's Connect transport around
   the whole tree (`ServerStateProvider` in `web/packages/console-host/src/root.tsx`),
   so a page reads an RPC with `useQuery(method, input)`. Pages' existing data layers
   move onto it in the Connect-porting task, not piecemeal.

5. **Heavy tables and lists.** `@tanstack/react-table` (v9) and `@tanstack/react-virtual`
   through `@loams/ui`'s `DataTable` (sortable columns, same look as `Table`) and
   `VirtualList`. Adopt them where a table or list is touched.

6. **Kept:** echarts (through `@loams/charts`), `@xyflow/react`, `lucide-react`.

7. **One version of each shared dependency.** `pnpm-workspace.yaml` has a `catalog:`
   for react, react-dom and their types, lucide-react, @base-ui/react, @tanstack/*,
   @connectrpc/connect-query, zod, echarts, @xyflow/react, tailwindcss and
   @tailwindcss/vite; packages depend on them as `"catalog:"`. A bump changes the
   catalog only, and only to a release at least 14 days old.

## Consequences

- A new colour, shadow or radius is a token first, in both themes, checked for AA
  contrast against the surfaces it sits on.
- A plugin that needs a primitive `@loams/ui` lacks adds it to `@loams/ui`.
- Plugin tests that need server state wrap the component in `ServerStateProvider`
  (exported from `@loams/console-host/testing`).
