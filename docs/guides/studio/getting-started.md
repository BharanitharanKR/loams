# Getting started with Loams Studio

Studio is the console's second mode ([overview](index.md)). It needs a `loams-agentd`: the local one on the desktop, a remote one in a browser.

## On the desktop

1. **Open Loams Desktop.** On the first run it opens in Studio (Q801). Later it opens in the mode you last used. You can switch with the header's **Console | Studio** control, **Mod+Shift+M**, or the **View** menu's **Studio**, **Console** and **Switch mode** items.
2. **Start the daemon if Studio asks.** Studio works through the local `loams-agentd`. While it looks for the daemon, the home says "Looking for the daemon…". If the daemon isn't running, the home says **Start the daemon** with a **Start** button. If it can't be reached at all, the home says **The daemon is unavailable**: check **Settings › Background agents** in Console mode. That page also decides whether agents keep running after you close the app.
3. **Add a project.** Use **Add** beside the **Projects** heading in Studio's sidebar. On the desktop it opens your OS folder picker: pick a git repository's folder. The project is that repository; its id is its path.
4. **Start a thread.** Click the project's name. Its page has a **New thread** composer: type a prompt and press Enter. By default the first run gets its own git worktree on a new branch. See [Projects and threads](projects-and-threads.md#starting-a-thread) and [The composer](composer.md).

The desktop also gives Studio:
- **Deep links:** `loams://open/studio/t/<threadId>` opens a thread, `loams://open/studio/g/<groupId>` a run group. `loams://open/agent/<id>` opens `/studio/t/<id>`.
- **A tray block:** up to five threads that are running or waiting for an approval, each opening its thread, then **Open Studio**.
- **OS notifications** when a turn finishes or a question is waiting ([Notifications](notifications.md)).
- **Open in editor** ([Settings](settings.md#editor-desktop-only)).
- **A native preview** ([Preview](preview.md)).

## In a browser

**Studio in the browser is behind the instance feature `studio` until SD1 Task 32.** The browser console shows the mode switcher only when the server's `GetInstance` lists `studio: true` among its features. No Loams server sets that feature on `dev` yet, so outside the dev server's demo (below) the browser console has no Studio today.

Where Studio is available, it needs a remote `loams-agentd`:

1. Run `loams-agentd serve --remote` on a machine you control ([Remote agentd](remote-agentd.md)).
2. In Studio, open **Settings** (Mod+,) and add the host under **Agentd hosts**, with a pasted access token for now.
3. **Add** (beside **Projects**) asks for a git URL (`https://`, `ssh://` or `user@host:path`) and clones it on the remote machine. The daemon checks the URL again and refuses local paths, `file:` and `ext::` URLs, and anything that starts with `-`.

**Pending:** the browser console's Content-Security-Policy does not name your agentd host yet, so a production console reaches only some hosts ([details](remote-agentd.md#the-consoles-content-security-policy)).

## Trying Studio without a daemon: the dev preview

The console's dev server can show Studio with sample data. You need Node 22 or later and pnpm (the root `package.json` pins the version):

```sh
pnpm install --frozen-lockfile          # from the repository root
cd web && pnpm dev                      # the console on http://localhost:5173/ui/
```

- `http://localhost:5173/ui/?desktop` is the **desktop edition with a fake bridge** (`web/apps/console/src/cordis/fake-desktop.ts`). Its fake agentd (`fake-agentd.ts`) answers Studio's calls with sample projects, threads, worktrees, a run group and a diff, so every Studio page has something to show. It opens in Studio.
- `http://localhost:5173/ui/?demo` is the **browser edition with an in-browser mock server**, which turns the `studio` feature on in dev builds only. Studio there has no host until you add one.

Neither runs agents. Both exist only in dev builds (`import.meta.env.DEV`); a production build drops them. The rest of the dev setup (the console API mock, `protoc`) is in [`web/README.md`](../../../web/README.md#develop).
