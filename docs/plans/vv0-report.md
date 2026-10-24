# VV0 report: Void and Vite+ spikes

> **Status: in progress.** Track **VV**, plan [VV1](2026-10-23-vv1-void-and-vite-plus.md), design [§79](../design/79-void-and-vite-plus.md).
> Each task's PR fills its own section below. Spike code lives under `spikes/vv0/<task>/` in the task's worktree and is not committed; each section gives the commands to reproduce it.
>
> **Accounts.** Tasks 2, 3, 5 and 7 deploy only to the owner's Cloudflare Free test account and AWS credit account (§70 §12.2), with scoped tokens the owner provides. Until the owner provides them, those tasks report only what can be proven locally, and label the rest "blocked".

## Summary

| Task | Question | Verdict |
|---|---|---|
| 1 | The console and `@loams/ui` under Vite+ | pending |
| 2 | `@loams/app` ports through Void's Cloudflare bindings | pending |
| 3 | Loams' shims around Void's `node` build | pending |
| 4 | Bun-compiled and `node` images in the light compose stack | pending |
| 5 | Hyperdrive to Loams Postgres over Tunnel, and TiDB Starter | pending |
| 6 | One `vite-plus` pin for the workspace | **Go.** 171 build, test and typecheck tasks in 75 projects pass on one `vite-plus` 1.3.0 pin. The peer warnings clear only with `peerDependencyRules.allowedVersions.vite`. electron-vite 6.0.0 runs on `vite-plus-core`. By the 14-day rule, the first pin is 1.1.0, with the desktop following on 2026-10-28 |
| 7 | The Loams Cloud control plane on Void | pending |
| 8 | Bun's ahead-of-time compile against Node | **Go** on cold start (5.5x faster than Node 24 for the process, 1.5x for the container); the image is 138 MB on distroless glibc and 118 MB on Alpine musl, so the under-120 MB target is met only on musl. Embedding `dist/client/` needs a one-line rewrite of Void's client root |

## Task 1

Pending.

## Task 2

Pending.

## Task 3

Pending.

## Task 4

Pending.

## Task 5

Pending.

## Task 6

**Question.** Can the whole workspace take one `vite-plus` pin, with `vite` aliased?

**Verdict: go.** Every JavaScript target is green on one pin. The design's "the peer warnings disappear" holds only with one more pnpm setting. The desktop builds on `vite-plus-core` through electron-vite 6.0.0, with the same bundle as PR #1049.

**The change tried (2026-10-24, `vite-plus` 1.3.0):**
- **`pnpm-workspace.yaml`:**
  - catalog entries `vite-plus: 1.3.0`, `vite: npm:@voidzero-dev/vite-plus-core@1.3.0` and `vitest: 5.0.5`;
  - the four `vite-plus@1.0.0>…` and `vitest@5.0.1>…` scoped overrides replaced by two global ones, `vite: npm:@voidzero-dev/vite-plus-core@1.3.0` and `vitest: 5.0.5`.
- **Every `package.json`** that named `vite`, `vitest` or `vite-plus` now says `catalog:`. That is 51 packages, plus `vite-plus` added to the console, `@loams/ui`, the desktop and `web/`.
- **Two version bumps:** `@vitest/coverage-v8` 5.0.3 to 5.0.5 in `plugins/`, and electron-vite 5.0.0 to 6.0.0 in the desktop.
- **Task 1's script changes** for the console and `@loams/ui`.

### Results

| Check | Result |
|---|---|
| `pnpm install` | Clean. The lockfile is 689 lines shorter (15,419 to 14,730). Before: `vite` 7.3.7 and 8.3.1, `vite-plus-core` 1.0.0, `vitest` 5.0.1 and 5.0.3, and `esbuild` 0.25.12. After: one `vite-plus-core` 1.3.0 (which reports Vite 8.3.4 in `bundledVersions`), one `vitest` 5.0.5, and no `esbuild` 0.25 |
| Peer warnings, without a rule | **4 unmet `vite` peers**, all for the same reason. `@voidzero-dev/vite-plus-core` identifies as its own version, 1.3.0, so ranges such as `^8.0.0` fail. The packages asking are `vitest` 5.0.5, `@vitest/mocker` 5.0.5, `@vitejs/plugin-react` 6.1.2, `@tailwindcss/vite` 4.3.3 and electron-vite 6.0.0. `dev` today has 5 unmet peers of its own: 3 for `vite` (the plugins' fork, which identifies as 1.0.0), one for `@vitest/coverage-v8` and one for `@vitest/browser-preview` |
| Peer warnings, with `peerDependencyRules.allowedVersions: { vite: 1.3.0 }` | `pnpm peers check`: "No peer dependency issues found". This rule moves with the pin, so a bump changes it in the same line |
| `nx run-many --targets=build,test,typecheck --projects='@loams/*,plugins*,loams-desktop-electron' --parallel=2 --skip-nx-cache` | **171 tasks in 75 projects, all green**, in 6 min 22 s, with `NX_NO_CLOUD=true` so nothing reached the remote cache |
| The desktop's `electron-vite build` (6.0.0 on `vite-plus-core` 1.3.0) | Green in 9 s. `out/main/index.js` is 2,065,819 bytes, with no `getElectronPath`: the same as #1049's 2,066.14 kB with plain Vite 8.3.4. So electron-vite 6 runs on `vite-plus-core`, and §79 §5.5's "until electron-vite runs on `vite-plus-core`" is already true. The desktop's unit tests passed in the run-many above. Its Playwright smoke wasn't run here, because it opens Electron windows on the shared machine's display. The VV1 PR runs it in CI |
| Nx | Unchanged. The targets keep their names, and `vp` runs inside them (Q1302) |

### Inputs to VV1

- **The 14-day rule decides which pin lands first.**
  - `vite-plus` 1.3.0 was published on 2026-10-16, so it is eligible on 2026-10-30. 1.2.0 (2026-10-14) is eligible on 2026-10-28.
  - **1.1.0** (2026-10-07) is eligible now. It bundles Vite 8.3.3, Rolldown 1.2.12 and tsdown 0.23.0, and it depends on Vitest 5.0.3, the version the workspace already uses.
  - electron-vite 6.0.0 (2026-10-14) is eligible on 2026-10-28. Until then, the desktop can't take the global `vite` alias: electron-vite 5 on Vite 8 inlines Electron's launcher, as #1049 found.
  - So the first VV1 pin is `vite-plus` 1.1.0 through the catalog for every package except the desktop, without a global `vite` override. The desktop keeps Vite 7.3.7 and electron-vite 5.0.0 until 2026-10-28, and then takes #1049's electron-vite 6.0.0 and the global alias.
- **The `vite` binary goes away.** Every script that runs `vite` becomes `vp` in the same PR (see Task 1).
- **`docs/monorepo.md`'s "Do not apply a global Vite override"** is replaced in that PR by the catalog entry and the `peerDependencyRules` line.

**Reproduce.** In a worktree off `dev`:
1. Apply the catalog entries, the overrides and the `peerDependencyRules` above, and Task 1's script changes.
2. Run `pnpm install --no-frozen-lockfile` and `pnpm peers check`.
3. Run `NX_NO_CLOUD=true NX_DAEMON=false pnpm exec nx run-many --targets=build,test,typecheck --projects='@loams/*,plugins*,loams-desktop-electron' --parallel=2 --skip-nx-cache`.
4. Run `pnpm --filter loams-desktop-electron run build`.

## Task 7

Pending.

## Task 8

**Verdict: go for the cold start, no-go for the image-size target as written.** A Bun-compiled Void app reaches its first 200 about **5.5x faster** than the same app on Node 24, against a target of at least 1.5x. Its image is **138 MB** on distroless and **118 MB** on Alpine, against a target of under 120 MB. The Bun 1.4.4 runtime alone is 109 MB, not the 77 MB §79 assumed. One Void path needs a one-line rewrite before `dist/client/` can be embedded (see "Static assets").

### What was built

- **The app.** There is no `web/examples/app-byoc-notes` yet, so the spike used a notes skeleton made with `void init --framework react --starter static --deploy none --toolchain vite`:
  - one SSR page with a loader (`pages/index.tsx` and `pages/index.server.ts`);
  - `routes/api/notes.ts` (GET and POST, in memory) and `routes/api/health.ts`;
  - no `void/*` binding imports, so the app is portable.

  `void.config.ts` sets `target` from `VOID_TARGET`.
- **Pins:** `void` 1.2.1, `@void/react` 1.2.1, `bun` 1.4.4 (from npm; its `install.js` was run by hand because npm 11 blocks install scripts), `hono` 4.13.13, React 19.3.1, and `vite` 8.3.5 (`void init` resolved `^8.0.10` to it).
- **Node:** builds ran on the host's Node 26.8.2, which meets Void's `>=24.21.0`. Measurements used Node 24.22.1, copied from `node:24-slim` (digest `sha256:55fb5f5a…`), with Node 26.8.2 as a reference.
- **The three builds:**
  1. `VOID_TARGET=node vite build` (2.1 s), run as `node dist/ssr/index.js` on `node:24-slim`, with production `node_modules` (12 MB: `hono`, `@hono/node-server`, `react`, `react-dom`);
  2. `VOID_TARGET=bun vite build` (1.2 s), run as `bun dist/ssr/index.js`;
  3. the same output, compiled with `bun build ./ssr/index.js --compile --bytecode --minify --sourcemap --format=esm --target=bun-linux-x64` (5.3 s cold, under 0.4 s warm), on `gcr.io/distroless/cc-debian12:nonroot`.

### Numbers

These are the median of 20 runs: spawn to the first 200 on `GET /` (an SSR page), polled every 2 ms. RSS is the median of 5 runs, read 2 s after the first 200. The host is a 14-core x86-64 machine shared with other build agents, with a load average of 1.5 to 6 during the runs. Each figure was repeated in a second round and agreed within 1 ms.

**The process on the host:**

| Runtime | Cold start (median) | Min | Speed-up against Node 24 | RSS at idle |
|---|---|---|---|---|
| Node 24.22.1, `target: "node"` | 96.0 ms (95.2 ms in round 2) | 82–86 ms | 1.0x | 71.5 MB |
| Node 26.8.2, `target: "node"` | 92.0 ms | 82.9 ms | 1.04x | 70.0 MB |
| `bun run`, `target: "bun"` | 46.0 ms | 41.0 ms | 2.1x | 39.7 MB |
| Bun compiled, no `--bytecode` | 40.9 ms | 36.7 ms | 2.3x | 42.3 MB |
| **Bun compiled, `--bytecode`, client embedded** | **17.6 ms** (17.5 ms) | 16.0 ms | **5.5x** | 34–41 MB |
| Bun compiled, `--bytecode --bytecode-order` | 17.9 ms | 16.3 ms | 5.4x | 39.8 MB |
| Bun compiled, `--bytecode`, client beside the binary | 20.4 ms | 16.3 ms | 4.7x | 34.3 MB |

On `GET /api/health` (no SSR), Node 24 took 80.6 ms and the compiled binary took 15.5 ms, a 5.2x speed-up.

**The images**, started with `podman run --rm --init --network host` (podman 6.1.1, rootless):

| Image | Base | Size | Cold start to the first 200 | Memory at idle (cgroup) |
|---|---|---|---|---|
| Node | `node:24-slim` | 247 MB | 218.5 ms | 25.6 MB |
| `bun run` | distroless `cc-debian12`, the `bun` binary and `node_modules` | 143 MB | 190.1 ms | 14.7 MB |
| **Bun compiled (glibc)** | distroless `cc-debian12` | **138 MB** | **148.1 ms** | 9.6 MB |
| Bun compiled (musl) | `alpine:3.22` with `libstdc++` and `libgcc` | **118 MB** | 148.0 ms | 9.6 MB |

- Inside a container, podman's own start-up (about 130 ms) dominates. The process-level gain is the one a long-lived container, a scale-from-zero container or a CLI sees.
- The binary is 112.7 MB uncompressed and 53.6 MB gzipped. The musl binary is 106.3 MB uncompressed and 50.1 MB gzipped. Registries store gzipped layers, so the pull is about 55 MB.
- The external source map (`--sourcemap`) is 1.6 MB and goes beside the binary. It isn't part of the image figures.

### Against §79 §7.6's targets

- **Cold start at least 1.5x faster: met.** The speed-up is 5.5x for the process, and 1.5x for the whole container (218.5 ms against 148.1 ms). `--bytecode` is what gives most of it: without it, the compiled binary is only 2.3x faster.
- **An image under 120 MB: not met on glibc (138 MB), met on musl (118 MB).** Bun 1.4.4's runtime is 109 MB (a compiled empty script is 109,123,760 bytes). That puts any glibc image above 130 MB, before any code. The proposed change, for the owner: restate the target as "smaller than the Node image" (138 MB against 247 MB is 44% smaller), or as "under 60 MB compressed". The alternative is the musl build on Alpine as the default.
- **`--bytecode-order` gains nothing measurable for an app this small** (17.9 ms against 17.6 ms). The order file is written only on a clean `process.exit()`. Void's generated `index.js` has no signal handler, so the spike added `process.on("SIGTERM", () => process.exit(0))` before compiling. That handler is also needed in a container in any case, because PID 1 ignores SIGTERM by default. Recommendation: skip `--bytecode-order` until an app's start-up is large enough to measure it.

### Static assets (§79 §7.6's verify item)

- **Void's static middleware reads from disk.** The generated `.void/app.ts` serves `dist/client/` with `serveStatic` from `hono/bun`, at `fileURLToPath(new URL("../../client/", import.meta.url))`. Inside a compiled binary, `import.meta.url` is `file:///$bunfs/root/<name>`, so the root becomes the absolute path `/client/`.
- Void also fetches `/.vite/manifest.json` through the same middleware to write the page's `<script>` tags. When the files aren't found, pages still render with a 200, but with no client script: **the page is never hydrated, and nothing fails loudly.** This is the failure to test for.
- **Beside the binary: works, but only at `/client/`.** The image copies `dist/client/` to `/client/` at the filesystem root. It doesn't work at any path relative to the binary.
- **Embedded with `--asset`: works after a one-line rewrite.** `--asset=./client` embeds the files at `/$bunfs/root/client/…`, and `Bun.file` and `fs.existsSync` read them. Before compiling, the build replaces `new URL("../../client/", import.meta.url)` with `new URL("./client/", import.meta.url)` in `dist/ssr/assets/app-*.js`. After that, `/`, the hashed JavaScript and the manifest are all served from the binary.
- **Recommendation for `loams app build --target bun --compile`: embed.** It gives one file and no layout rule. The rewrite is a text match on Void's output, so the build must fail when the pattern isn't found. The upstream fix would be an option or environment variable for the client root. Don't file it upstream without the owner's consent.
- **Side finding.** `dist/client/.vite/manifest.json` is publicly served on every target, Node included. It lists only hashed asset names. It's worth a note, but it isn't a blocker.

### Cross-compiling

- `--target=bun-linux-arm64`, `bun-linux-x64-musl` and `bun-linux-arm64-musl` all built on the x64 host. Bun downloaded each target's runtime from npm (`bun-linux-aarch64-v1.4.4` and the others).
- The sizes are 112.8 MB (arm64), 106.3 MB (x64 musl) and 105.3 MB (arm64 musl).
- The arm64 binaries were not run, because the host has no qemu-user or binfmt support. Running them is left to an arm64 CI runner.
- The x64 musl binary ran in `alpine:3.22` once `libstdc++` and `libgcc` were added. Without them it doesn't start.

### What fails, and the fallbacks

- **Observed:** only the static-asset path above. `hono`, `react-dom/server` and Void's runtime all compiled and ran unchanged.
- **Not exercised, in a portable notes skeleton:** native addons and packages that read files beside their own code. Void's own `better-sqlite3` and `@napi-rs/keyring` are build-time dependencies and aren't in `dist/ssr`. Task 4 exercises `pg` and an S3 client under the compiled binary.
- **The fallbacks, in order:**
  1. `bun run dist/ssr/index.js` on distroless with `node_modules` (2.1x faster than Node, 143 MB);
  2. `target: "node"` on `node:24-slim` (247 MB).

  The deploy records which runtime it used (§79 §7.6).

### The LGPL-2 notice (draft for Q1312, for counsel; nothing ships)

No binary is added to any release artefact. This is the text proposed to ship as `THIRD_PARTY_NOTICES/bun-runtime.txt` beside, or inside the image with, every Bun-compiled binary that Loams distributes, once counsel has reviewed it:

```
This program is a Bun single-file executable. It contains the Bun runtime
<BUN_VERSION> (https://github.com/oven-sh/bun, MIT), which includes
JavaScriptCore and other parts of WebKit, licensed under the GNU Lesser
General Public License version 2 (LGPL-2.0) and BSD-style licences.

The WebKit source for this build is at
https://github.com/oven-sh/WebKit/tree/<WEBKIT_COMMIT>, the commit pinned by
Bun <BUN_VERSION>. A copy of the LGPL-2.0 is in LICENSE.LGPL-2.0 beside this
notice.

This program's own source is Apache-2.0 and is at
https://github.com/ostrium-labs/loams/tree/<LOAMS_COMMIT>/<APP_PATH>. To
rebuild or relink it against a modified JavaScriptCore, build Bun from source
with your modified WebKit, then run:

  vite build   (with target "bun" in void.config.ts)
  bun build ./dist/ssr/index.js --compile --bytecode --minify --format=esm \
    --asset=./dist/client --target=<TARGET> --outfile <NAME> \
    --compile-executable-path=<your bun>

Bytecode in this program is tied to Bun <BUN_VERSION>. A different runtime
rebuilds it on first start.
```

Questions for counsel:
- whether `--compile-executable-path` relinking satisfies LGPL-2 §6 for a statically embedded JavaScriptCore;
- whether the WebKit source pointer must be an offer of physical source, or a URL is enough;
- whether the notice must be inside the binary (`--asset`) as well as beside it.

### Reproduce

Spike code: `spikes/vv0/task8/`, not committed. From an empty directory:

```sh
npm i --save-exact void@1.2.1 bun@1.4.4 && (cd node_modules/bun && node install.js)
mkdir scaffold && cd scaffold && echo '{"name":"notes-skel","private":true,"type":"module"}' > package.json
../node_modules/.bin/void init --framework react --starter static --deploy none --toolchain vite --no-git
npm i --save-exact @hono/node-server
# add routes/api/notes.ts, routes/api/health.ts, pages/index.server.ts; void.config.ts: target from VOID_TARGET
VOID_TARGET=node npx vite build && mv dist dist-node
VOID_TARGET=bun  npx vite build && mv dist dist-bun
cp -r dist-bun emb
sed -i 's#new URL("../../client/", import.meta.url)#new URL("./client/", import.meta.url)#' emb/ssr/assets/app-*.js
printf '\nprocess.on("SIGTERM", () => process.exit(0));\n' >> emb/ssr/index.js
(cd emb && ../../node_modules/.bin/bun build ./ssr/index.js --asset=./client --compile --bytecode \
  --minify --sourcemap --format=esm --target=bun-linux-x64 --outfile ../out/app-x64-embed)
# cold start: node harness/coldstart.mjs <label> 20 <port> / -- <command>
```

The images: `FROM gcr.io/distroless/cc-debian12:nonroot`, `COPY app /app` and `ENTRYPOINT ["/app"]`. The musl image uses `FROM alpine:3.22` with `apk add libstdc++ libgcc`.

**Cleanup:** the spike's images (`localhost/vv0-task8-*`) and its build directories were removed after the run. The base images `node:24-slim`, `distroless/cc-debian12:nonroot` and `alpine:3.22` were pulled for this task and are kept for Task 4.
