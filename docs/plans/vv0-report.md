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
| 3 | Loams' shims around Void's `node` build | **Go locally** for HTTP, cron and queue through one Lambda handler (unit tests and the Lambda emulator). `node` exports only the Hono app, so crons and queues are called through the build manifest. Real Lambda cold starts are blocked on the owner's AWS account |
| 4 | Bun-compiled and `node` images in the light compose stack | pending |
| 5 | Hyperdrive to Loams Postgres over Tunnel, and TiDB Starter | pending |
| 6 | One `vite-plus` pin for the workspace | pending |
| 7 | The Loams Cloud control plane on Void | pending |
| 8 | Bun's ahead-of-time compile against Node | **Go** on cold start (5.5x faster than Node 24 for the process, 1.5x for the container); the image is 138 MB on distroless glibc and 118 MB on Alpine musl, so the under-120 MB target is met only on musl. Embedding `dist/client/` needs a one-line rewrite of Void's client root |

## Task 1

Pending.

## Task 2

Pending.

## Task 3

**Verdict: go locally for HTTP, cron and queue; the Lambda part is blocked** (it needs the owner's test account and tokens, §70 §12.2). Run on 2026-10-24 with `void` 1.2.1, `vite` 8.3.4, `@hono/node-server` 1.x and Node 24.21.0 (the `node` npm package). Nothing was deployed.

**The app.** A portable Void app with `target: "node"`, one route (`routes/api/hello.ts`, `GET` and `POST`), one cron (`crons/tick.ts`, `cron = '*/5 * * * *'`) and one queue (`queues/jobs.ts`; it acks `n >= 0` and retries `n < 0`). `vite build` takes 52 ms and writes `dist/ssr/app.js` (88.7 kB, 25.6 kB gzip), `dist/ssr/index.js` and one chunk per route, cron and queue.

**The export shapes (§79 §8's verify item).**
- `dist/ssr/app.js` has one public export: the Hono app as `default`, with `fetch(request, env, ctx)`. The other exports (`defineHandler`, `defineQueue`, `defineScheduled`) are single-letter minified aliases. They are internal.
- **No `scheduled` or `queue` handler is exported** on the `node` target. Void compiles both into `app.js` as internal functions, and reaches them only through two HTTP routes on the same app: `POST /__void/scheduled` (`{cron, scheduledTime}`) and `POST /__void/queue` (`{queue, messages: [{id, timestamp, body, attempts}]}`, which returns `{ok, decisions: {<id>: {action: "ack" | "retry", delaySeconds?}}}`).
- **Each cron chunk** exports `cron` (a string or an array) and the handler as `default`, called as `default(controller, env, ctx)` with `controller = {cron, scheduledTime, noRetry()}`. **Each queue chunk** exports the handler as `default`, called as `default(batch, env)`. The chunk file names are hashed. `dist/ssr/.vite/manifest.json` maps each source path (`crons/tick.ts`, `queues/jobs.ts`) to its chunk.
- **The trigger routes' guard on `node`.** They accept the header `x-void-internal` only when it equals `__VOID_PROXY_TOKEN` from the raw environment. The `node` build never fills that store (`AsyncLocalStorage` with no `run`), so the token falls back to `__VOID_BUILD_KEY`. That is a random UUID, new on every build, and stored as plain text in `app.js`. `CRON_SECRET` is not read on this target.
- **`POST /__void/scheduled` is broken on `node`.** Under `@hono/node-server` it reads `c.executionCtx`, which throws "This context has no ExecutionContext", so it answers 500. `POST /__void/queue` works with the build key.
- **`dist/ssr` is not self-contained.** `app.js` imports `@hono/node-server/serve-static` and leaves it external, and that package's peer `hono` has to sit beside it too. The deploy artefact therefore carries `node_modules/@hono/node-server` and `node_modules/hono` (3.4 MB in all, the build included).

**The shim (spike, `spikes/vv0/task3/shim/lambda.mjs`, 140 lines).** One `handler(event, context)` takes three event shapes:
- **A Function URL event** (payload 2.0). It becomes a `Request`: path, query, headers, cookies, and a base64 or text body. `app.fetch(request, process.env, ctx)` answers it, and the `Response` maps back to `{statusCode, headers, cookies, body, isBase64Encoded}`. The shim answers `/__void/*` itself with 404, because the `node` build mounts the trigger routes on the public app.
- **An EventBridge Scheduler invocation** whose custom input is `{"loams": "schedule", "cron": "<expr>", "time": "<ISO>"}`. The shim imports every `crons/` chunk listed in the manifest, runs those whose `cron` export includes the expression, and throws an `AggregateError` if any fails. That is the same rule as Void's Workers `scheduled`. So the §71 `schedule` port provisions one Scheduler schedule per expression, with that input.
- **An SQS batch.** The queue name is the last part of `eventSourceARN`, matched to a `queues/` chunk by exact name, or else by the longest `-<name>` suffix that is unambiguous (Void's own rule, so a prefixed physical queue still resolves). The shim builds Void's batch (`messages[].ack()` and `retry()`, `ackAll()`, `retryAll()`) and returns `{batchItemFailures}`. It lists each message that was retried, or that had no decision when the handler threw. Records for an unknown queue all fail. **One loss:** `retry({delaySeconds})` becomes a plain redelivery after the visibility timeout; honouring the delay would need `ChangeMessageVisibility`.
- **The shim calls the chunks directly, not the trigger routes.** On `node` the routes are broken (crons) or guarded by a per-build key read out of the bundle (queues), and they are public.

**Tests.**
- `node --test shim/lambda.test.mjs` on Node 24.21.0: **7 passed.** They cover GET; a POST with a base64 body; the app's 404; the blocked `/__void/scheduled`; a cron that runs by its expression (and an unmatched one, which runs nothing); an SQS batch of `[ack, retry, ack]`, which reports `batchItemFailures: [b]`; and a prefixed queue name, plus an unknown one.
- **The AWS Lambda Runtime Interface Emulator** (the public image `public.ecr.aws/lambda/nodejs:24`, 439 MB, pulled without an account). `/var/task` mounts `app/dist`, the shim and the two packages, with the handler `shim/lambda.handler`. The HTTP event answers 200 with the JSON body, the Scheduler input answers `{"ran":1}` and logs the tick, and the SQS batch answers `{"batchItemFailures":[{"itemIdentifier":"b"}]}`.

**Cold start, local only (not Lambda; load average 7 to 14 from other agents' builds).**

| Measure | Median | Runs |
|---|---|---|
| A fresh Node 24 process that imports the shim and answers one Function URL event | 72.8 ms (an empty `node -e ''` takes 24.7 ms) | 20 |
| RIE: `INIT REPORT` for a fresh container | 68 ms | 7 |
| RIE: first invoke, from the client | 122 ms | 7 |
| RIE: the first container ever, with a cold disk cache | 1.6 s init | 1 |
| RIE: a warm invoke | 2 to 4 ms | — |

Real Lambda cold starts (Init Duration behind a Function URL) are **blocked**, and so are the EventBridge Scheduler and SQS event-source mappings: they need the owner's AWS credit account and a scoped token. Nothing here changes Q1311's answer (Node on Lambda) yet.

**Upstream notes** (not filed; no upstream posts without consent):
- `node`'s `/__void/scheduled` crashes on the missing `executionCtx`.
- The trigger key falls back to a per-build constant kept in the bundle.
- An exported `scheduled` and `queue` on the `node` target would let shims skip the manifest.

**Reproduce.**

```sh
cd spikes/vv0/task3
npm install -D void@1.2.1 vite@8.3.4 node@24.21.0 @hono/node-server@1
npm install-scripts approve node workerd esbuild better-sqlite3 && npm rebuild
(cd app && PATH=$PWD/../node_modules/.bin:$PATH node ../node_modules/vite/bin/vite.js build)
node_modules/.bin/node --test shim/lambda.test.mjs
node_modules/.bin/node cold.mjs
podman run --rm -p 39123:8080 -v $PWD/task:/var/task:ro,Z public.ecr.aws/lambda/nodejs:24 shim/lambda.handler
curl -XPOST localhost:39123/2015-03-31/functions/function/invocations -d '<event>'
```

npm 11 blocks install scripts by default, which is why the approve step is there. Without it the `node` package has no binary and `workerd` isn't fetched.

## Task 4

Pending.

## Task 5

Pending.

## Task 6

Pending.

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
