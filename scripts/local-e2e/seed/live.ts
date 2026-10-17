// Loams Live with the TypeScript SDK (@loams/client, sdks/typescript): two apps.
//
// - the server's default app: a `githubFeed` app with tables `repos` and
//   `events` (index `by_repo`, `by_type`), queries `events:recent`,
//   `events:byRepo`, `repos:top` and mutations `events:add`, `repos:bump`;
//   seeded with the first 400 events of the GH Archive hour (data/gharchive);
// - app `chinook-live` (CreateApp, addressed with the `loams-live-app`
//   header): a `playlists` table with a live query over it.
//
// Then a live query: `events:recent` is watched, an event is added, and the
// script checks that the watch delivers the new result.
//
// Idempotent: a deploy of the same bundle and schema is a new deployment with
// no changes; documents are added only when the table is empty.
//
//   node <tsx>/dist/cli.mjs scripts/local-e2e/seed/live.ts
// (scripts/local-e2e/seed-all.sh finds tsx in the workspace.)

import { readFileSync } from 'node:fs';
import { gunzipSync } from 'node:zlib';
import { Loams } from '../../../sdks/typescript/packages/client/src/index.ts';

const ENGINE = process.env.E2E_ENGINE ?? 'http://127.0.0.1:18080';
const DATA = process.env.E2E_DATA ?? '/mnt/Projects/loams-e2e/data';

type Js = null | boolean | number | bigint | string | Js[] | { [k: string]: Js };

// biome-ignore lint/suspicious/noExplicitAny: proto-es MessageInit for Value
function v(x: Js): any {
  if (x === null) return { kind: { case: 'nullValue', value: {} } };
  if (typeof x === 'boolean') return { kind: { case: 'boolValue', value: x } };
  if (typeof x === 'bigint') return { kind: { case: 'int64Value', value: x } };
  if (typeof x === 'number')
    return Number.isInteger(x)
      ? { kind: { case: 'int64Value', value: BigInt(x) } }
      : { kind: { case: 'doubleValue', value: x } };
  if (typeof x === 'string') return { kind: { case: 'stringValue', value: x } };
  if (Array.isArray(x)) return { kind: { case: 'arrayValue', value: { values: x.map(v) } } };
  return {
    kind: {
      case: 'objectValue',
      value: { fields: Object.fromEntries(Object.entries(x).map(([k, y]) => [k, v(y)])) },
    },
  };
}

// biome-ignore lint/suspicious/noExplicitAny: a Value back to JS
function js(x: any): unknown {
  const k = x?.kind;
  if (!k) return undefined;
  switch (k.case) {
    case 'nullValue':
      return null;
    case 'int64Value':
      return Number(k.value);
    case 'arrayValue':
      return k.value.values.map(js);
    case 'objectValue':
      return Object.fromEntries(Object.entries(k.value.fields).map(([a, b]) => [a, js(b)]));
    default:
      return k.value;
  }
}

const FEED = `
import { query, mutation, v } from "loams:server";

export const events = {
  recent: query({ args: { limit: v.int64() }, handler: async (ctx, { limit }) =>
    (await ctx.db.query("events").order("desc").take(Number(limit))) }),
  byRepo: query({ args: { repo: v.string() }, handler: async (ctx, { repo }) =>
    ctx.db.query("events").withIndex("by_repo", (q) => q.eq("repo", repo)).collect() }),
  byType: query({ args: { type: v.string() }, handler: async (ctx, { type }) =>
    ctx.db.query("events").withIndex("by_type", (q) => q.eq("type", type)).take(50) }),
  add: mutation({
    args: { type: v.string(), actor: v.string(), repo: v.string(), text: v.string(), at: v.string() },
    handler: async (ctx, e) => {
      const id = await ctx.db.insert("events", e);
      const repo = await ctx.db.query("repos").withIndex("by_name", (q) => q.eq("name", e.repo)).first();
      if (repo) await ctx.db.patch(repo._id, { events: repo.events + 1n });
      else await ctx.db.insert("repos", { name: e.repo, events: 1n });
      return id;
    } }),
  count: query(async (ctx) => BigInt((await ctx.db.query("events").collect()).length)),
};

export const repos = {
  top: query({ args: { limit: v.int64() }, handler: async (ctx, { limit }) => {
    const all = await ctx.db.query("repos").collect();
    all.sort((a, b) => (a.events < b.events ? 1 : a.events > b.events ? -1 : 0));
    return all.slice(0, Number(limit));
  } }),
};
`;

const PLAYLISTS = `
import { query, mutation, v } from "loams:server";
export const playlists = {
  list: query(async (ctx) => ctx.db.query("playlists").collect()),
  add: mutation({ args: { name: v.string(), tracks: v.int64() },
    handler: async (ctx, p) => ctx.db.insert("playlists", p) }),
};
`;

const enc = new TextEncoder();

async function main() {
  const loams = new Loams({ endpoint: ENGINE, transportOptions: { useBinaryFormat: false } });
  const info = await loams.instance.getInstance({});
  console.log('engine', info.serverVersion, 'live:', info.apiVersions.includes('loams.live.v1'));

  // 1. The default app.
  const dep = await loams.live.admin.deploy({
    bundle: enc.encode(FEED),
    message: 'local-e2e: githubFeed',
    schema: {
      tables: [
        { name: 'events', indexes: [{ name: 'by_repo', fields: ['repo'] }, { name: 'by_type', fields: ['type'] }] },
        { name: 'repos', indexes: [{ name: 'by_name', fields: ['name'] }] },
      ],
    },
  });
  console.log('deployed', dep.deployment?.id, 'changes', JSON.stringify(dep.changes ?? {}));

  const count = Number(js((await loams.tables.query({ function: 'events:count', args: v({}) })).result));
  if (count === 0) {
    const lines = gunzipSync(readFileSync(`${DATA}/gharchive/2024-01-15-15.json.gz`))
      .toString('utf8')
      .split('\n', 400)
      .filter(Boolean);
    let n = 0;
    for (const line of lines) {
      const e = JSON.parse(line);
      const p = e.payload ?? {};
      const text =
        (p.commits?.[0]?.message ?? p.pull_request?.title ?? p.issue?.title ?? p.description ?? '').slice(0, 200);
      await loams.tables.mutate({
        function: 'events:add',
        args: v({ type: e.type, actor: e.actor.login, repo: e.repo.name, text: String(text), at: e.created_at }),
      });
      n++;
    }
    console.log('added', n, 'events');
  } else console.log('events already there:', count);

  const top = js((await loams.tables.query({ function: 'repos:top', args: v({ limit: 5 }) })).result);
  console.log('repos:top', JSON.stringify(top));

  // 2. A live query: watch events:recent, add one, see the transition.
  const abort = new AbortController();
  const stream = loams.live.watch(
    { start: { case: 'initial', value: { version: 1n, queries: [{ queryId: 1, function: 'events:recent', args: v({ limit: 3 }) }] } } },
    { signal: abort.signal },
  );
  let transitions = 0;
  let sawNew = false;
  const marker = `local-e2e live check ${Date.now()}`;
  const watching = (async () => {
    for await (const t of stream) {
      transitions++;
      for (const u of t.updates ?? []) {
        if (u.update.case === 'value' && JSON.stringify(js(u.update.value)).includes(marker)) sawNew = true;
      }
      if (transitions === 1) {
        await loams.tables.mutate({
          function: 'events:add',
          args: v({ type: 'LiveCheck', actor: 'local-e2e', repo: 'ostrium-labs/loams', text: marker, at: new Date().toISOString() }),
        });
      }
      if (sawNew) break;
    }
  })();
  await Promise.race([watching, new Promise((r) => setTimeout(r, 30_000))]);
  abort.abort();
  console.log(`live query: ${transitions} transitions, saw the new event: ${sawNew}`);
  if (!sawNew) process.exitCode = 1;

  // 3. A second app, addressed by header.
  try {
    await loams.live.admin.createApp({ name: 'chinook-live', namespace: 'e2e' });
    console.log('created app chinook-live');
  } catch (e) {
    console.log('createApp chinook-live:', (e as Error).message);
  }
  const app = new Loams({
    endpoint: ENGINE,
    transportOptions: { useBinaryFormat: false, headers: { 'loams-live-app': 'chinook-live' } },
  });
  await app.live.admin.deploy({
    bundle: enc.encode(PLAYLISTS),
    message: 'local-e2e: playlists',
    schema: { tables: [{ name: 'playlists', indexes: [] }] },
  });
  const have = js((await app.tables.query({ function: 'playlists:list', args: v({}) })).result) as unknown[];
  if (!have?.length) {
    for (const [name, tracks] of [['Music', 3290], ['Movies', 0], ['TV Shows', 213], ['90s Music', 1477], ['Grunge', 15]] as const)
      await app.tables.mutate({ function: 'playlists:add', args: v({ name, tracks }) });
  }
  console.log('chinook-live playlists', JSON.stringify(js((await app.tables.query({ function: 'playlists:list', args: v({}) })).result)).slice(0, 200));
  const apps = await loams.live.admin.listApps({});
  console.log('apps', apps.apps.map((a) => a.name).join(', '));
}

main().catch((e) => {
  console.error('live seed failed:', e);
  process.exit(1);
});
