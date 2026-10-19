// SDKP Part B for TypeScript: the app scenario through the public facade.
//
// `typescript_app_collections`, `typescript_app_search` and
// `typescript_app_graph` replay `sdks/fixtures/recorded/app/` against the
// fixture server (`helpers/app-replay.ts` says how), so each passes only when
// the facade sends exactly the recorded requests and reads the recorded answers
// the way they mean. A live `loams dev` cannot replay a recording (its tokens
// and ids are its own), and replaying against one would create the `app`
// namespace a `--drift` re-record needs to be fresh, so the replay runs against
// the fixture server only.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { replayAppFixture } from './helpers/app-replay.js';
import { startFixtureServer, type FixtureServer } from './helpers/server.js';

let server: FixtureServer;

beforeAll(async () => {
  server = await startFixtureServer({ forceFixtures: true });
});

afterAll(async () => {
  await server?.stop();
});

describe('typescript_app_collections', () => {
  it('namespaces, collections, documents, tokens, paging and the two refusals', async () => {
    await expect(replayAppFixture(server.endpoint, 'app_collections')).resolves.toBeUndefined();
  });
});

describe('typescript_app_search', () => {
  it('text, vector and hybrid search, the next page, and the refused weighted sum', async () => {
    await expect(replayAppFixture(server.endpoint, 'app_search')).resolves.toBeUndefined();
  });
});

describe('typescript_app_graph', () => {
  it('a build without the graph feature refuses every graph call, typed', async () => {
    await expect(replayAppFixture(server.endpoint, 'app_graph')).resolves.toBeUndefined();
  });
});
