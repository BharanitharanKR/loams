// The e2e fixture itself (SD1 Task 29, moved here from Task 8 by ruling T8-3):
// a real loams-agentd starts in an isolated root, answers over both listeners
// with the Mock harness as its default, and leaves nothing behind when stopped.

import { existsSync } from 'node:fs';
import { expect, test } from '@playwright/test';
import { startAgentd } from './fixtures/agentd';

test('agentd_fixture_starts_and_stops', async () => {
  for (const mode of ['remote', 'local'] as const) {
    const agentd = await startAgentd({ mode, allowOrigin: 'http://127.0.0.1:5191' });
    try {
      expect(agentd.alive()).toBe(true);
      expect(agentd.home.startsWith(agentd.root)).toBe(true);
      const host = await agentd.call<{ host?: { features?: string[] } }>(
        '/loams.agentd.v1.StudioService/GetHost',
      );
      expect(host.host?.features).toContain('studio.v1');
      const harnesses = await agentd.call<{ harnesses?: { id?: string; installed?: boolean }[] }>(
        '/loams.agentd.v1.HarnessService/ListHarnesses',
      );
      expect(harnesses.harnesses?.some((h) => h.id === 'HARNESS_ID_MOCK' && h.installed)).toBe(
        true,
      );
      if (mode === 'remote') {
        // web_remote_requires_token's daemon half: no token, a foreign audience
        // and another subject are all refused.
        expect((await agentd.raw('/loams.agentd.v1.StudioService/GetHost', {}, null)).status).toBe(
          401,
        );
        const wrongAudience = agentd.signToken({ aud: 'someone-else' });
        expect(
          (await agentd.raw('/loams.agentd.v1.StudioService/GetHost', {}, wrongAudience)).status,
        ).toBe(401);
        const otherSubject = agentd.signToken({ sub: 'not-the-owner' });
        expect(
          (await agentd.raw('/loams.agentd.v1.StudioService/GetHost', {}, otherSubject)).status,
        ).toBe(403);
      }
    } finally {
      await agentd.stop();
    }
    expect(agentd.alive()).toBe(false);
    expect(existsSync(agentd.root)).toBe(false);
  }
});
