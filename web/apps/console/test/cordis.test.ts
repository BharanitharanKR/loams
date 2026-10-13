// The cordis console end to end in jsdom: the real catalog (base.yml), the
// real plugins, the browser platform over the in-memory mock.

import { parsePatch, THIRD_PARTY_FLAG } from '@loams/console-host';
import { createMockTransport } from '@loams/console-host/testing';
import { createWebPlatform } from '@loams/platform-web';
import { waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import samplesYml from '../catalog/samples.yml?raw';
import { startConsole } from '../src/cordis/start.js';

let dispose: (() => Promise<void>) | undefined;
afterEach(async () => {
  await dispose?.();
  dispose = undefined;
  document.body.innerHTML = '';
  window.location.hash = '';
});

async function start(
  features: Record<string, boolean> = {},
  grantDeclared = false,
  samples = false,
) {
  const root = document.createElement('div');
  document.body.append(root);
  const handle = await startConsole({
    platform: createWebPlatform({ transport: createMockTransport({ features }) }),
    root,
    base: '/ui/',
    patches: samples ? [parsePatch(samplesYml)] : undefined,
    grant: grantDeclared ? (manifest) => manifest.permissions : undefined,
  });
  dispose = handle.dispose;
  return { root, handle };
}

const navLabels = (root: HTMLElement) =>
  [...root.querySelectorAll('.lc-nav a')].map((a) => a.textContent);

describe('the cordis console', () => {
  it('boots the oss catalog and renders the first-party pages', async () => {
    const { root, handle } = await start();
    await waitFor(() =>
      expect(navLabels(root)).toEqual(['Status', 'Namespaces', 'Deployments', 'Approvals']),
    );
    const status = Object.fromEntries(handle.plugins().map((p) => [p.id, p.status]));
    expect(status).toMatchObject({
      shell: 'active',
      rpc: 'active',
      identity: 'active',
      'stack-status': 'active',
      namespaces: 'active',
      approvals: 'active',
      deployments: 'active',
    });
    // The sample third-party plugin is demo-only (catalog/samples.yml).
    expect(handle.plugins().find((p) => p.id === 'hello')).toBeUndefined();
    await waitFor(() => expect(root.textContent).toContain('Demo instance'));

    window.location.hash = '#/approvals';
    window.dispatchEvent(new HashChangeEvent('hashchange'));
    await waitFor(() =>
      expect(root.textContent).toContain('Drop the collection docs in production'),
    );
    expect(root.querySelector('.lc-chip')?.textContent).toBe('2 approvals waiting');

    window.location.hash = '#/namespaces';
    window.dispatchEvent(new HashChangeEvent('hashchange'));
    await waitFor(() => expect(root.textContent).toContain('search-prod'));
  }, 20_000); // the whole oss catalog boots here; a cold macOS runner needs over 5 s

  it('disabling a plugin removes its page and nav entry without a reload', async () => {
    const { root, handle } = await start();
    await waitFor(() => expect(navLabels(root)).toContain('Approvals'));
    await handle.disable('approvals');
    await waitFor(() => expect(navLabels(root)).toEqual(['Status', 'Namespaces', 'Deployments']));
    expect(root.querySelector('.lc-chip')).toBeNull();
  });

  it('runs the third-party sample in a sandboxed frame when the instance allows it', async () => {
    const { root, handle } = await start({ [THIRD_PARTY_FLAG]: true }, true, true);
    expect(handle.plugins().find((p) => p.id === 'hello')?.tier).toBe('third-party');
    await waitFor(() => expect(navLabels(root)).toContain('hello'));
    window.location.hash = '#/plugins/hello';
    window.dispatchEvent(new HashChangeEvent('hashchange'));
    const frame = await waitFor(() => {
      const f = root.querySelector('iframe');
      expect(f).toBeTruthy();
      return f as HTMLIFrameElement;
    });
    expect(frame.getAttribute('sandbox')).toBe('allow-scripts');
    expect(frame.getAttribute('src')).toBe('/ui/sandbox/frame.html#plugin=hello');
    expect(root.textContent).toContain('Unverified plugin: @loams/example-plugin-hello');
    expect(handle.plugins().find((p) => p.id === 'hello')?.granted).toEqual(['approvals:read']);
  });
});
