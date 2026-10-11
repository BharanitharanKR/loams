// The hosted console: the engine URL and health check, the "Connect your engine"
// screen, and the browser edition's catalog (real pages by default, sample pages
// only under "Demo data") booted over the in-memory mock.

import { composeCatalog, parseCatalog } from '@loams/console-host';
import { createMockTransport } from '@loams/console-host/testing';
import { createWebPlatform } from '@loams/platform-web';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import baseCatalog from '../catalog/base.yml?raw';
import { startConsole } from '../src/cordis/start.js';
import {
  HOSTED_PACKAGE,
  type HostedSession,
  hostedManifest,
  hostedPatch,
  hostedPlugin,
  webManifests,
  webModules,
  webPatches,
} from '../src/hosted/edition.js';
import { checkHealth, parseEngineUrl, savedEngine, saveEngine } from '../src/hosted/engine.js';
import { ConnectScreen } from '../src/hosted/screens.js';
import { redirectUri } from '../src/hosted/start.js';

let dispose: (() => Promise<void>) | undefined;
afterEach(async () => {
  cleanup();
  await dispose?.();
  dispose = undefined;
  document.body.innerHTML = '';
  window.location.hash = '';
});

describe('engine URL', () => {
  it('takes a bare tailnet name, an https URL or a loopback http URL', () => {
    expect(parseEngineUrl('loams.tail1.ts.net', 'https:')).toEqual({
      ok: true,
      origin: 'https://loams.tail1.ts.net',
    });
    expect(parseEngineUrl(' https://loams.tail1.ts.net/ui/ ', 'https:')).toEqual({
      ok: true,
      origin: 'https://loams.tail1.ts.net',
    });
    expect(parseEngineUrl('http://127.0.0.1:8080', 'https:')).toEqual({
      ok: true,
      origin: 'http://127.0.0.1:8080',
    });
  });

  it('refuses an empty value, a non-http scheme and mixed content', () => {
    expect(parseEngineUrl('', 'https:').ok).toBe(false);
    expect(parseEngineUrl('ftp://x', 'https:').ok).toBe(false);
    const mixed = parseEngineUrl('http://loams.lan:8080', 'https:');
    expect(mixed.ok).toBe(false);
    expect(mixed.ok ? '' : mixed.problem).toMatch(/tailscale serve/);
    // A console served over http (the Compose console on a LAN) may use one.
    expect(parseEngineUrl('http://loams.lan:8080', 'http:').ok).toBe(true);
  });

  it('remembers the engine in localStorage and forgets it', () => {
    saveEngine('https://loams.tail1.ts.net');
    expect(savedEngine()).toBe('https://loams.tail1.ts.net');
    saveEngine(undefined);
    expect(savedEngine()).toBeUndefined();
  });

  it('checks /health without credentials and tells the failures apart', async () => {
    const seen: [string, RequestInit | undefined][] = [];
    const ok = vi.fn(async (u: RequestInfo | URL, init?: RequestInit) => {
      seen.push([String(u), init]);
      return new Response('ok');
    });
    expect(await checkHealth('https://e.ts.net', ok as typeof fetch)).toMatchObject({
      ok: true,
      status: 200,
    });
    expect(seen[0]?.[0]).toBe('https://e.ts.net/health');
    expect(seen[0]?.[1]).toMatchObject({ mode: 'cors', credentials: 'omit' });

    const down = (async () => new Response('', { status: 503 })) as typeof fetch;
    expect(await checkHealth('https://e.ts.net', down)).toMatchObject({
      kind: 'http',
      status: 503,
    });

    const offline = (async () => {
      throw new TypeError('Failed to fetch');
    }) as typeof fetch;
    expect(await checkHealth('https://e.ts.net', offline)).toMatchObject({
      ok: false,
      kind: 'unreachable',
    });

    const stalled = ((_u: RequestInfo | URL, init?: RequestInit) =>
      new Promise((_r, reject) =>
        init?.signal?.addEventListener('abort', () => reject(new Error('aborted'))),
      )) as typeof fetch;
    expect(await checkHealth('https://e.ts.net', stalled, 10)).toMatchObject({ kind: 'timeout' });
  });

  it('the redirect URI is the bare origin, or the .html page it was opened on', () => {
    expect(redirectUri({ origin: 'https://console.loams.dev', pathname: '/projects/x' })).toBe(
      'https://console.loams.dev/',
    );
    expect(redirectUri({ origin: 'http://localhost:5173', pathname: '/ui/cordis.html' })).toBe(
      'http://localhost:5173/ui/cordis.html',
    );
  });
});

describe('Connect your engine', () => {
  it('tests the engine, then connects to its origin', async () => {
    const check = vi.fn(async () => ({ ok: true as const, status: 200, ms: 12 }));
    const onConnect = vi.fn();
    render(<ConnectScreen askApiKey check={check} onConnect={onConnect} />);
    expect(screen.getByText('Preview')).toBeTruthy();
    expect(screen.getByText(/needs Tailscale running/)).toBeTruthy();
    const connect = screen.getByRole('button', { name: 'Connect' }) as HTMLButtonElement;
    expect(connect.disabled).toBe(true);

    fireEvent.change(screen.getByLabelText('Engine URL'), {
      target: { value: 'loams.tail1.ts.net' },
    });
    fireEvent.change(screen.getByLabelText('API key (optional)'), { target: { value: 'lk_1' } });
    fireEvent.click(screen.getByRole('button', { name: 'Test connection' }));
    await screen.findByText('The engine answered');
    expect(check).toHaveBeenCalledWith('https://loams.tail1.ts.net');
    expect(connect.disabled).toBe(false);
    fireEvent.click(connect);
    expect(onConnect).toHaveBeenCalledWith({
      engine: 'https://loams.tail1.ts.net',
      apiKey: 'lk_1',
    });
  });

  it('explains an unreachable tailnet engine and still lets the person connect', async () => {
    const check = vi.fn(async () => ({
      ok: false as const,
      kind: 'unreachable' as const,
      message: 'Failed to fetch.',
    }));
    const onConnect = vi.fn();
    render(<ConnectScreen askApiKey={false} check={check} onConnect={onConnect} />);
    expect(screen.queryByLabelText('API key (optional)')).toBeNull();
    fireEvent.change(screen.getByLabelText('Engine URL'), {
      target: { value: 'https://loams.tail1.ts.net' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Test connection' }));
    await screen.findByText('This browser cannot reach the engine');
    expect(screen.getByText(/Tailscale is running on this device/)).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: 'Connect anyway' }));
    expect(onConnect).toHaveBeenCalledWith({
      engine: 'https://loams.tail1.ts.net',
      apiKey: undefined,
    });
  });
});

const navLabels = (root: HTMLElement) =>
  [...root.querySelectorAll('.lc-nav a')].map((a) => a.textContent);

async function boot(demo: boolean) {
  const root = document.createElement('div');
  document.body.append(root);
  const session: HostedSession = {
    engine: 'https://loams.tail1.ts.net',
    demo,
    setDemo: vi.fn(),
    changeEngine: vi.fn(),
    signOut: vi.fn(),
  };
  const handle = await startConsole({
    platform: createWebPlatform({ transport: createMockTransport({}) }),
    root,
    base: '/ui/',
    patches: [...webPatches(demo), hostedPatch],
    extraModules: { ...webModules, [HOSTED_PACKAGE]: async () => hostedPlugin(session) },
    extraManifests: [...webManifests, hostedManifest],
  });
  dispose = handle.dispose;
  return { root, handle, session };
}

describe('the browser edition', () => {
  it('composes over the base catalog with a module and manifest for every row', () => {
    for (const demo of [false, true]) {
      const rows = composeCatalog(parseCatalog(baseCatalog), ...webPatches(demo), hostedPatch);
      for (const row of rows.filter((r) => !r.disabled)) {
        if (row.name === HOSTED_PACKAGE) continue;
        if (row.name in webModules) {
          expect(webManifests.map((m) => (m as { name: string }).name)).toContain(row.name);
        }
      }
    }
  });

  it('loads only pages the engine serves, with the engine, Demo data and sign-out in the header', async () => {
    const { root, handle, session } = await boot(false);
    await waitFor(() => expect(navLabels(root)).toContain('Live'));
    const labels = navLabels(root);
    expect(labels).toEqual(expect.arrayContaining(['Live', 'Graph', 'Analytics', 'Status']));
    for (const sample of ['Deployments', 'SQL', 'Metrics', 'Pipelines', 'Git']) {
      expect(labels).not.toContain(sample);
    }
    const status = Object.fromEntries(handle.plugins().map((p) => [p.id, p.status]));
    expect(status).toMatchObject({ live: 'active', graph: 'active', house: 'active' });

    expect(root.querySelector('.lc-side .lc-preview')?.textContent).toBe('Preview');
    await waitFor(() => expect(root.textContent).toContain('loams.tail1.ts.net'));
    const toggle = root.querySelector('.lc-hosted-demo input') as HTMLInputElement;
    expect(toggle.checked).toBe(false);
    fireEvent.click(toggle);
    expect(session.setDemo).toHaveBeenCalledWith(true);
    const signOut = [...root.querySelectorAll('button')].find((b) => b.textContent === 'Sign out');
    expect(signOut).toBeTruthy();
    if (signOut) fireEvent.click(signOut);
    expect(session.signOut).toHaveBeenCalled();
  });

  it('adds the sample-data pages under Demo data', async () => {
    const { root } = await boot(true);
    await waitFor(() => expect(navLabels(root)).toContain('SQL'));
    expect(navLabels(root)).toEqual(
      expect.arrayContaining(['Live', 'Deployments', 'SQL', 'Metrics', 'Pipelines', 'Git']),
    );
    expect(root.textContent).toContain('Sample pages on');
  });
});
