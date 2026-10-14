// Settings › Agentd hosts and the web edition's remote agentd (SD1 Task 7): the
// registry, `?host=` by id only, pasted development tokens, and the projects
// client over the selected host. No network: the client is a fake.

import { createHostRegistry, HOSTS_KEY, localHostStore } from '@loams/agentd-client/hosts';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { AgentdProjectsClient } from '../src/agentd.js';
import { hostedAgentd } from '../src/hosts/agentd.js';
import { StudioView } from '../src/view.js';

const URL_A = 'https://devbox.example.ts.net:8443';
const URL_B = 'https://buildbox.example.ts.net:8443';

function wrap(children: ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

/** A fake projects client per host, recording the token each call saw. */
function fakeClients() {
  const tokens: (string | undefined)[] = [];
  const clientFor = vi.fn(
    (host: { id: string; name: string; url: string }, getToken: () => unknown) =>
      ({
        host: { id: host.id, name: host.name },
        listRepos: async () => {
          tokens.push((await getToken()) as string | undefined);
          return [{ path: `/srv/${host.name}`, name: `repo-of-${host.name}`, defaultBranch: null }];
        },
        cloneRepo: async () => {
          throw new Error('not in this test');
        },
      }) satisfies AgentdProjectsClient,
  );
  return { tokens, clientFor };
}

beforeEach(() => {
  localStorage.clear();
  window.matchMedia = ((query: string) => ({
    matches: query.includes('min-width'),
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  })) as unknown as typeof window.matchMedia;
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
});
afterEach(cleanup);

describe('the hosted agentd', () => {
  it('needs a host and a token, then lists the host projects', async () => {
    const { tokens, clientFor } = fakeClients();
    const agentd = hostedAgentd({ clientFor });
    expect(agentd.reach()).toEqual({ state: 'no-host' });
    const host = agentd.hosts.registry.add({ name: 'devbox', url: URL_A });
    expect(agentd.hosts.selected()?.id).toBe(host.id);
    // A host but no token yet: still not connected.
    expect(agentd.reach()).toEqual({ state: 'no-host' });
    await expect(agentd.listProjects()).rejects.toThrow(/No agentd host/);
    agentd.hosts.setPastedToken(host.id, '  pasted-token  ');
    expect(agentd.reach()).toEqual({ state: 'ready' });
    expect((await agentd.listProjects()).map((p) => p.name)).toEqual(['repo-of-devbox']);
    expect(tokens).toEqual(['pasted-token']);
    expect(agentd.cacheKey()).toBe(`host:${host.id}`);
  });

  it('pasted tokens are never stored', () => {
    const agentd = hostedAgentd({ clientFor: fakeClients().clientFor });
    const host = agentd.hosts.registry.add({ name: 'devbox', url: URL_A });
    agentd.hosts.setPastedToken(host.id, 'secret-token-value');
    expect(JSON.stringify({ ...localStorage })).not.toContain('secret-token-value');
    expect(localStorage.getItem(HOSTS_KEY)).toContain(URL_A);
  });

  it('uses the sign-in token when one is provided', async () => {
    const { tokens, clientFor } = fakeClients();
    const agentd = hostedAgentd({ clientFor, getToken: () => () => 'oidc-access-token' });
    expect(agentd.hosts.pastedTokens).toBe(false);
    agentd.hosts.registry.add({ name: 'devbox', url: URL_A });
    expect(agentd.reach()).toEqual({ state: 'ready' });
    await agentd.listProjects();
    expect(tokens).toEqual(['oidc-access-token']);
  });

  it('host_param_selects_by_id_only', async () => {
    const { clientFor } = fakeClients();
    const registry = createHostRegistry(localHostStore());
    const a = registry.add({ name: 'devbox', url: URL_A });
    const b = registry.add({ name: 'buildbox', url: URL_B });
    const agentd = hostedAgentd({ registry, clientFor, getToken: () => () => 't' });
    expect(agentd.hosts.selected()?.id).toBe(a.id);

    render(
      wrap(
        <StudioView agentd={agentd} route={{ kind: 'home' }} navigate={vi.fn()} hostParam={b.id} />,
      ),
    );
    await waitFor(() => expect(agentd.hosts.selected()?.id).toBe(b.id));
    const sidebar = screen.getByRole('navigation', { name: 'Projects' });
    expect(await within(sidebar).findByRole('link', { name: /repo-of-buildbox/ })).toBeTruthy();

    // A URL, or an unknown id, selects nothing.
    for (const param of ['https://evil.example', URL_A, 'h-unknown']) {
      act(() => agentd.hosts.select(param));
      expect(agentd.hosts.selected()?.id).toBe(b.id);
    }
  });
});

describe('Settings › Agentd hosts', () => {
  function showSettings(agentd = hostedAgentd({ clientFor: fakeClients().clientFor })) {
    render(wrap(<StudioView agentd={agentd} route={{ kind: 'settings' }} navigate={vi.fn()} />));
    return agentd;
  }

  it('adds a host, takes a pasted token under a warning, and removes it', async () => {
    const agentd = showSettings();
    expect(await screen.findByText('No agentd hosts')).toBeTruthy();
    expect(screen.getByText('Development sign-in')).toBeTruthy();

    const form = screen.getByRole('form', { name: 'Add an agentd host' });
    fireEvent.change(within(form).getByLabelText('Name'), { target: { value: 'devbox' } });
    fireEvent.change(within(form).getByLabelText('URL'), {
      target: { value: 'http://devbox.example.com' },
    });
    fireEvent.click(within(form).getByRole('button', { name: 'Add host' }));
    expect(await within(form).findByText(/https:\/\/ URL/)).toBeTruthy();
    expect(agentd.hosts.registry.list()).toEqual([]);

    fireEvent.change(within(form).getByLabelText('URL'), { target: { value: `${URL_A}/` } });
    fireEvent.click(within(form).getByRole('button', { name: 'Add host' }));
    const row = await screen.findByRole('listitem', { name: 'devbox' });
    expect(within(row).getByText(URL_A)).toBeTruthy();
    expect(within(row).getByText('in use')).toBeTruthy();
    expect(within(row).getByText('no token')).toBeTruthy();

    fireEvent.change(within(row).getByLabelText('Access token for devbox'), {
      target: { value: 'pasted' },
    });
    fireEvent.click(within(row).getByRole('button', { name: 'Set token' }));
    expect(await within(row).findByText('signed in')).toBeTruthy();
    expect(agentd.reach()).toEqual({ state: 'ready' });

    fireEvent.click(within(row).getByRole('button', { name: 'Remove' }));
    expect(await screen.findByText('No agentd hosts')).toBeTruthy();
    expect(agentd.reach()).toEqual({ state: 'no-host' });
  });

  it('works when storage is blocked', async () => {
    const getItem = vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('SecurityError');
    });
    const setItem = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('QuotaExceededError');
    });
    try {
      const agentd = showSettings();
      const form = await screen.findByRole('form', { name: 'Add an agentd host' });
      fireEvent.change(within(form).getByLabelText('URL'), { target: { value: URL_A } });
      fireEvent.click(within(form).getByRole('button', { name: 'Add host' }));
      expect(await screen.findByRole('listitem', { name: /devbox/ })).toBeTruthy();
      expect(agentd.hosts.registry.list()).toHaveLength(1);
    } finally {
      getItem.mockRestore();
      setItem.mockRestore();
    }
  });
});
