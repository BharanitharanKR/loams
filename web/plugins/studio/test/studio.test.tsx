// @loams/plugin-studio's skeleton (SD1 Task 15, §53 §5.1–§5.2): the manifest, the
// empty states without agentd, adding a project on each edition, and the layout.

import { validateManifest } from '@loams/console-host';
import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import pkg from '../package.json';
import type { StudioAgentd } from '../src/agentd.js';
import { LAYOUT_DEFAULTS, LAYOUT_KEY, StudioLayout } from '../src/layout/StudioLayout.js';
import { StudioView } from '../src/view.js';
import { desktopFake, webFake, wrap } from './fakes.js';

/** jsdom has no matchMedia: answer every query with `wide`. */
function viewport(wide: boolean) {
  window.matchMedia = ((query: string) => ({
    matches: query.includes('min-width') ? wide : !wide,
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  })) as unknown as typeof window.matchMedia;
}

function show(
  agentd: StudioAgentd,
  route: Parameters<typeof StudioView>[0]['route'] = { kind: 'home' },
) {
  const navigate = vi.fn();
  render(wrap(<StudioView agentd={agentd} route={route} navigate={navigate} />));
  return { navigate };
}

beforeEach(() => {
  localStorage.clear();
  viewport(true);
  // Empty's grain canvas observes its size; jsdom has no ResizeObserver.
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
});
afterEach(cleanup);

describe('the plugin', () => {
  it('studio_plugin_manifest_valid', () => {
    const m = validateManifest(pkg);
    expect(m.modes).toEqual(['studio']);
    expect(m.editions).toEqual(['oss', 'desktop', 'cloud']);
    expect(m.tier).toBe('first-party');
    expect(m.permissions).toEqual(['agentd:use', 'agentd:git-write']);
    expect(m.inject).toEqual(
      expect.arrayContaining(['router', 'slots', 'mode', 'platform', 'flags', 'agentd', 'desktop']),
    );
  });
});

describe('empty states', () => {
  it('empty_state_without_agentd_desktop', async () => {
    const { agentd, mock } = desktopFake({ running: false });
    show(agentd);
    expect(await screen.findByText('Start the daemon')).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: 'Start' }));
    await waitFor(() => expect(mock.calls).toContain('start'));
    // Started: the projects show.
    const sidebar = screen.getByRole('navigation', { name: 'Projects' });
    expect(await within(sidebar).findByRole('link', { name: /^loams/ })).toBeTruthy();
  });

  it('empty_state_without_host_web', async () => {
    show(webFake().agentd);
    expect(await screen.findByText('Studio needs a Loams server with agentd')).toBeTruthy();
    const link = screen.getByRole('link', { name: 'Agentd hosts' });
    expect(link.getAttribute('href')).toBe('#/studio/settings');
  });

  it('no projects, then no threads', async () => {
    show(desktopFake({ repos: [], folder: null }).agentd);
    expect(await screen.findByText('No projects yet')).toBeTruthy();
    cleanup();
    const withOne = desktopFake();
    show(withOne.agentd, { kind: 'project', projectId: '~/src/loams' });
    expect(await screen.findByText('No threads yet')).toBeTruthy();
  });

  it('an unknown project says so', async () => {
    show(desktopFake().agentd, { kind: 'project', projectId: '/nowhere' });
    expect(await screen.findByText('No such project')).toBeTruthy();
  });
});

describe('adding a project', () => {
  it('add_project_desktop_uses_folder_picker', async () => {
    const { agentd, daemon, pickFolder } = desktopFake();
    show(agentd);
    const sidebar = await screen.findByRole('navigation', { name: 'Projects' });
    await within(sidebar).findByRole('link', { name: /loams/ });
    fireEvent.click(screen.getByRole('button', { name: 'Add project' }));
    await waitFor(() => expect(pickFolder).toHaveBeenCalledOnce());
    // ProjectService.AddRepo over the agentd transport, not the desktop bridge.
    await waitFor(() => expect(daemon.calls).toContain('AddRepo:/home/omar/src/site'));
    expect(await within(sidebar).findByRole('link', { name: /^site/ })).toBeTruthy();
    // No URL dialog on the desktop.
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('a cancelled folder picker adds nothing', async () => {
    const { agentd, daemon, pickFolder } = desktopFake({ folder: null });
    show(agentd);
    await screen.findByRole('navigation', { name: 'Projects' });
    fireEvent.click(await screen.findByRole('button', { name: 'Add project' }));
    await waitFor(() => expect(pickFolder).toHaveBeenCalled());
    expect(daemon.calls.filter((c) => c.startsWith('AddRepo'))).toEqual([]);
  });

  it('add_project_web_clones', async () => {
    const { agentd, daemon } = webFake({
      host: { id: 'h1', name: 'build box' },
      repos: [{ path: '/srv/src/loams', name: 'loams', defaultBranch: 'dev' }],
    });
    const cloned = () => daemon.calls.filter((c) => c.startsWith('CloneRepo:'));
    show(agentd);
    fireEvent.click(await screen.findByRole('button', { name: 'Add project' }));
    const dialog = await screen.findByRole('dialog');
    const input = within(dialog).getByLabelText('Repository URL');
    // Only remote git URLs are offered to the daemon.
    fireEvent.change(input, { target: { value: 'file:///etc' } });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Clone' }));
    expect(await within(dialog).findByText(/https, ssh or user@host:path/)).toBeTruthy();
    expect(cloned()).toEqual([]);
    fireEvent.change(input, { target: { value: 'https://git.example.com/acme/api.git' } });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Clone' }));
    await waitFor(() =>
      expect(cloned()).toEqual(['CloneRepo:https://git.example.com/acme/api.git']),
    );
    const sidebar = screen.getByRole('navigation', { name: 'Projects' });
    expect(await within(sidebar).findByRole('link', { name: /api/ })).toBeTruthy();
  });
});

describe('the layout', () => {
  const regions = (panel = true) => (
    <StudioLayout
      sidebar={<p>projects</p>}
      panel={panel ? <p>panel body</p> : undefined}
      drawer={<p>terminal</p>}
      title="Thread"
    >
      <p>main body</p>
    </StudioLayout>
  );

  it('layout_sizes_persist_and_tolerate_storage_failure', () => {
    render(regions());
    const handle = screen.getByRole('separator', { name: 'Resize sidebar' });
    expect(handle.getAttribute('aria-valuenow')).toBe(String(LAYOUT_DEFAULTS.sidebar));
    fireEvent.keyDown(handle, { key: 'ArrowRight' });
    const stored = JSON.parse(localStorage.getItem(LAYOUT_KEY) ?? '{}');
    expect(stored.sidebar).toBeGreaterThan(LAYOUT_DEFAULTS.sidebar);
    cleanup();
    // A fresh mount reads the stored size.
    render(regions());
    expect(
      screen.getByRole('separator', { name: 'Resize sidebar' }).getAttribute('aria-valuenow'),
    ).toBe(String(stored.sidebar));
    cleanup();

    // Storage that throws: defaults, and resizing still works for the page.
    const get = vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('SecurityError');
    });
    const set = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('SecurityError');
    });
    render(regions());
    const h = screen.getByRole('separator', { name: 'Resize panel' });
    expect(h.getAttribute('aria-valuenow')).toBe(String(LAYOUT_DEFAULTS.panel));
    fireEvent.keyDown(h, { key: 'ArrowLeft' });
    expect(Number(h.getAttribute('aria-valuenow'))).toBeGreaterThan(LAYOUT_DEFAULTS.panel);
    get.mockRestore();
    set.mockRestore();
  });

  it('opens and closes the panel and the drawer', () => {
    render(regions());
    expect(screen.getByText('panel body')).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: 'Hide panel' }));
    expect(screen.queryByText('panel body')).toBeNull();
    expect(screen.queryByText('terminal')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Show terminal' }));
    expect(screen.getByText('terminal')).toBeTruthy();
    expect(JSON.parse(localStorage.getItem(LAYOUT_KEY) ?? '{}')).toMatchObject({
      panelOpen: false,
      drawerOpen: true,
    });
  });

  it('layout_narrow_uses_overlays', () => {
    viewport(false);
    render(regions());
    // The sidebar is a drawer behind a button, and the panel an overlay.
    expect(screen.queryByText('projects')).toBeNull();
    expect(screen.queryByRole('separator', { name: 'Resize sidebar' })).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Show projects' }));
    const drawer = screen.getByRole('dialog', { name: 'Projects' });
    expect(within(drawer).getByText('projects')).toBeTruthy();
    // The scrim and the close button both close it.
    fireEvent.click(screen.getAllByRole('button', { name: 'Close projects' })[1] as HTMLElement);
    expect(screen.queryByRole('dialog', { name: 'Projects' })).toBeNull();
    // The panel starts closed on a narrow window and opens over the thread.
    expect(screen.queryByText('panel body')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Show panel' }));
    expect(screen.getByRole('dialog', { name: 'Panel' })).toBeTruthy();
    act(() => {
      fireEvent.keyDown(screen.getByRole('dialog', { name: 'Panel' }), { key: 'Escape' });
    });
    expect(screen.queryByRole('dialog', { name: 'Panel' })).toBeNull();
  });
});
