import {
  type FlagsService,
  MIN_ENGINE,
  type SessionService,
  UPGRADE_COMMAND,
} from '@loams/console-host';
import { SlotProvider, SlotRegistry } from '@loams/slots';
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { useEffect } from 'react';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { DOCK_DEFAULT, DOCK_KEY, DOCK_MAX, DOCK_MIN, Layout, THEME_KEY } from '../src/layout.js';
import { HashRouter, type HashSource } from '../src/router.js';

function memory(start = '/'): HashSource {
  let path = start;
  return {
    get: () => path,
    set: (p) => {
      path = p;
    },
    listen: () => () => {},
  };
}

const who = { displayName: 'Omar Haddad' };
const session = {
  principal: () => who,
  subscribe: () => () => {},
} as unknown as SessionService;

function setup(start = '/', flags?: FlagsService, s: SessionService = session) {
  const slots = new SlotRegistry();
  const router = new HashRouter(slots, memory(start));
  const ui = render(
    <SlotProvider registry={slots}>
      <Layout router={router} session={s} flags={flags} />
    </SlotProvider>,
  );
  return { slots, router, ui };
}

const flags = {
  edition: 'oss',
  instanceName: 'Acme Loams',
  serverVersion: '0.1.0-preview.3',
  features: {},
  apiVersions: [],
  has: () => false,
} as FlagsService;

const section = (
  slots: SlotRegistry,
  label: string,
  group: string,
  order: number,
  href = `/${label.toLowerCase()}`,
) =>
  slots.register(
    { name: 'shell.nav.section', plugin: 'test', order, meta: { id: label, label, group, href } },
    () => null,
  );

beforeEach(() => {
  localStorage.clear();
  // The Logo's grain canvas observes its size; jsdom has no ResizeObserver.
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
});
afterEach(cleanup);

describe('shell chrome', () => {
  it('shows the Loams wordmark, the edition, a readable preview badge and the version', () => {
    setup('/', flags);
    expect(screen.getByRole('img', { name: 'Loams' })).toBeTruthy();
    expect(screen.getByText('OSS')).toBeTruthy();
    expect(screen.getByText('Preview').className).toBe('lc-preview');
    expect(screen.getByText('Loams 0.1.0-preview.3')).toBeTruthy();
  });

  it('breadcrumbs name the instance, the section and the page', () => {
    const { slots, router } = setup('/live', flags);
    act(() => {
      section(slots, 'Live', 'Data', 40);
      router.page({ id: 'live', path: '/live', title: 'Live' }, () => <p>live page</p>);
    });
    const trail = screen.getByRole('navigation', { name: 'Breadcrumb' });
    expect([...trail.querySelectorAll('li')].map((li) => li.textContent)).toEqual([
      'Acme Loams',
      'Data',
      'Live',
    ]);
    expect(within(trail).getByText('Live').closest('li')?.getAttribute('aria-current')).toBe(
      'page',
    );
  });

  it('toggles the theme and remembers it', () => {
    document.documentElement.classList.remove('dark');
    setup();
    fireEvent.click(screen.getByRole('button', { name: 'Use dark theme' }));
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(localStorage.getItem(THEME_KEY)).toBe('dark');
    fireEvent.click(screen.getByRole('button', { name: 'Use light theme' }));
    expect(document.documentElement.classList.contains('dark')).toBe(false);
    expect(localStorage.getItem(THEME_KEY)).toBe('light');
  });

  it('opens the nav drawer from the menu button and closes it again', () => {
    setup();
    const open = screen.getByRole('button', { name: 'Open navigation' });
    expect(open.getAttribute('aria-expanded')).toBe('false');
    fireEvent.click(open);
    expect(document.getElementById('lc-side')?.hasAttribute('data-open')).toBe(true);
    fireEvent.click(screen.getAllByRole('button', { name: 'Close navigation' })[0] as HTMLElement);
    expect(document.getElementById('lc-side')?.hasAttribute('data-open')).toBe(false);
  });

  it('switches the session environment from the sidebar', async () => {
    const envs = [
      { id: 'dev', name: 'Development', namespace: 'd', protected: false },
      { id: 'prod', name: 'Production', namespace: 'p', protected: true },
    ];
    let selected = 'dev';
    const listeners = new Set<() => void>();
    const withEnvs = {
      ...session,
      environments: () => envs,
      environment: () => envs.find((e) => e.id === selected),
      select: (id: string) => {
        selected = id;
        for (const l of listeners) l();
      },
      subscribe: (l: () => void) => {
        listeners.add(l);
        return () => listeners.delete(l);
      },
    } as unknown as SessionService;
    setup('/', undefined, withEnvs);
    const trigger = screen.getByRole('combobox', { name: 'Environment' });
    expect(trigger.textContent).toContain('Development');
    fireEvent.click(trigger);
    const option = await screen.findByRole('option', { name: 'Production' });
    // Let the list finish opening, then pick with the keyboard as a person would.
    await act(() => new Promise((r) => setTimeout(r, 400)));
    fireEvent.keyDown(option, { key: 'Enter' });
    expect(selected).toBe('prod');
  });
});

describe('shell layout', () => {
  it('nav_groups_and_orders_sections', () => {
    const { slots } = setup();
    act(() => {
      section(slots, 'Connectors', 'Integrate', 10);
      section(slots, 'Settings', 'Organisation', 20);
      section(slots, 'Live', 'Data', 40);
      section(slots, 'Postgres', 'Data', 20);
      section(slots, 'Durable', 'Compute', 10);
      section(slots, 'Cloud', 'Organisation', 10);
    });
    const nav = screen.getByRole('navigation', { name: 'Console' });
    const headings = [...nav.querySelectorAll('.lc-nav-heading')].map((h) => h.textContent);
    expect(headings).toEqual(['Data', 'Compute', 'Integrate', 'Organisation']);
    const labels = within(nav)
      .getAllByRole('link')
      .map((a) => a.textContent);
    expect(labels).toEqual(['Postgres', 'Live', 'Durable', 'Connectors', 'Cloud', 'Settings']);
  });

  it('existing_routes_still_render', () => {
    const { slots, router } = setup('/');
    act(() => {
      router.page(
        { id: 'status', path: '/', title: 'Status', nav: { group: 'Instance', order: 0 } },
        () => <p>status page</p>,
      );
      router.page(
        {
          id: 'ns',
          path: '/namespaces',
          title: 'Namespaces',
          nav: { group: 'Instance', order: 10 },
        },
        () => <p>ns page</p>,
      );
      router.page(
        { id: 'apr', path: '/approvals', title: 'Approvals', nav: { group: 'Operate', order: 20 } },
        () => <p>apr page</p>,
      );
      router.page(
        { id: 'odd', path: '/odd', title: 'Odd', nav: { group: 'Weird', order: 1 } },
        () => <p>odd page</p>,
      );
    });
    expect(screen.getByText('status page')).toBeTruthy();
    const nav = screen.getByRole('navigation', { name: 'Console' });
    const sectionOf = (name: string) =>
      within(nav)
        .getByRole('link', { name })
        .closest('.lc-nav-group')
        ?.querySelector('.lc-nav-heading')?.textContent;
    expect(sectionOf('Status')).toBe('Data');
    expect(sectionOf('Namespaces')).toBe('Data');
    expect(sectionOf('Approvals')).toBe('Organisation');
    expect(sectionOf('Odd')).toBe('Organisation');
    expect(within(nav).getByRole('link', { name: 'Status' }).getAttribute('aria-current')).toBe(
      'page',
    );
    expect(slots.entries('console.nav')).toHaveLength(4);
  });

  it('hides the dock and its toggle while the slot is empty', () => {
    setup();
    expect(screen.queryByRole('button', { name: /agent/i })).toBeNull();
    expect(screen.queryByRole('complementary')).toBeNull();
    fireEvent.keyDown(window, { key: 'j', ctrlKey: true });
    expect(screen.queryByRole('complementary')).toBeNull();
  });

  it('dock_toggle_and_shortcut', () => {
    const { slots } = setup();
    act(() => {
      slots.register({ name: 'shell.dock.right', plugin: 'agent' }, () => <p>chat here</p>);
    });
    const toggle = screen.getByRole('button', { name: /agent/i });
    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    expect(screen.queryByText('chat here')).toBeNull();
    fireEvent.click(toggle);
    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(screen.getByText('chat here')).toBeTruthy();
    expect(screen.getByRole('complementary', { name: /agent/i })).toBeTruthy();
    fireEvent.keyDown(window, { key: 'j', ctrlKey: true });
    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    fireEvent.keyDown(window, { key: 'J', metaKey: true });
    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    fireEvent.keyDown(window, { key: 'j' });
    expect(toggle.getAttribute('aria-expanded')).toBe('true');
  });

  it('dock_stays_mounted_when_closed_and_manages_focus', () => {
    const { slots } = setup();
    let mounts = 0;
    function Chat() {
      useEffect(() => {
        mounts++;
      }, []);
      return <textarea data-dock-autofocus aria-label="Message" />;
    }
    act(() => {
      slots.register({ name: 'shell.dock.right', plugin: 'agent' }, () => <Chat />);
    });
    const toggle = screen.getByRole('button', { name: /agent/i });
    expect(mounts).toBe(0);
    fireEvent.click(toggle);
    expect(document.activeElement).toBe(screen.getByLabelText('Message'));
    fireEvent.change(screen.getByLabelText('Message'), { target: { value: 'draft' } });
    fireEvent.keyDown(window, { key: 'j', ctrlKey: true });
    expect(screen.queryByRole('complementary')).toBeNull();
    expect(document.getElementById('lc-dock')?.hidden).toBe(true);
    expect(document.activeElement).toBe(toggle);
    fireEvent.keyDown(window, { key: 'j', ctrlKey: true });
    expect((screen.getByLabelText('Message') as HTMLTextAreaElement).value).toBe('draft');
    expect(mounts).toBe(1);
    expect(document.activeElement).toBe(screen.getByLabelText('Message'));
  });

  it('dock_width_clamped_and_persisted', () => {
    const { slots } = setup();
    act(() => {
      slots.register({ name: 'shell.dock.right', plugin: 'agent' }, () => <p>chat</p>);
    });
    fireEvent.click(screen.getByRole('button', { name: /agent/i }));
    const handle = screen.getByRole('separator');
    expect(Number(handle.getAttribute('aria-valuenow'))).toBe(DOCK_DEFAULT);
    expect(handle.getAttribute('aria-valuemin')).toBe(String(DOCK_MIN));
    expect(handle.getAttribute('aria-valuemax')).toBe(String(DOCK_MAX));
    for (let i = 0; i < 100; i++) fireEvent.keyDown(handle, { key: 'ArrowLeft' });
    expect(handle.getAttribute('aria-valuenow')).toBe(String(DOCK_MAX));
    for (let i = 0; i < 100; i++) fireEvent.keyDown(handle, { key: 'ArrowRight' });
    expect(handle.getAttribute('aria-valuenow')).toBe(String(DOCK_MIN));
    fireEvent.keyDown(handle, { key: 'ArrowLeft' });
    const saved = JSON.parse(localStorage.getItem(DOCK_KEY) ?? '{}');
    expect(saved.open).toBe(true);
    expect(saved.width).toBeGreaterThan(DOCK_MIN);
    expect(saved.width).toBeLessThanOrEqual(DOCK_MAX);

    // A fresh mount restores it, and a corrupt value falls back and clamps.
    cleanup();
    setupWithDock();
    expect(screen.getByRole('separator').getAttribute('aria-valuenow')).toBe(String(saved.width));
    cleanup();
    localStorage.setItem(DOCK_KEY, JSON.stringify({ open: true, width: 99999 }));
    setupWithDock();
    expect(screen.getByRole('separator').getAttribute('aria-valuenow')).toBe(String(DOCK_MAX));
    cleanup();
    localStorage.setItem(DOCK_KEY, '{nope');
    setupWithDock();
    expect(screen.queryByRole('separator')).toBeNull();
  });
});

function setupWithDock() {
  const r = setup();
  act(() => {
    r.slots.register({ name: 'shell.dock.right', plugin: 'agent' }, () => <p>chat</p>);
  });
  return r;
}

describe('engine support (design §54 §13.1)', () => {
  it('engine_older_than_n_minus_one_shows_upgrade_page', () => {
    setup('/', { ...flags, serverVersion: '0.1.0-preview.1' });
    const page = screen.getByRole('alert');
    expect(within(page).getByRole('heading', { name: 'Upgrade your engine' })).toBeTruthy();
    expect(page.textContent).toContain('This engine runs Loams 0.1.0-preview.1.');
    expect(page.textContent).toContain(`works with Loams ${MIN_ENGINE} and later`);
    expect(within(page).getByText(UPGRADE_COMMAND)).toBeTruthy();
    // No chrome and no page: nothing that would call a missing RPC.
    expect(screen.queryByRole('navigation')).toBeNull();
  });

  it('an engine without GetInstance gets the upgrade page too', () => {
    setup('/', { ...flags, serverVersion: '', instanceError: 'unimplemented' });
    expect(screen.getByRole('alert').textContent).toContain('does not answer GetInstance');
  });

  it('N−1 and an unreachable engine get the console', () => {
    setup('/', { ...flags, serverVersion: MIN_ENGINE });
    expect(screen.queryByRole('alert')).toBeNull();
    cleanup();
    setup('/', { ...flags, serverVersion: '', instanceError: 'unreachable' });
    expect(screen.queryByText('Upgrade your engine')).toBeNull();
  });
});
