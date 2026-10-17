import {
  type FlagsService,
  MIN_ENGINE,
  type SessionService,
  THEME_KEY,
  UPGRADE_COMMAND,
} from '@loams/console-host';
import { SlotProvider, SlotRegistry } from '@loams/slots';
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Layout } from '../src/layout.js';
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
  serverVersion: '0.1.0',
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
});
afterEach(cleanup);

describe('shell chrome', () => {
  it('shows the Loams wordmark and the version, with no preview badge', () => {
    setup('/', flags);
    expect(screen.getByRole('img', { name: 'Loams' })).toBeTruthy();
    expect(screen.queryByText('OSS')).toBeNull();
    expect(screen.queryByText('Preview')).toBeNull();
    expect(screen.getByText('Loams 0.1.0')).toBeTruthy();
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

  it('offers System, Dark and Light and saves the choice', async () => {
    setup('/', flags);
    fireEvent.click(screen.getByRole('button', { name: /^Theme: /i }));
    fireEvent.click(await screen.findByRole('menuitem', { name: /Light/ }));
    expect(localStorage.getItem(THEME_KEY)).toBe('light');
    expect(document.documentElement.classList.contains('dark')).toBe(false);
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
});

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
