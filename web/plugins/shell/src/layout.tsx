// The console layout, in the original console's look (web/apps/console/src/shell.tsx):
// a sidebar with the brand, the environment switcher and the product nav grouped by
// section; a top bar with breadcrumbs, the server switcher and overlays, the agent
// toggle, the theme toggle and the account menu; the page; and a resizable dock on
// the right (D666). Below 960px the sidebar is a drawer. Components get props and
// closures, never `ctx`.

import type { FlagsService, SessionService } from '@loams/console-host';
import { Slot, type SlotEntry, useSlot } from '@loams/slots';
import {
  Avatar,
  Badge,
  Empty,
  Logo,
  Menu,
  MenuLabel,
  SelectMenu,
  Toaster,
  ToastProvider,
} from '@loams/ui';
import { BookOpen, ChevronsUpDown, Menu as MenuIcon, Moon, PanelRight, Sun, X } from 'lucide-react';
import {
  type KeyboardEvent,
  type PointerEvent,
  useCallback,
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
} from 'react';
import { iconFor } from './icons.js';
import type { HashRouter } from './router.js';

export const DOCK_MIN = 320;
export const DOCK_MAX = 720;
export const DOCK_DEFAULT = 400;
export const DOCK_KEY = 'loams.shell.dock';
export const THEME_KEY = 'loams-theme';
const DOCK_STEP = 24;

/** The nav sections, in display order. Anything else follows them. */
export const SECTIONS = ['Data', 'Compute', 'Integrate', 'Organisation', 'Operate'] as const;
/**
 * The sections about the whole instance rather than its data and services: the
 * original console's "Organization" half of the sidebar, set apart below the rest.
 */
const ORG_SECTIONS: readonly string[] = ['Organisation', 'Operate'];

interface NavItem {
  key: string;
  label: string;
  href: string;
  icon?: string;
  group: string;
  order: number;
}

/**
 * Where the pages that predate `shell.nav.section` (registered through
 * `router.page(... nav)`) land, by path. Orders sit in the bands the product
 * pages leave free: Data 0 Overview, 10 Data, 20 Postgres, 30 WeSQL, 40 Live.
 */
const LEGACY: Record<string, Pick<NavItem, 'group' | 'icon' | 'order'>> = {
  '/': { group: 'Data', icon: 'overview', order: 0 },
  '/namespaces': { group: 'Data', icon: 'namespaces', order: 15 },
  '/approvals': { group: 'Organisation', icon: 'approvals', order: 50 },
};

/** The nav model from both slots, grouped in section order and sorted by `order`. */
export function navSections(
  sections: SlotEntry<'shell.nav.section'>[],
  legacy: SlotEntry<'console.nav'>[],
): [string, NavItem[]][] {
  const items: NavItem[] = [];
  for (const e of sections) {
    items.push({
      key: `s:${e.meta?.id ?? e.id}`,
      label: e.meta?.label ?? e.meta?.id ?? '',
      href: e.meta?.href ?? '/',
      icon: e.meta?.icon,
      group: e.meta?.group ?? 'Organisation',
      order: e.order,
    });
  }
  for (const e of legacy) {
    const href = e.meta?.href ?? '/';
    const known = LEGACY[href];
    const group = e.meta?.group ?? '';
    items.push({
      key: `c:${e.id}`,
      label: e.meta?.label ?? href,
      href,
      icon: known?.icon ?? (href.startsWith('/plugins/') ? 'plugin' : undefined),
      // Unmapped legacy pages join Organisation, after the first-party ones.
      group:
        known?.group ?? ((SECTIONS as readonly string[]).includes(group) ? group : 'Organisation'),
      order: known?.order ?? 1000 + e.order,
    });
  }
  const byGroup = new Map<string, NavItem[]>();
  for (const item of items) byGroup.set(item.group, [...(byGroup.get(item.group) ?? []), item]);
  const rank = (g: string) => {
    const i = (SECTIONS as readonly string[]).indexOf(g);
    return i < 0 ? SECTIONS.length : i;
  };
  return [...byGroup]
    .sort((a, b) => rank(a[0]) - rank(b[0]))
    .map(([g, list]) => [g, list.sort((a, b) => a.order - b.order)]);
}

const isActive = (path: string, href: string) =>
  path === href || (href !== '/' && path.startsWith(`${href}/`));

function useLocation(router: HashRouter) {
  return useSyncExternalStore(router.subscribe, () => router.current());
}

function Nav({ router }: { router: HashRouter }) {
  const sections = useSlot('shell.nav.section');
  const legacy = useSlot('console.nav');
  const location = useLocation(router);
  return (
    <nav className="lc-nav" aria-label="Console">
      {navSections(sections, legacy).map(([group, items]) => (
        <div
          key={group}
          className="lc-nav-group"
          data-scope={ORG_SECTIONS.includes(group) ? 'org' : undefined}
        >
          <h2 className="lc-nav-heading">{group}</h2>
          <ul>
            {items.map((item) => {
              const Icon = iconFor(item.icon);
              return (
                <li key={item.key}>
                  <a
                    href={`#${item.href}`}
                    aria-current={isActive(location.path, item.href) ? 'page' : undefined}
                  >
                    <Icon aria-hidden="true" size={16} strokeWidth={1.75} />
                    <span className="lc-nav-label">{item.label}</span>
                  </a>
                </li>
              );
            })}
          </ul>
        </div>
      ))}
    </nav>
  );
}

/** The environment the session reads, like the original console's project switcher. */
function EnvironmentSwitcher({ session }: { session: SessionService }) {
  useSyncExternalStore(session.subscribe, () => session.environment?.());
  const environments = session.environments?.() ?? [];
  const current = session.environment?.();
  if (!current || environments.length === 0) return null;
  return (
    <div className="lc-switcher">
      <SelectMenu
        label="Environment"
        value={current.id}
        onValueChange={(id) => session.select(id)}
        options={environments.map((e) => ({ value: e.id, label: e.name }))}
        className="lc-switcher-trigger"
        leading={
          <span className="lc-switcher-mark" aria-hidden="true">
            {current.name.slice(0, 1).toUpperCase()}
          </span>
        }
        icon={<ChevronsUpDown size={14} />}
      />
    </div>
  );
}

/** The page's place: instance, section, page; the last crumb is the current page. */
function Crumbs({ router, flags }: { router: HashRouter; flags?: FlagsService }) {
  const sections = useSlot('shell.nav.section');
  const legacy = useSlot('console.nav');
  const location = useLocation(router);
  const title = location.pageId ? router.title() : undefined;
  let group: string | undefined;
  let home: { label: string; href: string } | undefined;
  for (const [g, items] of navSections(sections, legacy)) {
    const hit = items
      .filter((i) => isActive(location.path, i.href))
      .sort((a, b) => b.href.length - a.href.length)[0];
    if (hit && (!home || hit.href.length > home.href.length)) {
      group = g;
      home = { label: hit.label, href: hit.href };
    }
  }
  const crumbs: { label: string; href?: string }[] = [
    { label: flags?.instanceName || 'Loams', href: '/' },
  ];
  if (group) crumbs.push({ label: group });
  if (home && home.href !== location.path) crumbs.push({ label: home.label, href: home.href });
  crumbs.push({ label: title ?? home?.label ?? 'Not found' });
  return (
    <nav aria-label="Breadcrumb" className="lc-trail">
      <ol>
        {crumbs.map((c, i) => {
          const last = i === crumbs.length - 1;
          return (
            // biome-ignore lint/suspicious/noArrayIndexKey: crumbs are positional
            <li key={i} aria-current={last ? 'page' : undefined}>
              {!last && c.href ? <a href={`#${c.href}`}>{c.label}</a> : <span>{c.label}</span>}
            </li>
          );
        })}
      </ol>
    </nav>
  );
}

function Outlet({ router }: { router: HashRouter }) {
  const location = useLocation(router);
  const title = location.pageId ? router.title() : undefined;
  useEffect(() => {
    document.title = title ? `${title} · Loams` : 'Loams';
  }, [title]);
  if (!location.pageId) {
    return (
      <Empty title="Nothing here">
        No plugin serves <code>{location.path}</code>. It may be disabled, or waiting for an API
        this instance does not serve.
      </Empty>
    );
  }
  return (
    <Slot
      name="console.page"
      slotKey={location.pageId}
      props={{ params: location.params }}
      fallback={<Empty title="Loading…" />}
    />
  );
}

const clampWidth = (n: number) => Math.min(DOCK_MAX, Math.max(DOCK_MIN, Math.round(n)));

interface DockState {
  open: boolean;
  width: number;
}

function readDock(): DockState {
  try {
    const raw = JSON.parse(localStorage.getItem(DOCK_KEY) ?? 'null') as Partial<DockState> | null;
    return {
      open: raw?.open === true,
      width: typeof raw?.width === 'number' ? clampWidth(raw.width) : DOCK_DEFAULT,
    };
  } catch {
    return { open: false, width: DOCK_DEFAULT };
  }
}

function useDock() {
  const [dock, setDock] = useState<DockState>(readDock);
  const update = useCallback((patch: Partial<DockState>) => {
    setDock((prev) => {
      const next = { ...prev, ...patch };
      try {
        localStorage.setItem(DOCK_KEY, JSON.stringify(next));
      } catch {
        // Storage blocked (private window): the dock still works this session.
      }
      return next;
    });
  }, []);
  return [dock, update] as const;
}

/** Light by default; the choice is kept under the same key the original console uses. */
export function useTheme(): [boolean, () => void] {
  const [dark, setDark] = useState(() => document.documentElement.classList.contains('dark'));
  const toggle = () => {
    const next = !dark;
    document.documentElement.classList.toggle('dark', next);
    try {
      localStorage.setItem(THEME_KEY, next ? 'dark' : 'light');
    } catch {
      // Private mode: the choice lasts for this page only.
    }
    setDark(next);
  };
  return [dark, toggle];
}

function DockHandle({ width, onWidth }: { width: number; onWidth: (w: number) => void }) {
  const onPointerDown = (e: PointerEvent<HTMLDivElement>) => {
    e.preventDefault();
    const move = (ev: globalThis.PointerEvent) => onWidth(window.innerWidth - ev.clientX);
    const up = () => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };
  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    const grow = e.key === 'ArrowLeft' ? 1 : e.key === 'ArrowRight' ? -1 : 0;
    if (grow) {
      e.preventDefault();
      onWidth(width + grow * DOCK_STEP);
    } else if (e.key === 'Home') onWidth(DOCK_MIN);
    else if (e.key === 'End') onWidth(DOCK_MAX);
  };
  return (
    // biome-ignore lint/a11y/useSemanticElements: a focusable window splitter has no native element
    <div
      className="lc-dock-handle"
      role="separator"
      aria-orientation="vertical"
      aria-label="Resize agent panel"
      aria-valuemin={DOCK_MIN}
      aria-valuemax={DOCK_MAX}
      aria-valuenow={width}
      tabIndex={0}
      onPointerDown={onPointerDown}
      onKeyDown={onKeyDown}
    />
  );
}

const KIND_LABEL: Record<number, string> = { 1: 'User', 2: 'Service account', 3: 'Agent' };

function UserMenu({ session }: { session: SessionService }) {
  useSyncExternalStore(session.subscribe, () => session.principal());
  const who = session.principal();
  if (!who) return null;
  const name = who.displayName || 'Signed in';
  return (
    <Menu
      label="Account"
      trigger={
        <button type="button" className="lc-user-btn" aria-label={`Account: ${name}`}>
          <Avatar name={name} size={28} />
        </button>
      }
    >
      <MenuLabel>
        <strong className="text-ink">{name}</strong>
        {who.email && <span>{who.email}</span>}
        <span>
          <Badge>{KIND_LABEL[who.kind] ?? 'Signed in'}</Badge>
        </span>
      </MenuLabel>
    </Menu>
  );
}

function Brand({ flags, onClose }: { flags?: FlagsService; onClose: () => void }) {
  const edition = flags?.features.desktop
    ? 'Desktop'
    : flags?.edition === 'cloud'
      ? 'Cloud'
      : flags?.edition === 'byoc'
        ? 'BYOC'
        : 'OSS';
  return (
    <div className="lc-brand">
      <a href="#/" aria-label="Loams home" className="lc-brand-link">
        <Logo height={18} />
      </a>
      <Badge>{edition}</Badge>
      <span
        className="lc-preview"
        title="Loams is in preview: interfaces and data formats may change between releases."
      >
        Preview
      </span>
      <button
        type="button"
        className="lc-side-close"
        aria-label="Close navigation"
        onClick={onClose}
      >
        <X size={18} aria-hidden="true" />
      </button>
    </div>
  );
}

export function Layout({
  router,
  session,
  flags,
}: {
  router: HashRouter;
  session: SessionService;
  /** GetInstance's answer: the instance name, edition and version for the chrome. */
  flags?: FlagsService;
}) {
  const hasDock = useSlot('shell.dock.right').length > 0;
  const [dock, setDock] = useDock();
  const [dark, toggleTheme] = useTheme();
  const [drawer, setDrawer] = useState(false);
  const location = useLocation(router);
  const open = hasDock && dock.open;
  const toggle = useCallback(() => setDock({ open: !dock.open }), [dock.open, setDock]);
  // The dock mounts on first open and then stays mounted (hidden, not removed) so the
  // agent's chat state survives Ctrl/Cmd+J.
  const [mounted, setMounted] = useState(open);
  const toggleRef = useRef<HTMLButtonElement>(null);
  const dockRef = useRef<HTMLElement>(null);
  const wasOpen = useRef(open);
  if (open && !mounted) setMounted(true);

  // biome-ignore lint/correctness/useExhaustiveDependencies: close the drawer per route
  useEffect(() => setDrawer(false), [location.path]);

  // Focus: into the element marked data-dock-autofocus (the composer) when the dock
  // opens, back to the toggle when it closes.
  useEffect(() => {
    if (open === wasOpen.current) return;
    wasOpen.current = open;
    const dockEl = dockRef.current;
    if (open) {
      const target = dockEl?.querySelector<HTMLElement>('[data-dock-autofocus]');
      (target ?? dockEl)?.focus();
    } else {
      toggleRef.current?.focus();
    }
  }, [open]);

  useEffect(() => {
    if (!hasDock) return;
    const onKey = (e: globalThis.KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && !e.altKey && e.key.toLowerCase() === 'j') {
        e.preventDefault();
        toggle();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [hasDock, toggle]);

  const version = flags?.serverVersion;
  return (
    <ToastProvider>
      <div className="lc-shell">
        {/* A plain container: the Console nav inside is the landmark; `complementary` is the agent dock. */}
        <div id="lc-side" className="lc-side" data-open={drawer || undefined}>
          <Brand flags={flags} onClose={() => setDrawer(false)} />
          <EnvironmentSwitcher session={session} />
          <Nav router={router} />
          <div className="lc-side-foot">
            <a href="https://loams.dev/docs" target="_blank" rel="noreferrer">
              <BookOpen size={16} aria-hidden="true" /> Documentation
            </a>
            <span className="lc-side-version">Loams {version ? version : 'console'}</span>
          </div>
        </div>
        {drawer && (
          <button
            type="button"
            className="lc-scrim"
            aria-label="Close navigation"
            onClick={() => setDrawer(false)}
          />
        )}
        <div className="lc-body">
          <header className="lc-header">
            <button
              type="button"
              className="lc-menu-btn"
              aria-label="Open navigation"
              aria-controls="lc-side"
              aria-expanded={drawer}
              onClick={() => setDrawer(true)}
            >
              <MenuIcon size={18} aria-hidden="true" />
            </button>
            <Crumbs router={router} flags={flags} />
            <div className="lc-header-tools">
              <div className="lc-header-start">
                <Slot name="shell.header.server" props={{}} />
              </div>
              <div className="lc-overlays">
                <Slot name="shell.overlay" props={{}} />
              </div>
              {hasDock && (
                <button
                  type="button"
                  ref={toggleRef}
                  className="lc-icon-btn"
                  aria-expanded={open}
                  aria-controls="lc-dock"
                  aria-keyshortcuts="Control+J Meta+J"
                  aria-label="Agent"
                  title="Agent (Ctrl/Cmd+J)"
                  onClick={toggle}
                >
                  <PanelRight aria-hidden="true" size={16} strokeWidth={1.75} />
                  <span className="lc-icon-btn-text">Agent</span>
                </button>
              )}
              <button
                type="button"
                className="lc-round-btn"
                onClick={toggleTheme}
                aria-label={dark ? 'Use light theme' : 'Use dark theme'}
                title={dark ? 'Use light theme' : 'Use dark theme'}
              >
                {dark ? (
                  <Sun size={16} aria-hidden="true" />
                ) : (
                  <Moon size={16} aria-hidden="true" />
                )}
              </button>
              <UserMenu session={session} />
            </div>
          </header>
          <div className="lc-content" style={{ ['--lc-dock-width' as string]: `${dock.width}px` }}>
            <main className="lc-main" id="lc-main">
              <Outlet router={router} />
            </main>
            {hasDock && mounted && (
              <aside
                className="lc-dock"
                id="lc-dock"
                aria-label="Agent panel"
                ref={dockRef}
                tabIndex={-1}
                hidden={!open}
              >
                <DockHandle width={dock.width} onWidth={(w) => setDock({ width: clampWidth(w) })} />
                <div className="lc-dock-body">
                  <Slot name="shell.dock.right" props={{}} />
                </div>
              </aside>
            )}
          </div>
        </div>
      </div>
      <Toaster />
    </ToastProvider>
  );
}
