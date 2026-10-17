// The shell's sidebar (design 59, D1078–D1079): the brand, the server indicator and
// "New thread" on top; projects and their threads (`shell.sidebar.threads`, Task 9) and the
// product nav in the middle; the docs link and version at the foot. 232px by default,
// resizable from 200 to 320px and collapsible (⌘B); below 960px it is a sheet. Task 10
// replaces the nav with pinned Pages and Settings.

import {
  type FlagsService,
  MODE_HOME,
  type ModeService,
  type SessionService,
} from '@loams/console-host';
import { Slot, useSlot } from '@loams/slots';
import {
  SelectMenu,
  Sheet,
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
} from '@loams/ui';
import { BookOpen, ChevronsUpDown, SquarePen } from 'lucide-react';
import { useSyncExternalStore } from 'react';
import { Brand, runShellCommand, SIDEBAR } from './frame.js';
import { iconFor } from './icons.js';
import { useMode } from './mode-switcher.js';
import { isActive, navSections, ORG_SECTIONS, useLocation, useModeFilter } from './nav.js';
import type { HashRouter } from './router.js';

export function ShellSidebar({
  router,
  flags,
  mode,
  session,
  width,
  onWidth,
  open,
  narrow,
  onClose,
}: {
  router: HashRouter;
  flags?: FlagsService;
  mode?: ModeService;
  session: SessionService;
  width: number;
  onWidth: (w: number) => void;
  open: boolean;
  narrow: boolean;
  onClose: () => void;
}) {
  const current = useMode(mode);
  const hasThreads = useSlot('shell.sidebar.threads').length > 0;
  const newThread = () =>
    runShellCommand('chat.new', () =>
      mode ? mode.set('studio', { navigate: MODE_HOME.studio }) : router.navigate(MODE_HOME.studio),
    );
  const body = (
    <>
      <SidebarHeader className="lc-side-head">
        <Brand flags={flags} home={MODE_HOME[current]} />
        <Slot name="shell.sidebar.server" props={{}} />
        <button
          type="button"
          className="lc-new-thread"
          onClick={newThread}
          aria-keyshortcuts="Control+N Meta+N"
        >
          <SquarePen size={15} strokeWidth={1.75} aria-hidden="true" /> New thread
        </button>
      </SidebarHeader>
      <SidebarContent>
        {hasThreads && (
          <section aria-label="Threads" className="lc-side-threads">
            <Slot name="shell.sidebar.threads" props={{}} />
          </section>
        )}
        <Nav router={router} mode={mode} session={session} />
      </SidebarContent>
      <SidebarFooter className="lc-side-foot">
        <a href="https://loams.dev/docs" target="_blank" rel="noreferrer">
          <BookOpen size={15} aria-hidden="true" /> Documentation
        </a>
        <span className="lc-side-version">Loams {flags?.serverVersion || 'console'}</span>
      </SidebarFooter>
    </>
  );
  if (narrow) {
    return (
      <Sheet open={open} onClose={onClose} title="Navigation" side="left">
        <nav id="lc-side" aria-label="Sidebar" className="lc-side lc-side-sheet">
          {body}
        </nav>
      </Sheet>
    );
  }
  // `display: contents`: the id the top bar's toggle controls, without a box of its own.
  return (
    <div id="lc-side" className="contents">
      <Sidebar
        label="Sidebar"
        width={width}
        onWidthChange={onWidth}
        min={SIDEBAR.min}
        max={SIDEBAR.max}
        open={open}
        className="lc-side"
      >
        {body}
      </Sidebar>
    </div>
  );
}

/**
 * The product nav, grouped by section. Above it, the project switcher and the open
 * project's pages when a plugin serves them (`shell.nav.context`); otherwise, in Console,
 * the session's environments.
 */
function Nav({
  router,
  mode,
  session,
}: {
  router: HashRouter;
  mode?: ModeService;
  session: SessionService;
}) {
  const sections = useSlot('shell.nav.section');
  const legacy = useSlot('console.nav');
  const location = useLocation(router);
  const filter = useModeFilter(mode);
  const model = navSections(sections, legacy, filter);
  // Only the most specific match is current (`/studio/settings`, not also `/studio`).
  const current = model
    .flatMap(([, items]) => items)
    .filter((i) => isActive(location.path, i.href))
    .sort((a, b) => b.href.length - a.href.length)[0]?.key;
  const hasContext = useSlot('shell.nav.context').length > 0;
  return (
    <>
      {hasContext ? (
        <Slot name="shell.nav.context" props={{}} />
      ) : (
        filter?.mode !== 'studio' && <EnvironmentSwitcher session={session} />
      )}
      <nav className="lc-nav" aria-label={filter?.mode === 'studio' ? 'Studio' : 'Console'}>
        {model.map(([group, items]) => (
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
                      aria-current={item.key === current ? 'page' : undefined}
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
    </>
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
