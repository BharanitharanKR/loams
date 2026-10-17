// Studio inside a stand-in for the shell's regions (design 59): the sidebar's thread list,
// the page, the inspector with Studio's thread tabs and the drawer with its terminal. Tests
// that used Studio's own sidebar, panel and drawer render this instead of `StudioView` alone.

import type { Location, PanelService, PanelState, RouterService } from '@loams/console-host';
import { type ComponentProps, useState, useSyncExternalStore } from 'react';
import type { StudioFactory } from '../src/factory.js';
import { createStudioKeys } from '../src/palette/commands.js';
import { STUDIO_TABS, StudioTerminalTab, StudioThreads, StudioThreadTab } from '../src/regions.js';
import { STUDIO_PAGES, type StudioRoute } from '../src/routes.js';
import { StudioView } from '../src/view.js';

/** The shell's PanelController without storage. */
export class TestPanel implements PanelService {
  #state: PanelState = { open: false, tab: undefined, props: {}, size: 280 };
  readonly #listeners = new Set<() => void>();
  state = (): PanelState => this.#state;
  open(o: { tab?: string; props?: Record<string, unknown> } = {}): void {
    this.#set({ open: true, tab: o.tab ?? this.#state.tab, props: o.props ?? {} });
  }
  close(): void {
    this.#set({ open: false });
  }
  toggle(tab?: string): void {
    if (this.#state.open && (tab === undefined || tab === this.#state.tab)) this.close();
    else this.open(tab === undefined ? {} : { tab });
  }
  resize(size: number): void {
    this.#set({ size });
  }
  subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };
  #set(patch: Partial<PanelState>) {
    this.#state = { ...this.#state, ...patch };
    for (const l of [...this.#listeners]) l();
  }
}

/** The router's location for a Studio route (the params the shell's router would give). */
export function locationOf(route: StudioRoute): Location {
  const kind = route.kind;
  const page = STUDIO_PAGES.find((p) => p.kind === kind);
  const params: Record<string, string> = {};
  if (route.kind === 'project') params.projectId = route.projectId;
  if (route.kind === 'group') params.groupId = route.groupId;
  if (route.kind === 'thread') {
    params.threadId = route.threadId;
    if (route.panel) params.panel = route.panel;
    if (route.file) params.path = route.file.path;
  }
  return { path: page?.path ?? '/studio', pageId: page?.id ?? 'studio', params };
}

/** A router on one Studio route; `navigate` is the test's spy. */
export function routerAt(route: StudioRoute, navigate: (to: string) => void): RouterService {
  const location = locationOf(route);
  return {
    current: () => location,
    subscribe: () => () => {},
    navigate,
    page: () => () => {},
  };
}

type ViewProps = ComponentProps<typeof StudioView>;

/** The page in the shell: pass the same props as `StudioView`. */
export function StudioInShell(
  allProps: Omit<ViewProps, 'inspector' | 'drawer'> & {
    inspector?: PanelService;
    drawer?: PanelService;
    /** The sidebar's Factory group (desktop). */
    factory?: StudioFactory;
  },
) {
  const { factory, ...props } = allProps;
  // One keys object and one pair of panels per mount, as the plugin shares them.
  const [own] = useState(() => ({
    keys: createStudioKeys({ edition: props.agentd.kind }),
    inspector: new TestPanel(),
    drawer: new TestPanel(),
  }));
  const keys = props.keys ?? own.keys;
  const inspector = props.inspector ?? own.inspector;
  const drawer = props.drawer ?? own.drawer;
  const router = routerAt(props.route, props.navigate);
  const params = router.current().params;
  const i = useSyncExternalStore(inspector.subscribe, inspector.state);
  const d = useSyncExternalStore(drawer.subscribe, drawer.state);
  const thread = props.route.kind === 'thread';
  const active = STUDIO_TABS.find((t) => `studio.${t.panel}` === i.tab) ?? STUDIO_TABS[0];
  return (
    <>
      <StudioThreads agentd={props.agentd} factory={factory} router={router} keys={keys} />
      <StudioView {...props} keys={keys} inspector={inspector} drawer={drawer} />
      {thread && i.open && active && (
        <aside aria-label="Inspector">
          <div role="tablist" aria-label="Inspector tabs">
            {STUDIO_TABS.map((t) => (
              <button
                key={t.panel}
                type="button"
                role="tab"
                aria-selected={t === active}
                onClick={() => inspector.open({ tab: `studio.${t.panel}` })}
              >
                {t.label}
              </button>
            ))}
          </div>
          <StudioThreadTab
            agentd={props.agentd}
            panel={active.panel}
            params={params}
            navigate={props.navigate}
          />
        </aside>
      )}
      {thread && d.open && (
        <section aria-label="Drawer">
          <StudioTerminalTab
            agentd={props.agentd}
            keys={keys}
            params={params}
            navigate={props.navigate}
          />
        </section>
      )}
    </>
  );
}
