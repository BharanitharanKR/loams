// Studio's layout (§53 §5.1; inventory (a), T3 Code's AppSidebarLayout, rebuilt):
// a resizable sidebar, the main area with its toolbar, a right-hand panel and a
// bottom drawer. Sizes and open states are kept per device under
// `loams.studio.panel`, and every storage access is wrapped. Below 768px the
// sidebar becomes a drawer and the panel an overlay. Tokens only, via Tailwind.

import { useMediaQuery } from '@loams/studio-ui';
import { cx, ResizeHandle } from '@loams/ui';
import { PanelBottom, PanelLeft, PanelRight, X } from 'lucide-react';
import { type ReactNode, useCallback, useEffect, useState } from 'react';
import { StudioCommands, useStudioCommand } from '../palette/commands.js';

export const LAYOUT_KEY = 'loams.studio.panel';

export interface LayoutState {
  sidebar: number;
  panel: number;
  drawer: number;
  panelOpen: boolean;
  drawerOpen: boolean;
}

export const LAYOUT_DEFAULTS: LayoutState = {
  sidebar: 272,
  panel: 420,
  drawer: 240,
  panelOpen: true,
  drawerOpen: false,
};

const LIMITS = {
  sidebar: [200, 480],
  panel: [300, 760],
  drawer: [120, 520],
} as const;
const STEP = 24;

type Size = keyof typeof LIMITS;

const NO_COMMANDS = new StudioCommands();

const clamp = (key: Size, n: number) =>
  Math.min(LIMITS[key][1], Math.max(LIMITS[key][0], Math.round(n)));

function read(): LayoutState {
  try {
    const raw = JSON.parse(
      localStorage.getItem(LAYOUT_KEY) ?? 'null',
    ) as Partial<LayoutState> | null;
    if (!raw || typeof raw !== 'object') return LAYOUT_DEFAULTS;
    const size = (key: Size) =>
      typeof raw[key] === 'number' ? clamp(key, raw[key] as number) : LAYOUT_DEFAULTS[key];
    return {
      sidebar: size('sidebar'),
      panel: size('panel'),
      drawer: size('drawer'),
      panelOpen: typeof raw.panelOpen === 'boolean' ? raw.panelOpen : LAYOUT_DEFAULTS.panelOpen,
      drawerOpen: typeof raw.drawerOpen === 'boolean' ? raw.drawerOpen : LAYOUT_DEFAULTS.drawerOpen,
    };
  } catch {
    return LAYOUT_DEFAULTS;
  }
}

/** The layout state, persisted per device; a blocked storage keeps it for the page. */
export function useLayoutState() {
  const [state, setState] = useState<LayoutState>(read);
  const update = useCallback((patch: Partial<LayoutState>) => {
    setState((prev) => {
      const next = { ...prev, ...patch };
      try {
        localStorage.setItem(LAYOUT_KEY, JSON.stringify(next));
      } catch {
        // Storage blocked (a private window): the layout still works this session.
      }
      return next;
    });
  }, []);
  return [state, update] as const;
}

/** A splitter on one of the layout's sizes; @loams/ui's ResizeHandle does the work. */
function Splitter({
  label,
  size,
  value,
  edge,
  onResize,
}: {
  label: string;
  size: Size;
  value: number;
  /** Which side of the splitter the resized region is on. */
  edge: 'left' | 'right' | 'below';
  onResize: (n: number) => void;
}) {
  return (
    <ResizeHandle
      label={label}
      value={value}
      min={LIMITS[size][0]}
      max={LIMITS[size][1]}
      step={STEP}
      orientation={edge === 'below' ? 'horizontal' : 'vertical'}
      // `end`: the region is before the handle (the sidebar); `start`: after it.
      edge={edge === 'left' ? 'end' : 'start'}
      onResize={(n) => onResize(clamp(size, n))}
    />
  );
}

const TOOL =
  'inline-grid size-8 shrink-0 cursor-pointer place-items-center rounded-pill border-0 bg-transparent ' +
  'text-muted hover:bg-raised hover:text-ink aria-pressed:text-ink';

/** An overlay region on a narrow window: a scrim and a panel from one side. */
function Overlay({
  label,
  side,
  onClose,
  children,
}: {
  label: string;
  side: 'left' | 'right';
  onClose: () => void;
  children: ReactNode;
}) {
  useEffect(() => {
    const onKey = (e: globalThis.KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);
  return (
    <>
      <button
        type="button"
        aria-label={`Close ${label.toLowerCase()}`}
        className="fixed inset-0 z-40 cursor-default border-0 bg-ink/30"
        onClick={onClose}
      />
      <div
        role="dialog"
        aria-modal="true"
        aria-label={label}
        className={cx(
          'fixed inset-y-0 z-50 flex w-[min(88vw,360px)] flex-col border-rule bg-surface shadow-modal',
          side === 'left' ? 'left-0 border-r' : 'right-0 border-l',
        )}
      >
        <div className="flex h-11 shrink-0 items-center justify-between border-b border-rule-soft px-3">
          <strong className="text-[0.86rem] text-ink">{label}</strong>
          <button
            type="button"
            className={TOOL}
            aria-label={`Close ${label.toLowerCase()}`}
            onClick={onClose}
          >
            <X size={16} aria-hidden="true" />
          </button>
        </div>
        <div className="min-h-0 flex-1 overflow-auto">{children}</div>
      </div>
    </>
  );
}

export function StudioLayout({
  sidebar,
  title,
  tools,
  panel,
  drawer,
  commands,
  children,
}: {
  sidebar: ReactNode;
  /** The toolbar's title (the thread, the project or the page). */
  title: ReactNode;
  /** Toolbar controls after the title (branch, git actions, harness; later tasks). */
  tools?: ReactNode;
  /** The right-hand panel (Diff, Files, Terminal, Preview, Plan); none hides it. */
  panel?: ReactNode;
  /** The bottom drawer (terminals); none hides it. */
  drawer?: ReactNode;
  /** Studio's command bus: the layout handles its panel, terminal and sidebar commands. */
  commands?: StudioCommands;
  children: ReactNode;
}) {
  const wide = useMediaQuery('md');
  const [state, update] = useLayoutState();
  const [sideOpen, setSideOpen] = useState(false);
  // On a narrow window the panel is an overlay, closed until asked for.
  const [overlayPanel, setOverlayPanel] = useState(false);
  const panelShown = panel !== undefined && (wide ? state.panelOpen : overlayPanel);
  const togglePanel = () =>
    wide ? update({ panelOpen: !state.panelOpen }) : setOverlayPanel((v) => !v);
  const closeSide = useCallback(() => setSideOpen(false), []);
  const closePanel = useCallback(() => setOverlayPanel(false), []);
  const bus = commands ?? NO_COMMANDS;
  const hasPanel = panel !== undefined;
  useStudioCommand(bus, 'rightPanel.toggle', commands && hasPanel ? togglePanel : undefined);
  useStudioCommand(
    bus,
    'rightPanel.show',
    commands && hasPanel
      ? () => (wide ? update({ panelOpen: true }) : setOverlayPanel(true))
      : undefined,
  );
  useStudioCommand(
    bus,
    'terminal.toggle',
    commands && drawer !== undefined ? () => update({ drawerOpen: !state.drawerOpen }) : undefined,
  );
  // On a wide window the sidebar is always there; on a narrow one it is an overlay.
  useStudioCommand(
    bus,
    'sidebar.toggle',
    commands && !wide ? () => setSideOpen((open) => !open) : undefined,
  );

  return (
    <div className="flex h-full min-h-0 bg-bg" data-studio-layout>
      {wide && (
        <>
          <div
            className="flex shrink-0 flex-col overflow-hidden bg-surface"
            style={{ width: state.sidebar }}
          >
            {sidebar}
          </div>
          <Splitter
            label="Resize sidebar"
            size="sidebar"
            value={state.sidebar}
            edge="left"
            onResize={(n) => update({ sidebar: n })}
          />
        </>
      )}
      {!wide && sideOpen && (
        <Overlay label="Projects" side="left" onClose={closeSide}>
          {sidebar}
        </Overlay>
      )}
      <section className="flex min-w-0 flex-1 flex-col">
        <div className="flex h-11 shrink-0 items-center gap-2 border-b border-rule-soft px-3">
          {!wide && (
            <button
              type="button"
              className={TOOL}
              aria-label="Show projects"
              aria-expanded={sideOpen}
              onClick={() => setSideOpen(true)}
            >
              <PanelLeft size={16} aria-hidden="true" />
            </button>
          )}
          <div className="min-w-0 flex-1 truncate text-[0.9rem] text-ink">{title}</div>
          {tools}
          {drawer !== undefined && (
            <button
              type="button"
              className={TOOL}
              aria-pressed={state.drawerOpen}
              aria-label={state.drawerOpen ? 'Hide terminal' : 'Show terminal'}
              title="Terminal"
              onClick={() => update({ drawerOpen: !state.drawerOpen })}
            >
              <PanelBottom size={16} aria-hidden="true" />
            </button>
          )}
          {panel !== undefined && (
            <button
              type="button"
              className={TOOL}
              aria-pressed={panelShown}
              aria-label={panelShown ? 'Hide panel' : 'Show panel'}
              title="Panel"
              onClick={togglePanel}
            >
              <PanelRight size={16} aria-hidden="true" />
            </button>
          )}
        </div>
        <div className="flex min-h-0 flex-1">
          <div className="min-w-0 flex-1 overflow-auto">{children}</div>
          {wide && panelShown && (
            <>
              <Splitter
                label="Resize panel"
                size="panel"
                value={state.panel}
                edge="right"
                onResize={(n) => update({ panel: n })}
              />
              <aside
                aria-label="Panel"
                className="flex shrink-0 flex-col overflow-hidden bg-bg"
                style={{ width: state.panel }}
              >
                {panel}
              </aside>
            </>
          )}
        </div>
        {drawer !== undefined && state.drawerOpen && (
          <>
            <Splitter
              label="Resize terminal"
              size="drawer"
              value={state.drawer}
              edge="below"
              onResize={(n) => update({ drawer: n })}
            />
            <section
              aria-label="Terminal"
              className="shrink-0 overflow-auto bg-surface"
              style={{ height: state.drawer }}
            >
              {drawer}
            </section>
          </>
        )}
      </section>
      {!wide && panelShown && (
        <Overlay label="Panel" side="right" onClose={closePanel}>
          {panel}
        </Overlay>
      )}
    </div>
  );
}
