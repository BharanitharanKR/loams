// The console layout (design 59, D1078): the frame (frame.tsx) holds the sidebar
// (sidebar.tsx), the top bar, which is the window's drag region, and the main region; the
// agent dock stays on the right of main until Task 8 turns it into the inspector (D666).
// Below 960px the sidebar is a sheet. Components get props and closures, never `ctx`.

import {
  capabilities,
  type FlagsService,
  localThemeStore,
  type Mode,
  type ModeService,
  type SessionService,
  ThemeController,
  type ThemeService,
} from '@loams/console-host';
import { Slot, useSlot } from '@loams/slots';
import { Toaster, ToastProvider, TooltipProvider } from '@loams/ui';
import { PanelRight } from 'lucide-react';
import {
  type KeyboardEvent,
  type PointerEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from 'react';
import { AppearanceMenu } from './appearance.js';
import { EngineTooOld } from './engine.js';
import { MainRegion, Outlet, TopBar, UserMenu, useNarrow, useSidebarState } from './frame.js';
import { MODE_LABEL, ModeSwitcher, useMode, useModeShortcut } from './mode-switcher.js';
import { navSections, useLocation } from './nav.js';
import { type ScopedCommand, ShellPalette } from './palette.js';
import type { HashRouter } from './router.js';
import { ShellSidebar } from './sidebar.js';

export {
  inMode,
  LEGACY,
  type ModeFilter,
  type NavItem,
  navSections,
  SECTIONS,
  STUDIO_SECTIONS,
} from './nav.js';

export const DOCK_MIN = 320;
export const DOCK_MAX = 720;
export const DOCK_DEFAULT = 400;
export const DOCK_KEY = 'loams.shell.dock';
const DOCK_STEP = 24;

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

/**
 * Sends a visitor without a session to sign-in (or a fresh install to setup), as the
 * original console's session gate did. Public pages (sign-in, setup) stay put.
 */
function useGate(router: HashRouter, session: SessionService) {
  const location = useLocation(router);
  const gate = useSyncExternalStore(session.subscribe, () => session.gate?.() ?? 'none');
  const isPublic = router.spec()?.public === true;
  const pending =
    location.pageId !== undefined && !isPublic && (gate === 'signed-out' || gate === 'setup');
  useEffect(() => {
    if (!pending) return;
    if (gate === 'setup') return router.navigate('/setup');
    const here = `${location.path}${location.query ? `?${new URLSearchParams(location.query)}` : ''}`;
    router.navigate(here === '/' ? '/sign-in' : `/sign-in?next=${encodeURIComponent(here)}`);
  }, [pending, gate, location, router]);
  return pending;
}

/** Ctrl or Cmd, as the palette shows a shortcut's modifier. */
function modKeys(...keys: string[]): string[] {
  let mac = false;
  try {
    mac = /mac|iphone|ipad|ipod/i.test(navigator.platform);
  } catch {
    // No navigator (tests, workers): the Ctrl spelling.
  }
  return mac ? ['⌘', ...keys.map((k) => (k === 'Shift' ? '⇧' : k))] : ['Ctrl', ...keys];
}

/**
 * The palette with the shell's own rows (SD1 Task 24): "Go to" for each mode's nav
 * entries, and the general commands (switch mode, theme, agent panel).
 */
function Palette({
  router,
  mode,
  theme,
  dock,
  open,
  onOpenChange,
}: {
  router: HashRouter;
  mode?: ModeService;
  theme: ThemeService;
  dock?: { open: boolean; toggle: () => void };
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  const sections = useSlot('shell.nav.section');
  const legacy = useSlot('console.nav');
  const current = useMode(mode);
  const modesOf = (id: string) => mode?.modesOf(id) ?? ['console'];
  const nav = (m: Mode): ScopedCommand[] => {
    const seen = new Set<string>();
    return navSections(sections, legacy, mode ? { mode: m, modesOf } : undefined).flatMap(
      ([group, items]) =>
        items
          .filter((i) => !seen.has(i.href) && seen.add(i.href))
          .map((i) => ({
            id: `go:${i.href}`,
            title: `Go to ${i.label}`,
            group: 'Go to',
            keywords: [group, i.href],
            mode: m,
            run: () => router.navigate(i.href),
          })),
    );
  };
  const other = mode?.available.find((m) => m !== current);
  const general: ScopedCommand[] = [
    ...(mode && other
      ? [
          {
            id: 'mode',
            title: `Switch to ${MODE_LABEL[other]}`,
            group: 'General',
            keywords: ['mode'],
            shortcut: modKeys('Shift', 'M'),
            mode: current,
            run: () => mode.set(other),
          },
        ]
      : []),
    ...(['system', 'dark', 'light'] as const).map((c) => ({
      id: `theme:${c}`,
      title: `Theme: ${c === 'system' ? 'System' : c === 'dark' ? 'Dark' : 'Light'}`,
      group: 'Appearance',
      keywords: ['appearance', 'theme', 'colour', 'dark mode'],
      mode: current,
      run: () => theme.set(c),
    })),
    ...(dock
      ? [
          {
            id: 'dock',
            title: dock.open ? 'Hide agent panel' : 'Show agent panel',
            group: 'General',
            keywords: ['agent', 'dock'],
            shortcut: modKeys('J'),
            mode: current,
            run: dock.toggle,
          },
        ]
      : []),
  ];
  return (
    <ShellPalette
      modes={mode ? { current, available: mode.available, modesOf } : undefined}
      nav={nav}
      general={general}
      open={open}
      onOpenChange={onOpenChange}
    />
  );
}

export function Layout({
  router,
  session,
  flags,
  mode,
  theme: given,
}: {
  router: HashRouter;
  session: SessionService;
  /** GetInstance's answer: the instance name, edition and version for the chrome. */
  flags?: FlagsService;
  /** Console or Studio (§53 §4.2): filters the nav and the header, and drives the switcher. */
  mode?: ModeService;
  /** System, Dark or Light (D1074); the platform provides it, and tests may leave it out. */
  theme?: ThemeService;
}) {
  const current = useMode(mode);
  useModeShortcut(mode);
  const hasDock = useSlot('shell.dock.right').length > 0;
  const [dock, setDock] = useDock();
  const theme = useMemo(() => given ?? new ThemeController(localThemeStore()), [given]);
  const [side, setSide] = useSidebarState();
  const narrow = useNarrow();
  const [sheetOpen, setSheetOpen] = useState(false);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const sidebarOpen = narrow ? sheetOpen : side.open;
  const toggleSidebar = useCallback(
    () => (narrow ? setSheetOpen((v) => !v) : setSide({ open: !side.open })),
    [narrow, side.open, setSide],
  );
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

  // biome-ignore lint/correctness/useExhaustiveDependencies: close the sheet per route
  useEffect(() => setSheetOpen(false), [location.path]);

  // Mod+B toggles the sidebar (D1078), except where the key is text editing's: a field,
  // rich text (bold) or a terminal. Task 12 folds this into the shell's one keymap.
  useEffect(() => {
    const onKey = (e: globalThis.KeyboardEvent) => {
      if (!(e.ctrlKey || e.metaKey) || e.altKey || e.shiftKey) return;
      if (e.key.toLowerCase() !== 'b' && e.code !== 'KeyB') return;
      if (keepsKeys(e.target)) return;
      e.preventDefault();
      toggleSidebar();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [toggleSidebar]);

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

  const gated = useGate(router, session);
  // An engine older than N−1, or without GetInstance, gets the upgrade page
  // instead of pages that would fail (§54 §13.1).
  const caps = flags ? capabilities(flags) : undefined;
  if (caps && !caps.supported) return <EngineTooOld caps={caps} />;
  if (gated) return <div className="lc-boot" aria-busy="true" />;
  if (router.spec()?.bare) {
    return (
      <ToastProvider>
        <main className="lc-bare" id="lc-main">
          <Outlet router={router} />
        </main>
        <Toaster />
      </ToastProvider>
    );
  }
  return (
    <ToastProvider>
      <TooltipProvider>
        <div
          className="lc-shell"
          data-mode={current}
          data-sidebar={sidebarOpen ? 'open' : 'closed'}
          style={{ ['--lc-dock-width' as string]: `${dock.width}px` }}
        >
          <ShellSidebar
            router={router}
            flags={flags}
            mode={mode}
            session={session}
            width={side.width}
            onWidth={(w) => setSide({ width: w })}
            open={sidebarOpen}
            narrow={narrow}
            onClose={() => (narrow ? setSheetOpen(false) : setSide({ open: false }))}
          />
          <div className="lc-body">
            <TopBar
              router={router}
              flags={flags}
              mode={mode}
              sidebarOpen={sidebarOpen}
              onToggleSidebar={toggleSidebar}
              onOpenPalette={() => setPaletteOpen(true)}
              tools={
                <>
                  {mode && <ModeSwitcher mode={mode} />}
                  <AppearanceMenu theme={theme} />
                  <UserMenu session={session} router={router} local={flags?.features.local} />
                </>
              }
              trailing={
                hasDock && (
                  <button
                    type="button"
                    ref={toggleRef}
                    className="lc-bar-btn"
                    aria-expanded={open}
                    aria-controls="lc-dock"
                    aria-keyshortcuts="Control+J Meta+J"
                    aria-label="Agent"
                    title="Agent (Ctrl/Cmd+J)"
                    onClick={toggle}
                  >
                    <PanelRight aria-hidden="true" size={16} strokeWidth={1.75} />
                  </button>
                )
              }
            />
            <div className="lc-work">
              <div className="lc-work-row">
                <main className="lc-main" id="lc-main">
                  <MainRegion router={router} mode={mode} />
                </main>
                {/* The agent dock stays here until Task 8 turns it into the inspector. */}
                {hasDock && mounted && (
                  <aside
                    className="lc-dock"
                    id="lc-dock"
                    aria-label="Agent panel"
                    ref={dockRef}
                    tabIndex={-1}
                    hidden={!open}
                  >
                    <DockHandle
                      width={dock.width}
                      onWidth={(w) => setDock({ width: clampWidth(w) })}
                    />
                    <div className="lc-dock-body">
                      <Slot name="shell.dock.right" props={{}} />
                    </div>
                  </aside>
                )}
              </div>
            </div>
          </div>
        </div>
        <Palette
          router={router}
          mode={mode}
          theme={theme}
          dock={hasDock ? { open, toggle } : undefined}
          open={paletteOpen}
          onOpenChange={setPaletteOpen}
        />
        <Toaster />
      </TooltipProvider>
    </ToastProvider>
  );
}

/** A field, rich text or a terminal keeps Mod+B (bold, or the shell program's key). */
function keepsKeys(target: EventTarget | null): boolean {
  if (!(target instanceof Element)) return false;
  return (
    target.closest(
      'input, textarea, select, [contenteditable]:not([contenteditable="false"]), .xterm',
    ) !== null
  );
}
