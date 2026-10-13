// The Console | Studio switcher in the header (design §53 §4.2, D900; SD1 Task 3):
// a segmented control shown only when both modes are available, with Mod+Shift+M.
// `syncModeWithRoute` makes a route of the other mode switch the mode first.

import { type Mode, type ModeService, modeOfPath } from '@loams/console-host';
import { Segmented } from '@loams/ui';
import { LayoutDashboard, SquareTerminal } from 'lucide-react';
import { useEffect, useSyncExternalStore } from 'react';
import type { HashRouter } from './router.js';

export const MODE_LABEL: Record<Mode, string> = { console: 'Console', studio: 'Studio' };

/** The current mode, re-rendering on a switch. */
export function useMode(mode: ModeService | undefined): Mode {
  return useSyncExternalStore(
    (listener) => mode?.onChange(listener) ?? (() => {}),
    () => mode?.current ?? 'console',
  );
}

/** The other available mode, if there is one. */
function other(mode: ModeService): Mode | undefined {
  return mode.available.find((m) => m !== mode.current);
}

/** Mod+Shift+M switches to the other mode (§53 §4.2). */
export function useModeShortcut(mode: ModeService | undefined): void {
  useEffect(() => {
    if (!mode) return;
    const onKey = (e: KeyboardEvent) => {
      if (!(e.ctrlKey || e.metaKey) || !e.shiftKey || e.altKey) return;
      if (e.key.toLowerCase() !== 'm') return;
      const next = other(mode);
      if (!next) return;
      e.preventDefault();
      mode.set(next);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [mode]);
}

const ICON = { console: LayoutDashboard, studio: SquareTerminal } as const;

export function ModeSwitcher({ mode }: { mode: ModeService }) {
  const current = useMode(mode);
  if (mode.available.length < 2) return null;
  return (
    <Segmented
      label="Mode"
      className="lc-mode"
      keyShortcuts="Control+Shift+M Meta+Shift+M"
      value={current}
      onValueChange={(m) => mode.set(m)}
      options={mode.available.map((m) => {
        const Icon = ICON[m];
        return {
          value: m,
          label: MODE_LABEL[m],
          title: `${MODE_LABEL[m]} (Ctrl/Cmd+Shift+M)`,
          icon: <Icon size={14} strokeWidth={1.75} aria-hidden="true" />,
        };
      })}
    />
  );
}

/**
 * Opening a route of the other mode switches the mode first (§53 §4.4), keeping the
 * route. A mode that is not available (Studio without its flag) is left alone: the
 * route then shows the shell's "Nothing here".
 */
export function syncModeWithRoute(router: HashRouter, mode: ModeService): () => void {
  const sync = () => {
    const location = router.current();
    const wanted = modeOfPath(location.path);
    if (wanted === mode.current || !mode.available.includes(wanted)) return;
    const raw = router.raw();
    mode.set(wanted, { navigate: raw });
  };
  sync();
  return router.subscribe(sync);
}
