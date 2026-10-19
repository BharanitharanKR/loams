// The application menu's shell commands (CF1 Task 14, D1084). The desktop's menu shows the
// D1081 chords without registering them; a click arrives as SHELL_COMMAND_EVENT (from
// `wireShellCommands`), and the shell runs it as it runs the chord. Studio's own commands
// (`chat.new`, `thread.next`) are Studio's listener's, so they are not handled here.
// Task 12's keymap folds `runMenuCommand` into its `run`, so a chord and a click share one path.

import { SHELL_COMMAND_EVENT } from '@loams/console-host';
import { useEffect, useRef } from 'react';

export const MENU_SHELL_COMMANDS = [
  'palette.toggle',
  'sidebar.toggle',
  'inspector.toggle',
  'drawer.toggle',
  'navigation.back',
  'navigation.forward',
  'settings.open',
  'thread.reopen',
] as const;
export type MenuShellCommand = (typeof MENU_SHELL_COMMANDS)[number];

const KNOWN = new Set<string>(MENU_SHELL_COMMANDS);

export interface MenuCommandHandlers {
  togglePalette(): void;
  toggleSidebar(): void;
  toggleInspector(): void;
  toggleDrawer(): void;
  openSettings(): void;
  /** ⌘⇧T's stack is Task 12's (W12); false when there is nothing to reopen or no stack yet. */
  reopenThread?(): boolean;
  back?(): void;
  forward?(): void;
}

/** Runs one menu command; true when the shell handled it. */
export function runMenuCommand(command: MenuShellCommand, h: MenuCommandHandlers): boolean {
  switch (command) {
    case 'palette.toggle':
      h.togglePalette();
      return true;
    case 'sidebar.toggle':
      h.toggleSidebar();
      return true;
    case 'inspector.toggle':
      h.toggleInspector();
      return true;
    case 'drawer.toggle':
      h.toggleDrawer();
      return true;
    case 'navigation.back':
      (h.back ?? (() => globalThis.history.back()))();
      return true;
    case 'navigation.forward':
      (h.forward ?? (() => globalThis.history.forward()))();
      return true;
    case 'settings.open':
      h.openSettings();
      return true;
    case 'thread.reopen':
      return h.reopenThread?.() ?? false;
  }
}

/** Listens for menu commands on `target` and runs the shell's; one taken elsewhere is left. */
export function useMenuCommands(h: MenuCommandHandlers, target: EventTarget = globalThis): void {
  const latest = useRef(h);
  latest.current = h;
  useEffect(() => {
    const onCommand = (event: Event) => {
      if (event.defaultPrevented) return;
      const detail = (event as CustomEvent<unknown>).detail;
      if (typeof detail !== 'string' || !KNOWN.has(detail)) return;
      if (runMenuCommand(detail as MenuShellCommand, latest.current)) event.preventDefault();
    };
    target.addEventListener(SHELL_COMMAND_EVENT, onCommand);
    return () => target.removeEventListener(SHELL_COMMAND_EVENT, onCommand);
  }, [target]);
}
