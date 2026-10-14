// Studio's commands and their keybindings (SD1 Task 24; §53 §5.10). Whatever can run a
// command registers a handler on the bus while it is mounted (the layout its panels,
// the view its routes); a key press resolves to a command through the keybinding model
// of @loams/studio-ui and runs it when a handler is there, and the palette lists the
// commands that have one. The shell keeps Mod+K, Mod+J and Mod+Shift+M in both modes.

import { SHELL_COMMAND_EVENT } from '@loams/console-host';
import {
  currentPlatform,
  type KeybindingCommand,
  KeybindingStore,
  resolveShortcutCommand,
  type ShortcutMatchContext,
} from '@loams/studio-ui';
import { useEffect, useRef } from 'react';

/** A handler for a keybinding command, or for one of the layout's own (`rightPanel.show`). */
export type CommandName = KeybindingCommand | 'rightPanel.show';

/** The handlers registered right now; the last one registered for a command wins. */
export class StudioCommands {
  readonly #handlers = new Map<string, (() => void)[]>();
  readonly #listeners = new Set<() => void>();
  #version = 0;

  register(command: CommandName, handler: () => void): () => void {
    const list = [...(this.#handlers.get(command) ?? []), handler];
    this.#handlers.set(command, list);
    this.#changed();
    return () => {
      const rest = (this.#handlers.get(command) ?? []).filter((h) => h !== handler);
      if (rest.length) this.#handlers.set(command, rest);
      else this.#handlers.delete(command);
      this.#changed();
    };
  }

  has(command: CommandName): boolean {
    return (this.#handlers.get(command)?.length ?? 0) > 0;
  }

  /** Runs the command; false when nothing handles it now. */
  run(command: CommandName): boolean {
    const handler = this.#handlers.get(command)?.at(-1);
    if (!handler) return false;
    handler();
    return true;
  }

  readonly getVersion = (): number => this.#version;

  readonly subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  };

  #changed(): void {
    this.#version++;
    for (const listener of [...this.#listeners]) listener();
  }
}

/**
 * The edition's shell commands (`SHELL_COMMAND_EVENT`; SD1 Task 28: the desktop menu's
 * "New thread"): Studio runs `chat.new` when a mounted view handles it, and says so
 * with `preventDefault()`; otherwise the sender opens Studio's home.
 */
export function listenShellCommands(
  commands: StudioCommands,
  target: EventTarget = globalThis,
): () => void {
  const onCommand = (event: Event) => {
    const detail = (event as CustomEvent<unknown>).detail;
    if (detail === 'chat.new' && commands.run('chat.new')) event.preventDefault();
  };
  target.addEventListener(SHELL_COMMAND_EVENT, onCommand);
  return () => target.removeEventListener(SHELL_COMMAND_EVENT, onCommand);
}

/** What Studio's keyboard layer needs: the bus, the bindings and the device. */
export interface StudioKeys {
  commands: StudioCommands;
  keybindings: KeybindingStore;
  platform: string;
  edition: 'desktop' | 'web';
}

export function createStudioKeys(
  o: { edition?: 'desktop' | 'web'; keybindings?: KeybindingStore; platform?: string } = {},
): StudioKeys {
  return {
    commands: new StudioCommands(),
    keybindings: o.keybindings ?? new KeybindingStore(),
    platform: o.platform ?? currentPlatform(),
    edition: o.edition ?? 'web',
  };
}

/** Registers `handler` for `command` while mounted; none (undefined) registers nothing. */
export function useStudioCommand(
  commands: StudioCommands,
  command: CommandName,
  handler: (() => void) | undefined,
): void {
  const latest = useRef(handler);
  latest.current = handler;
  const enabled = handler !== undefined;
  useEffect(() => {
    if (!enabled) return;
    return commands.register(command, () => latest.current?.());
  }, [commands, command, enabled]);
}

function isEditable(target: Element | null): boolean {
  if (!target) return false;
  if (target.closest('[contenteditable=""], [contenteditable="true"]')) return true;
  const tag = target.tagName;
  return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';
}

/** The focus half of the `when` context, read from where the key press happened. */
export function focusContext(target: EventTarget | null): Partial<ShortcutMatchContext> {
  const el = target instanceof Element ? target : null;
  return {
    terminalFocus: Boolean(el?.closest('[data-terminal]')),
    composerFocus: Boolean(el?.closest('[data-composer]')),
    editableFocus: isEditable(el),
  };
}

/**
 * Runs Studio's keybindings while mounted: a key press resolves against the current
 * bindings with the `when` context (focus, the open route, the edition) and runs the
 * command if something handles it. Presses inside a modal (the palette, a dialog) and
 * presses another handler already took are left alone.
 */
export function useStudioKeybindings(keys: StudioKeys, context: { threadOpen: boolean }): void {
  const ctx = useRef(context);
  ctx.current = context;
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.defaultPrevented) return;
      const target = e.target instanceof Element ? e.target : null;
      if (target?.closest('[aria-modal="true"], [data-slot="command-dialog"]')) return;
      const command = resolveShortcutCommand(e, keys.keybindings.resolved(), {
        platform: keys.platform,
        context: {
          ...focusContext(target),
          threadOpen: ctx.current.threadOpen,
          isDesktop: keys.edition === 'desktop',
          isWeb: keys.edition === 'web',
        },
      });
      if (!command || !keys.commands.has(command)) return;
      e.preventDefault();
      keys.commands.run(command);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [keys]);
}
