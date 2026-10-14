// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/keybindings.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the when-clause evaluation, resolution and shortcut labels kept; the terminal, thread-jump, model-picker and project-script helpers dropped. Loams' changes are Apache-2.0.

// Resolving a key press to a Studio command (SD1 Task 24): the last binding whose
// `when` clause holds and whose chord matches wins, as upstream. The context flags
// are Studio's: where focus is (a terminal, an editable field, the composer), whether
// a thread is open, and the edition. Unknown flags are false.

import {
  isMacPlatform,
  type KeybindingCommand,
  type KeybindingShortcut,
  type KeybindingWhenNode,
  matchesKeybindingShortcut,
  type ResolvedKeybindingsConfig,
  type ShortcutEventLike,
} from "./shared";

export interface ShortcutMatchContext {
  terminalFocus: boolean;
  /** A text field, textarea, select or rich-text editor owns the keyboard. */
  editableFocus: boolean;
  composerFocus: boolean;
  /** A thread's route is open (its panels exist). */
  threadOpen: boolean;
  isWeb: boolean;
  isDesktop: boolean;
  [key: string]: boolean;
}

export interface ShortcutMatchOptions {
  platform?: string;
  context?: Partial<ShortcutMatchContext>;
}

/** The browser's platform string, or "" where there is none (tests, workers). */
export function currentPlatform(): string {
  try {
    return typeof navigator === "undefined" ? "" : (navigator.platform ?? "");
  } catch {
    return "";
  }
}

function resolvePlatform(options: ShortcutMatchOptions | undefined): string {
  return options?.platform ?? currentPlatform();
}

function resolveContext(options: ShortcutMatchOptions | undefined): ShortcutMatchContext {
  return {
    terminalFocus: false,
    editableFocus: false,
    composerFocus: false,
    threadOpen: false,
    isWeb: true,
    isDesktop: false,
    ...options?.context,
  } as ShortcutMatchContext;
}

export function evaluateWhenNode(node: KeybindingWhenNode, context: ShortcutMatchContext): boolean {
  switch (node.type) {
    case "identifier":
      if (node.name === "true") return true;
      if (node.name === "false") return false;
      return Boolean(context[node.name]);
    case "not":
      return !evaluateWhenNode(node.node, context);
    case "and":
      return evaluateWhenNode(node.left, context) && evaluateWhenNode(node.right, context);
    case "or":
      return evaluateWhenNode(node.left, context) || evaluateWhenNode(node.right, context);
  }
}

function matchesWhenClause(
  whenAst: KeybindingWhenNode | undefined,
  context: ShortcutMatchContext,
): boolean {
  if (!whenAst) return true;
  return evaluateWhenNode(whenAst, context);
}

export function shortcutConflictKey(
  shortcut: KeybindingShortcut,
  platform = currentPlatform(),
): string {
  const useMetaForMod = isMacPlatform(platform);
  const metaKey = shortcut.metaKey || (shortcut.modKey && useMetaForMod);
  const ctrlKey = shortcut.ctrlKey || (shortcut.modKey && !useMetaForMod);
  return [
    shortcut.key,
    metaKey ? "meta" : "",
    ctrlKey ? "ctrl" : "",
    shortcut.shiftKey ? "shift" : "",
    shortcut.altKey ? "alt" : "",
  ].join("|");
}

export function effectiveShortcutsForCommand(
  keybindings: ResolvedKeybindingsConfig,
  command: KeybindingCommand,
  options?: ShortcutMatchOptions,
): KeybindingShortcut[] {
  const platform = resolvePlatform(options);
  const context = resolveContext(options);
  const claimedShortcuts = new Set<string>();
  const effective: KeybindingShortcut[] = [];

  for (let index = keybindings.length - 1; index >= 0; index -= 1) {
    const binding = keybindings[index];
    if (!binding) continue;
    if (!matchesWhenClause(binding.whenAst, context)) continue;

    const conflictKey = shortcutConflictKey(binding.shortcut, platform);
    if (claimedShortcuts.has(conflictKey)) {
      continue;
    }

    claimedShortcuts.add(conflictKey);
    if (binding.command === command) {
      effective.push(binding.shortcut);
    }
  }

  return effective;
}

function findEffectiveShortcutForCommand(
  keybindings: ResolvedKeybindingsConfig,
  command: KeybindingCommand,
  options?: ShortcutMatchOptions,
): KeybindingShortcut | null {
  return effectiveShortcutsForCommand(keybindings, command, options)[0] ?? null;
}

export function resolveShortcutCommand(
  event: ShortcutEventLike,
  keybindings: ResolvedKeybindingsConfig,
  options?: ShortcutMatchOptions,
): KeybindingCommand | null {
  const platform = resolvePlatform(options);
  const context = resolveContext(options);

  for (let index = keybindings.length - 1; index >= 0; index -= 1) {
    const binding = keybindings[index];
    if (!binding) continue;
    if (!matchesWhenClause(binding.whenAst, context)) continue;
    if (!matchesKeybindingShortcut(event, binding.shortcut, platform)) continue;
    return binding.command;
  }
  return null;
}

export function formatShortcutKeyLabel(key: string): string {
  if (key === " ") return "Space";
  if (key.length === 1) return key.toUpperCase();
  if (key === "escape") return "Esc";
  if (key === "arrowup") return "Up";
  if (key === "arrowdown") return "Down";
  if (key === "arrowleft") return "Left";
  if (key === "arrowright") return "Right";
  return key.slice(0, 1).toUpperCase() + key.slice(1);
}

/**
 * A shortcut's keys, one label each, for `<Shortcut keys>` (Loams): ⌃ ⌥ ⇧ ⌘ on macOS,
 * Ctrl Alt Shift Meta elsewhere, in upstream's order.
 */
export function shortcutKeys(shortcut: KeybindingShortcut, platform = currentPlatform()): string[] {
  const keyLabel = formatShortcutKeyLabel(shortcut.key);
  const useMetaForMod = isMacPlatform(platform);
  const showMeta = shortcut.metaKey || (shortcut.modKey && useMetaForMod);
  const showCtrl = shortcut.ctrlKey || (shortcut.modKey && !useMetaForMod);
  const keys: string[] = [];
  if (useMetaForMod) {
    if (showCtrl) keys.push("⌃");
    if (shortcut.altKey) keys.push("⌥");
    if (shortcut.shiftKey) keys.push("⇧");
    if (showMeta) keys.push("⌘");
  } else {
    if (showCtrl) keys.push("Ctrl");
    if (shortcut.altKey) keys.push("Alt");
    if (shortcut.shiftKey) keys.push("Shift");
    if (showMeta) keys.push("Meta");
  }
  keys.push(keyLabel);
  return keys;
}

export function formatShortcutLabel(
  shortcut: KeybindingShortcut,
  platform = currentPlatform(),
): string {
  const keys = shortcutKeys(shortcut, platform);
  return isMacPlatform(platform) ? keys.join("") : keys.join("+");
}

export function shortcutLabelForCommand(
  keybindings: ResolvedKeybindingsConfig,
  command: KeybindingCommand | null,
  options?: string | ShortcutMatchOptions,
): string | null {
  if (command === null) return null;
  const resolvedOptions = typeof options === "string" ? { platform: options } : options;
  const platform = resolvePlatform(resolvedOptions);
  const shortcut = findEffectiveShortcutForCommand(keybindings, command, resolvedOptions);
  return shortcut ? formatShortcutLabel(shortcut, platform) : null;
}

/** The keys of a command's effective shortcut, for the palette; null when it has none. */
export function shortcutKeysForCommand(
  keybindings: ResolvedKeybindingsConfig,
  command: KeybindingCommand,
  options?: ShortcutMatchOptions,
): string[] | null {
  const shortcut = findEffectiveShortcutForCommand(keybindings, command, options);
  return shortcut ? shortcutKeys(shortcut, resolvePlatform(options)) : null;
}
