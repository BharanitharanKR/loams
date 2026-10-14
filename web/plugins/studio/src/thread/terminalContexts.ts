// Terminal selections waiting for the composer (SD1 Task 22, §53 §5.7 "send selection
// to composer"). The terminal drawer adds one per "Add to composer"; the composer
// (SD1 Task 19) shows them as context chips and takes them when it sends. Kept per
// thread for the page's life, in memory only: a selection is terminal output, which
// is not written to storage.

import type { TerminalContextSelection } from '@loams/studio-ui';
import { useSyncExternalStore } from 'react';

/** At most this many waiting selections per thread; the oldest goes first. */
export const MAX_TERMINAL_CONTEXTS = 8;

const EMPTY: readonly TerminalContextSelection[] = Object.freeze([]);
const byThread = new Map<string, readonly TerminalContextSelection[]>();
const listeners = new Set<() => void>();

function changed(): void {
  for (const listener of [...listeners]) listener();
}

export function addTerminalContext(threadId: string, selection: TerminalContextSelection): void {
  const next = [...(byThread.get(threadId) ?? EMPTY), selection].slice(-MAX_TERMINAL_CONTEXTS);
  byThread.set(threadId, next);
  changed();
}

/** The waiting selections of a thread (the same array until it changes). */
export function terminalContexts(threadId: string): readonly TerminalContextSelection[] {
  return byThread.get(threadId) ?? EMPTY;
}

/** Removes and returns a thread's waiting selections (the composer, on send). */
export function takeTerminalContexts(threadId: string): readonly TerminalContextSelection[] {
  const taken = terminalContexts(threadId);
  if (byThread.delete(threadId)) changed();
  return taken;
}

export function removeTerminalContext(threadId: string, index: number): void {
  const list = byThread.get(threadId);
  if (!list?.[index]) return;
  const next = list.filter((_, i) => i !== index);
  if (next.length) byThread.set(threadId, next);
  else byThread.delete(threadId);
  changed();
}

export function useTerminalContexts(threadId: string): readonly TerminalContextSelection[] {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    () => terminalContexts(threadId),
  );
}
