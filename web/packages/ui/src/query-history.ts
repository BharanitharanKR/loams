// Recent queries per scope (a namespace, a database), kept in this browser's localStorage. Storage
// can be missing or throw (private windows, blocked site data): the history then lives only as
// long as the page, and nothing else breaks.

import { useCallback, useEffect, useState } from 'react';

export interface HistoryItem {
  text: string;
  /** Epoch milliseconds of the last run. */
  at: number;
  ok: boolean;
  elapsedMs?: number;
  rows?: number;
}

const PREFIX = 'loams.query-history.';
export const HISTORY_LIMIT = 30;

const memory = new Map<string, HistoryItem[]>();

export function readHistory(scope: string): HistoryItem[] {
  try {
    const raw = globalThis.localStorage?.getItem(PREFIX + scope);
    if (raw) {
      const list = JSON.parse(raw) as unknown;
      if (Array.isArray(list))
        return list.filter(
          (x): x is HistoryItem =>
            typeof x === 'object' && x !== null && typeof (x as HistoryItem).text === 'string',
        );
    }
  } catch {
    // fall through to the in-memory copy
  }
  return memory.get(scope) ?? [];
}

function writeHistory(scope: string, list: HistoryItem[]) {
  memory.set(scope, list);
  try {
    globalThis.localStorage?.setItem(PREFIX + scope, JSON.stringify(list));
  } catch {
    // the in-memory copy stands
  }
}

/** Adds a run to the top, dropping an earlier copy of the same text. */
export function pushHistory(scope: string, item: HistoryItem): HistoryItem[] {
  const text = item.text.trim();
  if (!text) return readHistory(scope);
  const list = [
    { ...item, text },
    ...readHistory(scope).filter((h) => h.text.trim() !== text),
  ].slice(0, HISTORY_LIMIT);
  writeHistory(scope, list);
  return list;
}

export function clearHistory(scope: string) {
  writeHistory(scope, []);
}

/** The history of one scope, and functions to add to and clear it. */
export function useQueryHistory(scope: string) {
  const [items, setItems] = useState<HistoryItem[]>(() => readHistory(scope));
  useEffect(() => setItems(readHistory(scope)), [scope]);
  const push = useCallback((item: HistoryItem) => setItems(pushHistory(scope, item)), [scope]);
  const clear = useCallback(() => {
    clearHistory(scope);
    setItems([]);
  }, [scope]);
  return { items, push, clear };
}
