// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/CommandPalette.logic.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/ui/LICENSE.t3code). Modified by The Loams Authors:
// the ranking of CommandPalette.logic.ts only, as plain TypeScript over CommandItem; no Effect, React or contracts. Loams' changes are Apache-2.0.

// The palette's ranking (SD1 Task 24, inventory (h)): T3 Code's `filterCommandPaletteGroups`
// and its two rank helpers, kept rule for rule so the console's palette orders results the
// way T3 Code's does, over @loams/ui's `CommandItem` instead of upstream's item type.
//
// - Every word of the query must appear in the row's search terms (its text, then its
//   keywords, in that order of importance).
// - The first term that matches decides: exact (3) > prefix (2) > substring (1) > only the
//   words (0), less 100 for each term before it.
// - Rows with a `recency` (threads) share one tier on their title, so the newest wins a tie,
//   unless the title is an exact match.
// - `secondary` rows sort after every other match; ties keep the given order.
// - Groups keep the order they first appear in, and rows are ranked within their group.
//
// Upstream's `>` actions-only prefix, submenus and directory browsing are not carried over.

import type { CommandItem } from './command';

/** A palette row the ranking can order: upstream's `searchRecency` and `secondary`. */
export interface RankableCommand extends Pick<CommandItem, 'id' | 'text' | 'keywords' | 'group'> {
  /** When the row's subject last changed (threads); larger is newer. */
  recency?: number;
  /** Sorts after every other match in its group (settings-like rows). */
  secondary?: boolean;
}

/** Upstream's `normalizeSearchText` (lib/utils.ts): accents folded, lower case, spaces collapsed. */
export function normalizeSearchText(value: string): string {
  return value.normalize('NFKD').replace(/\p{M}/gu, '').toLowerCase().replace(/\s+/g, ' ').trim();
}

function rankSearchFieldMatch(
  field: string,
  normalizedQuery: string,
  queryTokens: readonly string[],
): number {
  const normalizedField = normalizeSearchText(field);
  if (
    normalizedField.length === 0 ||
    !queryTokens.every((token) => normalizedField.includes(token))
  ) {
    return Number.NEGATIVE_INFINITY;
  }
  if (normalizedField === normalizedQuery) return 3;
  if (normalizedField.startsWith(normalizedQuery)) return 2;
  if (normalizedField.includes(normalizedQuery)) return 1;
  return 0;
}

function searchTerms(item: RankableCommand): string[] {
  return [item.text, ...(item.keywords ?? [])].filter((term) => term.length > 0);
}

/** Upstream's `rankCommandPaletteItemMatch`. */
export function rankCommandMatch(
  item: RankableCommand,
  normalizedQuery: string,
  queryTokens: readonly string[],
): number {
  const terms = searchTerms(item);
  if (terms.length === 0) return 0;
  for (const [index, field] of terms.entries()) {
    const fieldRank = rankSearchFieldMatch(field, normalizedQuery, queryTokens);
    if (fieldRank !== Number.NEGATIVE_INFINITY) {
      if (index === 0 && item.recency !== undefined) {
        // All non-exact thread title matches share a tier so recency breaks the tie.
        return 1_000 + Number(fieldRank === 3);
      }
      return 1_000 - index * 100 + fieldRank;
    }
  }
  return 0;
}

/**
 * Filters and orders `items` for `query` (upstream's `filterCommandPaletteGroups`, with
 * groups read from each row's `group`). An empty query returns the rows as given.
 */
export function rankCommands<T extends RankableCommand>(items: readonly T[], query: string): T[] {
  const normalizedQuery = normalizeSearchText(query);
  if (normalizedQuery.length === 0) return [...items];
  const queryTokens = normalizedQuery.split(' ');

  const groups = new Map<string, { item: T; index: number; rank: number }[]>();
  for (const [index, item] of items.entries()) {
    const haystack = normalizeSearchText(searchTerms(item).join(' '));
    if (!queryTokens.every((token) => haystack.includes(token))) continue;
    const key = item.group ?? '';
    const bucket = groups.get(key) ?? [];
    bucket.push({ item, index, rank: rankCommandMatch(item, normalizedQuery, queryTokens) });
    groups.set(key, bucket);
  }

  return [...groups.values()].flatMap((bucket) =>
    bucket
      .sort(
        (left, right) =>
          Number(left.item.secondary ?? false) - Number(right.item.secondary ?? false) ||
          right.rank - left.rank ||
          (right.item.recency ?? 0) - (left.item.recency ?? 0) ||
          left.index - right.index,
      )
      .map((entry) => entry.item),
  );
}
