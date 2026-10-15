// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/ProjectFilePicker.logic.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// over agentd's scored matches (highest score first, the daemon's order on ties) with normalizeSearchQuery from packages/shared/src/searchRanking.ts folded in. Loams' changes are Apache-2.0.
import type { FileMatch } from "./source";

export const PROJECT_FILE_PICKER_RESULT_LIMIT = 200;

export interface ProjectFilePickerMatch {
  readonly name: string;
  readonly nameMatchIndices: ReadonlyArray<number>;
  readonly path: string;
  readonly pathMatchIndices: ReadonlyArray<number>;
}

function normalizeSearchQuery(input: string, trimLeadingPattern: RegExp): string {
  const trimmed = input.trim();
  if (!trimmed) return "";
  return trimmed.replace(trimLeadingPattern, "").toLowerCase();
}

/**
 * First ordered subsequence of `query` inside `value`, as highlight indices.
 * Returns null when `value` does not contain the subsequence at all — the
 * entry still renders (the server matched it), just without highlights.
 */
function findMatchIndices(value: string, query: string): number[] | null {
  if (!query) return [];

  const normalizedValue = value.toLowerCase();
  const indices: number[] = [];
  let queryIndex = 0;

  for (let valueIndex = 0; valueIndex < normalizedValue.length; valueIndex += 1) {
    if (normalizedValue[valueIndex] !== query[queryIndex]) continue;
    indices.push(valueIndex);
    queryIndex += 1;
    if (queryIndex === query.length) return indices;
  }

  return null;
}

/**
 * Maps the daemon's search results to picker rows: files only, highest score first
 * (a stable sort, so ties keep the daemon's order), with highlight positions computed
 * with the same query normalization upstream applies.
 */
export function rankFileMatches(
  entries: ReadonlyArray<FileMatch>,
  rawQuery: string,
  limit = PROJECT_FILE_PICKER_RESULT_LIMIT,
): ProjectFilePickerMatch[] {
  if (limit <= 0) return [];

  const query = normalizeSearchQuery(rawQuery, /^[@./]+/).replaceAll(/\s/g, "");
  const matches: ProjectFilePickerMatch[] = [];

  const ranked = entries
    .filter((entry) => entry.kind !== "directory")
    .toSorted((left, right) => right.score - left.score);
  for (const entry of ranked) {
    const name = entry.path.slice(entry.path.lastIndexOf("/") + 1);
    matches.push({
      name,
      nameMatchIndices: findMatchIndices(name, query) ?? [],
      path: entry.path,
      pathMatchIndices: findMatchIndices(entry.path, query) ?? [],
    });
    if (matches.length >= limit) break;
  }

  return matches;
}
