// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/ProjectFilePicker.logic.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// ported to the Loams UI stack. Loams' changes are Apache-2.0.
import type { ProjectEntry } from "@t3tools/contracts";
import { normalizeSearchQuery } from "@t3tools/shared/searchRanking";

export const PROJECT_FILE_PICKER_RESULT_LIMIT = 200;

export interface ProjectFilePickerMatch {
  readonly name: string;
  readonly nameMatchIndices: ReadonlyArray<number>;
  readonly path: string;
  readonly pathMatchIndices: ReadonlyArray<number>;
}

function fileName(path: string): string {
  return path.slice(path.lastIndexOf("/") + 1);
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
 * Maps server search results to picker rows. Server ordering is preserved —
 * ranking already happened there; this pass only filters to files and
 * computes highlight positions with the same query normalization the server
 * applied.
 */
export function getProjectFilePickerMatches(
  entries: ReadonlyArray<ProjectEntry>,
  rawQuery: string,
  limit = PROJECT_FILE_PICKER_RESULT_LIMIT,
): ProjectFilePickerMatch[] {
  if (limit <= 0) return [];

  const query = normalizeSearchQuery(rawQuery, {
    trimLeadingPattern: /^[@./]+/,
  }).replaceAll(/\s/g, "");
  const matches: ProjectFilePickerMatch[] = [];

  for (const entry of entries) {
    if (entry.kind !== "file") continue;

    const name = fileName(entry.path);
    const nameMatchIndices = findMatchIndices(name, query);
    const pathMatchIndices = findMatchIndices(entry.path, query);
    matches.push({
      name,
      nameMatchIndices: nameMatchIndices ?? [],
      path: entry.path,
      pathMatchIndices: pathMatchIndices ?? [],
    });
    if (matches.length >= limit) break;
  }

  return matches;
}
