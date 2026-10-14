// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/search/HighlightedSearchLine.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// match ranges found in the line from the query (agentd's match carries only its column); the line is plain text, not syntax highlighted. Loams' changes are Apache-2.0.
import { memo, useMemo } from "react";

import type { ContentSearchQuery } from "../source";

interface Range {
  readonly start: number;
  readonly end: number;
}

interface Segment {
  readonly content: string;
  readonly isMatch: boolean;
  readonly start: number;
  readonly end: number;
}

const MAX_RANGES = 50;

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Where the query matches in the line; none when the pattern does not compile here. */
export function matchRanges(
  line: string,
  query: Pick<ContentSearchQuery, "query" | "caseSensitive" | "regex">,
): Range[] {
  if (!query.query) return [];
  let pattern: RegExp;
  try {
    pattern = new RegExp(
      query.regex ? query.query : escapeRegExp(query.query),
      query.caseSensitive ? "g" : "gi",
    );
  } catch {
    return [];
  }
  const ranges: Range[] = [];
  for (const match of line.matchAll(pattern)) {
    if (match[0].length === 0) continue;
    ranges.push({ start: match.index, end: match.index + match[0].length });
    if (ranges.length >= MAX_RANGES) break;
  }
  return ranges;
}

function splitLine(line: string, ranges: ReadonlyArray<Range>): Segment[] {
  const segments: Segment[] = [];
  let cursor = 0;
  for (const range of ranges) {
    if (range.start > cursor) {
      segments.push({
        content: line.slice(cursor, range.start),
        isMatch: false,
        start: cursor,
        end: range.start,
      });
    }
    segments.push({
      content: line.slice(range.start, range.end),
      isMatch: true,
      start: range.start,
      end: range.end,
    });
    cursor = range.end;
  }
  if (cursor < line.length) {
    segments.push({ content: line.slice(cursor), isMatch: false, start: cursor, end: line.length });
  }
  return segments;
}

export const HighlightedSearchLine = memo(function HighlightedSearchLine(props: {
  readonly line: string;
  readonly query: Pick<ContentSearchQuery, "query" | "caseSensitive" | "regex">;
}) {
  const segments = useMemo(
    () => splitLine(props.line, matchRanges(props.line, props.query)),
    [props.line, props.query],
  );
  return segments.map((segment) =>
    segment.isMatch ? (
      <mark
        className="rounded-sm bg-accent-soft text-ink"
        key={`${segment.start}:${segment.end}:match`}
      >
        {segment.content}
      </mark>
    ) : (
      <span key={`${segment.start}:${segment.end}:code`}>{segment.content}</span>
    ),
  );
});
