// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/chat/timelineScrollAnchoring.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the scroll modes, the follow re-arm band (from MessagesTimeline.logic.ts) and the remembered per-thread position, over a plain scroll element's metrics instead of LegendList's; plus the prepend anchor. Loams' changes are Apache-2.0.

// Stick to the bottom while the agent writes; stay put while the person reads
// (SD1 Task 18). The timeline follows the end only while it is within the re-arm band
// of the bottom. When older history is prepended, the first visible row keeps its
// place on screen: its offset from the viewport's top is captured before the update
// and restored after it.

export type TimelineScrollMode = "following-end" | "free-scrolling";

/**
 * The follow re-arm band: strict "at end" flickers false for a frame while
 * streaming content grows under the viewport, so follow re-arms within this
 * distance of the real content bottom instead.
 */
export const TIMELINE_FOLLOW_REARM_THRESHOLD_PX = 40;

export interface ScrollMetrics {
  readonly scrollTop: number;
  readonly scrollHeight: number;
  readonly clientHeight: number;
}

export function resolveTimelineIsAtEnd(metrics: ScrollMetrics): boolean {
  return (
    metrics.scrollHeight - metrics.scrollTop - metrics.clientHeight <=
    TIMELINE_FOLLOW_REARM_THRESHOLD_PX
  );
}

/** A row and how far its top sits below the viewport's top. */
export interface TimelineAnchor {
  readonly rowId: string;
  readonly offset: number;
}

/**
 * The first row whose bottom is below the viewport's top, from each row's top in
 * content coordinates (in order).
 */
export function captureTimelineAnchor(
  rows: ReadonlyArray<{ id: string; top: number; height: number }>,
  scrollTop: number,
): TimelineAnchor | null {
  for (const row of rows) {
    if (row.top + row.height > scrollTop) return { rowId: row.id, offset: row.top - scrollTop };
  }
  return null;
}

/** The scrollTop that puts `anchor`'s row back where it was, given its new top. */
export function restoreTimelineAnchor(anchor: TimelineAnchor, newTop: number): number {
  return Math.max(0, newTop - anchor.offset);
}

/** Whether rows were added before the first row the person had (older history). */
export function rowsWerePrepended(
  previous: ReadonlyArray<{ id: string }>,
  next: ReadonlyArray<{ id: string }>,
): boolean {
  const first = previous[0];
  if (!first || next.length <= previous.length) return false;
  const at = next.findIndex((row) => row.id === first.id);
  return at > 0;
}

export interface RememberedTimelinePosition {
  readonly anchor: TimelineAnchor | null;
  readonly atEnd: boolean;
}

// Bound the session cache.
const rememberedTimelinePositions = new Map<string, RememberedTimelinePosition>();

export function readTimelinePosition(threadKey: string) {
  return rememberedTimelinePositions.get(threadKey);
}

export function rememberTimelinePosition(threadKey: string, position: RememberedTimelinePosition) {
  rememberedTimelinePositions.delete(threadKey);
  rememberedTimelinePositions.set(threadKey, position);
  if (rememberedTimelinePositions.size > 100) {
    const oldest = rememberedTimelinePositions.keys().next().value;
    if (oldest !== undefined) rememberedTimelinePositions.delete(oldest);
  }
}
