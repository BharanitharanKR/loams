// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// packages/client-runtime/src/state/threadSort.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the timestamp and recency sorts kept over agentd's millisecond times; the settled ordering and the fractional pin-order keys dropped (agentd places a pin before or after a neighbour); no Effect. Loams' changes are Apache-2.0.

// Thread ordering (SD1 Task 17): upstream's recency rules, over agentd's times. A
// thread's "latest user message" is its last message (`Chat.last_message_at_ms`), else
// its update or creation time. Upstream's fractional pin-order keys are not carried
// over: agentd keeps the pinned order itself and places a pin before or after a
// neighbour (`ChangeSidebarPin`), so a drag sends one relative position.

export type SidebarThreadSortOrder = "updated_at" | "created_at";

export interface ThreadSortInput {
  /** Milliseconds since the epoch. */
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly latestUserMessageAt?: number | null;
}

/** A finite millisecond time, or null. Upstream parsed ISO strings; agentd sends numbers. */
export function toSortableTimestamp(value: number | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  return Number.isFinite(value) ? value : null;
}

function getFirstSortableTimestamp(...values: Array<number | null | undefined>): number | null {
  for (const value of values) {
    const timestamp = toSortableTimestamp(value);
    if (timestamp !== null) {
      return timestamp;
    }
  }

  return null;
}

function getLatestUserMessageTimestamp(thread: ThreadSortInput): number {
  const latest = toSortableTimestamp(thread.latestUserMessageAt);
  if (latest !== null) return latest;
  return getFirstSortableTimestamp(thread.updatedAt, thread.createdAt) ?? Number.NEGATIVE_INFINITY;
}

export function getThreadSortTimestamp(
  thread: ThreadSortInput,
  sortOrder: SidebarThreadSortOrder,
): number {
  if (sortOrder === "created_at") {
    return (
      getFirstSortableTimestamp(thread.createdAt, thread.updatedAt) ?? Number.NEGATIVE_INFINITY
    );
  }
  return getLatestUserMessageTimestamp(thread);
}

export function sortThreads<T extends { readonly id: string } & ThreadSortInput>(
  threads: readonly T[],
  sortOrder: SidebarThreadSortOrder,
): T[] {
  if (threads.length < 2) return [...threads];
  return threads
    .map((thread) => ({ thread, timestamp: getThreadSortTimestamp(thread, sortOrder) }))
    .sort(
      (left, right) =>
        right.timestamp - left.timestamp ||
        (left.thread.id < right.thread.id ? 1 : left.thread.id > right.thread.id ? -1 : 0),
    )
    .map(({ thread }) => thread);
}

export function getLatestThreadForProject<
  T extends {
    readonly id: string;
    readonly projectId: string | null;
    readonly archived: boolean;
  } & ThreadSortInput,
>(threads: readonly T[], projectId: string, sortOrder: SidebarThreadSortOrder): T | null {
  let latest: T | null = null;
  let latestTimestamp = Number.NEGATIVE_INFINITY;
  for (const thread of threads) {
    if (thread.projectId !== projectId || thread.archived) continue;
    const timestamp = getThreadSortTimestamp(thread, sortOrder);
    if (
      latest === null ||
      timestamp > latestTimestamp ||
      (timestamp === latestTimestamp && thread.id > latest.id)
    ) {
      latest = thread;
      latestTimestamp = timestamp;
    }
  }
  return latest;
}
