// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/session-logic.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// session and turn presentation (pending user inputs and approvals, the active plan, per-turn changed files, the running state, durations) derived from agentd transcript entries instead of T3's turn items; plain TypeScript, no Effect. Loams' changes are Apache-2.0.

// What a thread's transcript says beyond its rows (SD1 Task 18, inventory (b)): which
// turn each entry belongs to, what each turn changed, which questions are still open,
// the latest plan, and whether the agent is working.

import type {
  MessagePart,
  SessionMessageEntry,
  TodoItem,
  ToolDiffStat,
  UserInputQuestion,
} from "@loams/agentd-client";
import { type ToolPart, toolChangedFiles } from "./tools/presentation";

export type InputPart = Extract<MessagePart, { kind: "input" }>;

/** An approval the daemon is holding for this thread (G17; `ApprovalService`). */
export interface PendingApproval {
  /** The tool call id; `ResolveApproval`'s `callId`. */
  id: string;
  chatId: string;
  turnId: string;
  tool: string;
  /** The call's arguments, as the daemon decoded them. Shown as text only. */
  args: unknown;
  risk: "read" | "write" | "destructive" | "unknown";
  createdAtMs: number;
}

/** A question the agent asked and nobody has answered yet. */
export interface PendingUserInput {
  requestId: string;
  entryId: string;
  partId: string;
  questions: UserInputQuestion[];
}

export interface ActivePlanState {
  /** The entry and part of the latest plan (todo) call. */
  entryId: string;
  partId: string;
  items: TodoItem[];
}

/** One turn: a user message and the entries that answer it, up to the next user message. */
export interface TurnSummary {
  /** The user message's id: `ListCheckpoints`' and `RestoreCheckpoint`'s `turnId`. */
  turnId: string;
  /** The last entry of the turn, after which its summary row goes. */
  lastEntryId: string;
  /** Per-file stats the turn's tool calls reported, merged by path. */
  files: ToolDiffStat[];
  /** The turn is still being answered. */
  live: boolean;
  durationMs: number | null;
}

export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return "0s";
  if (ms < 1000) return `${Math.max(0, Math.round(ms))}ms`;
  const totalSeconds = Math.round(ms / 1000);
  if (totalSeconds < 60) return `${totalSeconds}s`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return seconds === 0 ? `${minutes}m` : `${minutes}m ${seconds}s`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest === 0 ? `${hours}h` : `${hours}h ${rest}m`;
}

export function isToolPart(part: MessagePart): part is ToolPart {
  return part.kind === "tool";
}

/** The agent is working: the latest assistant entry is still streaming. */
export function isThreadRunning(entries: ReadonlyArray<SessionMessageEntry>): boolean {
  for (let index = entries.length - 1; index >= 0; index -= 1) {
    const entry = entries[index];
    if (!entry) continue;
    if (entry.role === "user") return false;
    if (entry.role === "assistant") return entry.status === "streaming";
  }
  return false;
}

/** Every question part that has not been answered, oldest first. */
export function derivePendingUserInputs(
  entries: ReadonlyArray<SessionMessageEntry>,
): PendingUserInput[] {
  const pending: PendingUserInput[] = [];
  for (const entry of entries) {
    for (const part of entry.parts) {
      if (part.kind === "input" && !part.resolved) {
        pending.push({
          requestId: part.requestId,
          entryId: entry.id,
          partId: part.id,
          questions: part.questions,
        });
      }
    }
  }
  return pending;
}

/** The latest plan (todo) call in the thread, or null. */
export function deriveActivePlanState(
  entries: ReadonlyArray<SessionMessageEntry>,
): ActivePlanState | null {
  for (let index = entries.length - 1; index >= 0; index -= 1) {
    const entry = entries[index];
    if (!entry) continue;
    for (let p = entry.parts.length - 1; p >= 0; p -= 1) {
      const part = entry.parts[p];
      if (part?.kind === "tool" && part.call.kind === "todo") {
        return { entryId: entry.id, partId: part.id, items: part.call.items };
      }
    }
  }
  return null;
}

function mergeStats(into: Map<string, ToolDiffStat>, part: ToolPart): void {
  if (part.diffStats && part.diffStats.length > 0) {
    for (const stat of part.diffStats) {
      const existing = into.get(stat.path);
      into.set(stat.path, {
        path: stat.path,
        additions: (existing?.additions ?? 0) + stat.additions,
        deletions: (existing?.deletions ?? 0) + stat.deletions,
      });
    }
    return;
  }
  for (const path of toolChangedFiles(part)) {
    if (!into.has(path)) into.set(path, { path, additions: 0, deletions: 0 });
  }
}

/** The thread's turns, oldest first. Entries before the first user message have no turn. */
export function deriveTurns(entries: ReadonlyArray<SessionMessageEntry>): TurnSummary[] {
  const turns: TurnSummary[] = [];
  let current: { turn: TurnSummary; files: Map<string, ToolDiffStat>; startedAt: number } | null =
    null;
  const close = () => {
    if (!current) return;
    current.turn.files = [...current.files.values()];
    turns.push(current.turn);
  };
  for (const entry of entries) {
    if (entry.role === "user") {
      close();
      current = {
        turn: { turnId: entry.id, lastEntryId: entry.id, files: [], live: false, durationMs: null },
        files: new Map(),
        startedAt: entry.createdAt,
      };
      continue;
    }
    if (!current) continue;
    current.turn.lastEntryId = entry.id;
    if (entry.role === "assistant") {
      if (entry.status === "streaming") current.turn.live = true;
      if (typeof entry.durationMs === "number") {
        current.turn.durationMs = (current.turn.durationMs ?? 0) + entry.durationMs;
      }
      for (const part of entry.parts) if (part.kind === "tool") mergeStats(current.files, part);
    }
  }
  close();
  return turns;
}

/** The plain text of an entry's text parts, for copy and find. */
export function entryText(entry: SessionMessageEntry): string {
  return entry.parts
    .filter((part): part is Extract<MessagePart, { kind: "text" }> => part.kind === "text")
    .map((part) => part.text)
    .join("\n\n");
}
