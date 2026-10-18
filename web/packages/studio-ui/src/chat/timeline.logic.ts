// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/chat/MessagesTimeline.logic.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the row derivation retargeted from T3's turn items to agentd transcript entries and message parts, with upstream's stable-row reuse (`computeStableMessagesTimelineRows`, `isRowUnchanged`), the duration starts, user-message collapsing and the follow band kept; no minimap, worktree setup, MCP apps, HTML renders, attempts or citations; the markdown-link pattern made linear-time (CodeQL js/polynomial-redos). Loams' changes are Apache-2.0.

// Transcript entries → timeline rows (SD1 Task 18). One row per thing a person reads:
// a user message, an assistant text or reasoning part, a run of tool calls, a plan, a
// subagent, a question, an error, a turn's changed files. Row ids are stable across
// deltas (`<entry>/<part>`), and `computeStableTimelineRows` hands back the previous
// row object whenever its inputs are the same objects, so an append to one part
// re-renders only that part's row (the fold keeps every other entry and part as is).

import type { MessagePart, SessionMessageEntry, TodoItem } from "@loams/agentd-client";
import {
  deriveTurns,
  entryText,
  type InputPart,
  isThreadRunning,
  type TurnSummary,
} from "./session.logic";
import type { ToolPart } from "./tools/presentation";

type TextPart = Extract<MessagePart, { kind: "text" }>;
type ReasoningPart = Extract<MessagePart, { kind: "reasoning" }>;
type ImagePart = Extract<MessagePart, { kind: "image" }>;
type ForkPart = Extract<MessagePart, { kind: "fork" }>;

export type TimelineRow =
  | { kind: "user"; id: string; entry: SessionMessageEntry; text: string }
  | { kind: "system"; id: string; entry: SessionMessageEntry; text: string }
  | { kind: "text"; id: string; entryId: string; part: TextPart; streaming: boolean }
  | { kind: "reasoning"; id: string; entryId: string; part: ReasoningPart; streaming: boolean }
  | { kind: "work"; id: string; entryId: string; parts: ToolPart[] }
  | { kind: "subagent"; id: string; entryId: string; part: ToolPart }
  | { kind: "plan"; id: string; entryId: string; part: ToolPart; items: TodoItem[] }
  | { kind: "input"; id: string; entryId: string; part: InputPart }
  | { kind: "image"; id: string; entryId: string; part: ImagePart }
  | { kind: "error"; id: string; entryId: string; message: string }
  | { kind: "fork"; id: string; entryId: string; part: ForkPart }
  | {
      kind: "assistant-meta";
      id: string;
      entry: SessionMessageEntry;
      copyText: string;
      durationMs: number | null;
      aborted: boolean;
    }
  | { kind: "turn"; id: string; turn: TurnSummary; canRestore: boolean }
  | { kind: "working"; id: string; since: number };

export interface TimelineRowsInput {
  entries: ReadonlyArray<SessionMessageEntry>;
  /** Turn ids (user message ids) that have a checkpoint to restore to. */
  checkpoints?: ReadonlySet<string>;
}

const partRowId = (entryId: string, partId: string) => `${entryId}/${partId}`;

function isPlanPart(part: ToolPart): boolean {
  return part.call.kind === "todo";
}

/** Derives the rows for `entries`, oldest first. */
export function deriveTimelineRows(input: TimelineRowsInput): TimelineRow[] {
  const { entries } = input;
  const checkpoints = input.checkpoints ?? new Set<string>();
  const rows: TimelineRow[] = [];
  const turnsByLastEntry = new Map(deriveTurns(entries).map((turn) => [turn.lastEntryId, turn]));

  for (const entry of entries) {
    if (entry.role === "user") {
      rows.push({ kind: "user", id: entry.id, entry, text: entryText(entry) });
    } else if (entry.role === "system") {
      rows.push({ kind: "system", id: entry.id, entry, text: entryText(entry) });
    } else {
      const streaming = entry.status === "streaming";
      let work: ToolPart[] | null = null;
      let workId = "";
      const flush = () => {
        if (work && work.length > 0) {
          rows.push({ kind: "work", id: workId, entryId: entry.id, parts: work });
        }
        work = null;
      };
      entry.parts.forEach((part, index) => {
        const id = partRowId(entry.id, part.id);
        const lastPart = index === entry.parts.length - 1;
        if (part.kind === "tool" && !part.subagentRef && !isPlanPart(part)) {
          if (!work) {
            work = [];
            workId = `${id}:work`;
          }
          work.push(part);
          return;
        }
        flush();
        switch (part.kind) {
          case "text":
            if (part.text.length > 0 || (streaming && lastPart)) {
              rows.push({
                kind: "text",
                id,
                entryId: entry.id,
                part,
                streaming: streaming && lastPart,
              });
            }
            break;
          case "reasoning":
            rows.push({
              kind: "reasoning",
              id,
              entryId: entry.id,
              part,
              streaming: streaming && lastPart,
            });
            break;
          case "tool":
            if (isPlanPart(part) && part.call.kind === "todo") {
              rows.push({ kind: "plan", id, entryId: entry.id, part, items: part.call.items });
            } else {
              rows.push({ kind: "subagent", id, entryId: entry.id, part });
            }
            break;
          case "input":
            rows.push({ kind: "input", id, entryId: entry.id, part });
            break;
          case "image":
            rows.push({ kind: "image", id, entryId: entry.id, part });
            break;
          case "error":
            rows.push({ kind: "error", id, entryId: entry.id, message: part.message });
            break;
          case "fork":
            rows.push({ kind: "fork", id, entryId: entry.id, part });
            break;
        }
      });
      flush();
      if (!streaming) {
        const copyText = entryText(entry);
        if (copyText.length > 0 || entry.status === "aborted") {
          rows.push({
            kind: "assistant-meta",
            id: `${entry.id}:meta`,
            entry,
            copyText,
            durationMs: entry.durationMs ?? null,
            aborted: entry.status === "aborted",
          });
        }
      }
    }
    const turn = turnsByLastEntry.get(entry.id);
    if (turn && !turn.live && (turn.files.length > 0 || checkpoints.has(turn.turnId))) {
      rows.push({
        kind: "turn",
        id: `turn:${turn.turnId}`,
        turn,
        canRestore: checkpoints.has(turn.turnId),
      });
    }
  }

  if (isThreadRunning(entries)) {
    const last = entries.at(-1);
    rows.push({ kind: "working", id: "working", since: last?.createdAt ?? 0 });
  }
  return rows;
}

export interface StableTimelineRowsState {
  byId: Map<string, TimelineRow>;
  result: TimelineRow[];
}

export const EMPTY_STABLE_ROWS: StableTimelineRowsState = { byId: new Map(), result: [] };

/** Reuses each unchanged row object, so memoised rows skip rendering. */
export function computeStableTimelineRows(
  rows: TimelineRow[],
  previous: StableTimelineRowsState,
): StableTimelineRowsState {
  const next = new Map<string, TimelineRow>();
  let anyChanged = rows.length !== previous.byId.size;

  const result = rows.map((row, index) => {
    const prevRow = previous.byId.get(row.id);
    const nextRow = prevRow && isRowUnchanged(prevRow, row) ? prevRow : row;
    next.set(row.id, nextRow);
    if (!anyChanged && previous.result[index] !== nextRow) {
      anyChanged = true;
    }
    return nextRow;
  });

  return anyChanged ? { byId: next, result } : previous;
}

function sameItems<T>(a: ReadonlyArray<T>, b: ReadonlyArray<T>): boolean {
  return a.length === b.length && a.every((item, index) => item === b[index]);
}

/** Shallow field comparison per row variant: avoids deep equality cost. */
function isRowUnchanged(a: TimelineRow, b: TimelineRow): boolean {
  if (a.kind !== b.kind || a.id !== b.id) return false;
  switch (a.kind) {
    case "user":
    case "system":
      return a.entry === (b as typeof a).entry;
    case "text":
    case "reasoning": {
      const bt = b as typeof a;
      return a.part === bt.part && a.streaming === bt.streaming;
    }
    case "work":
      return sameItems(a.parts, (b as typeof a).parts);
    case "subagent":
    case "plan":
    case "input":
    case "image":
    case "fork":
      return a.part === (b as typeof a).part;
    case "error":
      return a.message === (b as typeof a).message;
    case "assistant-meta":
      return a.entry === (b as typeof a).entry;
    case "turn": {
      const bt = b as typeof a;
      return (
        a.canRestore === bt.canRestore &&
        a.turn.durationMs === bt.turn.durationMs &&
        a.turn.files.length === bt.turn.files.length &&
        a.turn.files.every((file, index) => {
          const other = bt.turn.files[index];
          return (
            other !== undefined &&
            file.path === other.path &&
            file.additions === other.additions &&
            file.deletions === other.deletions
          );
        })
      );
    }
    case "working":
      return a.since === (b as typeof a).since;
  }
}

const MAX_COLLAPSED_USER_MESSAGE_LINES = 8;
const MAX_COLLAPSED_USER_MESSAGE_LENGTH = 600;
// Neither the label nor the target may hold the bracket that opens the next
// match, so a failed match never rescans the text after it (linear time).
const MARKDOWN_LINK = /!?\[([^[\]\n]*)\]\((?:<[^<>\n]*>|[^\s)[\]]*)\)/g;

export function shouldCollapseUserMessage(text: string): boolean {
  const visible = text.replace(MARKDOWN_LINK, "$1");
  if (visible.trim().length === 0) {
    return false;
  }
  return (
    visible.length > MAX_COLLAPSED_USER_MESSAGE_LENGTH ||
    visible.split("\n").length > MAX_COLLAPSED_USER_MESSAGE_LINES
  );
}

/** The text find-in-thread searches in a row (what the row shows). */
export function timelineRowText(row: TimelineRow): string {
  switch (row.kind) {
    case "user":
    case "system":
      return row.text;
    case "text":
    case "reasoning":
      return row.part.text;
    case "work":
      return row.parts
        .map((part) => `${part.call.kind} ${JSON.stringify(part.call)} ${part.output ?? ""}`)
        .join("\n");
    case "subagent":
      return `${JSON.stringify(row.part.call)} ${row.part.subagentTail ?? ""}`;
    case "plan":
      return row.items.map((item) => item.text).join("\n");
    case "input":
      return row.part.questions.map((q) => `${q.header} ${q.question}`).join("\n");
    case "error":
      return row.message;
    case "fork":
      return row.part.sourceTitle;
    case "turn":
      return row.turn.files.map((file) => file.path).join("\n");
    case "image":
      return row.part.name;
    case "assistant-meta":
    case "working":
      return "";
  }
}

/** The ids of the rows whose text contains `query` (case-insensitive), in order. */
export function findTimelineMatches(rows: ReadonlyArray<TimelineRow>, query: string): string[] {
  const needle = query.trim().toLocaleLowerCase();
  if (needle.length === 0) return [];
  return rows
    .filter((row) => timelineRowText(row).toLocaleLowerCase().includes(needle))
    .map((row) => row.id);
}
