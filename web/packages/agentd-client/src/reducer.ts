// The session reducer: folds a session's `AgentEvent`s into what the chat UI
// renders (design §50 §8.3, §14.3). A port of the daemon's own fold
// (`loams_agentd_doc::fold_event_into_parts`), so a live stream and the
// stored transcript agree. Task 29 builds the session store on it.
//
// The `switch` ends in `assertNever`: a new `AgentEvent` variant fails to
// compile until it is handled here.

import type { AgentEvent } from './gen/AgentEvent';
import type { ContextUsage } from './gen/ContextUsage';
import type { DoneStatus } from './gen/DoneStatus';
import type { MessagePart } from './gen/MessagePart';
import type { SlashCommand } from './gen/SlashCommand';
import type { SubagentStatus } from './gen/SubagentStatus';
import type { ToolCall } from './gen/ToolCall';
import type { ToolDiffStat } from './gen/ToolDiffStat';

/** One session's live state. */
export interface SessionState {
  /** The current assistant message's parts, in order. */
  parts: MessagePart[];
  /** A turn is running. */
  running: boolean;
  /** The harness-native session id, once the harness reported one. */
  sessionId: string | null;
  /** The assistant message the parts belong to. */
  assistantMessageId: string | null;
  /** How the last turn ended. */
  lastStatus: DoneStatus | null;
  contextUsage: ContextUsage | null;
  usage: { inputTokens: number; outputTokens: number };
  /** The harness's slash commands, as last advertised. */
  commands: SlashCommand[];
}

export const EMPTY_SESSION: SessionState = {
  parts: [],
  running: false,
  sessionId: null,
  assistantMessageId: null,
  lastStatus: null,
  contextUsage: null,
  usage: { inputTokens: 0, outputTokens: 0 },
  commands: [],
};

/** A compile-time exhaustiveness check that also fails loudly at run time. */
export function assertNever(value: never): never {
  throw new Error(`unhandled variant: ${JSON.stringify(value)}`);
}

/** Lines with their terminators, as `similar::TextDiff::from_lines` splits them. */
function lines(text: string): string[] {
  return text.match(/[^\n]*\n|[^\n]+$/g) ?? [];
}

/** Above this many cells the exact count is skipped for a cheaper bound. */
const LCS_CELLS_MAX = 4_000_000;

/** Lines kept between two versions: their longest common subsequence. */
function keptLines(prev: string[], next: string[]): number {
  if (prev.length * next.length > LCS_CELLS_MAX) {
    // Huge diffs: lines common to both, counted with multiplicity.
    const counts = new Map<string, number>();
    for (const line of prev) counts.set(line, (counts.get(line) ?? 0) + 1);
    let kept = 0;
    for (const line of next) {
      const left = counts.get(line) ?? 0;
      if (left > 0) {
        counts.set(line, left - 1);
        kept += 1;
      }
    }
    return kept;
  }
  let row = new Array<number>(next.length + 1).fill(0);
  for (const line of prev) {
    const nextRow = new Array<number>(next.length + 1).fill(0);
    for (let j = 0; j < next.length; j += 1) {
      nextRow[j + 1] =
        next[j] === line ? (row[j] ?? 0) + 1 : Math.max(row[j + 1] ?? 0, nextRow[j] ?? 0);
    }
    row = nextRow;
  }
  return row[next.length] ?? 0;
}

/**
 * The diff stats a tool result folds into its part: a minimal line diff's
 * insertions and deletions, as `loams_agentd_doc::diff_stat` counts them.
 */
function diffStat(diff: { path: string; oldText?: string | null; newText: string }): ToolDiffStat {
  const next = lines(diff.newText);
  if (diff.oldText == null) {
    return { path: diff.path, additions: next.length, deletions: 0 };
  }
  const prev = lines(diff.oldText);
  const kept = keptLines(prev, next);
  return { path: diff.path, additions: next.length - kept, deletions: prev.length - kept };
}

/** Whether a tool call spawns a subagent (`ToolCall::is_subagent_spawn`). */
export function isSubagentSpawn(call: ToolCall): boolean {
  const name = call.kind === 'unknown' ? call.name : call.kind === 'mcp' ? call.tool : null;
  return name !== null && (name === 'Agent' || name.startsWith('Agent: '));
}

function foldParts(parts: MessagePart[], event: AgentEvent): MessagePart[] {
  switch (event.type) {
    case 'sessionStarted':
    case 'steered':
      return [];
    case 'textDelta': {
      const last = parts.at(-1);
      if (last?.kind === 'text') {
        return [...parts.slice(0, -1), { ...last, text: last.text + event.text }];
      }
      return [...parts, { kind: 'text', id: `t${parts.length}`, text: event.text }];
    }
    case 'reasoningDelta': {
      if (event.text === '') return parts;
      const last = parts.at(-1);
      if (last?.kind === 'reasoning') {
        return [...parts.slice(0, -1), { ...last, text: last.text + event.text }];
      }
      return [...parts, { kind: 'reasoning', id: `r${parts.length}`, text: event.text }];
    }
    case 'generatedImage': {
      const image: MessagePart = {
        kind: 'image',
        id: event.id,
        path: event.path,
        name: event.name,
        mimeType: event.mimeType,
      };
      return parts.some((p) => p.id === event.id)
        ? parts.map((p) => (p.id === event.id ? image : p))
        : [...parts, image];
    }
    case 'toolCall': {
      const existing = parts.some((p) => p.kind === 'tool' && p.id === event.id);
      if (existing) {
        return parts.map((p) =>
          p.kind === 'tool' && p.id === event.id ? { ...p, call: event.call } : p,
        );
      }
      return [
        ...parts,
        { kind: 'tool', id: event.id, call: event.call, isError: false, resolved: false },
      ];
    }
    case 'toolResult':
      // Outputs stay out of the transcript (the daemon's store keeps them,
      // `FetchToolBlob`); a diff folds to its line counts.
      return parts.map((p) =>
        p.kind === 'tool' && p.id === event.id
          ? {
              ...p,
              isError: event.isError,
              resolved: true,
              output: null,
              outputBytes: null,
              diff: null,
              diffStats: event.diff ? [diffStat(event.diff)] : null,
            }
          : p,
      );
    case 'inputRequested': {
      const id = `in-${event.requestId}`;
      if (parts.some((p) => p.id === id)) return parts;
      return [
        ...parts,
        {
          kind: 'input',
          id,
          requestId: event.requestId,
          questions: event.questions,
          resolved: false,
        },
      ];
    }
    case 'inputResolved':
      return parts.map((p) =>
        p.kind === 'input' && p.requestId === event.requestId ? { ...p, resolved: true } : p,
      );
    case 'error':
      return [...parts, { kind: 'error', id: `e${parts.length}`, message: event.message }];
    case 'done':
      return event.error
        ? [...parts, { kind: 'error', id: `e${parts.length}`, message: event.error }]
        : parts;
    case 'subagent': {
      // The parent keeps only the spawn chip; tagged traffic updates its lifecycle.
      const inner = event.event;
      let status: SubagentStatus | null = null;
      if (inner.type === 'done') status = inner.status === 'errored' ? 'failed' : 'done';
      else if (inner.type === 'userMessage' || inner.type === 'steered') status = 'running';
      return parts.map((p) => {
        if (p.kind !== 'tool' || p.id !== event.parentToolUseId || !isSubagentSpawn(p.call)) {
          return p;
        }
        if (status !== null) return { ...p, subagentStatus: status };
        const settled = p.subagentStatus === 'done' || p.subagentStatus === 'failed';
        return settled ? p : { ...p, subagentStatus: 'running' };
      });
    }
    case 'assistantMessageCompleted':
    case 'usage':
    case 'contextUsage':
    case 'availableCommands':
    case 'userMessage':
      return parts;
    default:
      return assertNever(event);
  }
}

/** Fold one event into a session's state. Never mutates `state`. */
export function reduceSession(state: SessionState, event: AgentEvent): SessionState {
  const parts = foldParts(state.parts, event);
  const next: SessionState = parts === state.parts ? { ...state } : { ...state, parts };
  switch (event.type) {
    case 'sessionStarted':
      return {
        ...next,
        running: true,
        sessionId: event.sessionId,
        assistantMessageId: event.assistantMessageId,
        lastStatus: null,
      };
    case 'steered':
      return { ...next, assistantMessageId: event.nextAssistantMessageId ?? null };
    case 'done':
      return {
        ...next,
        running: false,
        lastStatus: event.status,
        sessionId: event.sessionId ?? next.sessionId,
      };
    case 'contextUsage':
      return { ...next, contextUsage: { tokens: event.tokens, window: event.window } };
    case 'usage':
      return {
        ...next,
        usage: {
          inputTokens: next.usage.inputTokens + event.inputTokens,
          outputTokens: next.usage.outputTokens + event.outputTokens,
        },
      };
    case 'availableCommands':
      return { ...next, commands: event.commands };
    case 'textDelta':
    case 'reasoningDelta':
    case 'generatedImage':
    case 'assistantMessageCompleted':
    case 'toolCall':
    case 'toolResult':
    case 'error':
    case 'inputRequested':
    case 'inputResolved':
    case 'userMessage':
    case 'subagent':
      return next;
    default:
      return assertNever(event);
  }
}
