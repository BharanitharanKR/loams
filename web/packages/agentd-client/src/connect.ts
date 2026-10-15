// `@loams/agentd-client/connect`: loams-agentd's Connect surface, `loams.agentd.v1`,
// for the console (plan SD1 Task 6, §53 §6.1–§6.3). The transports, the method
// sets the host enforces, and the one mapping from the Connect transcript frame
// to the ndjson one, so both wires fold through `reduceTranscript`.
//
// A separate entry from `@loams/agentd-client`: Electron main imports that one for
// the ndjson types and must not pull in the browser Connect runtime.

import { toJson } from '@bufbuild/protobuf';
import { ValueSchema } from '@bufbuild/protobuf/wkt';
import type { Transport } from '@connectrpc/connect';
import type * as pb from '@loams/proto/agentd';
import type { MessagePart } from './gen/MessagePart';
import type { SessionMessageEntry } from './gen/SessionMessageEntry';
import type { JsonValue } from './gen/serde_json/JsonValue';
import type { ToolCall } from './gen/ToolCall';
import type { TranscriptUpdate } from './gen/TranscriptUpdate';
import { AGENTD_GIT_WRITE_METHODS, AGENTD_RENDERER_METHODS } from './methods.gen';
import { reduceTranscript, type TranscriptState } from './transcript';
import { createDesktopTransport, type DesktopTransportOptions } from './transport-desktop';
import { createRemoteAgentdTransport, type RemoteAgentdTransportOptions } from './transport-remote';

export { DESKTOP_AGENTD_BASE_URL, type DesktopTransportOptions } from './transport-desktop';
export type { RemoteAgentdTransportOptions } from './transport-remote';
export { AGENTD_GIT_WRITE_METHODS, AGENTD_RENDERER_METHODS };

/**
 * How the console reaches an agentd. The desktop goes through Electron main
 * (`kind: 'desktop'`); a browser calls a remote host directly with the person's
 * own token (`kind: 'remote'`, SD1 Task 7, ruling T7-9).
 */
export type AgentdTransportOptions = DesktopTransportOptions | RemoteAgentdTransportOptions;

export function createAgentdTransport(options: AgentdTransportOptions): Transport {
  switch (options.kind) {
    case 'desktop':
      return createDesktopTransport(options);
    case 'remote':
      return createRemoteAgentdTransport(options);
  }
}

/** `/loams.agentd.v1.GitService/CommitWorktree` for a service and method name. */
export function agentdMethodPath(typeName: string, method: string): string {
  return `/${typeName}/${method}`;
}

/** Writes to git: callable only by a plugin holding `agentd:git-write` (§53 §8.3). */
export function isGitWriteMethod(typeName: string, method: string): boolean {
  return AGENTD_GIT_WRITE_METHODS.has(agentdMethodPath(typeName, method));
}

// ---- the transcript: Connect → ndjson ----

const ROLE = { 1: 'user', 2: 'assistant', 3: 'system' } as const;
const STATUS = { 1: 'streaming', 2: 'complete', 3: 'aborted' } as const;
const SUBAGENT = { 1: 'running', 2: 'done', 3: 'failed' } as const;

/** A required enum: the daemon never sends `*_UNSPECIFIED` where a value is required. */
function known<T>(table: Record<number, T>, value: number, name: string): T {
  const out = table[value];
  if (out === undefined) throw new Error(`${name} ${value} is not a value this client knows`);
  return out;
}

function int(value: bigint): number {
  return Number(value);
}

function json(value: pb.McpCall['input']): JsonValue | undefined {
  return value === undefined ? undefined : (toJson(ValueSchema, value) as JsonValue);
}

function toolCall(call: pb.ToolCall | undefined): ToolCall {
  const c = call?.call;
  switch (c?.case) {
    case 'exec':
      return { kind: 'exec', command: c.value.command };
    case 'readFile':
      return { kind: 'readFile', path: c.value.path };
    case 'writeFile':
      return { kind: 'writeFile', path: c.value.path, content: c.value.content };
    case 'editFile':
      return {
        kind: 'editFile',
        path: c.value.path,
        old_string: c.value.oldString,
        new_string: c.value.newString,
      };
    case 'applyPatch':
      return { kind: 'applyPatch', path: c.value.path };
    case 'search':
      return { kind: 'search', pattern: c.value.pattern, path: c.value.path };
    case 'glob':
      return { kind: 'glob', pattern: c.value.pattern };
    case 'webFetch':
      return { kind: 'webFetch', url: c.value.url, prompt: c.value.prompt };
    case 'webSearch':
      return { kind: 'webSearch', query: c.value.query };
    case 'todo':
      return {
        kind: 'todo',
        items: c.value.items.map((i) => ({ text: i.text, done: i.done })),
      };
    case 'mcp':
      return {
        kind: 'mcp',
        server: c.value.server,
        tool: c.value.tool,
        input: json(c.value.input),
      };
    case 'unknown':
      return { kind: 'unknown', name: c.value.name, input: json(c.value.input) };
    default:
      throw new Error('ToolCall has no variant set');
  }
}

function part(p: pb.MessagePart): MessagePart {
  const v = p.part;
  switch (v.case) {
    case 'text':
      return { kind: 'text', id: v.value.id, text: v.value.text };
    case 'reasoning':
      return { kind: 'reasoning', id: v.value.id, text: v.value.text };
    case 'image':
      return {
        kind: 'image',
        id: v.value.id,
        path: v.value.path,
        name: v.value.name,
        mimeType: v.value.mimeType,
      };
    case 'tool': {
      const t = v.value;
      return {
        kind: 'tool',
        id: t.id,
        call: toolCall(t.call),
        isError: t.isError,
        resolved: t.resolved,
        output: t.output,
        diff: t.diff && { path: t.diff.path, oldText: t.diff.oldText, newText: t.diff.newText },
        outputRef: t.outputRef,
        outputBytes: t.outputBytes === undefined ? undefined : int(t.outputBytes),
        diffRef: t.diffRef,
        diffStats: t.diffStats?.stats.map((s) => ({
          path: s.path,
          additions: int(s.additions),
          deletions: int(s.deletions),
        })),
        subagentRef: t.subagentRef,
        subagentStatus:
          t.subagentStatus === undefined
            ? undefined
            : known(SUBAGENT, t.subagentStatus, 'SubagentStatus'),
        subagentTail: t.subagentTail,
      };
    }
    case 'input':
      return {
        kind: 'input',
        id: v.value.id,
        requestId: v.value.requestId,
        questions: v.value.questions.map((q) => ({
          id: q.id,
          header: q.header,
          question: q.question,
          options: [...q.options],
          multiSelect: q.multiSelect,
          prefill: q.prefill,
          multiline: q.multiline,
        })),
        resolved: v.value.resolved,
      };
    case 'error':
      return { kind: 'error', id: v.value.id, message: v.value.message };
    case 'fork':
      return {
        kind: 'fork',
        id: v.value.id,
        sourceChatId: v.value.sourceChatId,
        sourceTitle: v.value.sourceTitle,
      };
    default:
      throw new Error('MessagePart has no variant set');
  }
}

function entry(e: pb.SessionMessageEntry | undefined): SessionMessageEntry {
  if (!e) throw new Error('TranscriptUpsert.entry is required');
  return {
    id: e.id,
    role: known(ROLE, e.role, 'MessageRole'),
    parts: e.parts.map(part),
    createdAt: int(e.createdAtMs),
    deviceId: e.deviceId,
    status: e.status === undefined ? undefined : known(STATUS, e.status, 'MessageStatus'),
    continuationOf: e.continuationOf,
    durationMs: e.durationMs === undefined ? undefined : int(e.durationMs),
  };
}

/**
 * The one mapping from a Connect `TranscriptUpdate` to the ndjson one (the
 * inverse of `loams-agentd-connect`'s `Wire` impl). Unset optional fields are
 * left out, as the daemon's serde leaves them out of an ndjson frame.
 */
export function fromConnectTranscript(update: pb.TranscriptUpdate): TranscriptUpdate {
  const head = {
    contextUsage: update.contextUsage
      ? {
          tokens: update.contextUsage.tokens === undefined ? null : int(update.contextUsage.tokens),
          window: update.contextUsage.window === undefined ? null : int(update.contextUsage.window),
        }
      : null,
    ...(update.replayBaseline && {
      replayBaseline: {
        entries: Object.fromEntries(
          Object.entries(update.replayBaseline.entries).map(([id, lengths]) => [
            id,
            Object.fromEntries(Object.entries(lengths.parts).map(([p, n]) => [p, int(n)])),
          ]),
        ),
      },
    }),
    ...(update.historyPending && { historyPending: true }),
  };
  const frame = update.frame;
  switch (frame.case) {
    case 'reset':
      return { ...head, reset: frame.value.entries.map(entry) };
    case 'delta':
      return {
        ...head,
        upsert: frame.value.upsert.map((u) => ({ after: u.after ?? null, entry: entry(u.entry) })),
        append: frame.value.append.map((a) => ({
          entry: a.entry,
          part: a.part,
          text: a.text,
          len: int(a.len),
        })),
        remove: [...frame.value.remove],
        count: int(frame.value.count),
      };
    default:
      throw new Error('TranscriptUpdate has no frame');
  }
}

function isConnect(update: pb.TranscriptUpdate | TranscriptUpdate): update is pb.TranscriptUpdate {
  return '$typeName' in update && update.$typeName === 'loams.agentd.v1.TranscriptUpdate';
}

/** `reduceTranscript` for a frame from either wire. */
export function reduceAnyTranscript(
  state: TranscriptState,
  update: pb.TranscriptUpdate | TranscriptUpdate,
): TranscriptState {
  return reduceTranscript(state, isConnect(update) ? fromConnectTranscript(update) : update);
}
