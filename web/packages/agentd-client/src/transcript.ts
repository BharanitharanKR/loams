// The transcript fold: `WatchDocMessages` (ndjson) and `ThreadService.WatchTranscript`
// (Connect) send a `reset` frame, then deltas (design §50 §8.3, §53 §6.1). This
// applies one frame to the entries a client holds, as the daemon's own
// `apply_transcript_frame` does (crates/loams-agentd-proto/src/transcript.rs), so
// every viewport lands on the same transcript. A Connect frame is first mapped to
// the ndjson shape by `fromConnectTranscript` (`@loams/agentd-client/connect`), so
// there is one fold for both wires (plan SD1 Task 6, ruling T0-3).

import type { ContextUsage } from './gen/ContextUsage';
import type { MessagePart } from './gen/MessagePart';
import type { SessionMessageEntry } from './gen/SessionMessageEntry';
import type { TranscriptBaseline } from './gen/TranscriptBaseline';
import type { TranscriptUpdate } from './gen/TranscriptUpdate';

/** One thread's transcript as a client holds it. */
export interface TranscriptState {
  entries: SessionMessageEntry[];
  contextUsage: ContextUsage | null;
  /** The last frame's historical watermark, when it carried one. */
  replayBaseline: TranscriptBaseline | null;
  /** The first frame of an `openingTail` watch: older history follows. */
  historyPending: boolean;
  /**
   * Why the last delta did not apply (a missing anchor, a length or count that
   * disagrees). The entries are left as they were; resubscribe for a reset.
   */
  desync: string | null;
}

export const EMPTY_TRANSCRIPT: TranscriptState = {
  entries: [],
  contextUsage: null,
  replayBaseline: null,
  historyPending: false,
  desync: null,
};

const encoder = new TextEncoder();

/** The daemon counts text lengths in UTF-8 bytes. */
function byteLength(text: string): number {
  return encoder.encode(text).length;
}

class Desync extends Error {}

function applyFrame(
  current: SessionMessageEntry[],
  update: TranscriptUpdate,
): SessionMessageEntry[] {
  if ('reset' in update) return [...update.reset];
  let entries = current;
  if (update.remove.length > 0) {
    const gone = new Set(update.remove);
    entries = entries.filter((e) => !gone.has(e.id));
  } else {
    entries = [...entries];
  }
  for (const { after, entry } of update.upsert) {
    const existing = entries.findIndex((e) => e.id === entry.id);
    if (existing >= 0) entries.splice(existing, 1);
    let at = 0;
    if (after !== null) {
      const anchor = entries.findIndex((e) => e.id === after);
      if (anchor < 0) throw new Desync(`missing anchor ${after}`);
      at = anchor + 1;
    }
    entries.splice(at, 0, entry);
  }
  for (const { entry, part, text, len } of update.append) {
    const index = entries.findIndex((e) => e.id === entry);
    const target = entries[index];
    if (!target) throw new Desync(`missing append entry ${entry}`);
    let found = false;
    const parts = target.parts.map((p): MessagePart => {
      if (found || p.id !== part || (p.kind !== 'text' && p.kind !== 'reasoning')) return p;
      found = true;
      const next = p.text + text;
      if (byteLength(next) !== len) {
        throw new Desync(
          `append length mismatch on ${entry}#${part}: have ${byteLength(next)}, expected ${len}`,
        );
      }
      return { ...p, text: next };
    });
    if (!found) throw new Desync(`missing append part ${part}`);
    entries[index] = { ...target, parts };
  }
  if (entries.length !== update.count) {
    throw new Desync(`count mismatch: have ${entries.length}, expected ${update.count}`);
  }
  return entries;
}

/** Applies one ndjson `TranscriptUpdate` frame. Never throws: see `desync`. */
export function reduceTranscript(
  state: TranscriptState,
  update: TranscriptUpdate,
): TranscriptState {
  try {
    return {
      entries: applyFrame(state.entries, update),
      contextUsage: update.contextUsage ?? null,
      replayBaseline: update.replayBaseline ?? null,
      historyPending: update.historyPending ?? false,
      desync: null,
    };
  } catch (error) {
    if (!(error instanceof Desync)) throw error;
    return { ...state, desync: error.message };
  }
}
