// The transcript store Studio's thread page and the agent dock share (SD1 Task 18,
// §53 D917). One store per daemon transport; one watch per thread however many views
// show it. A watch opens with the first view and closes with the last; the entries
// stay cached, so reopening a thread paints at once and the new watch's reset frame
// replaces them. Frames fold through `@loams/agentd-client`'s `reduceTranscript`
// (ruling T6-5). A delta that does not apply (`desync`) reopens the watch for a reset;
// a failed watch keeps its entries, reports the error and retries with a backoff.

import {
  EMPTY_TRANSCRIPT,
  reduceTranscript,
  type TranscriptState,
  type TranscriptUpdate,
} from "@loams/agentd-client";
import { useCallback, useSyncExternalStore } from "react";

/** Opens a thread's transcript watch: a reset frame, then deltas, until `signal` aborts. */
export type TranscriptWatch = (
  chatId: string,
  signal: AbortSignal,
) => AsyncIterable<TranscriptUpdate>;

export type TranscriptStatus = "connecting" | "live" | "error";

export interface ThreadTranscript extends TranscriptState {
  status: TranscriptStatus;
  /** The last watch error, while `status` is `error`. */
  error: string | null;
}

const INITIAL: ThreadTranscript = { ...EMPTY_TRANSCRIPT, status: "connecting", error: null };
const MAX_DESYNC_RETRIES = 3;
const BACKOFF_MS = [500, 1000, 2000, 5000, 10000, 15000];

interface Thread {
  state: ThreadTranscript;
  listeners: Set<() => void>;
  abort: AbortController | null;
  timer: ReturnType<typeof setTimeout> | null;
  failures: number;
  desyncs: number;
}

export class TranscriptStore {
  readonly #watch: TranscriptWatch;
  readonly #threads = new Map<string, Thread>();

  constructor(watch: TranscriptWatch) {
    this.#watch = watch;
  }

  get(chatId: string): ThreadTranscript {
    return this.#threads.get(chatId)?.state ?? INITIAL;
  }

  /** Whether a watch is open for `chatId` (tests, diagnostics). */
  isWatching(chatId: string): boolean {
    return this.#threads.get(chatId)?.abort != null;
  }

  subscribe(chatId: string, listener: () => void): () => void {
    const thread = this.#thread(chatId);
    thread.listeners.add(listener);
    if (thread.listeners.size === 1) this.#open(chatId, thread);
    return () => {
      thread.listeners.delete(listener);
      if (thread.listeners.size === 0) this.#close(thread);
    };
  }

  #thread(chatId: string): Thread {
    let thread = this.#threads.get(chatId);
    if (!thread) {
      thread = {
        state: INITIAL,
        listeners: new Set(),
        abort: null,
        timer: null,
        failures: 0,
        desyncs: 0,
      };
      this.#threads.set(chatId, thread);
    }
    return thread;
  }

  #set(thread: Thread, state: ThreadTranscript) {
    thread.state = state;
    for (const listener of [...thread.listeners]) listener();
  }

  #close(thread: Thread) {
    thread.abort?.abort();
    thread.abort = null;
    if (thread.timer) clearTimeout(thread.timer);
    thread.timer = null;
  }

  #retryLater(chatId: string, thread: Thread) {
    if (thread.listeners.size === 0) return;
    const delay = BACKOFF_MS[Math.min(thread.failures, BACKOFF_MS.length - 1)] ?? 15000;
    thread.failures += 1;
    thread.timer = setTimeout(() => {
      thread.timer = null;
      if (thread.listeners.size > 0) this.#open(chatId, thread);
    }, delay);
  }

  #open(chatId: string, thread: Thread) {
    this.#close(thread);
    const abort = new AbortController();
    thread.abort = abort;
    if (thread.state.status === "error" && thread.state.entries.length === 0) {
      this.#set(thread, { ...thread.state, status: "connecting" });
    }
    void (async () => {
      try {
        for await (const update of this.#watch(chatId, abort.signal)) {
          if (abort.signal.aborted) return;
          const next = reduceTranscript(thread.state, update);
          if (next.desync) {
            thread.desyncs += 1;
            if (thread.desyncs > MAX_DESYNC_RETRIES) {
              this.#set(thread, { ...next, status: "error", error: `desync: ${next.desync}` });
              abort.abort();
              thread.abort = null;
              return;
            }
            // Resubscribe: the new watch starts with a reset frame.
            this.#open(chatId, thread);
            return;
          }
          if ("reset" in update) thread.desyncs = 0;
          thread.failures = 0;
          this.#set(thread, { ...next, status: "live", error: null });
        }
        if (abort.signal.aborted) return;
        // The daemon closed the stream (a restart): reopen after a pause.
        thread.abort = null;
        this.#retryLater(chatId, thread);
      } catch (cause) {
        if (abort.signal.aborted) return;
        thread.abort = null;
        this.#set(thread, {
          ...thread.state,
          status: "error",
          error: cause instanceof Error ? cause.message : String(cause),
        });
        this.#retryLater(chatId, thread);
      }
    })();
  }
}

const stores = new WeakMap<object, TranscriptStore>();

/**
 * The store for a daemon, keyed by its transport (or any object that names it), so
 * every view of a thread on that daemon shares one watch and one fold.
 */
export function transcriptStoreFor(key: object, watch: TranscriptWatch): TranscriptStore {
  let store = stores.get(key);
  if (!store) {
    store = new TranscriptStore(watch);
    stores.set(key, store);
  }
  return store;
}

/** A thread's transcript from `store`, live while the component is mounted. */
export function useThreadTranscript(store: TranscriptStore, chatId: string): ThreadTranscript {
  const subscribe = useCallback(
    (listener: () => void) => store.subscribe(chatId, listener),
    [store, chatId],
  );
  return useSyncExternalStore(subscribe, () => store.get(chatId));
}
