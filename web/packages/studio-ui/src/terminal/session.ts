// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// packages/client-runtime/src/state/terminalSession.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the Effect attach stream and atom state replaced by a plain registry over loams-agentd's SubscribeTerminal (base64 data, seq, afterSeq replay), with apps/web/src/state/terminalSessions.ts's attach folded in. Loams' changes are Apache-2.0.

// Terminal sessions (§53 §5.7, SD1 Task 22). One registry per daemon connection holds
// each terminal's output (a bounded buffer, output.ts), its status and the last `seq`
// it applied. A viewer attaches with `subscribe`; the first viewer starts the
// `SubscribeTerminal` stream, the last one to leave stops it, and the shell keeps
// running on the daemon. Attaching again resubscribes with `afterSeq` = the last seq,
// so the daemon replays only what this client has not seen; a registry that never
// saw the terminal (a reload) asks for the daemon's whole replay window. A stream
// that drops is reattached the same way, a few times, before the session shows an
// error. Output arrives base64 (PTY bytes, not UTF-8) and is decoded with one
// streaming decoder per session, so a character split across chunks survives.

import {
  appendOutput,
  DEFAULT_MAX_TERMINAL_BUFFER_BYTES,
  EMPTY_TERMINAL_OUTPUT_STATE,
  type TerminalOutputState,
} from "./output";

export {
  DEFAULT_MAX_TERMINAL_BUFFER_BYTES,
  INITIAL_TERMINAL_OUTPUT_CURSOR,
  readTerminalOutputUpdate,
  type TerminalOutputCursor,
  type TerminalOutputState,
  type TerminalOutputUpdate,
  terminalOutputText,
} from "./output";

/** A PTY on the daemon (`TerminalSession` in loams.agentd.v1). */
export interface TerminalInfo {
  id: string;
  cwd: string;
  /** The shell's basename (`zsh`, `bash`…). */
  shell: string;
}

/** One `SubscribeTerminal` item, with `data` still base64. */
export type TerminalStreamEvent =
  | { type: "data"; seq: number; data: string }
  | { type: "exit"; seq: number; exitCode: number; signal?: string };

/** What the drawer needs from loams-agentd's TerminalService (and ProjectService). */
export interface TerminalBackend {
  open(input: { chatId: string; cols: number; rows: number; cwd?: string }): Promise<TerminalInfo>;
  /** Replays after `afterSeq` (everything retained when undefined), then follows. */
  subscribe(
    terminalId: string,
    afterSeq: number | undefined,
    signal: AbortSignal,
  ): AsyncIterable<TerminalStreamEvent>;
  /** `data` is base64 of the input bytes. */
  write(terminalId: string, data: string): Promise<void>;
  resize(terminalId: string, cols: number, rows: number): Promise<void>;
  close(terminalId: string): Promise<void>;
}

export type TerminalSessionStatus = "connecting" | "running" | "exited" | "closed" | "error";

export interface TerminalSessionState {
  readonly output: TerminalOutputState;
  readonly status: TerminalSessionStatus;
  readonly error: string | null;
  readonly exitCode: number | null;
  readonly signal: string | null;
  /** The last `seq` applied; the next attach asks for what follows it. */
  readonly lastSeq: number | null;
  readonly version: number;
}

export const EMPTY_TERMINAL_SESSION_STATE = Object.freeze<TerminalSessionState>({
  output: EMPTY_TERMINAL_OUTPUT_STATE,
  status: "connecting",
  error: null,
  exitCode: null,
  signal: null,
  lastSeq: null,
  version: 0,
});

/** The daemon accepts at most 64 KiB of input per WriteTerminal; stay well under it. */
const MAX_WRITE_BYTES = 32 * 1024;
const encoder = new TextEncoder();

export function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(binary);
}

export function base64ToBytes(data: string): Uint8Array {
  const binary = atob(data);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

/**
 * Input text as base64 chunks the daemon takes. Always base64: the daemon's plain
 * UTF-8 fallback would misread input that happens to be valid base64 ("ls").
 */
export function encodeTerminalInput(text: string): string[] {
  const bytes = encoder.encode(text);
  const chunks: string[] = [];
  let offset = 0;
  while (offset < bytes.length) {
    let end = Math.min(offset + MAX_WRITE_BYTES, bytes.length);
    // Never cut a UTF-8 sequence in half.
    while (end < bytes.length && ((bytes[end] ?? 0) & 0xc0) === 0x80) end -= 1;
    chunks.push(bytesToBase64(bytes.subarray(offset, end)));
    offset = end;
  }
  return chunks;
}

/**
 * Folds one stream item into the state. Items at or before `lastSeq` are replays
 * this client already holds and are skipped.
 */
export function applyTerminalStreamEvent(
  current: TerminalSessionState,
  event: TerminalStreamEvent,
  decoder: TextDecoder,
  maxBufferBytes = DEFAULT_MAX_TERMINAL_BUFFER_BYTES,
): TerminalSessionState {
  if (current.lastSeq !== null && event.seq <= current.lastSeq) return current;
  switch (event.type) {
    case "data": {
      const text = decoder.decode(base64ToBytes(event.data), { stream: true });
      return {
        ...current,
        output: appendOutput(current.output, text, maxBufferBytes),
        status: "running",
        error: null,
        lastSeq: event.seq,
        version: current.version + 1,
      };
    }
    case "exit": {
      const tail = decoder.decode();
      return {
        ...current,
        output: tail ? appendOutput(current.output, tail, maxBufferBytes) : current.output,
        status: "exited",
        error: null,
        exitCode: event.exitCode,
        signal: event.signal ?? null,
        lastSeq: event.seq,
        version: current.version + 1,
      };
    }
  }
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** The daemon no longer has the terminal (a restart, or closed elsewhere). */
export function isTerminalGone(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  // Connect's `not_found` is code 5.
  return code === 5 || /terminal not found/i.test(messageOf(error));
}

interface Entry {
  state: TerminalSessionState;
  readonly listeners: Set<() => void>;
  decoder: TextDecoder;
  abort: AbortController | null;
}

export interface TerminalSessionsOptions {
  /** Waits before each reattach of a dropped stream; then the session shows an error. */
  retryDelays?: readonly number[];
  maxBufferBytes?: number;
}

/** The terminal sessions of one daemon connection. */
export class TerminalSessions {
  readonly backend: TerminalBackend;
  readonly #entries = new Map<string, Entry>();
  readonly #retryDelays: readonly number[];
  readonly #maxBufferBytes: number;

  constructor(backend: TerminalBackend, options: TerminalSessionsOptions = {}) {
    this.backend = backend;
    this.#retryDelays = options.retryDelays ?? [250, 1000, 3000];
    this.#maxBufferBytes = options.maxBufferBytes ?? DEFAULT_MAX_TERMINAL_BUFFER_BYTES;
  }

  /** A `useSyncExternalStore` snapshot: the same object until it changes. */
  get(terminalId: string): TerminalSessionState {
    return this.#entries.get(terminalId)?.state ?? EMPTY_TERMINAL_SESSION_STATE;
  }

  /** Attaches a viewer; the first one starts the stream, the last one to leave stops it. */
  subscribe(terminalId: string, listener: () => void): () => void {
    const entry = this.#entry(terminalId);
    entry.listeners.add(listener);
    if (entry.listeners.size === 1) this.#start(terminalId, entry);
    return () => {
      entry.listeners.delete(listener);
      if (entry.listeners.size === 0) {
        entry.abort?.abort();
        entry.abort = null;
      }
    };
  }

  async write(terminalId: string, text: string): Promise<void> {
    for (const chunk of encodeTerminalInput(text)) {
      await this.backend.write(terminalId, chunk);
    }
  }

  resize(terminalId: string, cols: number, rows: number): Promise<void> {
    return this.backend.resize(terminalId, cols, rows);
  }

  /** Stops the shell on the daemon and forgets the session; a gone terminal is fine. */
  async close(terminalId: string): Promise<void> {
    try {
      await this.backend.close(terminalId);
    } catch (error) {
      if (!isTerminalGone(error)) throw error;
    } finally {
      this.forget(terminalId);
    }
  }

  /** Forgets the session here only (its stream stops); the daemon is not told. */
  forget(terminalId: string): void {
    const entry = this.#entries.get(terminalId);
    if (!entry) return;
    entry.abort?.abort();
    this.#entries.delete(terminalId);
    this.#set(entry, { ...entry.state, status: "closed", version: entry.state.version + 1 });
  }

  #entry(terminalId: string): Entry {
    let entry = this.#entries.get(terminalId);
    if (!entry) {
      entry = {
        state: EMPTY_TERMINAL_SESSION_STATE,
        listeners: new Set(),
        decoder: new TextDecoder("utf-8"),
        abort: null,
      };
      this.#entries.set(terminalId, entry);
    }
    return entry;
  }

  #set(entry: Entry, state: TerminalSessionState): void {
    if (state === entry.state) return;
    entry.state = state;
    for (const listener of [...entry.listeners]) listener();
  }

  #start(terminalId: string, entry: Entry): void {
    if (entry.state.status === "exited" || entry.state.status === "closed") return;
    const controller = new AbortController();
    entry.abort = controller;
    void this.#run(terminalId, entry, controller.signal);
  }

  async #run(terminalId: string, entry: Entry, signal: AbortSignal): Promise<void> {
    let failures = 0;
    while (!signal.aborted) {
      try {
        const afterSeq = entry.state.lastSeq ?? undefined;
        for await (const event of this.backend.subscribe(terminalId, afterSeq, signal)) {
          if (signal.aborted) return;
          failures = 0;
          this.#set(
            entry,
            applyTerminalStreamEvent(entry.state, event, entry.decoder, this.#maxBufferBytes),
          );
          if (event.type === "exit") return;
        }
        if (signal.aborted || entry.state.status === "exited") return;
        // The stream ended without an exit: the connection dropped. Reattach.
      } catch (error) {
        if (signal.aborted) return;
        if (isTerminalGone(error)) {
          this.#set(entry, {
            ...entry.state,
            status: "closed",
            error: "This terminal is no longer running on the daemon.",
            version: entry.state.version + 1,
          });
          return;
        }
        if (failures >= this.#retryDelays.length) {
          this.#set(entry, {
            ...entry.state,
            status: "error",
            error: messageOf(error),
            version: entry.state.version + 1,
          });
          return;
        }
      }
      const delay = this.#retryDelays[failures];
      failures += 1;
      if (delay === undefined) {
        this.#set(entry, {
          ...entry.state,
          status: "error",
          error: "The terminal's stream keeps ending.",
          version: entry.state.version + 1,
        });
        return;
      }
      await sleep(delay, signal);
    }
  }
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = setTimeout(done, ms);
    function done() {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    }
    signal.addEventListener("abort", done);
  });
}
