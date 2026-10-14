// Stand-ins for the terminal tests: a daemon's TerminalService (TerminalBackend) whose
// streams the test drives, and an xterm surface that records what it is given.

import type { IBufferRange, IDisposable, ILinkProvider, ITheme } from '@xterm/xterm';
import type {
  TerminalBackend,
  TerminalInfo,
  TerminalStreamEvent,
  TerminalSurface,
  TerminalSurfaceOptions,
} from '../src';

const b64 = (text: string) => btoa(String.fromCharCode(...new TextEncoder().encode(text)));
const unb64 = (data: string) =>
  new TextDecoder().decode(Uint8Array.from(atob(data), (c) => c.charCodeAt(0)));

type Item = TerminalStreamEvent | { end: true; error?: unknown };

/** One SubscribeTerminal call: the test pushes events and ends it. */
export class FakeStream {
  readonly afterSeq: number | undefined;
  readonly #queue: Item[] = [];
  #wake: (() => void) | null = null;
  aborted = false;

  constructor(afterSeq: number | undefined, signal: AbortSignal) {
    this.afterSeq = afterSeq;
    signal.addEventListener('abort', () => {
      this.aborted = true;
      this.#push({ end: true });
    });
  }

  data(seq: number, text: string) {
    this.#push({ type: 'data', seq, data: b64(text) });
  }
  exit(seq: number, exitCode = 0) {
    this.#push({ type: 'exit', seq, exitCode });
  }
  end(error?: unknown) {
    this.#push({ end: true, error });
  }

  #push(item: Item) {
    this.#queue.push(item);
    this.#wake?.();
  }

  async *events(): AsyncGenerator<TerminalStreamEvent> {
    while (true) {
      const item = this.#queue.shift();
      if (!item) {
        await new Promise<void>((resolve) => {
          this.#wake = resolve;
        });
        this.#wake = null;
        continue;
      }
      if ('end' in item) {
        if (item.error) throw item.error;
        return;
      }
      yield item;
    }
  }
}

export function fakeBackend() {
  let next = 0;
  const streams = new Map<string, FakeStream[]>();
  const writes: { id: string; text: string; raw: string }[] = [];
  const resizes: { id: string; cols: number; rows: number }[] = [];
  const closed: string[] = [];
  const opened: { chatId: string; cwd?: string }[] = [];
  const backend: TerminalBackend = {
    async open(input) {
      next += 1;
      opened.push({ chatId: input.chatId, ...(input.cwd ? { cwd: input.cwd } : {}) });
      return { id: `pty-${next}`, cwd: '/home/omar/src/site', shell: 'zsh' } as TerminalInfo;
    },
    subscribe(id, afterSeq, signal) {
      const stream = new FakeStream(afterSeq, signal);
      streams.set(id, [...(streams.get(id) ?? []), stream]);
      return stream.events();
    },
    async write(id, data) {
      writes.push({ id, text: unb64(data), raw: data });
    },
    async resize(id, cols, rows) {
      resizes.push({ id, cols, rows });
    },
    async close(id) {
      closed.push(id);
    },
  };
  return {
    backend,
    writes,
    resizes,
    closed,
    opened,
    streams,
    /** The latest stream of a terminal. */
    stream: (id: string) => streams.get(id)?.at(-1),
  };
}

export interface FakeSurface extends TerminalSurface {
  readonly written: string[];
  readonly lines: string[];
  readonly pasted: string[];
  disposed: boolean;
  focused: number;
  theme: ITheme;
  selection: { text: string; range: IBufferRange } | null;
  links: ILinkProvider | null;
  keyHandler: ((event: KeyboardEvent) => boolean) | null;
  type(data: string): void;
  select(text: string, range: IBufferRange): void;
}

/** Creates fake surfaces and keeps them for the test to inspect. */
export function fakeSurfaces() {
  const all: FakeSurface[] = [];
  const create = (options: TerminalSurfaceOptions): FakeSurface => {
    const dataListeners = new Set<(d: string) => void>();
    const selectionListeners = new Set<() => void>();
    const disposable = (set: Set<never>, fn: never): IDisposable => {
      set.add(fn);
      return { dispose: () => set.delete(fn) };
    };
    const surface: FakeSurface = {
      cols: 80,
      rows: 24,
      written: [],
      lines: [],
      pasted: [],
      disposed: false,
      focused: 0,
      theme: options.theme,
      selection: null,
      links: null,
      keyHandler: null,
      open() {},
      write(data) {
        this.written.push(data);
        this.lines.push(...data.split(/\r?\n/));
      },
      reset() {
        this.written.length = 0;
        this.lines.length = 0;
      },
      paste(text) {
        this.pasted.push(text);
        for (const l of dataListeners) l(text);
      },
      focus() {
        this.focused += 1;
      },
      fit() {},
      setTheme(theme) {
        this.theme = theme;
      },
      hasSelection() {
        return this.selection !== null;
      },
      getSelection() {
        return this.selection?.text ?? '';
      },
      getSelectionPosition() {
        return this.selection?.range;
      },
      clearSelection() {
        this.selection = null;
        for (const l of selectionListeners) l();
      },
      getLine(index) {
        const text = this.lines[index];
        return text === undefined ? undefined : { isWrapped: false, translateToString: () => text };
      },
      onData: (l) => disposable(dataListeners as Set<never>, l as never),
      onResize: () => ({ dispose() {} }),
      onSelectionChange: (l) => disposable(selectionListeners as Set<never>, l as never),
      attachCustomKeyEventHandler(handler) {
        this.keyHandler = handler;
      },
      registerLinkProvider(provider) {
        this.links = provider;
        return { dispose: () => {} };
      },
      dispose() {
        this.disposed = true;
      },
      type(data) {
        for (const l of dataListeners) l(data);
      },
      select(text, range) {
        this.selection = { text, range };
        for (const l of selectionListeners) l();
      },
    };
    all.push(surface);
    return surface;
  };
  return { create, all, last: () => all.at(-1) };
}

export { b64 };
