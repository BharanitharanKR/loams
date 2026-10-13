// The console's modes, Console and Studio (design §53 §4, D900–D902; SD1 Task 3).
//
// A plugin declares the modes it belongs to (`modes` in its manifest, default
// `["console"]`). The host provides the `mode` service: which mode is current,
// which are available, and the switch. The shell renders the switcher and
// filters its nav by mode; the host loads a mode's plugins the first time the
// mode is entered, and keeps them loaded after that.

export type Mode = 'console' | 'studio';

/** Every mode, in the switcher's order. */
export const MODES: readonly Mode[] = ['console', 'studio'];

/** Each mode's home route. */
export const MODE_HOME: Record<Mode, string> = { console: '/', studio: '/studio' };

/** The per-device storage key of the last-used mode. */
export const MODE_KEY = 'loams.shell.mode';

export function isMode(value: unknown): value is Mode {
  return value === 'console' || value === 'studio';
}

/** The mode a route belongs to: anything under `/studio` is Studio's (§53 §4.4). */
export function modeOfPath(path: string): Mode {
  const pathname = path.split(/[?#]/, 1)[0] ?? '';
  return pathname === '/studio' || pathname.startsWith('/studio/') ? 'studio' : 'console';
}

/** The edition default (D902; Q801's default: the desktop opens in Studio from the first run). */
export function defaultMode(kind: 'web' | 'desktop'): Mode {
  return kind === 'desktop' ? 'studio' : 'console';
}

/**
 * The boot mode: the path's mode when the path names one (any `/studio` route, or a
 * Console route other than the shared root), then the stored mode, then the edition
 * default. Only an available mode can win; Console is the last resort.
 */
export function chooseBootMode(o: {
  path: string;
  stored: Mode | undefined;
  kind: 'web' | 'desktop';
  available: readonly Mode[];
}): Mode {
  const ok = (m: Mode | undefined): m is Mode => m !== undefined && o.available.includes(m);
  const pathname = o.path.split(/[?#]/, 1)[0] || '/';
  const fromPath = pathname === '/' ? undefined : modeOfPath(pathname);
  if (ok(fromPath)) return fromPath;
  if (ok(o.stored)) return o.stored;
  const fallback = defaultMode(o.kind);
  return ok(fallback) ? fallback : 'console';
}

/** The `mode` service (§53 §4.2). */
export interface ModeService {
  readonly current: Mode;
  /** The modes some catalog plugin declares (and whose instance flag, if any, is on). */
  readonly available: readonly Mode[];
  /** Switch, persist, and navigate to the mode's home or `opts.navigate`. */
  set(mode: Mode, opts?: { navigate?: string }): void;
  onChange(cb: (mode: Mode) => void): () => void;
  /** The modes a catalog row's plugin declares, by plugin id (`["console"]` if unknown). */
  modesOf(pluginId: string): readonly Mode[];
}

/** The slice of `Storage` the service uses; every access is wrapped in try/catch. */
export interface ModeStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export function readStoredMode(storage: ModeStorage | undefined): Mode | undefined {
  try {
    const value = storage?.getItem(MODE_KEY);
    return isMode(value) ? value : undefined;
  } catch {
    return undefined;
  }
}

export interface ModeControllerOptions {
  current: Mode;
  available: readonly Mode[];
  modes: ReadonlyMap<string, readonly Mode[]>;
  storage?: ModeStorage;
  navigate(to: string): void;
  /** Called the first time a mode is entered (it applies that mode's deferred plugins). */
  enter(mode: Mode): void;
}

export class ModeController implements ModeService {
  #current: Mode;
  readonly #listeners = new Set<(mode: Mode) => void>();
  readonly #entered = new Set<Mode>();

  constructor(private readonly o: ModeControllerOptions) {
    this.#current = o.current;
    this.#entered.add(o.current);
  }

  get current(): Mode {
    return this.#current;
  }

  get available(): readonly Mode[] {
    return this.o.available;
  }

  set(mode: Mode, opts: { navigate?: string } = {}): void {
    if (!this.o.available.includes(mode)) return;
    if (mode === this.#current) {
      if (opts.navigate !== undefined) this.o.navigate(opts.navigate);
      return;
    }
    this.#current = mode;
    try {
      this.o.storage?.setItem(MODE_KEY, mode);
    } catch {
      // Storage blocked (a private window): the switch still holds for this page.
    }
    if (!this.#entered.has(mode)) {
      this.#entered.add(mode);
      this.o.enter(mode);
    }
    for (const listener of [...this.#listeners]) listener(mode);
    this.o.navigate(opts.navigate ?? MODE_HOME[mode]);
  }

  onChange(cb: (mode: Mode) => void): () => void {
    this.#listeners.add(cb);
    return () => this.#listeners.delete(cb);
  }

  modesOf(pluginId: string): readonly Mode[] {
    return this.o.modes.get(pluginId) ?? ['console'];
  }
}
