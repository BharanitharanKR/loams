// The inspector and the drawer (design 59, D1078): open or closed, the selected tab, the props
// a caller passed with `open({ tab, props })` (not persisted), and the size, kept per device.
//
// A route profile gives some routes their own default and their own saved state: a Studio
// thread opens the inspector at 420px on the Diff tab (T9-6, owner 2026-10-18), and a person's
// resize or close there is kept for thread routes only; every other route keeps the closed
// 280px default and its own saved state.

import type { PanelService, PanelState } from '@loams/console-host';
import { compile } from './router.js';

export interface PanelProfile {
  /** Route patterns ("/studio/t/:threadId") the profile holds on. */
  routes: readonly string[];
  /** Where the profile's state is saved, per device. */
  key: string;
  /** The default before the person changes anything on these routes. */
  open: boolean;
  tab?: string;
  size: number;
}

export interface PanelConfig {
  key: string;
  min: number;
  max: number;
  size: number;
  profiles?: readonly PanelProfile[];
}

/** Studio's thread routes: the inspector opens on the Diff tab at 420px (T9-6). */
export const THREAD_INSPECTOR: PanelProfile = {
  routes: ['/studio/t/:threadId'],
  key: 'loams.shell.inspector.thread',
  open: true,
  tab: 'studio.diff',
  size: 420,
};

export const INSPECTOR: PanelConfig = {
  key: 'loams.shell.inspector',
  min: 240,
  max: 640,
  size: 280,
  profiles: [THREAD_INSPECTOR],
};
export const DRAWER: PanelConfig = { key: 'loams.shell.drawer', min: 160, max: 560, size: 280 };

type Store = Pick<Storage, 'getItem' | 'setItem'>;

function browserStorage(): Store | undefined {
  try {
    return globalThis.localStorage;
  } catch {
    return undefined;
  }
}

export class PanelController implements PanelService {
  #state: PanelState;
  #profile: PanelProfile | undefined;
  /** Below 960px the panel is a sheet over the page, so a profile does not open it by default. */
  #narrow = false;
  readonly #listeners = new Set<() => void>();

  constructor(
    private readonly config: PanelConfig,
    private readonly storage: Store | undefined = browserStorage(),
  ) {
    this.#state = this.#read();
  }

  state(): PanelState {
    return this.#state;
  }

  open(o: { tab?: string; props?: Record<string, unknown> } = {}): void {
    const tab = o.tab ?? this.#state.tab;
    const props = o.props ?? (tab === this.#state.tab ? this.#state.props : {});
    this.#set({ open: true, tab, props });
  }

  close(): void {
    this.#set({ open: false });
  }

  toggle(tab?: string): void {
    if (this.#state.open && (tab === undefined || tab === this.#state.tab)) this.close();
    else this.open(tab === undefined ? {} : { tab });
  }

  resize(size: number): void {
    this.#set({ size: this.#clamp(size) });
  }

  /**
   * Follows the route: entering or leaving a profile's routes swaps to that profile's saved
   * state (or its default). Within the same profile nothing changes.
   */
  route(path: string, o: { narrow?: boolean } = {}): void {
    const next = this.config.profiles?.find((p) =>
      p.routes.some((r) => compile(r).pattern.test(path)),
    );
    const narrow = o.narrow === true;
    if (next === this.#profile && narrow === this.#narrow) return;
    this.#profile = next;
    this.#narrow = narrow;
    this.#state = this.#read();
    for (const listener of [...this.#listeners]) listener();
  }

  readonly subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  #clamp(n: number): number {
    return Math.min(this.config.max, Math.max(this.config.min, Math.round(n)));
  }

  get #key(): string {
    return this.#profile?.key ?? this.config.key;
  }

  #read(): PanelState {
    const p = this.#profile;
    const size = p?.size ?? this.config.size;
    const fallback: PanelState = {
      open: (p?.open ?? false) && !this.#narrow,
      tab: p?.tab,
      props: {},
      size,
    };
    try {
      const raw = JSON.parse(
        this.storage?.getItem(this.#key) ?? 'null',
      ) as Partial<PanelState> | null;
      if (!raw || typeof raw !== 'object') return fallback;
      return {
        open: raw.open === true,
        tab: typeof raw.tab === 'string' ? raw.tab : p?.tab,
        props: {},
        size: typeof raw.size === 'number' ? this.#clamp(raw.size) : size,
      };
    } catch {
      return fallback;
    }
  }

  #set(patch: Partial<PanelState>): void {
    this.#state = { ...this.#state, ...patch };
    const { open, tab, size } = this.#state;
    try {
      this.storage?.setItem(this.#key, JSON.stringify({ open, tab, size }));
    } catch {
      // Storage blocked (a private window): the panel still works this session.
    }
    for (const listener of [...this.#listeners]) listener();
  }
}
