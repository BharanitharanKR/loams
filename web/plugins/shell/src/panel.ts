// The inspector and the drawer (design 59, D1078): open or closed, the selected tab, the props
// a caller passed with `open({ tab, props })` (not persisted), and the size, kept per device.

import type { PanelService, PanelState } from '@loams/console-host';

export interface PanelConfig {
  key: string;
  min: number;
  max: number;
  size: number;
}

export const INSPECTOR: PanelConfig = {
  key: 'loams.shell.inspector',
  min: 240,
  max: 640,
  size: 280,
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

  readonly subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  #clamp(n: number): number {
    return Math.min(this.config.max, Math.max(this.config.min, Math.round(n)));
  }

  #read(): PanelState {
    const fallback: PanelState = { open: false, tab: undefined, props: {}, size: this.config.size };
    try {
      const raw = JSON.parse(
        this.storage?.getItem(this.config.key) ?? 'null',
      ) as Partial<PanelState> | null;
      if (!raw || typeof raw !== 'object') return fallback;
      return {
        open: raw.open === true,
        tab: typeof raw.tab === 'string' ? raw.tab : undefined,
        props: {},
        size: typeof raw.size === 'number' ? this.#clamp(raw.size) : this.config.size,
      };
    } catch {
      return fallback;
    }
  }

  #set(patch: Partial<PanelState>): void {
    this.#state = { ...this.#state, ...patch };
    const { open, tab, size } = this.#state;
    try {
      this.storage?.setItem(this.config.key, JSON.stringify({ open, tab, size }));
    } catch {
      // Storage blocked (a private window): the panel still works this session.
    }
    for (const listener of [...this.#listeners]) listener();
  }
}
