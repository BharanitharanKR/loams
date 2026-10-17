// The console's theme (design 59, D1074): System follows `prefers-color-scheme` and is dark
// when the OS states no preference; Dark and Light are explicit. `.dark` on <html> is the
// switch the tokens read. The choice is per device: local storage on the web, the desktop's
// settings.json on the desktop (the platform supplies the store).

import type { ThemeService } from './services.js';

export type ThemeChoice = 'system' | 'dark' | 'light';
/** The key the original console and the shell's toggle always used. */
export const THEME_KEY = 'loams-theme';
/** "on" turns the optional panel easing on (D1076); anything else leaves panels snapping. */
export const MOTION_KEY = 'loams.shell.motion';

export function parseThemeChoice(value: unknown): ThemeChoice {
  return value === 'dark' || value === 'light' || value === 'system' ? value : 'system';
}

function osMatch(query: string): boolean {
  try {
    return globalThis.matchMedia?.(query).matches === true;
  } catch {
    return false;
  }
}

/** Dark unless the choice is Light, or System and the OS says light. */
export function resolveDark(
  choice: ThemeChoice,
  match: (query: string) => boolean = osMatch,
): boolean {
  if (choice !== 'system') return choice === 'dark';
  return !match('(prefers-color-scheme: light)');
}

export function applyTheme(
  dark: boolean,
  root: HTMLElement | undefined = globalThis.document?.documentElement,
): void {
  root?.classList.toggle('dark', dark);
}

export interface ThemeStore {
  read(): ThemeChoice;
  /** Saves the choice; `dark` is what it resolved to (the desktop's chrome needs it). */
  write(choice: ThemeChoice, dark: boolean): void;
}

function browserStorage(): Pick<Storage, 'getItem' | 'setItem'> | undefined {
  try {
    return globalThis.localStorage;
  } catch {
    return undefined;
  }
}

export function localThemeStore(
  storage: Pick<Storage, 'getItem' | 'setItem'> | undefined = browserStorage(),
): ThemeStore {
  return {
    read() {
      try {
        return parseThemeChoice(storage?.getItem(THEME_KEY));
      } catch {
        return 'system';
      }
    },
    write(choice) {
      try {
        storage?.setItem(THEME_KEY, choice);
      } catch {
        // Storage blocked (a private window): the choice holds for this page.
      }
    },
  };
}

type MediaLike = Pick<MediaQueryList, 'matches' | 'addEventListener' | 'removeEventListener'>;

export interface ThemeEnv {
  matchMedia?: (query: string) => MediaLike;
  root?: HTMLElement;
}

export class ThemeController implements ThemeService {
  #choice: ThemeChoice;
  #dark: boolean;
  readonly #listeners = new Set<() => void>();
  readonly #media: MediaLike | undefined;
  readonly #onMedia = () => this.#sync(false);

  constructor(
    private readonly store: ThemeStore,
    private readonly env: ThemeEnv = {},
  ) {
    this.#choice = store.read();
    const matchMedia = env.matchMedia ?? ((q: string) => globalThis.matchMedia?.(q));
    try {
      this.#media = matchMedia('(prefers-color-scheme: light)') ?? undefined;
    } catch {
      this.#media = undefined;
    }
    this.#media?.addEventListener?.('change', this.#onMedia);
    this.#dark = this.#resolve();
    applyTheme(this.#dark, env.root);
  }

  get choice(): ThemeChoice {
    return this.#choice;
  }

  get dark(): boolean {
    return this.#dark;
  }

  set(choice: ThemeChoice): void {
    this.#choice = choice;
    this.#sync(true);
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  dispose(): void {
    this.#media?.removeEventListener?.('change', this.#onMedia);
    this.#listeners.clear();
  }

  #resolve(): boolean {
    if (this.#choice !== 'system') return this.#choice === 'dark';
    return !(this.#media?.matches ?? false);
  }

  #sync(chosen: boolean): void {
    const dark = this.#resolve();
    const changed = dark !== this.#dark;
    this.#dark = dark;
    applyTheme(dark, this.env.root);
    // An OS flip under System is saved too: the desktop's native chrome follows it (D1083).
    if (chosen || changed) this.store.write(this.#choice, dark);
    for (const listener of [...this.#listeners]) listener();
  }
}
