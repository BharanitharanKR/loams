// The `router` service (§37 §5.4, AP1a Ruling 7).
//
// Pages register through `router.page(...)`, never by editing a route
// table: a page is a keyed `console.page` slot entry (key = the page id) and,
// with `nav`, a `console.nav` entry. Disposing the registering plugin
// removes both, so its routes disappear with it.
//
// Scaffold: hash routing (`#/approvals/apr_1`) in both shells, which needs no
// server fallback and survives reloads. A browser
// history router with `basename: '/ui'` follows AP1 Task 0's spike.

import type { Location, PageProps, PageSpec, RouterService } from '@loams/console-host';
import type { SlotRegistry } from '@loams/slots';
import type { ComponentType } from 'react';

interface Route {
  spec: PageSpec;
  pattern: RegExp;
  keys: string[];
}

/** Compiles "/approvals/:id" into a matcher. */
export function compile(path: string): { pattern: RegExp; keys: string[] } {
  const keys: string[] = [];
  const source = path
    .split('/')
    .map((part) => {
      if (part.startsWith(':')) {
        keys.push(part.slice(1));
        return '([^/]+)';
      }
      return part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    })
    .join('/');
  return { pattern: new RegExp(`^${source}/?$`), keys };
}

/** A route's query string as a plain record; a malformed escape is kept as written. */
export function parseQuery(search: string): Record<string, string> {
  const out: Record<string, string> = {};
  try {
    for (const [k, v] of new URLSearchParams(search)) out[k] = v;
  } catch {
    // URLSearchParams does not throw on bad escapes; nothing to do.
  }
  return out;
}

export interface HashSource {
  get(): string;
  set(path: string): void;
  listen(listener: () => void): () => void;
}

/** `window.location.hash`, as the default source. */
export const windowHash: HashSource = {
  // The raw hash: decoding happens once, per parameter, in the router.
  get: () => globalThis.location?.hash.replace(/^#/, '') || '/',
  set: (path) => {
    globalThis.location.hash = path;
  },
  listen: (listener) => {
    globalThis.addEventListener?.('hashchange', listener);
    return () => globalThis.removeEventListener?.('hashchange', listener);
  },
};

/** One breadcrumb; the last is the current page. */
export interface Crumb {
  label: string;
  href?: string;
}

export class HashRouter implements RouterService {
  #routes: Route[] = [];
  #listeners = new Set<() => void>();
  #current: Location;
  #trail: Crumb[] | undefined;
  readonly #unlisten: () => void;

  constructor(
    private readonly slots: SlotRegistry,
    private readonly source: HashSource = windowHash,
    private readonly plugin = 'shell',
  ) {
    this.#current = this.#resolve(source.get());
    this.#unlisten = source.listen(() => this.#update());
  }

  page(spec: PageSpec & { plugin?: string }, component: ComponentType<PageProps>): () => void {
    if (this.#routes.some((r) => r.spec.id === spec.id)) {
      throw new Error(`page ${spec.id} is already registered`);
    }
    const plugin = spec.plugin ?? this.plugin;
    const route = { spec, ...compile(spec.path) };
    this.#routes = [...this.#routes, route];
    const disposers = [
      this.slots.register({ name: 'console.page', plugin, key: spec.id }, component),
    ];
    // A parameterized page (`/approvals/:id`) has no concrete link to show.
    if (spec.nav && route.keys.length === 0) {
      disposers.push(
        this.slots.register(
          {
            name: 'console.nav',
            plugin,
            order: spec.nav.order,
            meta: { label: spec.nav.label ?? spec.title, href: spec.path, group: spec.nav.group },
          },
          () => null,
        ),
      );
    }
    this.#update();
    return () => {
      for (const dispose of disposers) dispose();
      this.#routes = this.#routes.filter((r) => r !== route);
      this.#update();
    };
  }

  navigate(to: string): void {
    this.source.set(to);
    this.#update();
  }

  current(): Location {
    return this.#current;
  }

  /** The title of the current page, for the document title. */
  title(): string | undefined {
    return this.spec()?.title;
  }

  /**
   * A page's own breadcrumbs (an org, a project, the record it shows), in place of
   * the shell's; the last crumb is also the tab title. Cleared when the route changes.
   */
  setTrail(trail: Crumb[] | undefined): void {
    if (JSON.stringify(trail) === JSON.stringify(this.#trail)) return;
    this.#trail = trail;
    for (const listener of this.#listeners) listener();
  }

  trail(): Crumb[] | undefined {
    return this.#trail;
  }

  /** The current page's spec (its title, and whether it is bare or public). */
  spec(): PageSpec | undefined {
    return this.#routes.find((r) => r.spec.id === this.#current.pageId)?.spec;
  }

  readonly subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  dispose(): void {
    this.#unlisten();
    this.#listeners.clear();
  }

  #resolve(raw: string): Location {
    // `/sign-in?next=%2Fteams`: the route is matched without its query.
    const q = raw.indexOf('?');
    const path = q < 0 ? raw : raw.slice(0, q);
    const query = q < 0 ? {} : parseQuery(raw.slice(q + 1));
    const located = this.#match(path);
    return Object.keys(query).length > 0 ? { ...located, query } : located;
  }

  #match(path: string): Location {
    for (const route of this.#routes) {
      const match = route.pattern.exec(path);
      if (!match) continue;
      const params: Record<string, string> = {};
      try {
        route.keys.forEach((key, i) => {
          params[key] = decodeURIComponent(match[i + 1] ?? '');
        });
      } catch {
        // A malformed escape in a user-supplied hash (`#/approvals/%`):
        // no route matches, and the router keeps working.
        return { path, params: {} };
      }
      return { path, pageId: route.spec.id, params };
    }
    return { path, params: {} };
  }

  #update(): void {
    const next = this.#resolve(this.source.get());
    const same =
      next.path === this.#current.path &&
      next.pageId === this.#current.pageId &&
      JSON.stringify(next.params) === JSON.stringify(this.#current.params) &&
      JSON.stringify(next.query) === JSON.stringify(this.#current.query);
    if (same) return;
    if (next.path !== this.#current.path) this.#trail = undefined;
    this.#current = next;
    for (const listener of this.#listeners) listener();
  }
}
