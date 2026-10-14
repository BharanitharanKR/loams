// The agentd host registry for Studio in the browser (design §53 §6.3, §4.4; plan
// SD1 Task 7). A host is a `loams-agentd serve --remote` the person runs: a name,
// its URL, and an optional note of its certificate's fingerprint for self-signed
// setups (the browser checks the certificate itself; the note is for the person).
//
// The list is kept under `loams.studio.hosts` in this browser. Every storage access
// is wrapped: when storage is blocked or full the registry still works for the page.
// A deep link names a host by id only (`?host=<id>`), never by URL, so a link can't
// point the console's token at another server.

import { agentdBaseUrl } from './transport-remote';

export const HOSTS_KEY = 'loams.studio.hosts';

export interface AgentdHost {
  /** Stable, opaque: `h-` and 1–60 lowercase letters, digits or `-`. */
  id: string;
  name: string;
  /** The normalised base URL (`agentdBaseUrl`). */
  url: string;
  /** The person's note of the certificate's SHA-256 fingerprint. */
  fingerprint?: string;
}

/** Where the list is kept. */
export interface HostStore {
  load(): AgentdHost[];
  save(hosts: readonly AgentdHost[]): void;
}

const ID = /^h-[a-z0-9-]{1,60}$/;
const MAX_HOSTS = 50;
const MAX_NAME = 80;
/** Hex pairs, with or without `:` (OpenSSL prints `AB:CD:…`), optionally `SHA256:`-prefixed. */
const FINGERPRINT = /^(?:sha256:)?(?:[0-9a-f]{2}:?){32}$/i;

export function isHostId(value: unknown): value is string {
  return typeof value === 'string' && ID.test(value);
}

/** A fingerprint note, normalised to upper-case `AB:CD:…`, or undefined when it is not one. */
export function fingerprintOf(value: string): string | undefined {
  const trimmed = value.trim();
  if (!FINGERPRINT.test(trimmed)) return undefined;
  const hex = trimmed
    .replace(/^sha256:/i, '')
    .replace(/:/g, '')
    .toUpperCase();
  return hex.match(/.{2}/g)?.join(':');
}

/** The hosts in `raw` that are well formed; anything else is dropped. */
export function parseHosts(raw: unknown): AgentdHost[] {
  if (!Array.isArray(raw)) return [];
  const hosts: AgentdHost[] = [];
  const seen = new Set<string>();
  for (const item of raw.slice(0, MAX_HOSTS)) {
    if (!item || typeof item !== 'object') continue;
    const { id, name, url, fingerprint } = item as Record<string, unknown>;
    if (!isHostId(id) || seen.has(id) || typeof name !== 'string' || typeof url !== 'string') {
      continue;
    }
    const base = agentdBaseUrl(url);
    if (!base) continue;
    const note = typeof fingerprint === 'string' ? fingerprintOf(fingerprint) : undefined;
    seen.add(id);
    hosts.push({
      id,
      name: name.slice(0, MAX_NAME) || base,
      url: base,
      ...(note ? { fingerprint: note } : {}),
    });
  }
  return hosts;
}

/** The registry in `localStorage` (or `storage()`, tests), tolerating any storage failure. */
export function localHostStore(
  storage: () => Pick<Storage, 'getItem' | 'setItem'> | undefined = () => globalThis.localStorage,
): HostStore {
  return {
    load() {
      try {
        return parseHosts(JSON.parse(storage()?.getItem(HOSTS_KEY) ?? '[]'));
      } catch {
        return [];
      }
    },
    save(hosts) {
      try {
        storage()?.setItem(HOSTS_KEY, JSON.stringify(hosts));
      } catch {
        // Blocked or full: the list lasts for this page.
      }
    },
  };
}

/** Why a host could not be added. */
export class HostError extends Error {
  override name = 'HostError';
  constructor(
    readonly code: 'invalid_url' | 'invalid_fingerprint' | 'duplicate' | 'too_many',
    message: string,
  ) {
    super(message);
  }
}

function newId(): string {
  const random =
    typeof crypto !== 'undefined' && 'randomUUID' in crypto
      ? crypto.randomUUID()
      : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return `h-${random
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, '')
    .slice(0, 36)}`;
}

export interface HostRegistry {
  list(): readonly AgentdHost[];
  get(id: string): AgentdHost | undefined;
  add(host: { name: string; url: string; fingerprint?: string }): AgentdHost;
  remove(id: string): void;
  /** Calls back after every change; returns the unsubscribe. */
  subscribe(listener: () => void): () => void;
}

/** The registry over `store` (default: this browser's `loams.studio.hosts`). */
export function createHostRegistry(store: HostStore = localHostStore()): HostRegistry {
  let hosts: readonly AgentdHost[] = store.load();
  const listeners = new Set<() => void>();
  const commit = (next: readonly AgentdHost[]) => {
    hosts = next;
    store.save(next);
    for (const listener of [...listeners]) listener();
  };
  return {
    list: () => hosts,
    get: (id) => hosts.find((host) => host.id === id),
    add({ name, url, fingerprint }) {
      const base = agentdBaseUrl(url);
      if (!base) {
        throw new HostError(
          'invalid_url',
          'Expected an https:// URL (http:// only for localhost), with no credentials or query',
        );
      }
      let note: string | undefined;
      if (fingerprint?.trim()) {
        note = fingerprintOf(fingerprint);
        if (!note) {
          throw new HostError(
            'invalid_fingerprint',
            'Expected a SHA-256 fingerprint (64 hex digits)',
          );
        }
      }
      if (hosts.some((host) => host.url === base)) {
        throw new HostError('duplicate', 'This host is already registered');
      }
      if (hosts.length >= MAX_HOSTS) throw new HostError('too_many', 'Too many hosts');
      const host: AgentdHost = {
        id: newId(),
        name: name.trim().slice(0, MAX_NAME) || new URL(base).host,
        url: base,
        ...(note ? { fingerprint: note } : {}),
      };
      commit([...hosts, host]);
      return host;
    },
    remove(id) {
      if (hosts.some((host) => host.id === id)) commit(hosts.filter((host) => host.id !== id));
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

/**
 * The host a `?host=` parameter names: a registered host's id, exactly. A URL, or
 * anything that is not a registered id, selects nothing.
 */
export function hostFromParam(
  hosts: readonly AgentdHost[],
  param: string | null | undefined,
): AgentdHost | undefined {
  if (!isHostId(param)) return undefined;
  return hosts.find((host) => host.id === param);
}
