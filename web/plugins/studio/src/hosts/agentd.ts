// Studio in the browser over a remote loams-agentd (design §53 §6.3, D909; plan SD1
// Task 7). The person registers their hosts (`@loams/agentd-client/hosts`), picks
// one in Settings › Agentd hosts or with `?host=<id>`, and Studio calls it through
// the web transport with their own access token for the audience `loams-agentd`.
//
// The token: the identity plugin's audience-specific token once MT1 provides one
// (`getToken`). Until then, in development, a token pasted in Settings, held in this
// page's memory only (never stored) and flagged with a warning.

import type { Transport } from '@connectrpc/connect';
import { createAgentdTransport } from '@loams/agentd-client/connect';
import {
  type AgentdHost,
  createHostRegistry,
  type HostRegistry,
  hostFromParam,
} from '@loams/agentd-client/hosts';
import type { TokenGetter } from '@loams/agentd-client/transport-remote';
import type { Reach, StudioAgentd } from '../agentd.js';
import { remotePreviewOpener, type StudioPreview } from '../preview/access.js';

/** The host controls Settings › Agentd hosts and `?host=` use. */
export interface StudioHosts {
  readonly registry: HostRegistry;
  /** The selected host, if any. */
  selected(): AgentdHost | undefined;
  /** Selects a registered host by id; anything else is ignored. */
  select(id: string | null | undefined): void;
  /** Whether Studio has a token for `id`. */
  hasToken(id: string): boolean;
  /** Whether tokens come from pasting (development) rather than the sign-in. */
  readonly pastedTokens: boolean;
  /** Development: a token for `id`, kept in this page's memory only. */
  setPastedToken(id: string, token: string): void;
  /** Calls back after any change of the hosts, the selection or a token. */
  subscribe(listener: () => void): () => void;
  /** Changes whenever `subscribe`'s listeners are called (a store snapshot). */
  version(): number;
}

export interface HostedAgentdOptions {
  registry?: HostRegistry;
  /** The sign-in's access token for the audience `loams-agentd` (MT1); absent: pasted tokens. */
  getToken?: (host: AgentdHost) => TokenGetter;
  /** Tests: the transport to a host (default: the web transport, SD1 Task 7). */
  transportFor?: (host: AgentdHost, getToken: TokenGetter) => Transport;
  /** Tests: `fetch` for the preview ticket (SD1 Task 26). */
  fetch?: typeof fetch;
}

/** Studio's agentd in the browser: the selected remote host. */
export function hostedAgentd(o: HostedAgentdOptions = {}): StudioAgentd & { hosts: StudioHosts } {
  const registry = o.registry ?? createHostRegistry();
  const pasted = new Map<string, string>();
  const listeners = new Set<() => void>();
  let selectedId: string | undefined = registry.list()[0]?.id;
  // One object per state and host: `reach()` is a useSyncExternalStore snapshot, and
  // a new host must re-render what the old one showed.
  const NO_HOST: Reach = { state: 'no-host' };
  let ready: { id: string; reach: Reach } | undefined;
  let version = 0;

  const tokenOf = (host: AgentdHost): TokenGetter =>
    o.getToken ? o.getToken(host) : () => pasted.get(host.id);
  const selected = () => (selectedId ? registry.get(selectedId) : undefined);
  const hasToken = (id: string) => (o.getToken ? true : pasted.has(id));
  const notify = () => {
    version += 1;
    for (const listener of [...listeners]) listener();
  };
  let client: { id: string; transport: Transport } | undefined;
  registry.subscribe(() => {
    if (selectedId && !registry.get(selectedId)) selectedId = registry.list()[0]?.id;
    else if (!selectedId) selectedId = registry.list()[0]?.id;
    client = undefined;
    notify();
  });

  /** The selected host's transport, made once per host and token source. */
  const transport = (): Transport | undefined => {
    const host = selected();
    if (!host || !hasToken(host.id)) return undefined;
    if (client?.id !== host.id) {
      const getToken = tokenOf(host);
      client = {
        id: host.id,
        transport: o.transportFor
          ? o.transportFor(host, getToken)
          : createAgentdTransport({ kind: 'remote', baseUrl: host.url, getToken }),
      };
    }
    return client.transport;
  };

  const hosts: StudioHosts = {
    registry,
    selected,
    select(id) {
      const host = hostFromParam(registry.list(), id);
      if (!host || host.id === selectedId) return;
      selectedId = host.id;
      client = undefined;
      notify();
    },
    hasToken,
    pastedTokens: !o.getToken,
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    version: () => version,
    setPastedToken(id, token) {
      const value = token.trim();
      if (!registry.get(id)) return;
      if (value) pasted.set(id, value);
      else pasted.delete(id);
      client = undefined;
      notify();
    },
  };

  // Previews through the selected host's `/preview/` (Q806, SD1 Task 26): a ticket per
  // preview, asked for with the same token as the API.
  const preview: StudioPreview = {
    reach: () => ({
      kind: 'remote',
      open: (name) => {
        const host = selected();
        if (!host) return Promise.reject(new Error('Select an agentd host first.'));
        return remotePreviewOpener({
          baseUrl: host.url,
          getToken: tokenOf(host),
          ...(o.fetch ? { fetch: o.fetch } : {}),
        })(name);
      },
    }),
  };

  return {
    kind: 'web',
    hosts,
    preview,
    reach: () => {
      const host = selected();
      if (!host || !hasToken(host.id)) return NO_HOST;
      if (ready?.id !== host.id) ready = { id: host.id, reach: { state: 'ready' } };
      return ready.reach;
    },
    subscribe: (listener) => hosts.subscribe(listener),
    cacheKey: () => `host:${selected()?.id ?? ''}`,
    host: () => {
      const host = selected();
      return host ? { id: host.id, name: host.name } : undefined;
    },
    transport,
  };
}
