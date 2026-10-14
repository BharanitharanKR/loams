// How Studio reaches loams-agentd for its projects (§53 §5.2, §6). The methods are
// named after the daemon's ndjson methods they map to (`ListRepos`, `AddRepo`,
// `CloneRepo`), so the Connect client of SD1 Tasks 5–7 (`rpc.agentd.projects`)
// replaces the two adapters below without changing the components.
//
//   desktop: the existing `desktop.agentd` bridge (status, start) plus its `repos`
//            group, which Electron main serves over the ndjson wire with the token
//            it alone holds; the folder picker is `desktop.shell.pickFolder`.
//   web:     the selected remote agentd host over the web transport (SD1 Task 7,
//            `hosts/agentd.ts`); with no host, Studio shows "Connect an agentd host".
//            `webAgentd` is the fixed-client form the tests use.

import type { AgentdStatus, IpcResult } from '@loams/desktop/contracts';
import type { StudioHosts } from './hosts/agentd.js';

/** Whether Studio can talk to a daemon right now. */
export type Reach =
  | { state: 'checking' }
  | { state: 'ready' }
  /** Desktop: the daemon is not running. */
  | { state: 'stopped' }
  | { state: 'unavailable'; reason: string }
  /** Web: no remote agentd host is connected. */
  | { state: 'no-host' };

/** A Studio project: a repository on the daemon's host (§53 §5.2). */
export interface Project {
  /** Stable within a host. Until projects are agentd spaces (Task 6), the repository path. */
  id: string;
  name: string;
  path: string;
  branch: string | null;
}

/** A repository row as `ListRepos`, `AddRepo` and `CloneRepo` return it. */
export interface Repo {
  path: string;
  name: string;
  defaultBranch: string | null;
}

export class StudioError extends Error {
  override name = 'StudioError';
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

export interface StudioAgentd {
  readonly kind: 'desktop' | 'web';
  reach(): Reach;
  /** Calls back when `reach()` may have changed. */
  subscribe(listener: () => void): () => void;
  /** A key for the query cache: projects of different hosts never mix. */
  cacheKey(): string;
  /** The connected agentd host, shown in the header (web). */
  host(): { id: string; name: string } | undefined;
  /** Web: the registered hosts and the selection (Settings › Agentd hosts, `?host=`). */
  hosts?: StudioHosts;
  /** Desktop: start the daemon. */
  start?(): Promise<void>;
  listProjects(): Promise<Project[]>;
  /** Desktop: the OS folder picker; null when cancelled. */
  pickFolder?(): Promise<string | null>;
  /** `AddRepo`: a folder on the daemon's host. */
  addProject?(path: string): Promise<Project>;
  /** `CloneRepo`: a remote git URL, cloned on the daemon's host. */
  cloneProject?(url: string): Promise<Project>;
}

export function projectOf(repo: Repo): Project {
  return { id: repo.path, name: repo.name, path: repo.path, branch: repo.defaultBranch };
}

/**
 * The git URLs Studio hands to `CloneRepo`: https, ssh, or scp-like `user@host:path`.
 * Never `file:`, git's `ext::` command transport, or anything starting with `-`
 * (Electron main checks the same again).
 */
export function isCloneUrl(u: string): boolean {
  if (u.length > 2048 || /\s/.test(u) || u.startsWith('-')) return false;
  if (/^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+:[A-Za-z0-9._~/-]+$/.test(u)) return true;
  try {
    const url = new URL(u);
    return (url.protocol === 'https:' || url.protocol === 'ssh:') && url.hostname !== '';
  } catch {
    return false;
  }
}

function unwrap<T>(r: IpcResult<T>): T {
  if (r.ok) return r.value;
  throw new StudioError(r.code, r.message);
}

/** The part of the desktop bridge Studio uses. */
export interface DesktopBridge {
  agentd: {
    status(): Promise<AgentdStatus>;
    onStatus(cb: (s: AgentdStatus) => void): () => void;
    start(): Promise<IpcResult<void>>;
    repos: {
      list(): Promise<IpcResult<Repo[]>>;
      add(path: string): Promise<IpcResult<Repo>>;
      clone(url: string): Promise<IpcResult<Repo>>;
    };
  };
  shell: { pickFolder(title?: string): Promise<string | null> };
}

function reachOf(s: AgentdStatus): Reach {
  if (s.state === 'running') return { state: 'ready' };
  if (s.state === 'stopped') return { state: 'stopped' };
  return { state: 'unavailable', reason: s.reason };
}

/** Studio over the desktop bridge (see the top of this file). */
export function desktopAgentd(bridge: DesktopBridge): StudioAgentd {
  let reach: Reach = { state: 'checking' };
  const listeners = new Set<() => void>();
  const set = (next: Reach) => {
    reach = next;
    for (const l of [...listeners]) l();
  };
  let watching = false;
  const watch = () => {
    if (watching) return;
    watching = true;
    bridge.agentd.onStatus((s) => set(reachOf(s)));
    bridge.agentd
      .status()
      .then((s) => set(reachOf(s)))
      .catch(() => set({ state: 'unavailable', reason: 'no_bridge' }));
  };
  return {
    kind: 'desktop',
    reach: () => {
      watch();
      return reach;
    },
    subscribe(listener) {
      watch();
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    cacheKey: () => 'local',
    host: () => undefined,
    async start() {
      unwrap(await bridge.agentd.start());
      // The bridge's status event follows; read it now so the page does not wait.
      set(reachOf(await bridge.agentd.status()));
    },
    listProjects: async () => unwrap(await bridge.agentd.repos.list()).map(projectOf),
    pickFolder: () => bridge.shell.pickFolder('Add a project folder'),
    addProject: async (path) => projectOf(unwrap(await bridge.agentd.repos.add(path))),
    cloneProject: async (url) => {
      if (!isCloneUrl(url))
        throw new StudioError('invalid_url', 'Expected an https, ssh or user@host:path git URL');
      return projectOf(unwrap(await bridge.agentd.repos.clone(url)));
    },
  };
}

/** A remote agentd's project methods; `hosts/agentd.ts` makes one per host. */
export interface AgentdProjectsClient {
  host: { id: string; name: string };
  listRepos(): Promise<Repo[]>;
  cloneRepo(url: string): Promise<Repo>;
}

/** Studio in the browser with one fixed host client, or none (tests; see the top of this file). */
export function webAgentd(o: { client?: AgentdProjectsClient } = {}): StudioAgentd {
  const client = o.client;
  // One object per state: `reach()` is a useSyncExternalStore snapshot.
  const reach: Reach = client ? { state: 'ready' } : { state: 'no-host' };
  return {
    kind: 'web',
    reach: () => reach,
    subscribe: () => () => {},
    cacheKey: () => `host:${client?.host.id ?? ''}`,
    host: () => client?.host,
    async listProjects() {
      if (!client) throw new StudioError('no_host', 'No agentd host is connected');
      return (await client.listRepos()).map(projectOf);
    },
    async cloneProject(url) {
      if (!client) throw new StudioError('no_host', 'No agentd host is connected');
      if (!isCloneUrl(url))
        throw new StudioError('invalid_url', 'Expected an https, ssh or user@host:path git URL');
      return projectOf(await client.cloneRepo(url));
    },
  };
}
