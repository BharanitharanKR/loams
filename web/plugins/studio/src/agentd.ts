// How Studio reaches loams-agentd (§53 §5.2, §6; SD1 Tasks 6, 15). Since Task 6 it
// is the console's `agentd` service (@loams/plugin-rpc): its reach and its Connect
// transport, the same on the desktop and the web. The desktop's own bridge is used
// only for what is not the daemon's: the folder picker. Projects are read and
// added through `loams.agentd.v1.ProjectService` with connect-query
// (projects/queries.ts) under `AgentdScope`, which follows the daemon as it comes
// and goes, so the components did not change when the two Task 15 adapters went.
// In a browser Studio's own host registry (SD1 Task 7, hosts/agentd.ts) is the
// `StudioAgentd`: its transport is the selected remote host's.

import type { AgentdReach, AgentdService } from '@loams/console-host';
import type { Repo as RepoMessage } from '@loams/proto/agentd';
import type { StudioHosts } from './hosts/agentd.js';
import type { StudioPreview } from './preview/access.js';

/** Whether Studio can talk to a daemon right now. */
export type Reach = AgentdReach;

/** A Studio project: a repository on the daemon's host (§53 §5.2). */
export interface Project {
  /** Stable within a host. Until projects are agentd spaces, the repository path. */
  id: string;
  name: string;
  path: string;
  branch: string | null;
}

/** Studio's view of the daemon: the console's `agentd`, plus the desktop's picker. */
export interface StudioAgentd extends AgentdService {
  /** A key for Studio's own query keys: the host's, so hosts never mix. */
  cacheKey(): string;
  /** Web: the registered hosts and the selection (Settings › Agentd hosts, `?host=`). */
  hosts?: StudioHosts;
  /** Desktop: the OS folder picker; null when cancelled. */
  pickFolder?(): Promise<string | null>;
  /** How the Preview tab reaches dev servers (SD1 Task 26); absent: port-forward hint. */
  preview?: StudioPreview;
}

export function studioAgentd(
  agentd: AgentdService,
  shell?: { pickFolder(title?: string): Promise<string | null> },
  preview?: StudioPreview,
): StudioAgentd {
  return {
    kind: agentd.kind,
    reach: () => agentd.reach(),
    subscribe: (listener) => agentd.subscribe(listener),
    transport: () => agentd.transport(),
    host: () => agentd.host(),
    cacheKey: () => {
      const host = agentd.host();
      return host ? `host:${host.id}` : 'local';
    },
    ...(agentd.start && { start: () => agentd.start?.() ?? Promise.resolve() }),
    ...(shell && { pickFolder: () => shell.pickFolder('Add a project folder') }),
    ...(preview && { preview }),
  };
}

export function projectOf(repo: Pick<RepoMessage, 'path' | 'name' | 'defaultBranch'>): Project {
  return { id: repo.path, name: repo.name, path: repo.path, branch: repo.defaultBranch ?? null };
}

/**
 * The git URLs Studio offers to `CloneRepo`: https, ssh, or scp-like
 * `user@host:path`. Never `file:`, git's `ext::` command transport, or anything
 * starting with `-` (the daemon refuses the same again, SD1 Task 6).
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
