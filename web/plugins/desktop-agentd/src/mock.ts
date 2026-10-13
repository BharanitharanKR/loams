// A fixture-backed `desktop.agentd` for the browser preview (fake-desktop.ts)
// and the tests, until loams-agentd ships (DD1 Tasks 6, 10, 11). No token, no URLs.
import type {
  AgentdBackground,
  AgentdRepo,
  AgentdSession,
  AgentdStatus,
  IpcResult,
  LoamsDesktopApi,
} from '@loams/desktop/contracts';

export type AgentdApi = LoamsDesktopApi['agentd'];

export function sampleSessions(now: number = Date.now()): AgentdSession[] {
  return [
    {
      id: 's_01hq7x',
      title: "Summarise yesterday's failed connector runs",
      harness: 'loams-agent',
      state: 'waiting_approval',
      updatedAtMs: now - 90_000,
      pendingApprovals: 1,
      resumable: true,
    },
    {
      id: 's_01hq7y',
      title: 'Refactor the ingest pipeline tests',
      harness: 'claude-code',
      state: 'running',
      updatedAtMs: now - 12_000,
      pendingApprovals: 0,
      resumable: true,
    },
    {
      id: 's_01hq6a',
      title: 'Branch Postgres for the migration dry run',
      harness: 'loams-agent',
      state: 'interrupted',
      updatedAtMs: now - 3_600_000,
      pendingApprovals: 0,
      resumable: true,
    },
    {
      id: 's_01hq5k',
      title: 'Old exploration',
      harness: 'codex',
      state: 'failed',
      updatedAtMs: now - 86_400_000,
      pendingApprovals: 0,
      resumable: false,
    },
  ];
}

/** FAKE sample repositories for the preview; no path is read. */
export function sampleRepos(): AgentdRepo[] {
  return [
    { path: '~/src/loams', name: 'loams', defaultBranch: 'dev' },
    { path: '~/src/acme-site', name: 'acme-site', defaultBranch: 'main' },
  ];
}

function repoName(pathOrUrl: string): string {
  return (
    pathOrUrl
      .replace(/\/+$/, '')
      .split(/[/:]/)
      .pop()
      ?.replace(/\.git$/, '') || pathOrUrl
  );
}

function addRepo(repos: AgentdRepo[], path: string): AgentdRepo {
  const known = repos.find((r) => r.path === path);
  if (known) return { ...known };
  const repo = { path, name: repoName(path), defaultBranch: 'main' };
  repos.push(repo);
  return { ...repo };
}

export interface MockAgentdOptions {
  background?: AgentdBackground;
  running?: boolean;
  sessions?: AgentdSession[];
  dataDir?: string;
  now?: number;
  /** Studio's projects (`agentd.repos`); FAKE sample paths, nothing on disk is read. */
  repos?: AgentdRepo[];
}

export interface MockAgentd {
  api: AgentdApi;
  /** Every call, in order (e.g. `start`, `stop:true`, `reattach:s_1`, `background:on`). */
  calls: string[];
  emit(s: AgentdStatus): void;
}

export function createMockAgentdApi(o: MockAgentdOptions = {}): MockAgentd {
  const now = o.now ?? Date.now();
  const dataDir = o.dataDir ?? '~/.config/Loams Desktop/agentd';
  let background: AgentdBackground = o.background ?? 'ask';
  const sessions = o.sessions ?? sampleSessions(now);
  const calls: string[] = [];
  const repos: AgentdRepo[] = (o.repos ?? sampleRepos()).map((r) => ({ ...r }));
  const listeners = new Set<(s: AgentdStatus) => void>();
  const runningStatus = (mode: 'service' | 'child'): AgentdStatus => ({
    state: 'running',
    mode,
    version: '0.2.1',
    protocol: 1,
    pid: 41234,
    startedAtMs: now - 42 * 60_000,
    dataDir,
    draining: false,
    runningTurns: sessions.filter((s) => s.state === 'running').length,
    pendingApprovals: sessions.reduce((n, s) => n + s.pendingApprovals, 0),
  });
  let status: AgentdStatus = o.running
    ? runningStatus(background === 'on' ? 'service' : 'child')
    : { state: 'stopped', dataDir, version: '0.2.1' };
  const emit = (s: AgentdStatus) => {
    status = s;
    for (const l of listeners) l(s);
  };
  const ok = { ok: true, value: undefined } as const;
  const notRunning = {
    ok: false,
    code: 'not_running',
    message: 'Background agents are not running',
  } as const;
  const api: AgentdApi = {
    status: async () => status,
    onStatus: (cb) => {
      listeners.add(cb);
      return () => void listeners.delete(cb);
    },
    sessions: async (): Promise<IpcResult<AgentdSession[]>> =>
      status.state === 'running'
        ? { ok: true, value: sessions.map((s) => ({ ...s })) }
        : notRunning,
    reattach: async (id) => {
      calls.push(`reattach:${id}`);
      if (status.state !== 'running') return notRunning;
      const s = sessions.find((x) => x.id === id);
      if (!s) return { ok: false, code: 'not_found', message: 'No such session' };
      if (!s.resumable) {
        return { ok: false, code: 'bad_params', message: 'This session cannot be resumed' };
      }
      return { ok: true, value: { route: `/agent/${encodeURIComponent(id)}` } };
    },
    start: async () => {
      calls.push('start');
      if (background === 'ask') {
        return {
          ok: false,
          code: 'consent_required',
          message: 'Choose whether agents keep running in the background first',
        };
      }
      emit(runningStatus(background === 'on' ? 'service' : 'child'));
      return ok;
    },
    stop: async (opts) => {
      calls.push(`stop:${opts.confirmed}`);
      if (!opts.confirmed) {
        return {
          ok: false,
          code: 'consent_required',
          message: "Stopping needs the user's confirmation",
        };
      }
      emit({ state: 'stopped', dataDir, version: '0.2.1' });
      return ok;
    },
    repos: {
      list: async () => {
        calls.push('repos');
        return status.state === 'running'
          ? { ok: true, value: repos.map((r) => ({ ...r })) }
          : notRunning;
      },
      add: async (path) => {
        calls.push(`repos.add:${path}`);
        if (status.state !== 'running') return notRunning;
        return { ok: true, value: addRepo(repos, path) };
      },
      clone: async (url) => {
        calls.push(`repos.clone:${url}`);
        if (status.state !== 'running') return notRunning;
        if (!/^(https|ssh):\/\/|^[\w.-]+@[\w.-]+:/.test(url)) {
          return { ok: false, code: 'invalid_url', message: 'Expected an https or ssh git URL' };
        }
        return { ok: true, value: addRepo(repos, `~/src/${repoName(url)}`) };
      },
    },
    background: {
      get: async () => background,
      set: async (v) => {
        calls.push(`background:${v}`);
        background = v;
        return ok;
      },
    },
  };
  return { api, calls, emit };
}
