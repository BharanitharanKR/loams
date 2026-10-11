// A fixture-backed GitClient (client.ts) the page runs against until `loams.repos.v1`
// is served. It is deterministic: every id, commit, path and timestamp derives from an
// index, so one repository can hold a million workspaces without allocating them, and the
// tests can name exact rows. Searching scans server-side in bounded windows and returns a
// continuation token, as a real paged scan over `_ws_names/` does (§19.4.4).
//
// It holds no secret values. Env files render redacted (§19.13.7), plain lines verbatim.

import {
  type AuditEvent,
  type AuditKind,
  type ChangedPath,
  type ChangeKind,
  type Environment,
  type EnvKey,
  type FileContent,
  type GitClient,
  GitError,
  MAX_CHANGED_HINT,
  MAX_FILE_BYTES,
  PAGE_SIZE,
  type Page,
  type Repo,
  type Snapshot,
  type TreeEntry,
  type Workspace,
  type WorkspaceSummary,
} from './client.js';

/** Fixed clock so the fixtures are stable. */
export const MOCK_NOW = Date.parse('2026-10-10T12:00:00Z');
/** Workspaces the large fixture repository holds. */
export const MONOREPO_WORKSPACES = 1_000_000;
/** How many workspaces one search page scans before it returns a continuation. */
export const SEARCH_SCAN_WINDOW = 50_000;

const iso = (msAgo: number) => new Date(MOCK_NOW - msAgo).toISOString();
const MIN = 60_000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;

/** A deterministic hex string of `len` digits (FNV-1a, chained). */
export function hex(seed: string, len = 40): string {
  let out = '';
  let h = 0x811c9dc5;
  let i = 0;
  while (out.length < len) {
    const s = `${seed}#${i++}`;
    for (let j = 0; j < s.length; j++) {
      h ^= s.charCodeAt(j);
      h = Math.imul(h, 0x01000193) >>> 0;
    }
    out += h.toString(16).padStart(8, '0');
  }
  return out.slice(0, len);
}

const REPOS: Repo[] = [
  {
    repoId: 'r01j9x0monorepo00000000000a',
    name: 'monorepo',
    defaultBranch: 'refs/heads/main',
    seq: 4_812_337,
    workspaceCount: MONOREPO_WORKSPACES,
    hasEnv: true,
    updatedAt: iso(2 * MIN),
  },
  {
    repoId: 'r01j9x0infra0000000000000b',
    name: 'infra',
    defaultBranch: 'refs/heads/main',
    seq: 18_204,
    workspaceCount: 42,
    hasEnv: true,
    updatedAt: iso(3 * HOUR),
  },
  {
    repoId: 'r01j9x0docssite00000000000c',
    name: 'docs-site',
    defaultBranch: 'refs/heads/main',
    seq: 921,
    workspaceCount: 0,
    hasEnv: false,
    updatedAt: iso(9 * DAY),
  },
];

const OWNERS = ['ada', 'grace', 'linus', 'margaret', 'ken'];

const repoById = (repoId: string) => {
  const r = REPOS.find((x) => x.repoId === repoId);
  if (!r) throw new GitError('not_found', `repository ${repoId} not found`, 'repo_not_found');
  return r;
};

/** `w` + 26 characters: a fixed prefix and the index in base 36. */
export const wsIdAt = (i: number) => `w01j9x${i.toString(36).padStart(21, '0')}`;

function wsIndex(wsId: string): number {
  if (!/^w01j9x[0-9a-z]{21}$/.test(wsId)) return -1;
  return Number.parseInt(wsId.slice(6), 36);
}

function summaryAt(repo: Repo, i: number): WorkspaceSummary {
  const agent = i % 3 !== 0;
  const owner = agent ? `agent:run-${(i * 7919) % 100_000}` : OWNERS[i % OWNERS.length]!;
  const overflow = i % 50 === 7;
  return {
    wsId: wsIdAt(i),
    name: agent ? '' : `${owner}/feature-${i}`,
    owner,
    track: repo.defaultBranch,
    n: (i % 30) + 1,
    changedCount: overflow ? MAX_CHANGED_HINT + 2_000 : (i * 7) % 40,
    agent,
    updatedAt: iso((i % 10_000) * MIN + (i % 59) * 1000),
  };
}

export const FILES: Record<string, { size: number; binary?: boolean }> = {
  'README.md': { size: 2_140 },
  'Cargo.toml': { size: 1_872 },
  'env/policy.toml': { size: 412 },
  'env/_shared.env': { size: 260 },
  'env/dev.env': { size: 318 },
  'env/main.env': { size: 322 },
  'crates/loams-git/Cargo.toml': { size: 940 },
  'crates/loams-git/src/lib.rs': { size: 3_204 },
  'crates/loams-git/src/wal.rs': { size: 18_771 },
  'crates/loams-git/src/workspace.rs': { size: 24_009 },
  'crates/loams-vfs/src/main.rs': { size: 6_512 },
  'docs/design/36-loams-git.md': { size: 96_400 },
  'web/plugins/git/src/index.tsx': { size: 1_420 },
  'assets/logo.png': { size: 48_213, binary: true },
  'generated/schema.json': { size: 5 * 1024 * 1024 },
};

const FILE_PATHS = Object.keys(FILES);

const KINDS: ChangeKind[] = ['modified', 'added', 'deleted', 'untracked', 'type_changed'];

function changedAt(i: number, count: number): ChangedPath[] {
  const out: ChangedPath[] = [];
  const seen = new Set<string>();
  for (let k = 0; k < count; k++) {
    const base = FILE_PATHS[(i + k) % FILE_PATHS.length]!;
    const path = seen.has(base) ? `${base.replace(/\.[^.]+$/, '')}_${k}.new` : base;
    seen.add(path);
    const kind = path.endsWith('.new') ? 'untracked' : KINDS[(i + k) % 3]!;
    out.push({ path, kind, staged: kind !== 'untracked' && (i + k) % 2 === 0 });
  }
  return out;
}

const ENV_FILE_TEXT: Record<string, string[]> = {
  'env/_shared.env': [
    '# Shared by every environment; later files override these keys.',
    'SENTRY_DSN',
    '# loams:plain',
    'LOG_LEVEL=info',
  ],
  'env/dev.env': ['DATABASE_URL', 'STRIPE_KEY', 'SESSION_SECRET', 'FEATURE_FLAGS_URL'],
  'env/main.env': ['DATABASE_URL', 'STRIPE_KEY', 'SESSION_SECRET', 'WEBHOOK_SIGNING_KEY'],
};

function fileText(path: string): string {
  const env = ENV_FILE_TEXT[path];
  if (env) {
    return `${env
      .map((line) =>
        line.startsWith('#') || line.includes('=')
          ? line
          : `${line}=<redacted fp=${hex(`fp:${path}:${line}`, 8)}>`,
      )
      .join('\n')}\n`;
  }
  if (path === 'env/policy.toml') {
    return [
      '[environments.dev]',
      'branch = "refs/heads/dev"',
      'files  = ["env/_shared.env", "env/dev.env"]',
      '[environments.main]',
      'branch = "refs/heads/main"',
      'files  = ["env/_shared.env", "env/main.env"]',
      '[keys.DATABASE_URL]',
      'rotate = { every = "30d", provider = "loams-postgres", role = "app", grace = "1h" }',
      '',
    ].join('\n');
  }
  if (path.endsWith('.md')) return `# ${path}\n\nFixture content for ${path}.\n`;
  if (path.endsWith('.toml')) return `[package]\nname = "${path.split('/').at(-2) ?? 'root'}"\n`;
  const lines = [`// ${path}`, '// Fixture content served by the Git page mock.'];
  for (let k = 1; k <= 40; k++) lines.push(`const line_${k} = ${k};`);
  return `${lines.join('\n')}\n`;
}

function changeKindsFor(i: number, ws: WorkspaceSummary) {
  if (ws.changedCount > MAX_CHANGED_HINT) return new Map<string, ChangeKind>();
  return new Map(changedAt(i, ws.changedCount).map((c) => [c.path, c.kind]));
}

function treeAt(dir: string, changes: Map<string, ChangeKind>): TreeEntry[] {
  const prefix = dir ? `${dir}/` : '';
  const dirs = new Map<string, TreeEntry>();
  const files: TreeEntry[] = [];
  const all = new Set([...FILE_PATHS, ...changes.keys()]);
  for (const path of all) {
    if (!path.startsWith(prefix)) continue;
    const rest = path.slice(prefix.length);
    const slash = rest.indexOf('/');
    if (slash >= 0) {
      const name = rest.slice(0, slash);
      const full = prefix + name;
      const entry: TreeEntry = dirs.get(name) ?? { name, path: full, kind: 'dir' };
      if (changes.has(path)) entry.change = 'modified';
      dirs.set(name, entry);
    } else {
      files.push({
        name: rest,
        path,
        kind: 'file',
        size: FILES[path]?.size ?? 64,
        change: changes.get(path),
        env: /^env\/.*\.env$/.test(path) || undefined,
      });
    }
  }
  const byName = (a: TreeEntry, b: TreeEntry) => a.name.localeCompare(b.name);
  return [...[...dirs.values()].sort(byName), ...files.sort(byName)];
}

const ENVIRONMENTS: Environment[] = [
  {
    name: 'dev',
    branch: 'refs/heads/dev',
    files: ['env/_shared.env', 'env/dev.env'],
    revision: 58,
    lastPromotion: {
      commit: hex('promote:dev'),
      seq: 4_812_301,
      at: iso(25 * MIN),
      status: 'promoted',
      added: ['FEATURE_FLAGS_URL'],
      changed: ['STRIPE_KEY'],
      removed: [],
    },
  },
  {
    name: 'main',
    branch: 'refs/heads/main',
    files: ['env/_shared.env', 'env/main.env'],
    revision: 31,
    lastPromotion: {
      commit: hex('promote:main'),
      seq: 4_812_330,
      at: iso(4 * MIN),
      status: 'pending',
      added: [],
      changed: ['STRIPE_KEY'],
      removed: ['LEGACY_API_TOKEN'],
    },
  },
];

const ENV_KEYS: Omit<EnvKey, 'fingerprint'>[] = [
  {
    key: 'DATABASE_URL',
    file: 'env/main.env',
    plain: false,
    environments: {
      dev: { status: 'live', pinnedVersion: 7, liveVersion: 7 },
      main: { status: 'rotated', pinnedVersion: 5, liveVersion: 6 },
    },
    rotation: {
      provider: 'loams-postgres',
      every: '30d',
      grace: '1h',
      status: 'ok',
      lastRotatedAt: iso(6 * DAY),
      nextDueAt: new Date(MOCK_NOW + 24 * DAY).toISOString(),
    },
  },
  {
    key: 'STRIPE_KEY',
    file: 'env/main.env',
    plain: false,
    environments: {
      dev: { status: 'live', pinnedVersion: 3, liveVersion: 3 },
      main: { status: 'pending', pinnedVersion: 3, liveVersion: 2 },
    },
  },
  {
    key: 'SESSION_SECRET',
    file: 'env/main.env',
    plain: false,
    environments: {
      dev: { status: 'live', pinnedVersion: 2, liveVersion: 2 },
      main: { status: 'live', pinnedVersion: 4, liveVersion: 4 },
    },
    rotation: {
      provider: 'random',
      every: '90d',
      grace: '10m',
      status: 'due',
      lastRotatedAt: iso(91 * DAY),
      nextDueAt: iso(1 * DAY),
    },
  },
  {
    key: 'SENTRY_DSN',
    file: 'env/_shared.env',
    plain: false,
    environments: {
      dev: { status: 'live', pinnedVersion: 1, liveVersion: 1 },
      main: { status: 'live', pinnedVersion: 1, liveVersion: 1 },
    },
    rotation: { provider: 'external', every: '-', grace: '0s', status: 'ok' },
  },
  {
    key: 'WEBHOOK_SIGNING_KEY',
    file: 'env/main.env',
    plain: false,
    environments: {
      dev: { status: 'absent' },
      main: { status: 'live', pinnedVersion: 9, liveVersion: 9 },
    },
    rotation: {
      provider: 'webhook',
      every: '7d',
      grace: '15m',
      status: 'failed',
      lastRotatedAt: iso(8 * DAY),
      nextDueAt: iso(1 * DAY),
    },
  },
  {
    key: 'FEATURE_FLAGS_URL',
    file: 'env/dev.env',
    plain: false,
    environments: {
      dev: { status: 'live', pinnedVersion: 1, liveVersion: 1 },
      main: { status: 'absent' },
    },
  },
  {
    key: 'LOG_LEVEL',
    file: 'env/_shared.env',
    plain: true,
    environments: {
      dev: { status: 'live' },
      main: { status: 'live' },
    },
  },
];

export const AUDIT_EVENTS = 250;
const AUDIT_KINDS: AuditKind[] = ['read', 'sealed', 'promoted', 'rotated', 'read', 'destroyed'];

function auditAt(i: number): AuditEvent {
  const kind = AUDIT_KINDS[i % AUDIT_KINDS.length]!;
  const environment = i % 2 === 0 ? 'main' : 'dev';
  const key = ENV_KEYS[i % ENV_KEYS.length]!;
  return {
    id: `evt-${hex(`audit:${i}`, 16)}`,
    kind,
    at: iso(i * 17 * MIN),
    principal: kind === 'promoted' || kind === 'rotated' ? 'system:env-worker' : OWNERS[i % 5]!,
    environment,
    subject: kind === 'read' ? wsIdAt(i * 3) : kind === 'promoted' ? 'deployment:web' : '',
    keys: [key.key],
    versions: [(i % 9) + 1],
  };
}

export interface MockOptions {
  /** Simulated latency per call; 0 answers on the next microtask. */
  latencyMs?: number;
}

function token(pos: number, total: number) {
  return pos < total ? `o:${pos}` : '';
}

function offset(pageToken?: string): number {
  if (!pageToken) return 0;
  const m = /^o:(\d+)$/.exec(pageToken);
  if (!m) throw new GitError('invalid_argument', 'bad page token', 'invalid_page_token');
  return Number(m[1]);
}

const clampSize = (n?: number) => Math.max(1, Math.min(n ?? PAGE_SIZE, 1000));

export function createMockGitClient(opts: MockOptions = {}): GitClient {
  const latency = opts.latencyMs ?? 120;

  function answer<T>(signal: AbortSignal | undefined, fn: () => T): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const abort = () => reject(new DOMException('aborted', 'AbortError'));
      if (signal?.aborted) return abort();
      const run = () => {
        signal?.removeEventListener('abort', onAbort);
        if (signal?.aborted) return abort();
        try {
          resolve(fn());
        } catch (e) {
          reject(e);
        }
      };
      const timer = latency > 0 ? setTimeout(run, latency) : undefined;
      const onAbort = () => {
        if (timer) clearTimeout(timer);
        abort();
      };
      signal?.addEventListener('abort', onAbort, { once: true });
      if (!timer) queueMicrotask(run);
    });
  }

  function workspace(repoId: string, wsId: string) {
    const repo = repoById(repoId);
    const i = wsIndex(wsId);
    if (i < 0 || i >= repo.workspaceCount) {
      throw new GitError('not_found', `workspace ${wsId} not found`, 'workspace_not_found');
    }
    return { repo, i, ws: summaryAt(repo, i) };
  }

  return {
    listRepos: (req, signal) =>
      answer(signal, () => {
        const start = offset(req.pageToken);
        const end = start + clampSize(req.pageSize);
        return {
          items: REPOS.slice(start, end),
          nextPageToken: token(end, REPOS.length),
          totalSize: REPOS.length,
        };
      }),

    listWorkspaces: (req, signal) =>
      answer(signal, (): Page<WorkspaceSummary> => {
        const repo = repoById(req.repoId);
        const total = repo.workspaceCount;
        const size = clampSize(req.pageSize);
        const start = offset(req.pageToken);
        const q = (req.query ?? '').trim().toLowerCase();
        if (!q) {
          const end = Math.min(start + size, total);
          const items: WorkspaceSummary[] = [];
          for (let i = start; i < end; i++) items.push(summaryAt(repo, i));
          return { items, nextPageToken: token(end, total), totalSize: total };
        }
        const items: WorkspaceSummary[] = [];
        let pos = start;
        const stop = Math.min(start + SEARCH_SCAN_WINDOW, total);
        while (pos < stop && items.length < size) {
          const ws = summaryAt(repo, pos++);
          if (
            ws.wsId.includes(q) ||
            ws.name.toLowerCase().includes(q) ||
            ws.owner.toLowerCase().includes(q)
          ) {
            items.push(ws);
          }
        }
        return { items, nextPageToken: token(pos, total) };
      }),

    getWorkspace: (req, signal) =>
      answer(signal, (): Workspace => {
        const { repo, i, ws } = workspace(req.repoId, req.wsId);
        const overflow = ws.changedCount > MAX_CHANGED_HINT;
        return {
          ...ws,
          repoId: repo.repoId,
          base: hex(`base:${repo.repoId}:${i}`),
          baseSeq: repo.seq - (i % 5_000),
          head: hex(`head:${repo.repoId}:${i}:${ws.n}`),
          headRef: ws.name ? `refs/heads/${ws.name.split('/').at(-1)}` : '',
          writer: `mount-${hex(`mount:${i}`, 8)}`,
          changed: overflow ? [] : changedAt(i, ws.changedCount),
          changedOverflow: overflow,
          conflicts: i % 97 === 4 ? ['crates/loams-git/src/wal.rs'] : [],
          fork:
            i % 11 === 5
              ? { repoId: repo.repoId, wsId: wsIdAt(Math.max(0, i - 5)), n: 1 }
              : undefined,
        };
      }),

    listSnapshots: (req, signal) =>
      answer(signal, (): Page<Snapshot> => {
        const { i, ws } = workspace(req.repoId, req.wsId);
        const total = ws.n + 1;
        const start = offset(req.pageToken);
        const end = Math.min(start + clampSize(req.pageSize), total);
        const items: Snapshot[] = [];
        // Latest first.
        for (let k = start; k < end; k++) {
          const n = ws.n - k;
          items.push({
            n,
            createdAt: iso(k * 9 * MIN + (i % 59) * 1000),
            writer: `mount-${hex(`mount:${i}:${n < 3 ? 'a' : 'b'}`, 8)}`,
            principal: ws.owner,
            label: n === 0 ? 'created' : n % 10 === 0 ? `checkpoint-${n / 10}` : '',
            head: hex(`head:${req.repoId}:${i}:${n}`),
            changedCount: Math.min(ws.changedCount, n * 2),
            takeover:
              n === 3
                ? { fromMount: `mount-${hex(`mount:${i}:a`, 8)}`, fromN: 2, reason: 'writer idle' }
                : undefined,
          });
        }
        return { items, nextPageToken: token(end, total), totalSize: total };
      }),

    getTree: (req, signal) =>
      answer(signal, () => {
        const { i, ws } = workspace(req.repoId, req.wsId);
        return treeAt(req.path, changeKindsFor(i, ws));
      }),

    readFile: (req, signal) =>
      answer(signal, (): FileContent => {
        const { i, ws } = workspace(req.repoId, req.wsId);
        const changes = changeKindsFor(i, ws);
        const meta = FILES[req.path];
        if (!meta && !changes.has(req.path)) {
          throw new GitError('not_found', `${req.path} not found`, 'path_not_found');
        }
        if (changes.get(req.path) === 'deleted') {
          throw new GitError('not_found', `${req.path} is deleted in this workspace`, 'path_deleted');
        }
        const size = meta?.size ?? 64;
        if (meta?.binary) return { path: req.path, size, binary: true, text: '', truncated: false };
        if (size > MAX_FILE_BYTES) {
          const head = `{\n  "note": "first ${MAX_FILE_BYTES} bytes of a generated file"\n`;
          return { path: req.path, size, binary: false, text: head, truncated: true };
        }
        return { path: req.path, size, binary: false, text: fileText(req.path), truncated: false };
      }),

    listEnvironments: (req, signal) =>
      answer(signal, () => (repoById(req.repoId).hasEnv ? ENVIRONMENTS : [])),

    listEnvKeys: (req, signal) =>
      answer(signal, () =>
        repoById(req.repoId).hasEnv
          ? ENV_KEYS.map((k) => ({ ...k, fingerprint: hex(`fp:${k.file}:${k.key}`, 8) }))
          : [],
      ),

    listEnvAudit: (req, signal) =>
      answer(signal, (): Page<AuditEvent> => {
        if (!repoById(req.repoId).hasEnv) return { items: [], nextPageToken: '', totalSize: 0 };
        const all: AuditEvent[] = [];
        for (let k = 0; k < AUDIT_EVENTS; k++) {
          const e = auditAt(k);
          if (!req.environment || e.environment === req.environment) all.push(e);
        }
        const start = offset(req.pageToken);
        const end = Math.min(start + clampSize(req.pageSize ?? 50), all.length);
        return {
          items: all.slice(start, end),
          nextPageToken: token(end, all.length),
          totalSize: all.length,
        };
      }),
  };
}
