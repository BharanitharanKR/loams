// A FAKE loams-agentd for the dev preview of the desktop edition (`?desktop`; SD1 Task 17).
// Dev-only: fake-desktop.ts installs it, and only main.tsx's `import.meta.env.DEV` branch
// loads that. The desktop's agentd transport calls `loams-app://console/loams.agentd.v1.*`
// through the page's `fetch` (@loams/agentd-client transport-desktop.ts); in a browser that
// scheme goes nowhere, so this wraps `fetch` and answers those paths from an in-memory
// Connect router with sample projects and threads. Nothing else is intercepted, and no
// daemon, file or network is touched.

import { createConnectRouter } from '@connectrpc/connect';
import { createFetchHandler } from '@connectrpc/connect/protocol';
import {
  FileService,
  GitService,
  HarnessId,
  HarnessService,
  ProjectService,
  QueueService,
  ReasoningLevel,
  RunGroupService,
  SessionStatus,
  StudioService,
  ThreadService,
  WorkspaceEntryKind,
} from '@loams/proto/agentd';

const BASE = 'loams-app://console/';
const now = Date.now();
const ago = (minutes: number) => BigInt(now - minutes * 60_000);

const REPOS = [
  { path: '~/src/loams', name: 'loams', defaultBranch: 'dev' },
  { path: '~/src/acme-site', name: 'acme-site', defaultBranch: 'main' },
];

const chats = [
  {
    id: 'demo-login',
    title: 'Fix the login redirect loop',
    cwd: '~/src/loams',
    createdAtMs: ago(180),
    lastMessageAtMs: ago(2),
    lastSeenAtMs: ago(1),
  },
  {
    id: 'demo-build',
    title: 'Speed up the console build',
    cwd: '/wt/loams-build',
    branch: 'perf/build',
    createdAtMs: ago(240),
    lastMessageAtMs: ago(14),
    lastSeenAtMs: ago(30),
  },
  {
    id: 'demo-readme',
    title: 'Draft the Studio README',
    cwd: '~/src/loams',
    createdAtMs: ago(600),
    lastMessageAtMs: ago(55),
    lastSeenAtMs: ago(50),
  },
  {
    id: 'demo-r1',
    title: 'Retry: flaky e2e (Claude Code)',
    cwd: '/wt/loams-r1',
    createdAtMs: ago(20),
    lastMessageAtMs: ago(3),
    lastSeenAtMs: ago(3),
  },
  {
    id: 'demo-r2',
    title: 'Retry: flaky e2e (Codex)',
    cwd: '/wt/loams-r2',
    createdAtMs: ago(20),
    lastMessageAtMs: ago(4),
    lastSeenAtMs: ago(4),
  },
  {
    id: 'demo-hero',
    title: 'New hero section',
    cwd: '~/src/acme-site',
    branch: 'landing',
    createdAtMs: ago(90),
    lastMessageAtMs: ago(8),
    lastSeenAtMs: ago(9),
  },
  {
    id: 'demo-forms',
    title: 'Validate the contact form',
    cwd: '~/src/acme-site',
    createdAtMs: ago(2000),
    lastMessageAtMs: ago(1500),
    lastSeenAtMs: ago(1400),
  },
];

const sessions = [
  { chatId: 'demo-login', status: SessionStatus.WORKING, startedAtMs: ago(3), updatedAtMs: ago(0) },
  { chatId: 'demo-r1', status: SessionStatus.WORKING, startedAtMs: ago(6), updatedAtMs: ago(0) },
  { chatId: 'demo-r2', status: SessionStatus.AWAITING_INPUT, updatedAtMs: ago(1) },
  { chatId: 'demo-forms', status: SessionStatus.ERRORED, updatedAtMs: ago(1500) },
];

const worktrees: Record<
  string,
  { path: string; branch: string; chatIds: string[]; dirty?: boolean }[]
> = {
  '~/src/loams': [
    { path: '~/src/loams', branch: 'dev', chatIds: ['demo-login', 'demo-readme'] },
    { path: '/wt/loams-build', branch: 'perf/build', chatIds: ['demo-build'] },
    { path: '/wt/loams-r1', branch: 'studio/r1', chatIds: ['demo-r1'] },
    { path: '/wt/loams-r2', branch: 'studio/r2', chatIds: ['demo-r2'] },
    { path: '/wt/loams-old', branch: 'old-spike', chatIds: [] },
  ],
  '~/src/acme-site': [
    { path: '~/src/acme-site', branch: 'main', chatIds: ['demo-hero', 'demo-forms'] },
  ],
};

const pins = ['demo-readme'];

/** A watch that sends its state once and stays open, as the daemon's do. */
async function* once<T>(value: T, signal: AbortSignal): AsyncGenerator<T> {
  yield value;
  await new Promise<void>((resolve) =>
    signal.addEventListener('abort', () => resolve(), { once: true }),
  );
}

function fakeRouter() {
  return (
    createConnectRouter()
      .service(ProjectService, { listRepos: () => ({ repos: REPOS }) })
      .service(ThreadService, {
        watchThreads: (_req, ctx) => once({ chats }, ctx.signal),
        watchSessions: (_req, ctx) => once({ sessions }, ctx.signal),
        watchSidebarPreferences: (_req, ctx) =>
          once(
            {
              preferences: {
                revision: 1n,
                synced: true,
                initialized: true,
                pinnedSessionIds: pins,
              },
            },
            ctx.signal,
          ),
        markThreadSeen: () => ({ ok: true }),
      })
      .service(GitService, {
        listWorktrees: (req) => ({ worktrees: worktrees[req.repoPath] ?? [] }),
        listBranches: () => ({ branches: ['dev', 'main', 'perf/build'] }),
      })
      // The composer's catalogs (SD1 Task 19).
      .service(StudioService, {
        getHost: () => ({ host: { version: 'fake', features: ['studio.v1', 'run-groups'] } }),
      })
      .service(HarnessService, {
        listHarnesses: () => ({
          harnesses: [
            {
              id: HarnessId.CLAUDE_CODE,
              name: 'Claude Code',
              installed: true,
              enabled: true,
              reasoningLevels: [ReasoningLevel.LOW, ReasoningLevel.MEDIUM, ReasoningLevel.HIGH],
            },
            { id: HarnessId.CODEX, name: 'Codex', installed: true, enabled: true },
          ],
        }),
        listModels: () => ({
          models: [
            { id: 'sonnet', label: 'Sonnet' },
            { id: 'opus', label: 'Opus' },
          ],
        }),
        listCommands: () => ({
          commands: [
            { name: 'review', description: 'Review the working tree' },
            { name: 'compact', description: 'Compact the conversation' },
          ],
        }),
        listSkills: () => ({
          skills: [
            {
              name: 'release-notes',
              path: '~/.claude/skills/release-notes/SKILL.md',
              description: 'Draft release notes',
              enabled: true,
            },
          ],
        }),
      })
      .service(FileService, {
        search: ({ query }) => ({
          matches: ['README.md', 'web/plugins/studio/src/view.tsx', 'docs/plans/README.md']
            .filter((path) => path.toLowerCase().includes(query.toLowerCase()))
            .map((path) => ({
              path,
              name: path.slice(path.lastIndexOf('/') + 1),
              kind: WorkspaceEntryKind.FILE,
              score: 10n,
            })),
        }),
      })
      .service(QueueService, {
        watchQueue: (req, ctx) =>
          once(
            {
              items:
                req.chatId === 'demo-login'
                  ? [{ id: 'q1', text: 'Then add a regression test for the redirect.' }]
                  : [],
            },
            ctx.signal,
          ),
      })
      .service(RunGroupService, {
        watchRunGroups: (_req, ctx) =>
          once(
            {
              groups: [
                {
                  id: 'demo-group',
                  prompt: 'Fix the flaky studio e2e test',
                  createdAtMs: ago(20),
                  members: [{ chatId: 'demo-r1' }, { chatId: 'demo-r2' }],
                },
              ],
            },
            ctx.signal,
          ),
      })
  );
}

/** Answers the desktop agentd route from the fake router; everything else is the page's fetch. */
export function installFakeAgentd(): void {
  const handlers = new Map(
    fakeRouter().handlers.map((h) => [h.requestPath, createFetchHandler(h)] as const),
  );
  const pageFetch = globalThis.fetch.bind(globalThis);
  globalThis.fetch = (input, init) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    if (!url.startsWith(BASE)) return pageFetch(input, init);
    const path = `/${url.slice(BASE.length)}`;
    const handler = handlers.get(path);
    if (!handler) return Promise.resolve(new Response(null, { status: 404 }));
    return handler(new Request(`http://fake-agentd${path}`, init));
  };
}
