// A FAKE loams-agentd for the dev preview of the desktop edition (`?desktop`; SD1 Task 17).
// Dev-only: fake-desktop.ts installs it, and only main.tsx's `import.meta.env.DEV` branch
// loads that. The desktop's agentd transport calls `loams-app://console/loams.agentd.v1.*`
// through the page's `fetch` (@loams/agentd-client transport-desktop.ts); in a browser that
// scheme goes nowhere, so this wraps `fetch` and answers those paths from an in-memory
// Connect router with sample projects and threads. Nothing else is intercepted, and no
// daemon, file or network is touched.

import { type ConnectRouter, createConnectRouter } from '@connectrpc/connect';
import { createFetchHandler } from '@connectrpc/connect/protocol';
import {
  ApprovalRisk,
  ApprovalService,
  CheckState,
  DiffService,
  DiffSide,
  FileService,
  GitService,
  HarnessId,
  HarnessService,
  NotificationService,
  PreviewService,
  ProjectService,
  QueueService,
  ReasoningLevel,
  ReviewService,
  RunGroupService,
  RunGroupState,
  RunMemberStatus,
  SessionStatus,
  StudioService,
  ThreadService,
  WorkspaceEntryKind,
} from '@loams/proto/agentd';
import { pendingDemoApprovals, transcriptFrames } from './fake-agentd-transcripts.js';

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
    lastMessagePreview:
      'The test waited on a fixed timeout. It now waits for the composer to be ready, and passes 50 runs in a row.',
  },
  {
    id: 'demo-r2',
    title: 'Retry: flaky e2e (Codex)',
    cwd: '/wt/loams-r2',
    createdAtMs: ago(20),
    lastMessageAtMs: ago(4),
    lastSeenAtMs: ago(4),
    lastMessagePreview: 'I want to run the e2e suite to check the retry. May I?',
  },
  {
    id: 'demo-r3',
    title: 'Retry: flaky e2e (OpenCode)',
    cwd: '/wt/loams-r3',
    createdAtMs: ago(20),
    lastMessageAtMs: ago(1),
    lastSeenAtMs: ago(1),
    lastMessagePreview: 'Reading playwright.config.ts and the studio spec…',
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
  { chatId: 'demo-r3', status: SessionStatus.WORKING, startedAtMs: ago(9), updatedAtMs: ago(0) },
  { chatId: 'demo-forms', status: SessionStatus.ERRORED, updatedAtMs: ago(1500) },
];

const worktrees: Record<
  string,
  { path: string; branch: string; chatIds: string[]; dirty?: boolean; head?: string }[]
> = {
  '~/src/loams': [
    { path: '~/src/loams', branch: 'dev', chatIds: ['demo-login', 'demo-readme'] },
    { path: '/wt/loams-build', branch: 'perf/build', chatIds: ['demo-build'] },
    { path: '/wt/loams-r1', branch: 'studio/r1', chatIds: ['demo-r1'], head: '9f3c2a71e0' },
    { path: '/wt/loams-r2', branch: 'studio/r2', chatIds: ['demo-r2'], head: '41d07be2c9' },
    { path: '/wt/loams-r3', branch: 'studio/r3', chatIds: ['demo-r3'], head: 'c2b8e14a77' },
    { path: '/wt/loams-old', branch: 'old-spike', chatIds: [] },
  ],
  '~/src/acme-site': [
    { path: '~/src/acme-site', branch: 'main', chatIds: ['demo-hero', 'demo-forms'] },
  ],
};

const pins = ['demo-readme'];

// The run group's three variants (SD1 Task 25): one done with its check passed, one waiting
// for an approval, one still running; two ask for the same tool call.
const GROUP_MEMBERS = [
  {
    chatId: 'demo-r1',
    harness: HarnessId.CLAUDE_CODE,
    model: 'sonnet',
    status: RunMemberStatus.DONE,
    filesChanged: 2,
    additions: 18,
    deletions: 6,
    elapsedMs: 412_000n,
    tokens: 48_210n,
    cwd: '/wt/loams-r1',
    branch: 'studio/r1',
    check: {
      state: CheckState.PASSED,
      exitCode: 0,
      logTail: [
        '> playwright test studio.spec.ts --repeat-each=50',
        'Running 50 tests using 4 workers',
        '  50 passed (2.1m)',
      ],
    },
  },
  {
    chatId: 'demo-r2',
    harness: HarnessId.CODEX,
    status: RunMemberStatus.AWAITING_INPUT,
    filesChanged: 3,
    additions: 41,
    deletions: 12,
    elapsedMs: 380_000n,
    startedAtMs: ago(7),
    cwd: '/wt/loams-r2',
    branch: 'studio/r2',
    check: {},
  },
  {
    chatId: 'demo-r3',
    harness: HarnessId.OPENCODE,
    status: RunMemberStatus.RUNNING,
    filesChanged: 1,
    additions: 4,
    deletions: 1,
    elapsedMs: 0n,
    startedAtMs: ago(9),
    cwd: '/wt/loams-r3',
    branch: 'studio/r3',
    check: {},
  },
];

const GROUP_MATRIX = [
  {
    path: 'web/apps/console/test/e2e/studio.spec.ts',
    perMember: [
      { chatId: 'demo-r1', additions: 14, deletions: 5 },
      { chatId: 'demo-r2', additions: 22, deletions: 9 },
      { chatId: 'demo-r3', additions: 4, deletions: 1 },
    ],
  },
  {
    path: 'web/apps/console/playwright.config.ts',
    perMember: [{ chatId: 'demo-r2', additions: 3, deletions: 3 }],
  },
  {
    path: 'web/plugins/studio/src/thread/Composer.tsx',
    perMember: [
      { chatId: 'demo-r1', additions: 4, deletions: 1 },
      { chatId: 'demo-r2', additions: 16, deletions: 0 },
    ],
  },
];

/** A small patch for the pair diff: what `b` (or `a`, from the base) changed. */
function pairPatch(path: string | undefined): string {
  const file = path ?? 'web/apps/console/test/e2e/studio.spec.ts';
  return `diff --git a/${file} b/${file}
index 1111111..2222222 100644
--- a/${file}
+++ b/${file}
@@ -10,9 +10,10 @@ test.describe('studio', () => {
   test('sends a prompt', async ({ page }) => {
     await page.goto('/#/studio');
-    await page.waitForTimeout(2000);
-    await page.getByRole('textbox').fill('hello');
+    const composer = page.getByRole('textbox', { name: 'Message' });
+    await expect(composer).toBeEditable();
+    await composer.fill('hello');
     await page.keyboard.press('Enter');
-    await expect(page.getByText('hello')).toBeVisible();
+    await expect(page.getByText('hello')).toBeVisible({ timeout: 15_000 });
   });
 });
`;
}

/** A watch that sends its state once and stays open, as the daemon's do. */
async function* once<T>(value: T, signal: AbortSignal): AsyncGenerator<T> {
  yield value;
  await new Promise<void>((resolve) =>
    signal.addEventListener('abort', () => resolve(), { once: true }),
  );
}

/** Waits `ms` (forever when absent), or less if the watch is cancelled; true if it was. */
function pause(ms: number | undefined, signal: AbortSignal): Promise<boolean> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve(true);
    const timer = ms === undefined ? undefined : setTimeout(() => resolve(false), ms);
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        resolve(true);
      },
      { once: true },
    );
  });
}

/** A watch that sends its frames `gapMs` apart and then stays open. */
async function* frames<T>(values: T[], gapMs: number, signal: AbortSignal): AsyncGenerator<T> {
  for (const [i, value] of values.entries()) {
    if (i > 0 && (await pause(gapMs, signal))) return;
    yield value;
  }
  await pause(undefined, signal);
}

/** The demo working-tree change of a checkout: one spec file. */
const SPEC_OLD = `test.describe('studio', () => {
  test('sends a prompt', async ({ page }) => {
    await page.goto('/#/studio');
    await page.waitForTimeout(2000);
    await page.getByRole('textbox').fill('hello');
    await page.keyboard.press('Enter');
  });
});
`;
const SPEC_NEW = SPEC_OLD.replace(
  "    await page.waitForTimeout(2000);\n    await page.getByRole('textbox').fill('hello');\n",
  "    const composer = page.getByRole('textbox', { name: 'Message' });\n    await expect(composer).toBeEditable();\n    await composer.fill('hello');\n",
);
const SPEC_PATH = 'web/apps/console/test/e2e/studio.spec.ts';
const SPEC_PATCH = `diff --git a/${SPEC_PATH} b/${SPEC_PATH}
index 1111111..2222222 100644
--- a/${SPEC_PATH}
+++ b/${SPEC_PATH}
@@ -1,8 +1,9 @@
 test.describe('studio', () => {
   test('sends a prompt', async ({ page }) => {
     await page.goto('/#/studio');
-    await page.waitForTimeout(2000);
-    await page.getByRole('textbox').fill('hello');
+    const composer = page.getByRole('textbox', { name: 'Message' });
+    await expect(composer).toBeEditable();
+    await composer.fill('hello');
     await page.keyboard.press('Enter');
   });
 });
`;

function checkoutDiff(cwd: string) {
  return {
    cwd,
    checkoutId: `demo:${cwd}`,
    checksum: `demo-sum:${cwd}`,
    patch: SPEC_PATCH,
    files: [{ path: SPEC_PATH, status: 'modified', additions: 3, deletions: 2 }],
    additions: 3,
    deletions: 2,
    updatedAtMs: ago(1),
  };
}

/** Every demo checkout's diff, for `WatchCheckoutDiffs`. */
const CHECKOUT_DIFFS = [...new Set(chats.map((c) => c.cwd))].map(checkoutDiff);

/** One review comment on the spec, in each checkout (the Diff panel's comments). */
function reviewComments(checkoutId: string) {
  return [
    {
      id: `${checkoutId}#c1`,
      checkoutId,
      path: SPEC_PATH,
      side: DiffSide.NEW,
      line: 5,
      body: 'Name the textbox in the page too, so this locator does not match the search field.',
      diffChecksum: checkoutId.replace(/^demo:/, 'demo-sum:'),
      createdAtMs: ago(6),
    },
  ];
}

const bash = (command: string) => ({
  kind: {
    case: 'structValue' as const,
    value: { fields: { command: { kind: { case: 'stringValue' as const, value: command } } } },
  },
});

export interface FakeAgentdOptions {
  /** The gap between a transcript's frames (ms). */
  transcriptGapMs?: number;
}

/** The fake daemon's services, on any Connect router (the preview's, or a test transport). */
export function fakeAgentdRoutes(router: ConnectRouter, o: FakeAgentdOptions = {}): ConnectRouter {
  const gap = o.transcriptGapMs ?? 1500;
  return (
    router
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
        // The thread page (SD1 Task 18): the transcript, its restore points and its usage.
        watchTranscript: (req, ctx) => frames(transcriptFrames(req.chatId), gap, ctx.signal),
        listCheckpoints: (req) => ({
          checkpoints: [
            {
              turnId: `${req.chatId}-u1`,
              ref: `refs/loams/checkpoints/${req.chatId}/turn/1/pre`,
              createdAtMs: ago(12),
              filesChanged: 1,
            },
          ],
        }),
        getThreadUsage: () => ({
          turns: [],
          totals: { inputTokens: 14_200n, outputTokens: 4_200n, durationMs: 59_000n },
        }),
      })
      .service(GitService, {
        listWorktrees: (req) => ({ worktrees: worktrees[req.repoPath] ?? [] }),
        listBranches: () => ({ branches: ['dev', 'main', 'perf/build'] }),
        listGitHistory: () => ({
          page: {
            commits: [
              { sha: '5e1f00aa7c', subject: 'console: the thread list in Console mode' },
              { sha: '41d07be2c9', subject: 'studio: the pair diff for run groups' },
              { sha: '9f3c2a71e0', subject: 'agentd: supervised threads by default' },
            ].map((c, i) => ({
              ...c,
              authorName: 'Priya Raman',
              authorEmail: 'priya@example.com',
              authoredAt: new Date(now - (i + 1) * 3_600_000).toISOString(),
            })),
          },
        }),
        watchChangeRequest: (req, ctx) =>
          once({ status: { cwd: req.cwd, branch: req.branch ?? '' } }, ctx.signal),
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
      .service(PreviewService, {
        // The Preview tab's dev servers (SD1 Task 26).
        watchPreviews: (_req, ctx) =>
          once(
            {
              snapshot: {
                proxyPort: 7331,
                projectName: 'loams',
                services: [
                  {
                    id: 'demo-web',
                    name: 'web',
                    hostname: 'laptop.loams.localhost',
                    port: 5173,
                    pid: 4242,
                    projectName: 'loams',
                    startedAtMs: ago(12),
                  },
                  {
                    id: 'demo-api',
                    name: 'api',
                    hostname: 'laptop.loams-api.localhost',
                    port: 3000,
                    pid: 4243,
                    projectName: 'loams',
                    startedAtMs: ago(12),
                  },
                ],
              },
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
                  prompt:
                    'Fix the flaky studio e2e test\n\nIt fails about one run in ten on CI, waiting for the composer.',
                  base: 'dev',
                  baseSha: '5e1f00aa',
                  repoPath: '~/src/loams',
                  state: RunGroupState.RUNNING,
                  checkActionId: 'demo-e2e',
                  createdAtMs: ago(20),
                  members: GROUP_MEMBERS,
                },
              ],
            },
            ctx.signal,
          ),
        getRunGroupComparison: (req) => ({
          matrix: GROUP_MATRIX,
          pairDiff: { cwd: '', patch: pairPatch(req.path), checksum: 'demo-pair' },
        }),
        runGroupCheck: () => ({ runs: [] }),
      })
      // "Approve for all variants" (SD1 Task 25): one tool call waiting in two members.
      .service(ApprovalService, {
        watchApprovals: (_req, ctx) =>
          once(
            {
              pending: pendingDemoApprovals().map((a, i) => ({
                id: `demo-ap-${i}`,
                chatId: a.chatId,
                turnId: a.turnId,
                tool: a.tool,
                args: bash(a.command),
                risk: ApprovalRisk.WRITE,
                createdAtMs: ago(1),
              })),
            },
            ctx.signal,
          ),
        resolveApproval: () => ({}),
      })
      .service(DiffService, {
        getCheckoutDiff: (req) => ({ diff: checkoutDiff(req.cwd) }),
        watchCheckoutDiffs: (_req, ctx) => once({ diffs: CHECKOUT_DIFFS }, ctx.signal),
        getCheckoutFileDiffText: (req) => ({
          text: {
            diffChecksum: req.diffChecksum,
            ...(req.path === SPEC_PATH ? { oldText: SPEC_OLD, newText: SPEC_NEW } : {}),
          },
        }),
      })
      // No notifications: the bell's watch stays open and quiet.
      .service(NotificationService, {
        watchNotifications: (_req, ctx) => frames([], gap, ctx.signal),
      })
      // The Diff panel's review comments (SD1 Task 20): one per checkout, kept read-only.
      .service(ReviewService, {
        watchReviewComments: (req, ctx) =>
          once(
            { frame: { case: 'reset', value: { comments: reviewComments(req.checkoutId) } } },
            ctx.signal,
          ),
        upsertReviewComment: (req) => ({ id: req.comment?.id ?? 'demo-comment' }),
        deleteReviewComment: (req) => ({ id: req.id }),
        resolveReviewComment: (req) => ({ id: req.id }),
      })
  );
}

/** Answers the desktop agentd route from the fake router; everything else is the page's fetch. */
export function installFakeAgentd(): void {
  const handlers = new Map(
    fakeAgentdRoutes(createConnectRouter()).handlers.map(
      (h) => [h.requestPath, createFetchHandler(h)] as const,
    ),
  );
  const pageFetch = globalThis.fetch.bind(globalThis);
  globalThis.fetch = (input, init) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    if (!url.startsWith(BASE)) return pageFetch(input, init);
    const path = `/${url.slice(BASE.length)}`;
    const handler = handlers.get(path);
    if (!handler) {
      // A method the preview has no fake for yet: say which, so the next one is easy to add.
      console.warn(`fake agentd: no fake for ${path}`);
      return Promise.resolve(new Response(null, { status: 404 }));
    }
    return handler(new Request(`http://fake-agentd${path}`, init));
  };
}
