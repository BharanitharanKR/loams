// A daemon for the sidebar's tests (SD1 Task 17): ProjectService, ThreadService's watches
// and mutations, GitService's worktrees, and optionally approvals and run groups, as an
// in-memory Connect router behind the desktop's real `agentd` endpoint. A watch yields
// its state, then again after each change, until the client closes it.

import type { MessageInitShape } from '@bufbuild/protobuf';
import { Code, ConnectError, createRouterTransport } from '@connectrpc/connect';
import { desktopAgentdEndpoint } from '@loams/platform-electron';
import { createMockAgentdApi } from '@loams/plugin-desktop-agentd';
import {
  ApprovalMode,
  type ApprovalSchema,
  ApprovalService,
  type ChatSchema,
  GitService,
  ProjectService,
  type RunGroupSchema,
  RunGroupService,
  type SessionSchema,
  ThreadService,
  type WorktreeInfoSchema,
} from '@loams/proto/agentd';
import { studioAgentd } from '../src/agentd.js';

type ChatInit = MessageInitShape<typeof ChatSchema>;
type SessionInit = MessageInitShape<typeof SessionSchema>;
type ApprovalInit = MessageInitShape<typeof ApprovalSchema>;
type GroupInit = MessageInitShape<typeof RunGroupSchema>;
type WorktreeInit = MessageInitShape<typeof WorktreeInfoSchema>;

/** A value a watch streams: each `set` wakes every open stream. */
export function live<T>(initial: T) {
  let value = initial;
  const waiters = new Set<() => void>();
  return {
    get: () => value,
    set(next: T) {
      value = next;
      for (const wake of [...waiters]) wake();
    },
    async *stream(signal: AbortSignal): AsyncGenerator<T> {
      yield value;
      while (!signal.aborted) {
        await new Promise<void>((resolve) => {
          const done = () => {
            waiters.delete(done);
            resolve();
          };
          waiters.add(done);
          signal.addEventListener('abort', done, { once: true });
        });
        if (signal.aborted) return;
        yield value;
      }
    },
  };
}

export const REPOS = [
  { path: '~/src/loams', name: 'loams', defaultBranch: 'dev' },
  { path: '~/src/acme-site', name: 'acme-site', defaultBranch: 'main' },
];

const T0 = Date.parse('2026-10-14T09:00:00Z');
export const at = (minutes: number) => BigInt(T0 + minutes * 60_000);

/** The default threads: two in loams (one in its own worktree, two sharing one), one in acme-site, one elsewhere. */
export function sampleChats(): ChatInit[] {
  return [
    {
      id: 't-login',
      title: 'Fix the login redirect',
      cwd: '~/src/loams',
      createdAtMs: at(0),
      lastMessageAtMs: at(30),
    },
    {
      id: 't-fix',
      title: 'Speed up the build',
      cwd: '/wt/loams-fix',
      branch: 'fix/build',
      createdAtMs: at(1),
      lastMessageAtMs: at(20),
    },
    {
      id: 't-a',
      title: 'Draft the README',
      cwd: '/wt/loams-shared',
      createdAtMs: at(2),
      lastMessageAtMs: at(10),
    },
    {
      id: 't-b',
      title: 'Review the README',
      cwd: '/wt/loams-shared',
      createdAtMs: at(3),
      lastMessageAtMs: at(5),
    },
    {
      id: 't-site',
      title: 'New landing page',
      cwd: '~/src/acme-site/web',
      branch: 'landing',
      createdAtMs: at(4),
      lastMessageAtMs: at(40),
    },
    { id: 't-lost', title: 'Scratch notes', cwd: '/tmp/elsewhere', createdAtMs: at(5) },
  ];
}

export function sampleWorktrees(): Record<string, WorktreeInit[]> {
  return {
    '~/src/loams': [
      { path: '~/src/loams', branch: 'dev', chatIds: ['t-login'] },
      { path: '/wt/loams-fix', branch: 'fix/build', chatIds: ['t-fix'] },
      { path: '/wt/loams-shared', branch: 'readme', chatIds: ['t-a', 't-b'] },
      { path: '/wt/loams-old', branch: 'old', chatIds: [] },
      { path: '/wt/loams-wip', branch: 'wip', chatIds: [], dirty: true },
    ],
  };
}

export function sidebarDaemon(
  o: {
    chats?: ChatInit[];
    sessions?: SessionInit[];
    pins?: string[];
    approvals?: ApprovalInit[] | 'unimplemented';
    groups?: GroupInit[] | 'unimplemented';
    worktrees?: Record<string, WorktreeInit[]>;
  } = {},
) {
  const calls: string[] = [];
  const chats = live<ChatInit[]>(o.chats ?? sampleChats());
  const sessions = live<SessionInit[]>(o.sessions ?? []);
  let revision = 1n;
  const pins = live<string[]>(o.pins ?? []);
  const approvals = live<ApprovalInit[]>(
    o.approvals === 'unimplemented' ? [] : (o.approvals ?? []),
  );
  const groups = live<GroupInit[]>(o.groups === 'unimplemented' ? [] : (o.groups ?? []));
  const worktrees = o.worktrees ?? sampleWorktrees();
  const prefs = () => ({ revision, synced: true, initialized: true, pinnedSessionIds: pins.get() });

  const place = (id: string, list: string[], p: { after?: string; before?: string }) => {
    const rest = list.filter((x) => x !== id);
    const i =
      p.after !== undefined
        ? rest.indexOf(p.after) + 1
        : p.before !== undefined
          ? rest.indexOf(p.before)
          : rest.length;
    rest.splice(i < 0 ? rest.length : i, 0, id);
    return rest;
  };
  const updateChat = (id: string, patch: Partial<ChatInit>) =>
    chats.set(chats.get().map((c) => (c.id === id ? ({ ...c, ...patch } as ChatInit) : c)));

  const transport = createRouterTransport(({ service }) => {
    service(ProjectService, {
      listRepos: () => ({ repos: REPOS }),
    });
    service(ThreadService, {
      async *watchThreads(_req, ctx) {
        for await (const v of chats.stream(ctx.signal)) yield { chats: v };
      },
      async *watchSessions(_req, ctx) {
        for await (const v of sessions.stream(ctx.signal)) yield { sessions: v };
      },
      async *watchSidebarPreferences(_req, ctx) {
        for await (const _ of pins.stream(ctx.signal)) yield { preferences: prefs() };
      },
      createThread: (req) => {
        calls.push(`CreateThread:${req.cwd}`);
        chats.set([...chats.get(), { id: req.chatId, cwd: req.cwd, createdAtMs: at(60) }]);
        return { ok: true };
      },
      renameThread: (req) => {
        calls.push(`RenameThread:${req.chatId}:${req.title}`);
        updateChat(req.chatId, { title: req.title });
        return { ok: true };
      },
      setThreadArchived: (req) => {
        calls.push(`SetThreadArchived:${req.chatId}:${req.archived}`);
        updateChat(req.chatId, { archived: req.archived });
        return { ok: true };
      },
      deleteThread: (req) => {
        calls.push(`DeleteThread:${req.chatId}`);
        chats.set(chats.get().filter((c) => c.id !== req.chatId));
        return { ok: true };
      },
      forkThread: (req) => {
        calls.push(`ForkThread:${req.sourceChatId}`);
        const source = chats.get().find((c) => c.id === req.sourceChatId);
        const chat: ChatInit = {
          cwd: source?.cwd,
          createdAtMs: at(70),
          id: req.chatId,
          title: `${source?.title ?? ''} (fork)`,
          parentChatId: req.sourceChatId,
        };
        chats.set([...chats.get(), chat]);
        return { chat };
      },
      markThreadSeen: (req) => {
        calls.push(`MarkThreadSeen:${req.chatId}`);
        updateChat(req.chatId, { lastSeenAtMs: at(1000) });
        return { ok: true };
      },
      changeSidebarPin: (req) => {
        const change = req.change?.change;
        if (!change?.case) throw new ConnectError('no change', Code.InvalidArgument);
        if (change.case === 'pin' || change.case === 'reorder') {
          const { sessionId, after, before } = change.value;
          calls.push(`ChangeSidebarPin:${change.case}:${sessionId}:${after ?? ''}:${before ?? ''}`);
          revision += 1n;
          pins.set(place(sessionId, pins.get(), { after, before }));
        } else if (change.case === 'unpin') {
          calls.push(`ChangeSidebarPin:unpin:${change.value.sessionId}`);
          revision += 1n;
          pins.set(pins.get().filter((x) => x !== change.value.sessionId));
        }
        return { ok: true, preferences: prefs() };
      },
    });
    service(GitService, {
      listWorktrees: (req) => {
        const list = worktrees[req.repoPath];
        if (!list) throw new ConnectError('not a repository', Code.NotFound);
        return { worktrees: list };
      },
      deleteWorktree: (req) => {
        calls.push(`DeleteWorktree:${req.repoPath}:${req.worktreePath}`);
        const list = worktrees[req.repoPath];
        if (list) worktrees[req.repoPath] = list.filter((w) => w.path !== req.worktreePath);
        return { ok: true };
      },
    });
    if (o.approvals !== 'unimplemented') {
      service(ApprovalService, {
        async *watchApprovals(_req, ctx) {
          for await (const v of approvals.stream(ctx.signal)) yield { pending: v };
        },
        setThreadApprovalMode: (req) => {
          calls.push(`SetThreadApprovalMode:${req.chatId.length > 0}:${ApprovalMode[req.mode]}`);
          return {};
        },
      });
    }
    if (o.groups !== 'unimplemented') {
      service(RunGroupService, {
        async *watchRunGroups(_req, ctx) {
          for await (const v of groups.stream(ctx.signal)) yield { groups: v };
        },
      });
    }
  });

  const mock = createMockAgentdApi({ running: true, background: 'off' });
  const agentd = studioAgentd(desktopAgentdEndpoint({ agentd: mock.api }, transport), {
    pickFolder: async () => null,
  });
  return { agentd, transport, calls, chats, sessions, pins, approvals, groups, worktrees };
}
