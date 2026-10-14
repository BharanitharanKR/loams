// The palette's threads and run groups (SD1 Task 17; rulings T24-6, T24-7): Task 24's
// `StudioPaletteSource`, filled from agentd. Each call reads the first frame of
// `WatchThreads` (and `WatchRunGroups`) and closes the stream, and places the threads in
// projects as the sidebar does (`foldThreads`: `ListRepos`, then each repository's
// `ListWorktrees`). A daemon without run groups (before Task 12) answers none. The other
// rows (files, git actions, harnesses) stay with the source they come from.

import { createClient, type Transport } from '@connectrpc/connect';
import {
  type Chat,
  GitService,
  ProjectService,
  type RunGroup,
  RunGroupService,
  ThreadService,
} from '@loams/proto/agentd';
import type { PaletteRunGroup, PaletteThread, ProjectWorktree } from '@loams/studio-ui';
import { projectOf } from '../agentd.js';
import { pendingPaletteSource, type StudioPaletteSource } from '../palette/source.js';
import { foldThreads, worktreesOf } from './data.js';

/** The first frame of a watch; the stream is closed as soon as it arrives. */
async function firstFrame<T>(
  open: (signal: AbortSignal) => AsyncIterable<T>,
): Promise<T | undefined> {
  const abort = new AbortController();
  try {
    for await (const frame of open(abort.signal)) return frame;
    return undefined;
  } finally {
    abort.abort();
  }
}

async function placedThreads(transport: Transport, groups: readonly RunGroup[] = []) {
  const threads = createClient(ThreadService, transport);
  const [chats, repos] = await Promise.all([
    firstFrame((signal) => threads.watchThreads({}, { signal })),
    createClient(ProjectService, transport)
      .listRepos({})
      .then((r) => r.repos.map(projectOf))
      .catch(() => []),
  ]);
  const git = createClient(GitService, transport);
  const worktrees = new Map<string, ProjectWorktree[]>();
  await Promise.all(
    repos.map((p) =>
      git.listWorktrees({ repoPath: p.path }).then(
        (r) => void worktrees.set(p.id, worktreesOf(r.worktrees)),
        () => undefined,
      ),
    ),
  );
  return foldThreads({
    chats: (chats?.chats ?? []) as Chat[],
    sessions: [],
    approvals: [],
    groups,
    projects: repos,
    worktrees,
  });
}

export function agentdPaletteSource(
  agentd: { transport(): Transport | undefined },
  rest: StudioPaletteSource = pendingPaletteSource(),
): StudioPaletteSource {
  return {
    ...rest,
    async threads(): Promise<PaletteThread[]> {
      const transport = agentd.transport();
      if (!transport) return [];
      const { threads } = await placedThreads(transport);
      return threads.map((t) => ({
        id: t.id,
        title: t.title,
        projectId: t.projectId ?? '',
        branch: t.branch,
        updatedAt: t.updatedAt,
        archived: t.archived,
      }));
    },
    async runGroups(): Promise<PaletteRunGroup[]> {
      const transport = agentd.transport();
      if (!transport) return [];
      let frame: { groups: RunGroup[] } | undefined;
      try {
        frame = await firstFrame((signal) =>
          createClient(RunGroupService, transport).watchRunGroups({}, { signal }),
        );
      } catch {
        // No run groups on this daemon yet (Task 12): no rows.
        return [];
      }
      if (!frame || frame.groups.length === 0) return [];
      const { groups } = await placedThreads(transport, frame.groups);
      return groups.map((g) => ({
        id: g.id,
        title: g.title,
        ...(g.projectId ? { projectId: g.projectId } : {}),
        members: g.memberIds.length,
        updatedAt: g.createdAt,
      }));
    },
  };
}
