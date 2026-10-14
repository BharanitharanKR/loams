// Studio's thread rows (SD1 Task 17; §53 §5.3): what the sidebar knows about a thread,
// from agentd's `WatchThreads`, `WatchSessions`, `WatchApprovals`, `WatchRunGroups` and
// `ListWorktrees`, folded by the plugin into one plain shape. The vendored rules in this
// folder read only these types, never the wire's.

/** A thread's run, from its session (`SessionStatus`). */
export type ThreadRunStatus = "idle" | "working" | "awaiting-input" | "errored";

/** The two fields upstream's worktree clean-up rule reads (`worktreeCleanup.ts`). */
export interface ThreadShell {
  readonly id: string;
  /** A linked worktree the thread runs in; null in the project's main checkout. */
  readonly worktreePath: string | null;
}

export interface SidebarThread extends ThreadShell {
  readonly title: string;
  /** The project (a repository's path) the thread belongs to; null when none matches. */
  readonly projectId: string | null;
  readonly branch: string | null;
  readonly cwd: string | null;
  readonly archived: boolean;
  /** Milliseconds since the epoch. */
  readonly createdAt: number;
  /** The last message, else the creation time. */
  readonly updatedAt: number;
  readonly lastMessageAt: number | null;
  /** When the person last opened it (`MarkThreadSeen`); null when never. */
  readonly lastSeenAt: number | null;
  readonly status: ThreadRunStatus;
  /** When the session's current status began, for "Working 2m". */
  readonly statusSince: number | null;
  /** Tool calls waiting for the person (`WatchApprovals`, supervised mode). */
  readonly pendingApprovals: number;
  /** The run group it is a member of (§53 §5.5), when run groups exist. */
  readonly groupId: string | null;
  readonly parentId: string | null;
}

/** A run group's row: its members are threads (§53 §5.5). */
export interface SidebarRunGroup {
  readonly id: string;
  readonly title: string;
  readonly projectId: string | null;
  readonly memberIds: readonly string[];
  readonly createdAt: number;
  readonly state: "running" | "ready" | "resolved" | "unknown";
}
