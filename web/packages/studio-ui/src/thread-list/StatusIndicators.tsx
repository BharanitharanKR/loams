// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/ThreadStatusIndicators.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the status label, the worktree indicator and a run-group badge, styled with @loams/ui token utilities and a native title in place of the Tooltip; the pull-request, terminal and environment indicators and every atom, store and query dropped. Loams' changes are Apache-2.0.

// A thread row's badges (SD1 Task 17; §53 §5.3): its status (pending approval, awaiting
// input, working, errored, or an unread completion), the worktree it runs in, and the
// run group it belongs to. Each is an image with an accessible name, so a screen reader
// reads the row's state; the row's own label leads with the title.

import { FolderGit2Icon, LayersIcon } from "lucide-react";
import type { ThreadStatusPill } from "./logic";
import { formatWorktreePathForDisplay } from "./worktreeCleanup";

export function ThreadWorktreeIndicator({
  thread,
}: {
  thread: {
    readonly id: string;
    readonly branch: string | null;
    readonly worktreePath: string | null;
  };
}) {
  const worktreePath = thread.worktreePath?.trim();
  if (!worktreePath) {
    return null;
  }

  const displayPath = formatWorktreePathForDisplay(worktreePath);
  const tooltip = thread.branch
    ? `Worktree: ${displayPath} (${thread.branch})`
    : `Worktree: ${displayPath}`;

  return (
    <span
      role="img"
      aria-label={tooltip}
      title={tooltip}
      data-testid={`thread-worktree-${thread.id}`}
      className="inline-flex shrink-0 items-center justify-center text-faint"
    >
      <FolderGit2Icon aria-hidden="true" className="size-3" />
    </span>
  );
}

/** A member of a run group (§53 §5.5): Studio's own badge; upstream has no run groups. */
export function RunGroupBadge({ title }: { title: string }) {
  const label = `Run group: ${title}`;
  return (
    <span
      role="img"
      aria-label={label}
      title={label}
      className="inline-flex shrink-0 items-center justify-center text-info"
    >
      <LayersIcon aria-hidden="true" className="size-3" />
    </span>
  );
}

export function ThreadStatusLabel({
  status,
  compact = false,
}: {
  status: ThreadStatusPill;
  compact?: boolean;
}) {
  const pulse = status.pulse ? "motion-safe:animate-pulse" : "";
  if (compact) {
    return (
      <span
        role="img"
        aria-label={status.label}
        title={status.label}
        className={`inline-flex size-3.5 shrink-0 items-center justify-center ${status.colorClass}`}
      >
        <span
          data-status-dot=""
          className={`size-[7px] rounded-pill ${status.dotClass} ${pulse}`}
        />
      </span>
    );
  }

  return (
    <span
      role="img"
      aria-label={status.label}
      title={status.label}
      className={`inline-flex shrink-0 items-center gap-1 text-[0.7rem] ${status.colorClass}`}
    >
      <span data-status-dot="" className={`size-1.5 rounded-pill ${status.dotClass} ${pulse}`} />
      <span aria-hidden="true">{status.label}</span>
    </span>
  );
}
