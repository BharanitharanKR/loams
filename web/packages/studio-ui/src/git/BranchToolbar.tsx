// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/BranchToolbar.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// a presentational toolbar: the workspace selector, the branch picker and a "Create worktree" step, fed by the container (loams-agentd's ListRefs and ListWorktrees); the environment (host) selector, composer strip, draft store and panel layout dropped. Loams' changes are Apache-2.0.

// The branch toolbar (§53 §5.7; SD1 Task 23): where work happens (the project's
// checkout, a new worktree, or the previous one) and on which branch.

import { Button } from "@loams/ui";
import { FolderPlus } from "lucide-react";
import type { ReactNode } from "react";
import { BranchSelector } from "./BranchSelector";
import {
  type BranchRef,
  type EnvMode,
  type PreviousWorktreeSeed,
  resolveBranchToolbarValue,
  resolvePreviousWorktreeLabel,
} from "./BranchToolbar.logic";
import { EnvModeSelector } from "./EnvModeSelector";

export interface BranchToolbarProps {
  refs: readonly BranchRef[];
  refsLoading?: boolean;
  envMode: EnvMode;
  onEnvModeChange: (mode: EnvMode) => void;
  /** The worktree the toolbar targets, or null for the project's own checkout. */
  activeWorktreePath: string | null;
  /** The checkout's branch (null on a detached HEAD or while loading). */
  currentBranch: string | null;
  /** In "New worktree" mode, the base the worktree starts from. */
  baseBranch?: string | null;
  /** The workspace cannot change (a thread already runs somewhere). */
  envLocked?: boolean;
  /** The branch cannot change (a thread's branch is its own). */
  branchLocked?: boolean;
  previousWorktree?: PreviousWorktreeSeed | null;
  onUsePreviousWorktree?: () => void;
  onSelectBranch: (ref: BranchRef) => void;
  /** Creates the worktree ("New worktree" mode, before it exists); the container confirms. */
  onCreateWorktree?: () => void;
  /** Controls after the branch: the git actions, the PR. */
  children?: ReactNode;
}

export function BranchToolbar({
  refs,
  refsLoading = false,
  envMode,
  onEnvModeChange,
  activeWorktreePath,
  currentBranch,
  baseBranch = null,
  envLocked = false,
  branchLocked = false,
  previousWorktree = null,
  onUsePreviousWorktree,
  onSelectBranch,
  onCreateWorktree,
  children,
}: BranchToolbarProps) {
  const value = resolveBranchToolbarValue({
    envMode,
    activeWorktreePath,
    activeThreadBranch: baseBranch,
    currentGitBranch: currentBranch,
  });
  const creating = envMode === "worktree" && !activeWorktreePath;
  return (
    <div
      className="flex min-w-0 items-center gap-1.5"
      role="toolbar"
      aria-label="Branch and git"
      data-studio-git-toolbar
    >
      <EnvModeSelector
        envLocked={envLocked}
        effectiveEnvMode={envMode}
        activeWorktreePath={activeWorktreePath}
        onEnvModeChange={onEnvModeChange}
        previousWorktreeLabel={
          previousWorktree ? resolvePreviousWorktreeLabel(previousWorktree) : null
        }
        previousWorktreeBranch={previousWorktree?.branch ?? null}
        {...(onUsePreviousWorktree ? { onUsePreviousWorktree } : {})}
      />
      <BranchSelector
        refs={refs}
        loading={refsLoading}
        value={value}
        effectiveEnvMode={envMode}
        activeWorktreePath={activeWorktreePath}
        locked={branchLocked}
        onSelect={onSelectBranch}
      />
      {creating && onCreateWorktree && (
        <Button size="sm" variant="secondary" onClick={onCreateWorktree} disabled={!value}>
          <FolderPlus size={13} aria-hidden="true" />
          Create worktree
        </Button>
      )}
      {children}
    </div>
  );
}
