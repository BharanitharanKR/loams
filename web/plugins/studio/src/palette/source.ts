// Where Studio's palette reads agentd data (SD1 Task 24): threads, run groups, the open
// thread's files and git actions, and the installed harnesses. The palette reads only
// this interface, so the connect-query clients of Task 6 (`rpc.agentd.*`: WatchChats,
// WatchRunGroups, SearchWorkspaceFiles, the git methods of Task 9, ListHarnesses) fill
// it without the palette changing. Until then `pendingPaletteSource` answers with no
// rows, and with the harnesses Settings › Studio knows, without models.
//
// Projects are not here: the palette reads them as the rest of Studio does
// (`useProjects`), through `StudioAgentd`.

import type { PaletteRunGroup, PaletteThread } from '@loams/studio-ui';
import { HARNESSES } from '../settings/Settings.js';

/** The git toolbar's actions (§53 §5.7; G2–G5). */
export type GitAction = 'commit' | 'push' | 'pull-request' | 'merge';

export interface PaletteGitAction {
  action: GitAction;
  label: string;
  /** Why it cannot run now ("Nothing to commit"); the row is shown but disabled. */
  disabledReason?: string;
}

export interface PaletteHarness {
  id: string;
  label: string;
  models: readonly string[];
}

export interface StudioPaletteSource {
  /** Every thread on the host, archived ones flagged. */
  threads(): Promise<PaletteThread[]>;
  runGroups(): Promise<PaletteRunGroup[]>;
  /** File paths in a thread's worktree matching `query` (names only, capped by the host). */
  searchFiles(threadId: string, query: string): Promise<string[]>;
  /** The git actions the thread's worktree allows now. */
  gitActions(threadId: string): Promise<PaletteGitAction[]>;
  /**
   * Runs one. The source owns any confirmation a destructive action needs (Task 23's
   * dialogs); the palette only asks.
   */
  runGitAction(threadId: string, action: GitAction): Promise<void>;
  harnesses(): Promise<PaletteHarness[]>;
}

/** The source until Task 6's client: no agentd rows yet. */
export function pendingPaletteSource(): StudioPaletteSource {
  return {
    threads: async () => [],
    runGroups: async () => [],
    searchFiles: async () => [],
    gitActions: async () => [],
    runGitAction: async () => {
      throw new Error('Git actions need the agentd client (SD1 Task 6)');
    },
    harnesses: async () => HARNESSES.map((h) => ({ id: h.value, label: h.label, models: [] })),
  };
}
