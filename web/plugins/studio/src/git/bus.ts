// The palette's way into the git toolbar (SD1 Tasks 23 and 24, T24-6). The toolbar of
// an open thread registers here; the palette's `gitActions` reads its rows and
// `runGitAction` asks it to open the same confirmation dialog its own buttons open. So
// a git command from the palette is confirmed exactly as one from the toolbar, and the
// palette never writes to git itself.

import type { GitDialogAction, GitPaletteAction } from '@loams/studio-ui';
import type { GitAction, PaletteGitAction, StudioPaletteSource } from '../palette/source.js';

export interface GitToolbarEntry {
  /** The rows the toolbar's menu has now, with their disabled reasons. */
  actions(): GitPaletteAction[];
  /** Opens the action's confirmation dialog. */
  request(action: GitDialogAction): void;
}

const TO_DIALOG: Record<GitAction, GitDialogAction> = {
  commit: 'commit',
  push: 'push',
  'pull-request': 'create_pr',
  merge: 'merge',
};

const TO_PALETTE: Record<GitDialogAction, GitAction> = {
  commit: 'commit',
  push: 'push',
  create_pr: 'pull-request',
  merge: 'merge',
};

export class GitActionBus {
  private readonly entries = new Map<string, GitToolbarEntry>();

  /** Registers a thread's toolbar; returns its unregister. */
  register(threadId: string, entry: GitToolbarEntry): () => void {
    this.entries.set(threadId, entry);
    return () => {
      if (this.entries.get(threadId) === entry) this.entries.delete(threadId);
    };
  }

  actions(threadId: string): PaletteGitAction[] {
    return (this.entries.get(threadId)?.actions() ?? []).map((row) => ({
      action: TO_PALETTE[row.action],
      label: row.label,
      ...(row.disabledReason ? { disabledReason: row.disabledReason } : {}),
    }));
  }

  async run(threadId: string, action: GitAction): Promise<void> {
    const entry = this.entries.get(threadId);
    if (!entry) throw new Error('Open the thread to run its git actions.');
    entry.request(TO_DIALOG[action]);
  }
}

/** A palette source whose git rows and git commands are the toolbar's (T24-6). */
export function withGitActions(
  source: StudioPaletteSource,
  bus: GitActionBus,
): StudioPaletteSource {
  return {
    ...source,
    gitActions: async (threadId) => bus.actions(threadId),
    runGitAction: (threadId, action) => bus.run(threadId, action),
  };
}
