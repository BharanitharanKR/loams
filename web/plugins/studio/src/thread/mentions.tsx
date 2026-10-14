// "Mention in composer" (SD1 Task 21): the file tree hands a path to the thread's
// composer. The composer is SD1 Task 19's; until it lands, a thread's mentions wait on
// this small per-thread queue and show as chips under the Files tab. Task 19's composer
// reads the same queue (`mentionsFor(threadId)`), turns each path into its file chip and
// clears it, and the strip here then shows nothing.

import { X } from 'lucide-react';
import { useSyncExternalStore } from 'react';

export interface ComposerMentions {
  /** The paths waiting for the composer, oldest first, without repeats. */
  list(): readonly string[];
  mention(path: string): void;
  remove(path: string): void;
  clear(): void;
  subscribe(listener: () => void): () => void;
}

export function createComposerMentions(): ComposerMentions {
  let paths: readonly string[] = [];
  const listeners = new Set<() => void>();
  const set = (next: readonly string[]) => {
    paths = next;
    for (const listener of listeners) listener();
  };
  return {
    list: () => paths,
    mention: (path) => {
      if (!paths.includes(path)) set([...paths, path]);
    },
    remove: (path) => set(paths.filter((p) => p !== path)),
    clear: () => set([]),
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

const byThread = new Map<string, ComposerMentions>();

/** The thread's queue, shared by the Files tab and the composer. */
export function mentionsFor(threadId: string): ComposerMentions {
  let mentions = byThread.get(threadId);
  if (!mentions) {
    mentions = createComposerMentions();
    byThread.set(threadId, mentions);
  }
  return mentions;
}

export function useMentions(mentions: ComposerMentions): readonly string[] {
  return useSyncExternalStore(mentions.subscribe, mentions.list, mentions.list);
}

const basename = (path: string) => path.slice(path.lastIndexOf('/') + 1) || path;

/** The waiting mentions as chips, each removable. Nothing when there are none. */
export function MentionChips({ mentions }: { mentions: ComposerMentions }) {
  const paths = useMentions(mentions);
  if (paths.length === 0) return null;
  return (
    <div className="flex shrink-0 flex-wrap items-center gap-1.5 border-t border-rule-soft px-2 py-2">
      <span className="text-[0.75rem] text-muted">For the composer</span>
      <ul
        aria-label="Mentions for the composer"
        className="m-0 flex list-none flex-wrap gap-1.5 p-0"
      >
        {paths.map((path) => (
          <li
            key={path}
            title={path}
            className="inline-flex items-center gap-1 rounded-pill border border-rule bg-surface py-0.5 pr-1 pl-2 font-mono text-[0.75rem] text-ink"
          >
            @{basename(path)}
            <button
              type="button"
              aria-label={`Remove ${path}`}
              onClick={() => mentions.remove(path)}
              className="inline-flex size-4 cursor-pointer items-center justify-center rounded-pill border-0 bg-transparent p-0 text-muted hover:bg-raised hover:text-ink"
            >
              <X aria-hidden className="size-3" />
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}
