// Per-thread composer drafts (SD1 Task 19; §53 §5.6): the prompt, its uploaded
// attachments, the harness, model and reasoning picked for the next run, the worktree
// choice for a first run, the prompt history (↑) and the stash. One zustand store
// (Q819), kept in localStorage under `loams.studio.drafts`, so a draft survives a
// reload and a mode switch (the Studio page unmounts; the store does not).
//
// Written for Studio, with T3 Code's composerDraftStore.ts (4 600 lines on Effect) as
// the reference: only what Studio sends is kept. Every storage access is wrapped, and
// what is read back is checked with zod, so a blocked or corrupted storage leaves
// drafts in memory for the page's life instead of failing. Terminal selections are
// not drafts: they wait in memory only (thread/terminalContexts.ts, T22-8).

import { z } from "zod";
import { useStore } from "zustand";
import { createStore, type StoreApi } from "zustand/vanilla";

export const DRAFTS_STORAGE_KEY = "loams.studio.drafts";

/** Bounds: drafts kept, history and stash entries per thread, prompt length. */
export const MAX_DRAFTS = 50;
export const MAX_HISTORY = 30;
export const MAX_STASH = 10;
export const MAX_PROMPT_CHARS = 100_000;

const attachmentSchema = z.object({
  id: z.string().min(1).max(128),
  name: z.string().min(1).max(255),
  mimeType: z.string().min(1).max(100),
  size: z.number().int().nonnegative(),
  /** The file's path on the daemon's host, from `UploadCommit`. */
  path: z.string().min(1).max(4096),
});
export type DraftAttachment = z.infer<typeof attachmentSchema>;

const draftSchema = z.object({
  prompt: z.string().max(MAX_PROMPT_CHARS).default(""),
  attachments: z.array(attachmentSchema).max(32).default([]),
  harness: z.string().max(64).optional(),
  model: z.string().max(256).optional(),
  reasoning: z.string().max(32).optional(),
  /** First run only: a new worktree (the default) or the project's checkout. */
  worktree: z.enum(["worktree", "local"]).optional(),
  base: z.string().max(256).optional(),
  history: z.array(z.string().max(MAX_PROMPT_CHARS)).max(MAX_HISTORY).default([]),
  stash: z.array(z.string().max(MAX_PROMPT_CHARS)).max(MAX_STASH).default([]),
  updatedAt: z.number().default(0),
});
export type ComposerDraft = z.infer<typeof draftSchema>;

const storedSchema = z.object({
  version: z.literal(1),
  drafts: z.record(z.string(), z.unknown()),
});

export const EMPTY_DRAFT: ComposerDraft = Object.freeze({
  prompt: "",
  attachments: [],
  history: [],
  stash: [],
  updatedAt: 0,
}) as ComposerDraft;

export type DraftPatch = Partial<Omit<ComposerDraft, "history" | "stash" | "updatedAt">>;

export interface ComposerDraftState {
  drafts: Readonly<Record<string, ComposerDraft>>;
  /** Merges fields into a thread's draft. */
  update(threadId: string, patch: DraftPatch): void;
  /** After a send: the prompt and attachments go; choices, history and stash stay. */
  clearSent(threadId: string, sentPrompt: string): void;
  /** Saves the prompt to the stash and clears it. */
  stash(threadId: string): void;
  /** Takes a stash entry back into the prompt (the current prompt is stashed in turn). */
  unstash(threadId: string, index: number): void;
  remove(threadId: string): void;
}

/** Where drafts are kept; null when storage is unavailable. */
export type DraftStorage = Pick<Storage, "getItem" | "setItem"> | null;

function defaultStorage(): DraftStorage {
  try {
    return typeof window === "undefined" ? null : window.localStorage;
  } catch {
    return null;
  }
}

/** Reads the stored drafts; anything unreadable is dropped, never thrown. */
export function readStoredDrafts(storage: DraftStorage): Record<string, ComposerDraft> {
  let raw: string | null = null;
  try {
    raw = storage?.getItem(DRAFTS_STORAGE_KEY) ?? null;
  } catch {
    return {};
  }
  if (!raw) return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return {};
  }
  const stored = storedSchema.safeParse(parsed);
  if (!stored.success) return {};
  const drafts: Record<string, ComposerDraft> = {};
  for (const [threadId, value] of Object.entries(stored.data.drafts)) {
    if (!threadId || threadId.length > 256) continue;
    const draft = draftSchema.safeParse(value);
    if (draft.success) drafts[threadId] = draft.data;
  }
  return drafts;
}

function isEmptyDraft(draft: ComposerDraft): boolean {
  return (
    draft.prompt === "" &&
    draft.attachments.length === 0 &&
    draft.history.length === 0 &&
    draft.stash.length === 0 &&
    draft.harness === undefined &&
    draft.model === undefined &&
    draft.reasoning === undefined &&
    draft.worktree === undefined &&
    draft.base === undefined
  );
}

function writeStoredDrafts(storage: DraftStorage, drafts: Record<string, ComposerDraft>): void {
  const kept = Object.entries(drafts)
    .filter(([, draft]) => !isEmptyDraft(draft))
    .sort(([, a], [, b]) => b.updatedAt - a.updatedAt)
    .slice(0, MAX_DRAFTS);
  try {
    storage?.setItem(
      DRAFTS_STORAGE_KEY,
      JSON.stringify({ version: 1, drafts: Object.fromEntries(kept) }),
    );
  } catch {
    // Quota or a blocked storage: the drafts stay in memory for this page.
  }
}

function withoutUndefined<T extends object>(value: T): T {
  return Object.fromEntries(Object.entries(value).filter(([, v]) => v !== undefined)) as T;
}

export function createComposerDraftStore(
  storage: DraftStorage = defaultStorage(),
  now: () => number = Date.now,
): StoreApi<ComposerDraftState> {
  const store = createStore<ComposerDraftState>()((set, get) => {
    const put = (threadId: string, next: (draft: ComposerDraft) => ComposerDraft) => {
      const current = get().drafts[threadId] ?? EMPTY_DRAFT;
      const drafts = { ...get().drafts, [threadId]: { ...next(current), updatedAt: now() } };
      set({ drafts });
    };
    return {
      drafts: readStoredDrafts(storage),
      update(threadId, patch) {
        put(threadId, (draft) =>
          withoutUndefined({
            ...draft,
            ...patch,
            ...(patch.prompt !== undefined
              ? { prompt: patch.prompt.slice(0, MAX_PROMPT_CHARS) }
              : {}),
          }),
        );
      },
      clearSent(threadId, sentPrompt) {
        put(threadId, (draft) => ({
          ...draft,
          prompt: "",
          attachments: [],
          history: sentPrompt.trim()
            ? [...draft.history.filter((entry) => entry !== sentPrompt), sentPrompt].slice(
                -MAX_HISTORY,
              )
            : draft.history,
        }));
      },
      stash(threadId) {
        put(threadId, (draft) =>
          draft.prompt.trim()
            ? { ...draft, prompt: "", stash: [draft.prompt, ...draft.stash].slice(0, MAX_STASH) }
            : draft,
        );
      },
      unstash(threadId, index) {
        put(threadId, (draft) => {
          const entry = draft.stash[index];
          if (entry === undefined) return draft;
          const rest = draft.stash.filter((_, i) => i !== index);
          return {
            ...draft,
            prompt: entry,
            stash: draft.prompt.trim() ? [draft.prompt, ...rest].slice(0, MAX_STASH) : rest,
          };
        });
      },
      remove(threadId) {
        const { [threadId]: _removed, ...drafts } = get().drafts;
        set({ drafts });
      },
    };
  });
  store.subscribe((state, previous) => {
    if (state.drafts !== previous.drafts) writeStoredDrafts(storage, state.drafts);
  });
  return store;
}

let defaultStore: StoreApi<ComposerDraftState> | null = null;

/** The page's draft store (one per page, so drafts outlive the composer's mount). */
export function composerDraftStore(): StoreApi<ComposerDraftState> {
  defaultStore ??= createComposerDraftStore();
  return defaultStore;
}

/** Tests: forget the page's store, so the next one reads storage again. */
export function resetComposerDraftStoreForTests(): void {
  defaultStore = null;
}

/** A thread's draft (the same object until it changes). */
export function useComposerDraft(
  threadId: string,
  store: StoreApi<ComposerDraftState> = composerDraftStore(),
): ComposerDraft {
  return useStore(store, (state) => state.drafts[threadId] ?? EMPTY_DRAFT);
}
