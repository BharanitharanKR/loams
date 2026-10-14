// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/CommandPalette.logic.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the item builders of CommandPalette.logic.ts, as plain TypeScript over Studio's thread, project, run group and file rows; no Effect, React or contracts. Loams' changes are Apache-2.0.

// Studio's palette rows (SD1 Task 24, inventory (h)): threads, projects, run groups and
// files, built as upstream builds its thread and project items (search terms in order of
// importance, the id last; threads carry their recency so the ranking can break ties
// newest first). The shell's palette ranks and renders them (@loams/ui `rankCommands`
// and `CommandPalette`); this module only turns Studio's rows into palette rows.

import type { KeybindingCommand } from "../keybindings/shared";

/** How many threads an empty query lists (upstream's "Recent Threads"). */
export const RECENT_THREAD_LIMIT = 12;

/** A palette row as Studio contributes it to the shell's `palette.command` slot. */
export interface StudioPaletteItem {
  id: string;
  title: string;
  group: string;
  detail?: string;
  keywords?: string[];
  recency?: number;
  secondary?: boolean;
  disabled?: boolean;
  /** The keybinding whose chord the row shows. */
  shortcutCommand?: KeybindingCommand;
  run: () => void | Promise<void>;
}

export interface PaletteThread {
  id: string;
  title: string;
  projectId: string;
  branch?: string | null;
  /** Milliseconds since the epoch of the thread's last activity. */
  updatedAt: number;
  archived?: boolean;
}

export interface PaletteProject {
  id: string;
  name: string;
  path: string;
}

export interface PaletteRunGroup {
  id: string;
  title: string;
  projectId?: string;
  members: number;
  updatedAt: number;
}

/** Newest first; upstream's `sortThreads` with the `updated_at` order. */
export function sortThreads<T extends Pick<PaletteThread, "updatedAt" | "id">>(
  threads: readonly T[],
): T[] {
  return [...threads].sort((a, b) => b.updatedAt - a.updatedAt || a.id.localeCompare(b.id));
}

export function buildThreadActionItems(input: {
  threads: readonly PaletteThread[];
  activeThreadId?: string;
  projectTitleById: ReadonlyMap<string, string>;
  open: (thread: PaletteThread) => void | Promise<void>;
  limit?: number;
  group?: string;
}): StudioPaletteItem[] {
  const sorted = sortThreads(input.threads.filter((thread) => !thread.archived));
  const visible = input.limit === undefined ? sorted : sorted.slice(0, input.limit);

  return visible.map((thread) => {
    const projectTitle = input.projectTitleById.get(thread.projectId);
    const descriptionParts: string[] = [];
    if (projectTitle) descriptionParts.push(projectTitle);
    if (thread.branch) descriptionParts.push(`#${thread.branch}`);
    if (thread.id === input.activeThreadId) descriptionParts.push("Current thread");

    return {
      id: `thread:${thread.id}`,
      title: thread.title || "Untitled thread",
      group: input.group ?? "Threads",
      detail: descriptionParts.join(" · "),
      keywords: [
        projectTitle ?? "",
        thread.branch ?? "",
        // Last so pasted IDs never outrank title matches for shared substrings.
        thread.id,
      ],
      recency: thread.updatedAt,
      run: () => input.open(thread),
    };
  });
}

export function buildProjectActionItems(input: {
  projects: readonly PaletteProject[];
  open: (project: PaletteProject) => void | Promise<void>;
  valuePrefix?: string;
  group?: string;
}): StudioPaletteItem[] {
  return input.projects.map((project) => ({
    id: `${input.valuePrefix ?? "project"}:${project.id}`,
    title: project.name,
    group: input.group ?? "Projects",
    detail: project.path,
    keywords: [project.path],
    run: () => input.open(project),
  }));
}

/** Run groups (§53 §5.5): Studio's own, upstream has no compare view. */
export function buildRunGroupItems(input: {
  groups: readonly PaletteRunGroup[];
  projectTitleById: ReadonlyMap<string, string>;
  open: (group: PaletteRunGroup) => void | Promise<void>;
}): StudioPaletteItem[] {
  return sortThreads(input.groups).map((group) => {
    const projectTitle = group.projectId ? input.projectTitleById.get(group.projectId) : undefined;
    return {
      id: `group:${group.id}`,
      title: group.title || "Untitled run group",
      group: "Run groups",
      detail: [projectTitle, `${group.members} runs`].filter(Boolean).join(" · "),
      keywords: [projectTitle ?? "", group.id],
      recency: group.updatedAt,
      run: () => input.open(group),
    };
  });
}

/** Files of the open thread's worktree (quick open): the name, then the folder. */
export function buildFileItems(input: {
  paths: readonly string[];
  open: (path: string) => void | Promise<void>;
  limit?: number;
}): StudioPaletteItem[] {
  return input.paths.slice(0, input.limit ?? 50).map((path) => {
    const slash = path.lastIndexOf("/");
    return {
      id: `file:${path}`,
      title: slash < 0 ? path : path.slice(slash + 1),
      group: "Files",
      detail: slash < 0 ? "" : path.slice(0, slash),
      keywords: [path],
      run: () => input.open(path),
    };
  });
}
