// Studio's routes (Shared contracts; §53 §4.4, SD1 Ruling 3): all under `/studio` in
// the shell's hash router, so a link names its mode. The factory's `/studio/factory`
// routes belong to @loams/plugin-factory (SD1 Task 27).

import { httpUrl } from './factory.js';

export type Panel = 'diff' | 'files' | 'terminal' | 'preview' | 'plan';
export const PANELS: readonly Panel[] = ['diff', 'files', 'terminal', 'preview', 'plan'];

/**
 * A thread a factory app hands over (§53 §5.9, SD1 Task 27): "Start thread from
 * issue" (Plane) and "Fix this error" (GlitchTip). It arrives in the home route's
 * query (`/studio?from=plane&title=…&prompt=…&branch=…&link=…`), so every field is
 * checked here: a known source, bounded text, a git-safe branch, an http(s) link.
 */
export interface ThreadDraft {
  from: 'plane' | 'glitchtip';
  title: string;
  prompt: string;
  branch?: string;
  link?: string;
}

const DRAFT_SOURCES: readonly ThreadDraft['from'][] = ['plane', 'glitchtip'];
const MAX_TITLE = 200;
const MAX_PROMPT = 8000;
/** A branch name git and a shell both take as a name: no leading `-`, no `..`. */
const BRANCH = /^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$/;

/** The draft in a home route's params, or undefined when it is missing or malformed. */
export function draftOf(params: Record<string, string>): ThreadDraft | undefined {
  const from = params.from as ThreadDraft['from'];
  const prompt = params.prompt?.slice(0, MAX_PROMPT);
  if (!DRAFT_SOURCES.includes(from) || !prompt) return undefined;
  const branch = params.branch;
  const link = httpUrl(params.link);
  return {
    from,
    title: (params.title ?? '').slice(0, MAX_TITLE),
    prompt,
    ...(branch && BRANCH.test(branch) && !branch.includes('..') ? { branch } : {}),
    ...(link ? { link } : {}),
  };
}

export type StudioRoute =
  | { kind: 'home'; draft?: ThreadDraft }
  | { kind: 'project'; projectId: string }
  | { kind: 'thread'; threadId: string; panel?: Panel }
  | { kind: 'group'; groupId: string }
  | { kind: 'settings' };

/** The page specs `index.tsx` registers, by route kind. */
export const STUDIO_PAGES = [
  { id: 'studio', path: '/studio', title: 'Studio', kind: 'home' },
  { id: 'studio-project', path: '/studio/p/:projectId', title: 'Project', kind: 'project' },
  { id: 'studio-thread', path: '/studio/t/:threadId', title: 'Thread', kind: 'thread' },
  { id: 'studio-group', path: '/studio/g/:groupId', title: 'Run group', kind: 'group' },
  { id: 'studio-settings', path: '/studio/settings', title: 'Studio settings', kind: 'settings' },
] as const;

export function isPanel(value: string | undefined): value is Panel {
  return (PANELS as readonly string[]).includes(value ?? '');
}

/** The route of a page from its kind and the router's params (query included). */
export function routeOf(
  kind: (typeof STUDIO_PAGES)[number]['kind'],
  params: Record<string, string>,
): StudioRoute {
  switch (kind) {
    case 'project':
      return { kind, projectId: params.projectId ?? '' };
    case 'thread':
      return {
        kind,
        threadId: params.threadId ?? '',
        ...(isPanel(params.panel) ? { panel: params.panel } : {}),
      };
    case 'group':
      return { kind, groupId: params.groupId ?? '' };
    case 'home': {
      const draft = draftOf(params);
      return draft ? { kind, draft } : { kind };
    }
    default:
      return { kind };
  }
}

/** A hash-route path (no `#`), for links and `router.navigate`. */
export function studioHref(route: StudioRoute): string {
  switch (route.kind) {
    case 'home': {
      if (!route.draft) return '/studio';
      const { from, title, prompt, branch, link } = route.draft;
      const q = new URLSearchParams({ from, title, prompt });
      if (branch) q.set('branch', branch);
      if (link) q.set('link', link);
      return `/studio?${q}`;
    }
    case 'project':
      return `/studio/p/${encodeURIComponent(route.projectId)}`;
    case 'thread':
      return `/studio/t/${encodeURIComponent(route.threadId)}${route.panel ? `?panel=${route.panel}` : ''}`;
    case 'group':
      return `/studio/g/${encodeURIComponent(route.groupId)}`;
    case 'settings':
      return '/studio/settings';
  }
}
