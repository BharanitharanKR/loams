// The factory's in-context links into Studio (SD1 Task 27; §53 §5.9): a Plane issue
// or a GlitchTip error becomes a thread draft that Studio's home opens. The draft
// travels in the route's query (`/studio?from=…&title=…&prompt=…&branch=…&link=…`),
// the grammar `@loams/plugin-studio`'s `routeOf` reads: plugins share routes, not
// values (§37 §5.4). Studio checks every field again; nothing here is trusted there.

import type { GlitchtipIssue, PlaneIssue } from './panels/dto.js';

export interface ThreadDraft {
  from: 'plane' | 'glitchtip';
  title: string;
  prompt: string;
  branch?: string;
  /** http(s) only (§53 D919); anything else is dropped. */
  link?: string;
}

/** What `glitchtip.latestEvent` returns (apps/desktop-electron/src/main/factory/ops.ts). */
export interface GlitchtipEvent {
  release?: string;
  culprit?: string;
  stack?: string;
}

/** A stack trace longer than this is cut: the composer is not a log viewer. */
const MAX_STACK = 4000;

const isHttp = (url: string | undefined): url is string => {
  if (!url) return false;
  try {
    const u = new URL(url);
    return u.protocol === 'https:' || u.protocol === 'http:';
  } catch {
    return false;
  }
};

/** A branch named after the issue key (`MKT-42` → `mkt-42-fix-the-login-redirect`). */
export function branchFor(key: string, title: string): string {
  const slug = (s: string) =>
    s
      .normalize('NFKD')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');
  const head = slug(key) || 'issue';
  const tail = slug(title).slice(0, 40).replace(/-+$/, '');
  return tail ? `${head}-${tail}` : head;
}

/** "Start thread from issue" (Plane): its key, its title, and a branch named after the key. */
export function draftFromPlaneIssue(issue: PlaneIssue): ThreadDraft {
  const key = issue.id ?? 'issue';
  const title = issue.title ?? '';
  return {
    from: 'plane',
    title: title ? `${key}: ${title}` : key,
    prompt: [
      `Work on Plane issue ${key}${title ? `: ${title}` : ''}.`,
      issue.priority ? `Priority: ${issue.priority}.` : undefined,
    ]
      .filter(Boolean)
      .join('\n'),
    branch: branchFor(key, title),
  };
}

/** "Fix this error" (GlitchTip): the error, its release and its stack trace. */
export function draftFromGlitchtipIssue(
  issue: GlitchtipIssue,
  event: GlitchtipEvent | undefined,
): ThreadDraft {
  const title = issue.title ?? `GlitchTip issue ${issue.id ?? ''}`.trim();
  const stack = event?.stack ? event.stack.slice(0, MAX_STACK) : undefined;
  const lines = [
    `Fix this error from GlitchTip: ${title}`,
    event?.culprit ? `Culprit: ${event.culprit}` : undefined,
    event?.release ? `Release: ${event.release}` : undefined,
    issue.count !== undefined ? `Events: ${issue.count}` : undefined,
    '',
    stack ? `Stack trace:\n${stack}` : 'The stack trace could not be read.',
  ];
  return {
    from: 'glitchtip',
    title,
    prompt: lines.filter((l) => l !== undefined).join('\n'),
    ...(isHttp(issue.permalink) ? { link: issue.permalink } : {}),
  };
}

/** The Studio route that opens a draft. */
export function threadDraftHref(draft: ThreadDraft): string {
  const q = new URLSearchParams({ from: draft.from, title: draft.title, prompt: draft.prompt });
  if (draft.branch) q.set('branch', draft.branch);
  if (isHttp(draft.link)) q.set('link', draft.link);
  return `/studio?${q}`;
}
