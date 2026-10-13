// Studio's routes (Shared contracts; §53 §4.4, SD1 Ruling 3): all under `/studio` in
// the shell's hash router, so a link names its mode. Factory routes are Task 27's.

export type Panel = 'diff' | 'files' | 'terminal' | 'preview' | 'plan';
export const PANELS: readonly Panel[] = ['diff', 'files', 'terminal', 'preview', 'plan'];

export type StudioRoute =
  | { kind: 'home' }
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
    default:
      return { kind };
  }
}

/** A hash-route path (no `#`), for links and `router.navigate`. */
export function studioHref(route: StudioRoute): string {
  switch (route.kind) {
    case 'home':
      return '/studio';
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
