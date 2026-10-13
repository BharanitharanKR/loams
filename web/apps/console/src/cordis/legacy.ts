// Old console addresses → the console's hash routes (API2 Task 10).
//
// The original React console routed by path (`/ui/projects/docs/environments/dev`,
// `/ui/teams`, `/ui/sign-in?next=…`, `/ui/consent?client_id=…`), and the cordis
// console was served at `/ui/cordis.html` beside it. The console is now
// index.html at the base path's root and routes by hash, so a bookmark or a
// server redirect to an old address lands on the same page: the path (with its
// query) moves into the hash. The engine and the dev server answer any unknown
// path under the base with index.html, so this runs on every old address.

/** Query flags that stay on the page's own query: they choose how the console boots. */
const BOOT_FLAGS = new Set(['desktop', 'demo']);

export interface Where {
  pathname: string;
  search: string;
  hash: string;
}

/**
 * The address an old console URL now lives at, or undefined when it is already a
 * current one (`<base>`, `<base>#/teams`). `base` is the build's base path (`/ui/`).
 */
export function legacyUrl(where: Where, base: string): string | undefined {
  const root = base.endsWith('/') ? base : `${base}/`;
  const bare = root.slice(0, -1);
  let rest: string;
  if (where.pathname === root || where.pathname === bare) rest = '';
  else if (where.pathname.startsWith(root)) rest = where.pathname.slice(root.length);
  else return undefined;
  rest = rest.replace(/\/+$/, '');
  const entry = rest === 'index.html' || rest === 'cordis.html';
  if (entry) rest = '';
  if (!rest) {
    // `<base>cordis.html?desktop#/data` → `<base>?desktop#/data`; `/ui` → `/ui/`.
    if (!entry && where.pathname === root) return undefined;
    return `${root}${where.search}${where.hash}`;
  }
  // A path route: it and its query go into the hash; boot flags stay outside.
  const outer = new URLSearchParams();
  const inner = new URLSearchParams();
  for (const [k, v] of new URLSearchParams(where.search)) {
    (BOOT_FLAGS.has(k) ? outer : inner).append(k, v);
  }
  const o = outer.toString();
  const i = inner.toString();
  return `${root}${o ? `?${o}` : ''}#/${rest}${i ? `?${i}` : ''}`;
}
