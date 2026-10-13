// The sidebar's project context (`shell.nav.context`), as the original console's
// sidebar had it: a project switcher, then the open project's Overview, its
// environments, Agents and Access. On an org page no project is open, so the last
// one opened stays, marked as such, for one-click return.

import { useQuery } from '@connectrpc/connect-query';
import type { RouterService } from '@loams/console-host';
import { admin } from '@loams/proto';
import { SelectMenu } from '@loams/ui';
import { Bot, ChevronsUpDown, Layers, LayoutGrid, Lock, Shield } from 'lucide-react';
import { useEffect, useSyncExternalStore } from 'react';

export const LAST_PROJECT_KEY = 'loams-project';

/** The project a route is in: `/projects/<slug>/…`. */
export function projectOf(path: string): string | undefined {
  const m = /^\/projects\/([^/]+)/.exec(path);
  if (!m?.[1]) return undefined;
  try {
    return decodeURIComponent(m[1]);
  } catch {
    return undefined;
  }
}

function readLast(): string | undefined {
  try {
    return localStorage.getItem(LAST_PROJECT_KEY) ?? undefined;
  } catch {
    return undefined;
  }
}

function writeLast(slug: string) {
  try {
    localStorage.setItem(LAST_PROJECT_KEY, slug);
  } catch {
    // Not saved; the switcher falls back to the first project.
  }
}

function Item({
  href,
  path,
  exact,
  sub,
  children,
}: {
  href: string;
  path: string;
  exact?: boolean;
  sub?: boolean;
  children: React.ReactNode;
}) {
  const active = path === href || (!exact && path.startsWith(`${href}/`));
  return (
    <li>
      <a
        href={`#${href}`}
        className={sub ? 'la-side-sub' : undefined}
        aria-current={active ? 'page' : undefined}
      >
        {children}
      </a>
    </li>
  );
}

export function ProjectContext({ router }: { router: RouterService }) {
  const location = useSyncExternalStore(router.subscribe, () => router.current());
  const open = projectOf(location.path);
  useEffect(() => {
    if (open) writeLast(open);
  }, [open]);
  const projects = useQuery(admin.ProjectService.method.listProjects, {});
  const list = projects.data?.projects ?? [];
  const slug = open ?? readLast();
  const current = list.find((p) => p.slug === slug) ?? list[0];
  const envs = useQuery(
    admin.ProjectService.method.listEnvironments,
    { project: current?.slug ?? '' },
    { enabled: Boolean(current) },
  );
  if (!current) return null;
  const idle = !open;
  const base = `/projects/${current.slug}`;
  return (
    <div className="la-side">
      <div className="lc-switcher">
        <SelectMenu
          label={idle ? 'Open a project (last opened shown)' : 'Project'}
          value={current.slug}
          onValueChange={(next) => router.navigate(`/projects/${next}`)}
          options={list.map((p) => ({ value: p.slug, label: p.name }))}
          className={idle ? 'lc-switcher-trigger la-switcher-idle' : 'lc-switcher-trigger'}
          leading={
            <span className="lc-switcher-mark" aria-hidden="true">
              {current.name.slice(0, 1).toUpperCase()}
            </span>
          }
          icon={<ChevronsUpDown size={14} />}
        />
      </div>
      <nav className="lc-nav la-side-nav" aria-label="Project">
        <div className="lc-nav-group" data-idle={idle ? '' : undefined}>
          <h2 className="lc-nav-heading">
            {current.name}
            {idle && <span className="la-side-note"> · last opened</span>}
          </h2>
          <ul>
            <Item href={base} path={location.path} exact>
              <LayoutGrid aria-hidden="true" size={16} strokeWidth={1.75} />
              <span className="lc-nav-label">Overview</span>
            </Item>
            <li>
              <span className="la-side-sub-title">
                <Layers aria-hidden="true" size={16} strokeWidth={1.75} /> Environments
              </span>
            </li>
            {(envs.data?.environments ?? []).map((e) => (
              <Item key={e.slug} href={`${base}/environments/${e.slug}`} path={location.path} sub>
                <span className="env-dot" data-slug={e.slug} aria-hidden="true" />
                <span className="lc-nav-label">{e.name}</span>
                {e.protected && <Lock size={12} aria-label="protected" className="la-side-lock" />}
              </Item>
            ))}
            <Item href={`${base}/agents`} path={location.path}>
              <Bot aria-hidden="true" size={16} strokeWidth={1.75} />
              <span className="lc-nav-label">Agents</span>
              <span className="la-side-count">{current.agentCount}</span>
            </Item>
            <Item href={`${base}/access`} path={location.path}>
              <Shield aria-hidden="true" size={16} strokeWidth={1.75} />
              <span className="lc-nav-label">Access</span>
            </Item>
          </ul>
        </div>
      </nav>
    </div>
  );
}
