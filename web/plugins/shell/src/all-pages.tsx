// All pages (UX-2): every page the plugins register, by the area it belongs to, with a pin
// for each. The sidebar's Pages list shows only the pins (D1080); its "All pages" row opens
// this, so a page that is not pinned is still one click away, not only in the palette.

import { useSlot } from '@loams/slots';
import { Pin } from 'lucide-react';
import { useEffect, useState } from 'react';
import { iconFor } from './icons.js';
import { useLocation } from './nav.js';
import {
  currentProject,
  isSettingsPage,
  PAGES_CHANGED_EVENT,
  type PageEntry,
  pageEntries,
  readPinned,
  togglePinned,
} from './pages.js';
import type { HashRouter } from './router.js';

/** The areas in display order; a page of any other group follows under "More". */
const AREAS = ['Data', 'Compute', 'Integrate', 'Studio', 'Factory'] as const;

/** The pins of the open project, live across the sidebar, the palette and this page. */
export function usePinned(project: string | undefined): string[] {
  const [pinned, setPinned] = useState(() => readPinned(project));
  useEffect(() => {
    setPinned(readPinned(project));
    const onChange = () => setPinned(readPinned(project));
    window.addEventListener(PAGES_CHANGED_EVENT, onChange);
    return () => window.removeEventListener(PAGES_CHANGED_EVENT, onChange);
  }, [project]);
  return pinned;
}

/** The pages by area, Settings pages left out (Settings lists them). */
export function pagesByArea(all: readonly PageEntry[]): { area: string; pages: PageEntry[] }[] {
  const pages = all.filter((p) => !isSettingsPage(p));
  const known = new Set<string>(AREAS);
  const groups: { area: string; pages: PageEntry[] }[] = AREAS.map((area) => ({
    area,
    pages: pages.filter((p) => p.group === area),
  }));
  groups.push({ area: 'More', pages: pages.filter((p) => !known.has(p.group)) });
  return groups.filter((g) => g.pages.length > 0);
}

export function AllPagesPage({ router }: { router: HashRouter }) {
  const sections = useSlot('shell.nav.section');
  const legacy = useSlot('console.nav');
  const location = useLocation(router);
  const project = currentProject(location.path);
  const pinned = usePinned(project);
  const groups = pagesByArea(pageEntries(sections, legacy, project));
  return (
    <div className="lc-page">
      <header className="lc-page-head">
        <h1>All pages</h1>
        <p>
          Every page of this console. Pin the ones you use to the sidebar
          {project ? ` for ${project}` : ''}.
        </p>
      </header>
      <div className="lc-all-pages">
        {groups.map((g) => (
          <section key={g.area} aria-labelledby={`lc-area-${g.area}`}>
            <h2 id={`lc-area-${g.area}`}>{g.area}</h2>
            <ul>
              {g.pages.map((p) => {
                const Icon = iconFor(p.icon);
                const on = pinned.includes(p.id);
                return (
                  <li key={p.id}>
                    <a href={`#${p.href}`} className="lc-all-pages-link">
                      <Icon aria-hidden="true" size={15} strokeWidth={1.75} />
                      <span>{p.label}</span>
                    </a>
                    <button
                      type="button"
                      className="lc-all-pages-pin"
                      aria-pressed={on}
                      aria-label={on ? `Unpin ${p.label}` : `Pin ${p.label}`}
                      title={on ? 'Unpin from the sidebar' : 'Pin to the sidebar'}
                      onClick={() => togglePinned(project, p.id)}
                    >
                      <Pin
                        size={14}
                        strokeWidth={1.75}
                        fill={on ? 'currentColor' : 'none'}
                        aria-hidden="true"
                      />
                    </button>
                  </li>
                );
              })}
            </ul>
          </section>
        ))}
      </div>
    </div>
  );
}
