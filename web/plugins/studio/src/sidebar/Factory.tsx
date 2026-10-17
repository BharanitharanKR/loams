// The sidebar's Factory group (§53 §5.9, D916; SD1 Task 27): Forgejo, Zulip, Plane,
// GlitchTip, OpenPanel, Matomo (Q813), Langfuse and OpenObserve, each with its
// configured state. On the desktop each opens on the factory's pages under
// `/studio/factory`; a links-only source (the web, until SF1: D922) links to the
// apps' own UIs in a new tab, and lists an app it cannot link without a link.

import { cx } from '@loams/ui';
import { useQuery } from '@tanstack/react-query';
import { ExternalLink, Factory } from 'lucide-react';
import {
  FACTORY_HOME,
  type FactoryApp,
  type FactoryState,
  factoryHref,
  httpUrl,
  STATE_LABEL,
  type StudioFactory,
} from '../factory.js';

const ROW =
  'mx-2 flex items-center gap-2.5 rounded-sm px-2.5 py-1.5 text-[0.88rem] text-muted no-underline';

const DOT: Record<FactoryState, string> = {
  ok: 'bg-grow',
  auth_failed: 'bg-danger',
  unreachable: 'bg-danger',
  locked: 'bg-danger',
  unconfigured: 'border border-faint bg-transparent',
};

function State({ state }: { state: FactoryState }) {
  return (
    <span className="ml-auto flex shrink-0 items-center gap-1.5 text-[0.7rem] text-faint">
      <span aria-hidden="true" className={cx('inline-block size-1.5 rounded-pill', DOT[state])} />
      {STATE_LABEL[state]}
    </span>
  );
}

function Row({ app, kind }: { app: FactoryApp; kind: StudioFactory['kind'] }) {
  const label = <span className="min-w-0 flex-1 truncate">{app.label}</span>;
  if (kind === 'desktop') {
    return (
      <a href={`#${factoryHref(app)}`} className={cx(ROW, 'hover:bg-raised hover:text-ink')}>
        {label}
        <State state={app.state} />
      </a>
    );
  }
  const url = app.state === 'ok' ? httpUrl(app.url) : undefined;
  if (!url) {
    return (
      <span className={ROW}>
        {label}
        <State state={app.state} />
      </span>
    );
  }
  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className={cx(ROW, 'hover:bg-raised hover:text-ink')}
    >
      {label}
      <ExternalLink size={12} aria-hidden="true" className="shrink-0 text-faint" />
      <State state={app.state} />
    </a>
  );
}

export function FactoryGroup({ factory }: { factory: StudioFactory }) {
  const apps = useQuery({
    queryKey: ['studio', 'factory', factory.kind, 'apps'],
    queryFn: () => factory.list(),
  });
  return (
    <nav aria-label="Factory" className="shrink-0 border-t border-rule-soft py-2">
      <div className="flex h-8 items-center justify-between gap-2 pr-2 pl-4">
        <h2 className="m-0 text-micro font-medium text-faint">Factory</h2>
        {factory.kind === 'desktop' && (
          <a
            href={`#${FACTORY_HOME}`}
            aria-label="All factory apps"
            title="All factory apps"
            className="inline-grid size-7 place-items-center rounded-pill text-muted hover:bg-raised hover:text-ink"
          >
            <Factory size={14} strokeWidth={1.75} aria-hidden="true" />
          </a>
        )}
      </div>
      {apps.isPending && <p className="m-0 px-4 py-1 text-[0.84rem] text-faint">Loading…</p>}
      {apps.isError && (
        <p role="alert" className="m-0 px-4 py-1 text-[0.84rem] text-danger">
          Could not list the factory apps: {apps.error.message}
        </p>
      )}
      {apps.isSuccess && apps.data.length === 0 && (
        <p className="m-0 px-4 py-1 text-[0.84rem] text-faint">No factory apps.</p>
      )}
      {apps.isSuccess && apps.data.length > 0 && (
        <ul className="m-0 list-none p-0">
          {apps.data.map((app) => (
            <li key={app.id}>
              <Row app={app} kind={factory.kind} />
            </li>
          ))}
        </ul>
      )}
    </nav>
  );
}
