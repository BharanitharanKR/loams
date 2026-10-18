// @loams/plugin-data-studio: Loams Desktop's data browser. Namespaces,
// collections, documents, search, SQL and ingest, against any active server
// that serves loams.collection.v1.

import type { Transport } from '@connectrpc/connect';
import {
  type FlagsService,
  type PageProps,
  type PlatformService,
  type PluginModule,
  service,
} from '@loams/console-host';
import type { Context } from '@loams/cordis';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { Empty } from '@loams/ui';
import { useMemo } from 'react';
import { createDataClient } from './client.js';
import { CollectionPage } from './pages/collection.js';
import { CollectionsPage, SEARCH_PATH } from './pages/collections.js';
import { SqlPage } from './pages/sql.js';
import { TablesSource } from './palette.js';
import type { Tab } from './shared.js';

export { createDataClient } from './client.js';
export { TablesSource, tableStatement } from './palette.js';

export const DATA_PLANE_API = 'loams.collection.v1';

type View = 'collections' | 'search-entry' | 'sql' | Exclude<Tab, 'sql'>;

export function DataRoute({
  transport,
  platform,
  flags,
  navigate,
  params,
  view,
}: {
  transport: Transport;
  platform: Pick<PlatformService, 'fetch' | 'baseUrl'>;
  flags: Pick<FlagsService, 'has'>;
  navigate: (to: string, opts?: { replace?: boolean }) => void;
  params: PageProps['params'];
  view: View;
}) {
  const client = useMemo(
    () => createDataClient(transport, platform.fetch, platform.baseUrl),
    [transport, platform],
  );
  if (!flags.has(DATA_PLANE_API)) {
    return (
      <div className="lc-page ds-page">
        <Empty title="This server has no data plane.">
          It does not serve collections (loams.collection.v1). Switch to a server that does, such
          as This computer on Loams Desktop.
        </Empty>
      </div>
    );
  }
  const ns = params.ns || 'default';
  if (view === 'collections')
    return <CollectionsPage client={client} ns={ns} navigate={navigate} />;
  if (view === 'search-entry')
    return <CollectionsPage client={client} ns={ns} navigate={navigate} target="search" />;
  if (view === 'sql')
    return <SqlPage client={client} ns={ns} navigate={navigate} table={params.table} />;
  return (
    <CollectionPage
      client={client}
      ns={ns}
      coll={params.coll ?? ''}
      tab={view}
      navigate={navigate}
      params={params}
    />
  );
}

const plugin: PluginModule = {
  name: 'data-studio',
  inject: ['desktop', 'router', 'slots', 'transport', 'platform', 'flags', 'session'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const transport = service(ctx, 'transport');
    const platform = service(ctx, 'platform');
    const flags = service(ctx, 'flags');
    const navigate = (to: string, opts?: { replace?: boolean }) => router.navigate(to, opts);
    // The static `/sql` route is registered before `/:coll`, because the router
    // takes the first match: a collection named "sql" is not reachable by URL.
    const routes: { id: string; path: string; title: string; view: View }[] = [
      // The visible name is "Data explorer" (Q812), so it is not confused with Loams Studio;
      // the routes and the plugin id stay, to keep links working.
      { id: 'data', path: '/data', title: 'Data explorer', view: 'collections' },
      // The Search entry (UX-2): a collection picker that opens Search; `?ns=` picks the namespace.
      { id: 'search', path: SEARCH_PATH, title: 'Search', view: 'search-entry' },
      { id: 'data-ns', path: '/data/:ns', title: 'Data explorer', view: 'collections' },
      { id: 'data-sql', path: '/data/:ns/sql', title: 'SQL', view: 'sql' },
      { id: 'data-coll', path: '/data/:ns/:coll', title: 'Collection', view: 'documents' },
      { id: 'data-search', path: '/data/:ns/:coll/search', title: 'Search', view: 'search' },
      { id: 'data-ingest', path: '/data/:ns/:coll/ingest', title: 'Ingest', view: 'ingest' },
      { id: 'data-schema', path: '/data/:ns/:coll/schema', title: 'Schema', view: 'schema' },
    ];
    for (const r of routes) {
      ctx.effect(() =>
        router.page(
          { id: r.id, path: r.path, title: r.title, plugin: 'data-studio' },
          ({ params }) => (
            <DataRoute
              transport={transport}
              platform={platform}
              flags={flags}
              navigate={navigate}
              params={params}
              view={r.view}
            />
          ),
        ),
      );
    }
    // The palette's tables (CF1 Task 11): the open environment's namespace, or the
    // Data explorer's own default when no environment is selected.
    const session = service(ctx, 'session');
    const tablesClient = createDataClient(transport, platform.fetch, platform.baseUrl);
    ctx.effect(() =>
      slots.register({ name: 'shell.palette.source', plugin: 'data-studio' }, (p) => (
        <TablesSource
          {...p}
          client={tablesClient}
          namespace={
            flags.has(DATA_PLANE_API) ? (session.environment()?.namespace ?? 'default') : undefined
          }
          navigate={navigate}
        />
      )),
    );
    // The Pages (D1080, UX-2): the Data explorer, its SQL editor (over the default
    // namespace, as `/data` is) and the Search entry, so each can be pinned.
    const pages = [
      { id: 'data', label: 'Data explorer', href: '/data', icon: 'data', order: 20 },
      { id: 'data-sql', label: 'SQL', href: '/data/default/sql', icon: 'query', order: 21 },
      { id: 'search', label: 'Search', href: SEARCH_PATH, icon: 'search', order: 22 },
    ];
    for (const p of pages) {
      ctx.effect(() =>
        slots.register(
          {
            name: 'shell.nav.section',
            plugin: 'data-studio',
            order: p.order,
            meta: { id: p.id, label: p.label, href: p.href, icon: p.icon, group: 'Data' },
          },
          () => null,
        ),
      );
    }
  },
};

export default plugin;
