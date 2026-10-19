// @loams/plugin-durable: the durable execution page. It talks to the engine's
// Resonate server through `platform.fetch` on `/durable/`: in Loams Desktop the
// protocol proxy routes that prefix to the local engine's durable listener; in a
// browser it is the engine's main port, which serves the same API under
// `/durable/` behind its authentication (B12). An engine without it answers 404,
// which the page shows.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import type { LoamsDesktopApi } from '@loams/desktop/contracts';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { Empty } from '@loams/ui';
import { type ReactNode, useEffect, useState } from 'react';
import { createDurableApi, envelope } from './envelope.js';
import { mockRunSource } from './graph/sample.js';
import { promiseRunSource, withSamples } from './graph/source.js';
import { DurablePage } from './pages.js';

export { createDurableApi, decodeValue, EnvelopeError, envelope } from './envelope.js';
export { ExecutionGraph } from './graph/ExecutionGraph.js';
export { GraphTab } from './graph/GraphTab.js';
export {
  applyEvent,
  fromPromises,
  type RunEvent,
  type RunGraph,
  type Step,
} from './graph/model.js';
export { mockMetricStream, mockRunSource } from './graph/sample.js';
export { promiseRunSource, type RunGraphSource, withSamples } from './graph/source.js';
export { DurablePage } from './pages.js';
export { buildRuns } from './runs.js';

/**
 * Durable execution is served by this computer's engine only: on another server the
 * desktop's proxy answers 404 (apps/desktop-electron/src/main/protocol/route.ts). The
 * page says so instead of trying, and failing, to load.
 */
export function LocalOnly({
  desktop,
  children,
}: {
  desktop: LoamsDesktopApi;
  children: ReactNode;
}) {
  const [kind, setKind] = useState<string | undefined | null>(null);
  useEffect(() => {
    let live = true;
    desktop.servers
      .list()
      .then(({ servers, activeId }) => {
        if (live) setKind(servers.find((s) => s.id === activeId)?.kind);
      })
      .catch(() => live && setKind(undefined));
    return () => {
      live = false;
    };
  }, [desktop]);
  if (kind === null) return null;
  if (kind !== undefined && kind !== 'local') {
    return (
      <div className="lc-page">
        <header className="lc-page-head">
          <h1>Durable execution</h1>
        </header>
        <Empty title="Durable execution runs on this computer">
          Promises, schedules and runs come from the local engine's Resonate server. Switch to This
          computer in the server menu to see them.
        </Empty>
      </div>
    );
  }
  return <>{children}</>;
}

const plugin: PluginModule = {
  name: 'durable',
  inject: ['desktop', 'router', 'slots', 'platform'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const platform = service(ctx, 'platform');
    // Only Loams Desktop has the `desktop` service (web.yml leaves it out of `inject`,
    // and the guard refuses a service a row does not inject): no server menu to check.
    const desktop: LoamsDesktopApi | undefined =
      platform.kind === 'desktop' ? service(ctx, 'desktop') : undefined;
    const api = createDurableApi(envelope((...a) => platform.fetch(...a), platform.baseUrl));
    // The engine's runs, plus fixture-backed sample runs for what it does not serve yet.
    const graph = withSamples(promiseRunSource(api), mockRunSource());
    ctx.effect(() =>
      router.page({ id: 'durable', path: '/durable', title: 'Durable', plugin: 'durable' }, () =>
        desktop ? (
          <LocalOnly desktop={desktop}>
            <DurablePage api={api} graph={graph} />
          </LocalOnly>
        ) : (
          <DurablePage api={api} graph={graph} />
        ),
      ),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'durable',
          order: 60,
          meta: {
            id: 'durable',
            label: 'Durable',
            href: '/durable',
            icon: 'durable',
            group: 'Compute',
          },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
