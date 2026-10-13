// @loams/plugin-durable: Loams Desktop's durable execution page. It talks to
// the local engine's Resonate server through `platform.fetch` on `/durable/`
// (the desktop protocol proxy routes that prefix to the engine).

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
    const desktop = service(ctx, 'desktop');
    const api = createDurableApi(envelope((...a) => platform.fetch(...a), platform.baseUrl));
    // The engine's runs, plus fixture-backed sample runs for what it does not serve yet.
    const graph = withSamples(promiseRunSource(api), mockRunSource());
    ctx.effect(() =>
      router.page({ id: 'durable', path: '/durable', title: 'Durable', plugin: 'durable' }, () => (
        <LocalOnly desktop={desktop}>
          <DurablePage api={api} graph={graph} />
        </LocalOnly>
      )),
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
