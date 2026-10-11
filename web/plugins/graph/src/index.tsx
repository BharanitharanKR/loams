// @loams/plugin-graph: the Graph page (§48 §18.2, D758). GQL over loams.graph.v1, served
// by the loams engine behind its `graph` feature. The page detects the package from
// GetInstance.services[] and talks to it over the console's Connect transport.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { GraphPage } from './GraphPage.js';

export { createGraphClient, toFailure } from './client.js';
export { GRAPH_PACKAGE, type GraphAvailability, graphAvailability } from './detect.js';
export { DOCS_URL, GraphPage } from './GraphPage.js';

const plugin: PluginModule = {
  name: 'graph',
  // `flags` is injected so the page restarts, and detects again, when the server's
  // packages change (the desktop's local engine came up).
  inject: ['platform', 'router', 'slots', 'transport', 'flags'],
  apply(ctx: Context) {
    const platform = service(ctx, 'platform');
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const transport = service(ctx, 'transport');
    const openExternal = (url: string) => {
      platform
        .openExternal(url)
        .catch((e: unknown) => console.warn('graph: could not open', url, e));
    };
    ctx.effect(() =>
      router.page({ id: 'graph', path: '/graph', title: 'Graph', plugin: 'graph' }, () => (
        <GraphPage transport={transport} openExternal={openExternal} />
      )),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'graph',
          order: 90,
          meta: { id: 'graph', label: 'Graph', href: '/graph', icon: 'graph', group: 'Integrate' },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
