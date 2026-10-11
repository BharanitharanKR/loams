// @loams/plugin-metrics: the Metrics page. Live dashboards of the loams_* metrics (usage per
// namespace and function, Postgres, Loams SQL, Live, House), laid out as @loams-core/dashboards
// specs on react-grid-layout and drawn with @loams/charts. No metrics service has a proto
// yet, so the page runs on the fixture-backed mock client (src/mock.ts) behind the typed
// MetricsClient interface (src/client.ts).

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { MetricsPage } from './MetricsPage.js';
import { createMockMetricsClient } from './mock.js';

export { formatValue, METRICS, type MetricDef, metricDef } from './catalog.js';
export type { MetricQuery, MetricStreamQuery, MetricsClient } from './client.js';
export { VersionConflict } from './client.js';
export { DEFAULT_DASHBOARDS } from './defaults.js';
export { MetricsPage } from './MetricsPage.js';
export { createMockMetricsClient } from './mock.js';

const plugin: PluginModule = {
  name: 'metrics',
  inject: ['router', 'slots'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    // One client per plugin lifetime, so saves survive moving between pages.
    const client = createMockMetricsClient({ latencyMs: 150 });
    ctx.effect(() =>
      router.page({ id: 'metrics', path: '/metrics', title: 'Metrics', plugin: 'metrics' }, () => (
        <MetricsPage client={client} />
      )),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'metrics',
          order: 20,
          meta: {
            id: 'metrics',
            label: 'Metrics',
            href: '/metrics',
            icon: 'metrics',
            group: 'Operate',
          },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
