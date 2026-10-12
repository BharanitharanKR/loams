// The live half of the Overview: KPIs with sparklines, the request/error
// chart, resource usage per product, the service map, recent deployments and
// alerts. Reads everything through an `OverviewMetricsClient`; when that
// client serves fixtures the section says "Sample data" so nobody mistakes it
// for their own numbers.

import { Badge, Notice } from '@loams/ui';
import { useLoad } from '../load.js';
import { TrafficChart, UsageChart } from './charts.js';
import { KpiStrip } from './kpis.js';
import { AlertsPanel, DeploymentsPanel } from './lists.js';
import { MiniCanvas } from './mini-canvas.js';
import type { OverviewMetricsClient } from './types.js';

export function LiveDashboard({
  client,
  now,
}: {
  client: OverviewMetricsClient;
  /** Clock for the relative times in the lists; tests pass a fixed one. */
  now?: () => number;
}) {
  const kpis = useLoad(() => client.kpis(), client);
  return (
    <section aria-labelledby="overview-live" className="flex flex-col gap-4 mb-6">
      <div className="flex items-center gap-2">
        {/* Not "Live": that is a product, with its own card below. */}
        <h2 id="overview-live" className="m-0">
          Activity
        </h2>
        {client.sample && (
          <Badge title="Generated in the browser, not read from a server">Sample data</Badge>
        )}
      </div>
      {kpis.state === 'ok' && <KpiStrip kpis={kpis.data} client={client} />}
      {kpis.state === 'error' && (
        <Notice tone="danger" title="Not reachable">
          {kpis.message}
        </Notice>
      )}
      <div className="grid gap-4 grid-cols-[repeat(auto-fit,minmax(22rem,1fr))]">
        <TrafficChart client={client} />
        <UsageChart client={client} />
      </div>
      <div className="grid gap-4 grid-cols-[repeat(auto-fit,minmax(22rem,1fr))]">
        <MiniCanvas client={client} />
        <AlertsPanel client={client} now={now} />
      </div>
      <DeploymentsPanel client={client} now={now} />
    </section>
  );
}
