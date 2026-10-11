// Per-node live charts, once the pipeline runs: throughput (events/s) and lag (seconds),
// drawn by @loams/charts' LiveChart over the client's metric streams. The chart specs are
// ChartSpecs (@loams-core/chart-specs' model), so the charts compiler, its theme
// decisions and its screen-reader table fallback all apply.

import { LiveChart } from '@loams/charts';
import { Card } from '@loams/ui';
import { type ComponentProps, useMemo } from 'react';
import type { Metric, PipelinesClient } from './client.js';
import type { PipelineNode } from './model.js';

type ChartSpec = ComponentProps<typeof LiveChart>['spec'];

export const METRICS_WINDOW_MS = 5 * 60_000;

const TITLE: Record<Metric, string> = { throughput: 'Throughput', lag: 'Lag' };
const UNIT: Record<Metric, string> = { throughput: 'events/s', lag: 's' };

/** The ChartSpec for one node's series: time on x, value on y, area for throughput. */
export function metricSpec(metric: Metric): ChartSpec {
  return {
    chartType: metric === 'throughput' ? 'area' : 'line',
    title: `${TITLE[metric]} (${UNIT[metric]})`,
    encodings: {
      x: { field: 't', type: 'temporal' },
      y: { field: 'v', type: 'quantitative' },
    },
    chartProperties: { unit: UNIT[metric], smooth: false },
  };
}

function MetricChart({
  client,
  deploymentId,
  node,
  metric,
  height,
}: {
  client: PipelinesClient;
  deploymentId: string;
  node: PipelineNode;
  metric: Metric;
  height: number;
}) {
  const spec = useMemo(() => metricSpec(metric), [metric]);
  // One stream per chart, opened once per deployment: LiveChart consumes it.
  const stream = useMemo(
    () => client.metrics(deploymentId, node.id, metric),
    [client, deploymentId, node.id, metric],
  );
  return (
    <LiveChart
      spec={spec}
      data={[]}
      stream={stream}
      windowMs={METRICS_WINDOW_MS}
      height={height}
      ariaLabel={`${node.label} ${TITLE[metric].toLowerCase()} in ${UNIT[metric]}, last five minutes`}
    />
  );
}

export function NodeMetrics({
  client,
  deploymentId,
  node,
  height = 120,
  headingLevel = 3,
}: {
  client: PipelinesClient;
  deploymentId: string;
  node: PipelineNode;
  height?: number;
  headingLevel?: 2 | 3;
}) {
  return (
    <Card title={node.label} headingLevel={headingLevel}>
      <div className="grid grid-cols-1 gap-2">
        <MetricChart
          client={client}
          deploymentId={deploymentId}
          node={node}
          metric="throughput"
          height={height}
        />
        <MetricChart
          client={client}
          deploymentId={deploymentId}
          node={node}
          metric="lag"
          height={height}
        />
      </div>
    </Card>
  );
}
