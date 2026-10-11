// The dashboard's chart specs. `@loams/charts` compiles these through the
// `@loams-core/charts` compiler; the type is taken from `LiveChart`'s own
// props so this file never disagrees with the chart package about it.

import type { LiveChart } from '@loams/charts';
import type { ComponentProps } from 'react';
import type { Resource } from './types.js';

export type ChartSpec = ComponentProps<typeof LiveChart>['spec'];

/** Requests and errors per second, one line each, over a moving window. */
export const trafficSpec: ChartSpec = {
  chartType: 'Line Chart',
  title: 'Requests and errors',
  encodings: {
    x: { field: 't', type: 'temporal' },
    y: { field: 'v', type: 'quantitative' },
    color: { field: 'series', type: 'nominal' },
  },
};

const RESOURCE_TITLE: Record<Resource, string> = {
  cpu: 'CPU by product (cores)',
  memory: 'Memory by product',
  storage: 'Storage by product',
};

/**
 * One resource, stacked by product, so the top line is the total. The chart
 * package stacks on the chart type's name ("Stacked ..."), not on a property.
 */
export function usageSpec(resource: Resource): ChartSpec {
  return {
    chartType: 'Stacked Area Chart',
    title: RESOURCE_TITLE[resource],
    encodings: {
      x: { field: 't', type: 'temporal' },
      y: { field: 'v', type: 'quantitative' },
      color: { field: 'product', type: 'nominal' },
    },
  };
}
