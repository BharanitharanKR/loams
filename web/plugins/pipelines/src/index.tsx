// @loams/plugin-pipelines: the Pipelines page, a visual builder for CN2 connector
// pipelines (docs/plans/2026-10-10-cn2-connectors.md). The flow service has no Connect
// API in @loams/proto yet, so the page runs on the fixture-backed mock client; swapping
// in the real client changes only `createClient` below.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { createMockPipelinesClient } from './mock.js';
import { PipelinesPage } from './PipelinesPage.js';

export type { Deployment, Metric, MetricPoint, PipelinesClient } from './client.js';
export { createMockPipelinesClient, type MockOptions } from './mock.js';
export type * from './model.js';
export { PipelinesPage } from './PipelinesPage.js';
export { fromPipelineSpec, type PipelineSpec, toPipelineSpec } from './spec.js';

const createClient = () => createMockPipelinesClient();

const plugin: PluginModule = {
  name: 'pipelines',
  inject: ['router', 'slots'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const client = createClient();
    ctx.effect(() =>
      router.page({ id: 'pipelines', path: '/pipelines', title: 'Pipelines', plugin: 'pipelines' }, () => (
        <PipelinesPage client={client} />
      )),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'pipelines',
          order: 85,
          meta: {
            id: 'pipelines',
            label: 'Pipelines',
            href: '/pipelines',
            icon: 'pipelines',
            group: 'Integrate',
          },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
