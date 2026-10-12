// @loams/plugin-deployments: the Deployments page. Deployment pipelines and Git
// environment promotion as React Flow graphs, and a deploy frequency / failure-rate
// timeline on @loams/charts. There is no deployments API yet: the page runs on the
// typed DeployClient (types.ts) backed by a fixture mock that streams stage updates.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { DeploymentsPage } from './DeploymentsPage.js';
import { createMockDeployClient } from './mock.js';

export {
  DeploymentsPage,
  FAILURE_SPEC,
  FREQUENCY_SPEC,
  TIMELINE_WINDOW_MS,
} from './DeploymentsPage.js';
export { deploymentGraph, promotionGraph } from './layout.js';
export { createMockDeployClient, type MockDeployClient } from './mock.js';
export * from './model.js';
export type * from './types.js';

const plugin: PluginModule = {
  name: 'deployments',
  inject: ['router', 'slots'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    // Replaced by a Connect client once loams.deployments.v1 exists.
    const client = createMockDeployClient();
    ctx.effect(() =>
      router.page(
        { id: 'deployments', path: '/deployments', title: 'Deployments', plugin: 'deployments' },
        () => <DeploymentsPage client={client} />,
      ),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'deployments',
          order: 20,
          meta: {
            id: 'deployments',
            label: 'Deployments',
            href: '/deployments',
            icon: 'deployments',
            group: 'Compute',
          },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
