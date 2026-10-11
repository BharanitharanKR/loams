// @loams/plugin-connectors: the connector catalog (generated at build time from
// connectors/registry and connectors/schemas), a detail page per connector, and a configure form
// generated from its JSON Schema that exports instance YAML. Secrets are never stored.
// CN2 adds the create-instance wizard and the running-instance pages (checkpoints, lag,
// throughput, errors, pause and resume) over a `FlowClient`. No `FlowService` exists yet, so the
// plugin uses the fixture-backed `createMockFlowClient` until the generated client lands.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { CatalogPage } from './catalog-page.js';
import { DetailPage } from './detail-page.js';
import type { FlowClient } from './flow-client.js';
import { InstanceDetailPage, InstancesPage } from './instances-page.js';
import { createMockFlowClient } from './mock-flow.js';
import { WizardPage } from './wizard-page.js';

export { CatalogPage } from './catalog-page.js';
export { ConfigureForm } from './configure-form.js';
export { DetailPage } from './detail-page.js';
export * from './flow-client.js';
export * from './form.js';
export { InstanceDetailPage, InstancesPage } from './instances-page.js';
export { createMockFlowClient, fixtureInstances } from './mock-flow.js';
export { WizardPage } from './wizard-page.js';

const plugin: PluginModule = {
  name: 'connectors',
  inject: ['desktop', 'router', 'slots'],
  apply(ctx: Context) {
    const desktop = service(ctx, 'desktop');
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const navigate = (to: string) => router.navigate(to);
    // Mocked until `loams.flow.v1.FlowService` lands (CN1 Task 3); see flow-client.ts.
    const flow: FlowClient = createMockFlowClient();
    // The router takes the first match: the longer and literal paths go first, so
    // `/connectors/instances` is not read as a connector id.
    ctx.effect(() =>
      router.page(
        {
          id: 'connector-instance',
          path: '/connectors/instances/:name',
          title: 'Connector instance',
          plugin: 'connectors',
        },
        ({ params }) => (
          <InstanceDetailPage
            flow={flow}
            name={decodeURIComponent(params.name as string)}
            navigate={navigate}
          />
        ),
      ),
    );
    ctx.effect(() =>
      router.page(
        {
          id: 'connector-instances',
          path: '/connectors/instances',
          title: 'Connector instances',
          plugin: 'connectors',
        },
        () => <InstancesPage flow={flow} navigate={navigate} />,
      ),
    );
    ctx.effect(() =>
      router.page(
        { id: 'connector-new', path: '/connectors/:id/new', title: 'New instance', plugin: 'connectors' },
        ({ params }) => (
          <WizardPage desktop={desktop} flow={flow} id={params.id as string} navigate={navigate} />
        ),
      ),
    );
    ctx.effect(() =>
      router.page(
        { id: 'connector', path: '/connectors/:id', title: 'Connector', plugin: 'connectors' },
        ({ params }) => (
          <DetailPage desktop={desktop} id={params.id as string} navigate={navigate} />
        ),
      ),
    );
    ctx.effect(() =>
      router.page(
        { id: 'connectors', path: '/connectors', title: 'Connectors', plugin: 'connectors' },
        () => <CatalogPage desktop={desktop} navigate={navigate} />,
      ),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'connectors',
          order: 80,
          meta: {
            id: 'connectors',
            label: 'Connectors',
            href: '/connectors',
            icon: 'connectors',
            group: 'Integrate',
          },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
