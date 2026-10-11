// @loams/plugin-canvas: the project canvas. A project's services and their dependencies and
// data flows on a React Flow canvas, with live status and a metric sparkline per service.
// No project service contract exists yet, so the page runs on the fixture-backed mock
// client (mock.ts); a real client implements the same CanvasClient interface.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { CanvasPage } from './CanvasPage.js';
import { createMockCanvasClient, SEED_PROJECT_ID } from './mock.js';

export { AddServiceDialog } from './AddService.js';
export { CanvasPage, initialPositions, toFlowEdges } from './CanvasPage.js';
export type {
  CanvasClient,
  CreatedService,
  CreateServiceInput,
  EdgeKind,
  Position,
  Project,
  Service,
  ServiceEdge,
  ServiceKind,
  ServiceStatus,
  StatusUpdate,
} from './client.js';
export { KIND_ORDER, KINDS, serviceHref } from './kinds.js';
export { layeredLayout } from './layout.js';
export { createMockCanvasClient, SEED_PROJECT_ID, seedProject } from './mock.js';

const plugin: PluginModule = {
  name: 'canvas',
  inject: ['desktop', 'router', 'slots'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    // One client per plugin lifetime, so positions and added services survive navigation.
    const client = createMockCanvasClient();
    const navigate = (to: string) => router.navigate(to);
    ctx.effect(() =>
      router.page({ id: 'canvas', path: '/canvas', title: 'Canvas', plugin: 'canvas' }, () => (
        <CanvasPage client={client} projectId={SEED_PROJECT_ID} navigate={navigate} />
      )),
    );
    ctx.effect(() =>
      router.page(
        { id: 'canvas-project', path: '/canvas/:project', title: 'Canvas', plugin: 'canvas' },
        ({ params }) => (
          <CanvasPage
            client={client}
            projectId={params.project ?? SEED_PROJECT_ID}
            navigate={navigate}
          />
        ),
      ),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'canvas',
          order: 5,
          meta: { id: 'canvas', label: 'Canvas', href: '/canvas', icon: 'canvas', group: 'Data' },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
