// @loams/plugin-live: Loams Desktop's Live page. Tables, documents, a live
// Watch query and mutations over loams.live.v1. The desktop protocol proxy
// sends that prefix to the engine's Live listener (see transport.ts).

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import type { LoamsDesktopApi } from '@loams/platform-electron';
import { createMockAdminApi } from './admin-mock.js';
import { createLiveApi } from './client.js';
import { DeploymentsPage } from './deployments.js';
import { LivePage } from './pages.js';
import { liveTransport } from './transport.js';

export type { LiveAdminApi } from './admin.js';
export { validatorText } from './admin.js';
export { createMockAdminApi } from './admin-mock.js';
export { createLiveApi } from './client.js';
export { DeploymentsPage } from './deployments.js';
export { LivePage } from './pages.js';
export { liveTransport } from './transport.js';
export { fromJs, toJs } from './value.js';

const plugin: PluginModule = {
  name: 'live',
  inject: ['desktop', 'router', 'slots', 'transport'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const desktop = service(ctx, 'desktop') as LoamsDesktopApi;
    const api = createLiveApi(liveTransport({ transport: service(ctx, 'transport') }));
    // LiveAdminService (LV1 Task 10) is not served yet: the Deployments page
    // runs on fixtures until it is, then this becomes a client over the transport.
    const admin = createMockAdminApi();
    const navigate = (to: string) => router.navigate(to);
    // The router takes the first match: the longer path goes first.
    ctx.effect(() =>
      router.page(
        {
          id: 'live-deployments',
          path: '/live/deployments',
          title: 'Live deployments',
          plugin: 'live',
        },
        () => <DeploymentsPage api={admin} navigate={navigate} />,
      ),
    );
    ctx.effect(() =>
      router.page({ id: 'live', path: '/live', title: 'Live', plugin: 'live' }, () => (
        <LivePage api={api} desktop={desktop} navigate={navigate} />
      )),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'live',
          order: 50,
          meta: { id: 'live', label: 'Live', href: '/live', icon: 'live', group: 'Data' },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
