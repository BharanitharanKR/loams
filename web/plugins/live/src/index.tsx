// @loams/plugin-live: Loams Desktop's Live page. Tables, documents, a live
// Watch query and mutations over loams.live.v1. The desktop protocol proxy
// sends that prefix to the engine's Live listener (see transport.ts).

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import type { LoamsDesktopApi } from '@loams/platform-electron';
import { createLiveAdminApi } from './admin-client.js';
import { createLiveApi } from './client.js';
import { DeploymentsPage } from './deployments.js';
import { LivePage } from './pages.js';
import { liveTransport } from './transport.js';

export type { LiveAdminApi } from './admin.js';
export { validatorText } from './admin.js';
export { createLiveAdminApi } from './admin-client.js';
export { createMockAdminApi } from './admin-mock.js';
export { createLiveApi } from './client.js';
export { DeploymentsPage } from './deployments.js';
export { LivePage } from './pages.js';
export { liveTransport } from './transport.js';
export { fromJs, toJs } from './value.js';

/** Catalog config. */
export interface LiveConfig {
  /**
   * Register the Deployments page, over the engine's LiveAdminService (default true).
   */
  deployments?: boolean;
}

const plugin: PluginModule = {
  name: 'live',
  inject: ['desktop', 'platform', 'router', 'slots', 'transport'],
  apply(ctx: Context, config?: LiveConfig) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    // The desktop bridge exists only in the Electron shell; a browser console's catalog
    // row narrows `inject` to leave it out, so it is read only on the desktop platform.
    const desktop =
      service(ctx, 'platform').kind === 'desktop'
        ? (service(ctx, 'desktop') as LoamsDesktopApi)
        : undefined;
    const transport = liveTransport({ transport: service(ctx, 'transport') });
    const api = createLiveApi(transport);
    // LiveAdminService beside LiveService: on the engine's main port (LV1 Task 12),
    // and through the desktop's proxy, which routes the whole `loams.live.v1.` prefix.
    const admin = createLiveAdminApi(transport);
    const go = (to: string) => router.navigate(to);
    const deployments = config?.deployments ?? true;
    // The router takes the first match: the longer path goes first.
    if (deployments)
      ctx.effect(() =>
        router.page(
          {
            id: 'live-deployments',
            path: '/live/deployments',
            title: 'Live deployments',
            plugin: 'live',
          },
          () => <DeploymentsPage api={admin} navigate={go} />,
        ),
      );
    // Without the Deployments page, the Live page has nowhere to link to.
    const navigate = deployments ? go : undefined;
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
