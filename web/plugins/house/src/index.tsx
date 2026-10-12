// @loams/plugin-house: the Analytics (House) page (§49 §18.3, D778). ClickHouse SQL over
// loams.house.v1, served by the House front and proxied by the engine's main port. The
// page talks to it over the console's Connect transport (connect.ts) and detects it from
// `GetInstance.services[]`; the sample House in mock.ts stays for tests and demos.

import type { PluginModule } from '@loams/console-host';
import { service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { createHouseClient } from './connect.js';
import { HousePage } from './HousePage.js';

export {
  HOUSE_SERVICE,
  type HouseClient,
  HouseError,
  type HouseFailure,
  type QueryFrame,
  toFailure,
} from './client.js';
export { createHouseClient, decodeRows, fromConnect, HOUSE_PACKAGE } from './connect.js';
export { HousePage, PACKAGE } from './HousePage.js';
export { createMockHouseClient } from './mock.js';
export { isAllowedSetting, settingProblem } from './settings.js';

const plugin: PluginModule = {
  name: 'house',
  inject: ['router', 'slots', 'transport'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const client = createHouseClient(service(ctx, 'transport'));
    ctx.effect(() =>
      router.page(
        { id: 'analytics', path: '/analytics', title: 'Analytics', plugin: 'house' },
        () => <HousePage client={client} />,
      ),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'house',
          order: 85,
          meta: {
            id: 'analytics',
            label: 'Analytics',
            href: '/analytics',
            icon: 'chart',
            group: 'Data',
          },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
