// @loams/plugin-house: the Analytics (House) page (§49 §18.3, D778). ClickHouse SQL over
// loams.house.v1, served by the House front and proxied by the engine's main port.
//
// `loams.house.v1` is not in @loams/proto yet (HS1 Task 8 is writing it), so the page
// runs against the sample House in mock.ts and says so. When the proto lands, a Connect
// `HouseClient` over the console's `transport` replaces `createMockHouseClient` here,
// and `available()` reads `GetInstance.services[]`.

import type { PluginModule } from '@loams/console-host';
import { service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { HousePage } from './HousePage.js';
import { createMockHouseClient } from './mock.js';

export {
  HOUSE_SERVICE,
  type HouseClient,
  HouseError,
  type HouseFailure,
  type QueryFrame,
  toFailure,
} from './client.js';
export { HousePage, PACKAGE } from './HousePage.js';
export { createMockHouseClient } from './mock.js';
export { isAllowedSetting, settingProblem } from './settings.js';

const plugin: PluginModule = {
  name: 'house',
  inject: ['router', 'slots'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const client = createMockHouseClient();
    ctx.effect(() =>
      router.page({ id: 'analytics', path: '/analytics', title: 'Analytics', plugin: 'house' }, () => (
        <HousePage client={client} sample />
      )),
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
