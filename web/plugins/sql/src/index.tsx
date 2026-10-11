// @loams/plugin-sql: the Loams SQL page (§47, SQ1): TiDB-based, MySQL-compatible
// databases with branches and a serverless lifecycle. It supersedes @loams/plugin-wesql.
// loams.sqldb.v1 is not in @loams/proto yet (SQ1 Task 7), so the page runs on the
// fixture-backed client in mock.ts; a Connect client of the same SqlClient interface
// replaces it when the protos land.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { createMockSqlClient } from './mock.js';
import { SqlPage } from './SqlPage.js';

export {
  type Branch,
  type ConnectionInfo,
  type Database,
  type DatabaseStage,
  type QueryResult,
  type SqlClient,
  SqlFailure,
} from './client.js';
export { createMockSqlClient, defaultFixtures } from './mock.js';
export { SqlPage } from './SqlPage.js';

const plugin: PluginModule = {
  name: 'sql',
  inject: ['desktop', 'router', 'slots'],
  apply(ctx: Context) {
    const desktop = service(ctx, 'desktop');
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const client = createMockSqlClient();
    const copy = (text: string) => desktop.shell.clipboardWrite(text);
    ctx.effect(() =>
      router.page({ id: 'sql', path: '/sql', title: 'Loams SQL', plugin: 'sql' }, () => (
        <SqlPage client={client} copy={copy} mocked />
      )),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'sql',
          order: 39,
          meta: { id: 'sql', label: 'SQL', href: '/sql', icon: 'sql', group: 'Data' },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
