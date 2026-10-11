// @loams/plugin-postgres: the local Neon stack page (branches, connect, SQL) and the
// loams.postgres.v1 control plane tab (projects, branches, roles, databases, operations).

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { createMockPgControl } from './control/mock.js';
import { PostgresPage } from './page.js';

export { Branches } from './branches.js';
export { BranchAccess } from './control/BranchAccess.js';
export { BranchTree } from './control/BranchTree.js';
export { ControlPlane } from './control/ControlPlane.js';
export { fixtureState } from './control/fixtures.js';
export { createMockPgControl } from './control/mock.js';
export { Operations } from './control/Operations.js';
export { PasswordOnce } from './control/PasswordOnce.js';
export { branchTree, deleteBlocker } from './control/tree.js';
export * from './control/types.js';
export { PostgresPage } from './page.js';
export { flattenTimelines } from './tree.js';

const plugin: PluginModule = {
  name: 'postgres',
  inject: ['desktop', 'router', 'slots'],
  apply(ctx: Context) {
    const desktop = service(ctx, 'desktop');
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    // loams.postgres.v1 is not generated in @loams/proto yet: the control plane tab runs on
    // the fixture-backed mock, and says so, until a Connect client over `transport` lands.
    const control = createMockPgControl();
    ctx.effect(() =>
      router.page(
        { id: 'postgres', path: '/postgres', title: 'Postgres', plugin: 'postgres' },
        () => <PostgresPage desktop={desktop} control={control} />,
      ),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'postgres',
          order: 30,
          meta: {
            id: 'postgres',
            label: 'Postgres',
            href: '/postgres',
            icon: 'postgres',
            group: 'Data',
          },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
