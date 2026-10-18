// @loams/plugin-postgres: the local Neon stack page (branches, connect, SQL) and the
// loams.postgres.v1 control plane tab (projects, branches, roles, databases, operations).

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { useEffect, useState } from 'react';
import { resolvePgControl } from './control/connect.js';
import { createMockPgControl } from './control/mock.js';
import type { PgControlClient } from './control/types.js';
import { PostgresPage } from './page.js';

export { Branches } from './branches.js';
export { BranchAccess } from './control/BranchAccess.js';
export { BranchTree } from './control/BranchTree.js';
export { ControlPlane } from './control/ControlPlane.js';
export {
  createConnectPgControl,
  PgControlUnreachable,
  resolvePgControl,
} from './control/connect.js';
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
    // loams.postgres.v1 is pg-control's, on its own listener (PG2 R9.1), not the engine's:
    // Electron main forwards loams-app://console/pg-control/ there with the API key. When
    // nothing answers, the tab runs on the fixture-backed mock and says so.
    let control: Promise<PgControlClient> | undefined;
    const resolve = () => {
      control ??= resolvePgControl(
        { baseUrl: `${globalThis.location?.origin ?? 'loams-app://console'}/pg-control/` },
        () => createMockPgControl(),
      ).then((client) => {
        // Probe again on the next visit when pg-control was not up yet.
        if (client.mock) control = undefined;
        return client;
      });
      return control;
    };
    ctx.effect(() =>
      router.page(
        { id: 'postgres', path: '/postgres', title: 'Postgres', plugin: 'postgres' },
        () => <PostgresRoute desktop={desktop} resolve={resolve} />,
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

function PostgresRoute({
  desktop,
  resolve,
}: {
  desktop: Parameters<typeof PostgresPage>[0]['desktop'];
  resolve: () => Promise<PgControlClient>;
}) {
  const [control, setControl] = useState<PgControlClient>();
  useEffect(() => {
    let live = true;
    void resolve().then((c) => live && setControl(c));
    return () => {
      live = false;
    };
  }, [resolve]);
  return <PostgresPage desktop={desktop} control={control} />;
}

export default plugin;
