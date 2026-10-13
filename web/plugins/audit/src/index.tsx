// @loams/plugin-audit: the org's audit log on loams.admin.v1 (API2 Task 10, AP1a
// Task 5), live over `AuditService/WatchAuditEvents`: the snapshot, then every
// event as it happens, filtered by project and outcome as the original page was.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { AuditPage } from './audit.js';

export { fold, KEEP, useAuditWatch } from './audit.js';

const plugin: PluginModule = {
  name: 'audit',
  inject: ['router', 'slots', 'session', 'rpc.projects', 'rpc.audit'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const session = service(ctx, 'session');
    ctx.effect(() =>
      router.page({ id: 'audit', path: '/audit', title: 'Audit log', plugin: 'audit' }, () => (
        <AuditPage router={router} session={session} />
      )),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'audit',
          order: 4,
          meta: {
            id: 'audit',
            label: 'Audit log',
            href: '/audit',
            icon: 'audit',
            group: 'Organisation',
          },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
