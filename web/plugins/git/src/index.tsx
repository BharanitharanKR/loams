// @loams/plugin-git: the Git page (§36 §19, GT1). Repositories, virtual workspaces
// (searchable and virtualised, since a repository can hold millions), workspace detail
// with snapshots and a lazy code browser, and env secrets (key names only, never values).
// `loams.repos.v1` is not generated yet; the page runs on the fixture-backed mock until
// it is (client.ts `createGitClient`).

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { createGitClient } from './client.js';
import { GitPage } from './GitPage.js';

export type * from './client.js';
export { createGitClient, toFailure } from './client.js';
export { GitPage } from './GitPage.js';
export { createMockGitClient } from './mock.js';

const plugin: PluginModule = {
  name: 'git',
  inject: ['router', 'slots', 'transport'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const transport = service(ctx, 'transport');
    const client = createGitClient(transport);
    ctx.effect(() =>
      router.page({ id: 'git', path: '/git', title: 'Git', plugin: 'git' }, () => (
        <GitPage client={client} preview />
      )),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'git',
          order: 95,
          meta: { id: 'git', label: 'Git', href: '/git', icon: 'git', group: 'Integrate' },
        },
        () => null,
      ),
    );
  },
};

export default plugin;
