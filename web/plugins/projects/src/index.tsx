// @loams/plugin-projects: a project and its environments on loams.admin.v1 (API2
// Task 10, AP1a Task 5), as the original React console had them: the project's
// overview, an environment (stats, 30-day activity, collections, API keys and the
// Connect snippet) and the project's access; and, in the sidebar, the project
// switcher with the open project's pages (`shell.nav.context`).

import { type PageProps, type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import type { ReactNode } from 'react';
import { AccessPage } from './access.js';
import { ProjectContext } from './context.js';
import { EnvironmentPage } from './environment.js';
import { ProjectPage } from './project.js';

export { LAST_PROJECT_KEY, projectOf } from './context.js';
export { connectSnippet } from './environment.js';

const plugin: PluginModule = {
  name: 'projects',
  inject: [
    'router',
    'slots',
    'session',
    'flags',
    'platform',
    'rpc.org',
    'rpc.projects',
    'rpc.agents',
    'rpc.keys',
    'rpc.audit',
  ],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const session = service(ctx, 'session');
    const platform = service(ctx, 'platform');
    const page = (
      spec: { id: string; path: string; title: string },
      render: (params: Record<string, string>) => ReactNode,
    ) =>
      ctx.effect(() =>
        router.page({ ...spec, plugin: 'projects' }, ({ params }: PageProps) => render(params)),
      );

    page({ id: 'project', path: '/projects/:project', title: 'Project' }, (params) => (
      <ProjectPage
        project={params.project ?? ''}
        router={router}
        session={session}
        auditPath="/audit"
      />
    ));
    page(
      {
        id: 'environment',
        path: '/projects/:project/environments/:environment',
        title: 'Environment',
      },
      (params) => (
        <EnvironmentPage
          project={params.project ?? ''}
          environment={params.environment ?? ''}
          router={router}
          session={session}
          server={platform.serverUrl ?? platform.baseUrl}
        />
      ),
    );
    page({ id: 'access', path: '/projects/:project/access', title: 'Access' }, (params) => (
      <AccessPage project={params.project ?? ''} router={router} session={session} />
    ));
    ctx.effect(() =>
      slots.register({ name: 'shell.nav.context', plugin: 'projects' }, () => (
        <ProjectContext router={router} />
      )),
    );
  },
};

export default plugin;
