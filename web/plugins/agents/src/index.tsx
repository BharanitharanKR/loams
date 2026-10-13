// @loams/plugin-agents: a project's agents on loams.admin.v1 (API2 Task 10, AP1a
// Task 5), as the original React console had them: the list with registration, and
// an agent's policy, trust policies, live tokens (with revocation), the token
// exchange and its activity.

import { type PageProps, type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { AgentPage } from './agent.js';
import { AgentsPage } from './agents.js';

export { exchangeSnippet } from './agent.js';

const plugin: PluginModule = {
  name: 'agents',
  inject: [
    'router',
    'session',
    'platform',
    'rpc.org',
    'rpc.projects',
    'rpc.agents',
    'rpc.keys',
    'rpc.audit',
  ],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const session = service(ctx, 'session');
    const platform = service(ctx, 'platform');
    ctx.effect(() =>
      router.page(
        { id: 'agents', path: '/projects/:project/agents', title: 'Agents', plugin: 'agents' },
        ({ params }: PageProps) => (
          <AgentsPage project={params.project ?? ''} router={router} session={session} />
        ),
      ),
    );
    ctx.effect(() =>
      router.page(
        { id: 'agent', path: '/projects/:project/agents/:agent', title: 'Agent', plugin: 'agents' },
        ({ params }: PageProps) => (
          <AgentPage
            project={params.project ?? ''}
            agent={params.agent ?? ''}
            router={router}
            session={session}
            server={platform.serverUrl ?? platform.baseUrl}
          />
        ),
      ),
    );
  },
};

export default plugin;
