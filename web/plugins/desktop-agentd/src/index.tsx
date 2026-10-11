// @loams/plugin-desktop-agentd: Settings › Background agents, the status panel
// for the loams-agentd daemon (DD1 Tasks 6, 10, 12). The daemon's token stays in
// Electron main; this plugin only sees `desktop.agentd`'s token-free values.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { AgentdSection } from './agentd-section.js';

export { AgentdSection } from './agentd-section.js';
export { ConsentDialog, StopDialog } from './consent-dialog.js';
export {
  type AgentdApi,
  createMockAgentdApi,
  type MockAgentd,
  type MockAgentdOptions,
  sampleSessions,
} from './mock.js';

const plugin: PluginModule = {
  name: 'desktop-agentd',
  inject: ['desktop', 'router', 'slots'],
  apply(ctx: Context) {
    const desktop = service(ctx, 'desktop');
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    // A preload without the agentd namespace (an older shell) gets no section.
    if (!desktop.agentd) return;
    ctx.effect(() =>
      slots.register(
        {
          name: 'console.settings.section',
          plugin: 'desktop-agentd',
          order: 40,
          meta: { id: 'background-agents', label: 'Background agents' },
        },
        () => <AgentdSection agentd={desktop.agentd} navigate={(p) => router.navigate(p)} />,
      ),
    );
  },
};

export default plugin;
