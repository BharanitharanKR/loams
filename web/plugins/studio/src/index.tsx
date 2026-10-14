// @loams/plugin-studio: Loams Studio, the agentic development environment in the
// console (design §53; SD1 Task 15). `modes: ["studio"]`, so the host applies it
// the first time Studio mode is entered. It registers Studio's routes, its nav
// entries and its header context. It reaches loams-agentd through the console's
// `agentd` service on the desktop (SD1 Task 6), and on the web through the selected
// remote agentd host (SD1 Task 7, hosts/agentd.ts; `?host=<id>` selects one), in
// both cases with connect-query over the host's transport (agentd.ts).

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { type StudioAgentd, studioAgentd } from './agentd.js';
import { desktopFactory } from './factory.js';
import { StudioContext } from './header.js';
import { hostedAgentd } from './hosts/agentd.js';
import { createStudioKeys } from './palette/commands.js';
import { StudioPalette } from './palette/StudioPalette.js';
import { routeOf, STUDIO_PAGES } from './routes.js';
import { agentdPaletteSource } from './sidebar/paletteSource.js';
import { StudioView } from './view.js';

export {
  isCloneUrl,
  type Project,
  projectOf,
  type Reach,
  type StudioAgentd,
  studioAgentd,
} from './agentd.js';
export {
  desktopFactory,
  type FactoryApp,
  type FactoryPull,
  type StudioFactory,
} from './factory.js';
export {
  type HostedAgentdOptions,
  hostedAgentd,
  type StudioHosts,
} from './hosts/agentd.js';
export { LAYOUT_DEFAULTS, LAYOUT_KEY, StudioLayout } from './layout/StudioLayout.js';
export {
  type CommandName,
  createStudioKeys,
  StudioCommands,
  type StudioKeys,
  useStudioCommand,
  useStudioKeybindings,
} from './palette/commands.js';
export { StudioPalette } from './palette/StudioPalette.js';
export {
  type GitAction,
  type PaletteGitAction,
  type PaletteHarness,
  pendingPaletteSource,
  type StudioPaletteSource,
} from './palette/source.js';
export {
  routeOf,
  STUDIO_PAGES,
  type StudioRoute,
  studioHref,
  type ThreadDraft,
} from './routes.js';
export { KeybindingsCard } from './settings/Keybindings.js';
export { FactoryGroup } from './sidebar/Factory.js';
export { agentdPaletteSource } from './sidebar/paletteSource.js';
export { StudioSidebar } from './sidebar/Sidebar.js';
export { BranchPr, DraftCard } from './thread/FactoryLinks.js';
export {
  agentdTerminalBackend,
  fileViewerHref,
  isStudioShortcut,
  ThreadTerminalDrawer,
  terminalSessionsFor,
} from './thread/TerminalDrawer.js';
export {
  addTerminalContext,
  removeTerminalContext,
  takeTerminalContexts,
  terminalContexts,
  useTerminalContexts,
} from './thread/terminalContexts.js';
export { StudioView } from './view.js';

const plugin: PluginModule = {
  name: 'studio',
  // `desktop` only on the desktop edition: the catalog rows narrow this list.
  inject: ['router', 'slots', 'mode', 'platform', 'flags', 'agentd'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const platform = service(ctx, 'platform');
    const desktop = platform.kind === 'desktop' ? service(ctx, 'desktop') : undefined;
    const agentd: StudioAgentd = desktop
      ? studioAgentd(service(ctx, 'agentd'), desktop.shell)
      : hostedAgentd();
    // The Factory group (SD1 Task 27): the desktop's factory bridge; on the web nothing
    // until SF1 (D922).
    const factory = desktop?.factory ? desktopFactory(desktop) : undefined;
    const navigate = (to: string) => router.navigate(to);
    // One keyboard layer and palette source for every Studio page (SD1 Task 24). The
    // palette's threads and run groups come from agentd (SD1 Task 17).
    const keys = createStudioKeys({ edition: platform.kind });
    const paletteSource = agentdPaletteSource(agentd);

    for (const page of STUDIO_PAGES) {
      ctx.effect(() =>
        router.page(
          { id: page.id, path: page.path, title: page.title, plugin: 'studio' },
          ({ params }) => (
            <StudioView
              agentd={agentd}
              factory={factory}
              route={routeOf(page.kind, params)}
              navigate={navigate}
              hostParam={params.host}
              keys={keys}
              openExternal={(url) => platform.openExternal(url)}
            />
          ),
        ),
      );
    }
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'studio',
          order: 0,
          meta: {
            id: 'studio-home',
            label: 'Home',
            href: '/studio',
            icon: 'studio',
            group: 'Studio',
            mode: 'studio',
          },
        },
        () => null,
      ),
    );
    ctx.effect(() =>
      slots.register(
        {
          name: 'shell.nav.section',
          plugin: 'studio',
          order: 90,
          meta: {
            id: 'studio-settings',
            label: 'Settings',
            href: '/studio/settings',
            icon: 'settings',
            group: 'Studio',
            mode: 'studio',
          },
        },
        () => null,
      ),
    );
    ctx.effect(() =>
      slots.register(
        { name: 'palette.command', plugin: 'studio', meta: { mode: 'studio' } },
        ({ query, publish }) => (
          <StudioPalette
            query={query}
            publish={publish}
            agentd={agentd}
            source={paletteSource}
            router={router}
            keys={keys}
            copy={(text) => platform.clipboardWrite(text)}
          />
        ),
      ),
    );
    ctx.effect(() =>
      slots.register(
        { name: 'shell.header.server', plugin: 'studio', meta: { mode: 'studio' } },
        () => <StudioContext agentd={agentd} router={router} notify={(n) => platform.notify(n)} />,
      ),
    );
  },
};

export default plugin;
