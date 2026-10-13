// @loams/plugin-studio: Loams Studio, the agentic development environment in the
// console (design §53; SD1 Task 15). `modes: ["studio"]`, so the host applies it
// the first time Studio mode is entered. It registers Studio's routes, its nav
// entries and its header context; it reaches loams-agentd through the desktop
// bridge on the desktop, and through a stub until SD1 Task 7 on the web (agentd.ts).

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
// Brings in the `desktop` service type (a module augmentation of `Services`).
import type {} from '@loams/platform-electron';
import { desktopAgentd, type StudioAgentd, webAgentd } from './agentd.js';
import { StudioContext } from './header.js';
import { routeOf, STUDIO_PAGES } from './routes.js';
import { StudioView } from './view.js';

export {
  type AgentdProjectsClient,
  type DesktopBridge,
  desktopAgentd,
  isCloneUrl,
  type Project,
  type Reach,
  type StudioAgentd,
  webAgentd,
} from './agentd.js';
export { LAYOUT_DEFAULTS, LAYOUT_KEY, StudioLayout } from './layout/StudioLayout.js';
export { routeOf, STUDIO_PAGES, type StudioRoute, studioHref } from './routes.js';
export { StudioView } from './view.js';

const plugin: PluginModule = {
  name: 'studio',
  // `desktop` only on the desktop edition: the catalog rows narrow this list.
  inject: ['router', 'slots', 'mode', 'platform', 'flags'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const platform = service(ctx, 'platform');
    const agentd: StudioAgentd =
      platform.kind === 'desktop' ? desktopAgentd(service(ctx, 'desktop')) : webAgentd();
    const navigate = (to: string) => router.navigate(to);

    for (const page of STUDIO_PAGES) {
      ctx.effect(() =>
        router.page(
          { id: page.id, path: page.path, title: page.title, plugin: 'studio' },
          ({ params }) => (
            <StudioView agentd={agentd} route={routeOf(page.kind, params)} navigate={navigate} />
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
        { name: 'shell.header.server', plugin: 'studio', meta: { mode: 'studio' } },
        () => <StudioContext agentd={agentd} router={router} />,
      ),
    );
  },
};

export default plugin;
