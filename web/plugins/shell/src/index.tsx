// @loams/plugin-shell: the layout (the `root` slot), the navigation (from
// `console.nav`), the page outlet (`console.page`) and the `router` service
// (§37 §5.4–§5.5). Components get props and closures, never `ctx`.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import type { SlotRegistry } from '@loams/slots';
import { Layout } from './layout.js';
import { syncModeWithRoute } from './mode-switcher.js';
import { HashRouter } from './router.js';

export {
  inMode,
  Layout,
  type ModeFilter,
  navSections,
  SECTIONS,
  THEME_KEY,
  useTheme,
} from './layout.js';
export {
  MODE_LABEL,
  ModeSwitcher,
  syncModeWithRoute,
  useMode,
  useModeShortcut,
} from './mode-switcher.js';

export { compile, HashRouter, type HashSource, windowHash } from './router.js';

const plugin: PluginModule = {
  name: 'shell',
  inject: ['slots', 'session', 'flags', 'mode'],
  apply(ctx: Context) {
    const slots = service(ctx, 'slots') as SlotRegistry;
    const session = service(ctx, 'session');
    const flags = service(ctx, 'flags');
    const mode = service(ctx, 'mode');
    const router = new HashRouter(slots);
    ctx.effect(() => () => router.dispose(), 'router');
    ctx.provide('router', router);
    // A route of the other mode switches the mode first (§53 §4.4).
    ctx.effect(() => syncModeWithRoute(router, mode), 'mode-route');
    ctx.effect(() =>
      slots.register({ name: 'root', plugin: 'shell' }, () => (
        <Layout router={router} session={session} flags={flags} mode={mode} />
      )),
    );
  },
};

export default plugin;
