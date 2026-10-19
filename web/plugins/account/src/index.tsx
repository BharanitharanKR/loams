// @loams/plugin-account: sign-in, first-run setup and OAuth consent on loams.auth.v1
// (API2 Task 10, AP1a Task 5), full-screen as the original React console drew them.
// The shell sends a visitor without a session here (the `session` service's gate,
// @loams/plugin-identity); sign-in and setup are public, consent needs a session.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { ConsentPage, SetupPage, SignInPage } from './pages.js';

export { safeNext } from './pages.js';

const plugin: PluginModule = {
  name: 'account',
  inject: ['router', 'session', 'flags', 'platform', 'rpc.auth'],
  apply(ctx: Context) {
    const router = service(ctx, 'router');
    const session = service(ctx, 'session');
    const flags = service(ctx, 'flags');
    const platform = service(ctx, 'platform');
    ctx.effect(() =>
      router.page(
        {
          id: 'sign-in',
          path: '/sign-in',
          title: 'Sign in',
          plugin: 'account',
          bare: true,
          public: true,
        },
        () => (
          <SignInPage router={router} session={session} flags={flags} server={platform.baseUrl} />
        ),
      ),
    );
    ctx.effect(() =>
      router.page(
        {
          id: 'setup',
          path: '/setup',
          title: 'Set up Loams',
          plugin: 'account',
          bare: true,
          public: true,
        },
        () => <SetupPage router={router} session={session} tokenNeeded={!flags.signInOff} />,
      ),
    );
    ctx.effect(() =>
      router.page(
        { id: 'consent', path: '/consent', title: 'Allow access', plugin: 'account', bare: true },
        () => <ConsentPage router={router} session={session} />,
      ),
    );
  },
};

export default plugin;
