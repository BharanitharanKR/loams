// The hosted and BYOC console's boot: sign in (OIDC + PKCE) when an issuer is
// configured, pick the engine (the deploy's LOAMS_ENGINE_URL, or the one the
// person connects on first run), then start the cordis console against that
// engine with the access token as a bearer.

import type { ConsoleHandle } from '@loams/console-host';
import { createWebPlatform } from '@loams/platform-web';
import { type ReactNode, StrictMode } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import type { RuntimeConfig } from '../runtime-config.js';
import { OidcAuth } from './auth.js';
import { demoEnabled, setDemoEnabled } from './demo.js';
import {
  HOSTED_PACKAGE,
  type HostedSession,
  hostedManifest,
  hostedPatch,
  hostedPlugin,
  webManifests,
  webModules,
  webPatches,
} from './edition.js';
import { savedEngine, saveEngine } from './engine.js';
import { BootError, type ConnectResult, ConnectScreen, SignInScreen } from './screens.js';

/** `?connect` opens the connect screen even when an engine is remembered. */
const CONNECT_PARAM = 'connect';

export type StartFn = (options: {
  platform: ReturnType<typeof createWebPlatform>;
  root: HTMLElement;
  patches: ReturnType<typeof webPatches>;
  extraModules: typeof webModules;
  extraManifests: unknown[];
}) => Promise<ConsoleHandle>;

/** The page's address without query or hash: the registered OIDC redirect URI. */
export function redirectUri(loc: Pick<Location, 'origin' | 'pathname'> = globalThis.location) {
  const path = loc.pathname.endsWith('.html') ? loc.pathname : '/';
  return `${loc.origin}${path}`;
}

function show(root: HTMLElement, node: ReactNode): Root {
  const r = createRoot(root);
  r.render(<StrictMode>{node}</StrictMode>);
  return r;
}

export async function startHosted(
  config: RuntimeConfig,
  root: HTMLElement,
  start: StartFn,
): Promise<ConsoleHandle | undefined> {
  const here = new URL(globalThis.location.href);
  const auth = config.auth ? new OidcAuth(config.auth, redirectUri()) : undefined;

  if (auth && config.auth) {
    const result = await auth.completeSignIn(here);
    if (result.kind !== 'none') {
      // Drop `code` and `state` from the address bar and history; keep the route.
      globalThis.history.replaceState(
        null,
        '',
        `${redirectUri()}${result.kind === 'signed-in' ? result.returnTo : ''}`,
      );
    }
    if (!auth.signedIn()) {
      if (result.kind === 'none' && auth.wasSignedIn()) {
        // A reload: the tokens were in memory only. Renew against the issuer's session.
        await auth.signIn(here.hash, true);
        return undefined;
      }
      const issuer = config.auth.issuer;
      const error = result.kind === 'error' ? result.message : undefined;
      show(
        root,
        <SignInScreen issuer={issuer} error={error} onSignIn={() => auth.signIn(here.hash)} />,
      );
      return undefined;
    }
  }

  const fixed = config.server;
  let apiKey: string | undefined;
  const engine = await new Promise<string>((resolve) => {
    const remembered = fixed ?? savedEngine();
    if (remembered && !(here.searchParams.has(CONNECT_PARAM) && !fixed)) {
      resolve(remembered);
      return;
    }
    const screen = show(
      root,
      <ConnectScreen
        initial={remembered}
        askApiKey={!auth}
        onConnect={(r: ConnectResult) => {
          saveEngine(r.engine);
          apiKey = r.apiKey;
          screen.unmount();
          globalThis.history.replaceState(null, '', redirectUri());
          resolve(r.engine);
        }}
      />,
    );
  });

  const demo = demoEnabled();
  const session: HostedSession = {
    engine,
    user: auth?.user()?.email ?? auth?.user()?.name,
    demo,
    setDemo(on) {
      setDemoEnabled(on);
      globalThis.location.reload();
    },
    changeEngine: fixed
      ? undefined
      : () => globalThis.location.assign(`${redirectUri()}?${CONNECT_PARAM}`),
    signOut: auth
      ? () => void auth.signOut()
      : apiKey
        ? () => globalThis.location.assign(redirectUri())
        : undefined,
  };

  try {
    return await start({
      platform: createWebPlatform({
        baseUrl: engine,
        token: auth ? () => auth.accessToken() : () => apiKey,
      }),
      root,
      patches: [...webPatches(demo), hostedPatch],
      extraModules: { ...webModules, [HOSTED_PACKAGE]: async () => hostedPlugin(session) },
      extraManifests: [...webManifests, hostedManifest],
    });
  } catch (e) {
    show(root, <BootError message={e instanceof Error ? e.message : String(e)} />);
    return undefined;
  }
}
