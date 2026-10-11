// The hosted and BYOC console's boot, by the deploy's LOAMS_AUTH_MODE:
//   oidc       sign in (Authorization Code + PKCE), then the access token is the bearer;
//   tailscale  no sign-in: the engine identifies the tailnet user, and the header shows
//              whoever GET /api/v1/session says it is;
//   apikey     the connect screen asks for an API key (memory only), sent as the bearer.
// Then pick the engine (the deploy's LOAMS_ENGINE_URL, or the one the person connects
// on first run) and start the cordis console against it. A config the console cannot
// use (an unknown mode, oidc without a client) stops at a screen that says so.

import type { ConsoleHandle } from '@loams/console-host';
import { createWebPlatform } from '@loams/platform-web';
import { type ReactNode, StrictMode } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import type { RuntimeConfig } from '../runtime-config.js';
import { type Callback, OidcAuth } from './auth.js';
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
import { fetchIdentity, savedEngine, saveEngine } from './engine.js';
import {
  BootError,
  ConfigErrorScreen,
  type ConnectResult,
  ConnectScreen,
  SignInScreen,
} from './screens.js';

/** `?connect` opens the connect screen even when an engine is remembered (and not fixed). */
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

const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

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
  const mode = config.authMode;
  if (config.configError || !mode) {
    show(
      root,
      <ConfigErrorScreen
        message={config.configError ?? 'config.json sets no sign-in mode.'}
        mode={mode}
      />,
    );
    return undefined;
  }
  const auth =
    mode === 'oidc' && config.auth ? new OidcAuth(config.auth, redirectUri()) : undefined;

  if (auth && config.auth) {
    const result = await auth
      .completeSignIn(here)
      .catch((e: unknown): Callback => ({ kind: 'error', message: errorText(e) }));
    if (result.kind !== 'none') {
      // Drop `code` and `state` from the address bar and history; keep the route.
      globalThis.history.replaceState(
        null,
        '',
        `${redirectUri()}${result.kind === 'signed-in' ? result.returnTo : ''}`,
      );
    }
    if (!auth.signedIn()) {
      let error = result.kind === 'error' ? result.message : undefined;
      if (result.kind === 'none' && auth.wasSignedIn()) {
        // A reload: the tokens were in memory only. Renew against the issuer's session.
        try {
          await auth.signIn(here.hash, true);
          return undefined;
        } catch (e) {
          // The issuer is unreachable: say so on the sign-in screen.
          error = errorText(e);
        }
      }
      const issuer = config.auth.issuer;
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
    // In apikey mode the screen also takes the API key, which lives in memory only, so
    // it is shown on every load (prefilled, and testing the remembered engine at once).
    const reconnect = here.searchParams.has(CONNECT_PARAM) && !fixed;
    if (remembered && mode !== 'apikey' && !reconnect) {
      resolve(remembered);
      return;
    }
    const screen = show(
      root,
      <ConnectScreen
        initial={remembered}
        fixed={Boolean(fixed)}
        askApiKey={mode === 'apikey'}
        mode={mode}
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

  let user: string | undefined;
  let identityProblem: string | undefined;
  if (auth) {
    user = auth.user()?.email ?? auth.user()?.name;
  } else if (mode === 'tailscale') {
    // Not a gate: the engine refuses what it cannot attribute, and the header says why
    // it shows no one.
    const identity = await fetchIdentity(engine);
    if (identity.ok) user = identity.user;
    else identityProblem = identity.message;
  }

  const demo = demoEnabled();
  const session: HostedSession = {
    engine,
    authMode: mode,
    user,
    identityProblem,
    demo,
    setDemo(on) {
      setDemoEnabled(on);
      globalThis.location.reload();
    },
    changeEngine: fixed
      ? undefined
      : () => globalThis.location.assign(`${redirectUri()}?${CONNECT_PARAM}`),
    // Tailscale mode has nothing to sign out of: the identity is the device's tailnet login.
    signOut: auth
      ? () => void auth.signOut()
      : mode === 'apikey'
        ? // The key lives in memory: drop it and ask again on the connect screen.
          () => globalThis.location.assign(`${redirectUri()}?${CONNECT_PARAM}`)
        : undefined,
  };

  try {
    return await start({
      platform: createWebPlatform({
        baseUrl: engine,
        token: auth ? () => auth.accessToken() : mode === 'apikey' ? () => apiKey : () => undefined,
      }),
      root,
      patches: [...webPatches(demo), hostedPatch],
      extraModules: { ...webModules, [HOSTED_PACKAGE]: async () => hostedPlugin(session) },
      extraManifests: [...webManifests, hostedManifest],
    });
  } catch (e) {
    show(root, <BootError message={errorText(e)} />);
    return undefined;
  }
}
