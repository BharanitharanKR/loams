// The hosted console's boot. The person picks how to sign in on the connect screen
// (the deploy's LOAMS_AUTH_MODE is the default, tailscale unless set; the choice is
// remembered in this browser):
//   tailscale  no sign-in: the engine identifies the tailnet user, and the header shows
//              whoever GET /api/v1/session says it is;
//   oidc       sign in at auth.loams.dev (Authorization Code + PKCE, the deploy's
//              client), then the access token is the bearer;
//   apikey     the connect screen asks for an API key (memory only), sent as the bearer.
// The engine is the deploy's LOAMS_ENGINE_URL or the one the person connects on first
// run. A config the console cannot use (an unknown mode, oidc without a client) stops
// at a screen that says so; it never falls back to another mode.

import type { ConsoleHandle } from '@loams/console-host';
import { createWebPlatform } from '@loams/platform-web';
import { type ReactNode, StrictMode } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { type AuthMode, offeredModes, type RuntimeConfig } from '../runtime-config.js';
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
import { fetchIdentity, savedEngine, savedMode, saveEngine, saveMode } from './engine.js';
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
  if (config.configError || !config.authMode) {
    show(
      root,
      <ConfigErrorScreen message={config.configError ?? 'config.json sets no sign-in mode.'} />,
    );
    return undefined;
  }
  const modes = offeredModes(config);
  const remembered = savedMode();
  let mode: AuthMode = remembered && modes.includes(remembered) ? remembered : config.authMode;
  const oidc = config.auth;
  const auth = oidc ? new OidcAuth(oidc, redirectUri()) : undefined;
  const fixed = config.server;
  const knownEngine = fixed ?? savedEngine();
  const reconnect = here.searchParams.has(CONNECT_PARAM) && (!fixed || modes.length > 1);
  const toConnect = () => globalThis.location.assign(`${redirectUri()}?${CONNECT_PARAM}`);

  if (mode === 'oidc' && auth && oidc && !reconnect) {
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
      show(
        root,
        <SignInScreen
          issuer={oidc.issuer}
          error={error}
          onSignIn={() => auth.signIn(here.hash)}
          onOtherMode={toConnect}
        />,
      );
      return undefined;
    }
  }

  let apiKey: string | undefined;
  const engine = await new Promise<string | undefined>((resolve) => {
    // In apikey mode the screen also takes the API key, which lives in memory only, so
    // it is shown on every load (prefilled, and testing the remembered engine at once).
    if (knownEngine && remembered && mode !== 'apikey' && !reconnect) {
      resolve(knownEngine);
      return;
    }
    const screen = show(
      root,
      <ConnectScreen
        initial={knownEngine}
        fixed={Boolean(fixed)}
        modes={modes}
        initialMode={mode}
        onConnect={(r: ConnectResult) => {
          saveEngine(r.engine);
          saveMode(r.mode);
          mode = r.mode;
          apiKey = r.apiKey;
          if (r.mode === 'oidc' && auth && !auth.signedIn()) {
            // Off to the issuer; the callback comes back here with the mode remembered.
            void auth.signIn('').catch((e: unknown) => {
              screen.unmount();
              show(root, <BootError message={errorText(e)} />);
            });
            resolve(undefined);
            return;
          }
          screen.unmount();
          globalThis.history.replaceState(null, '', redirectUri());
          resolve(r.engine);
        }}
      />,
    );
  });
  if (!engine) return undefined;

  let user: string | undefined;
  let identityProblem: string | undefined;
  if (mode === 'oidc' && auth) {
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
    // Back to the connect screen: another engine, or another way to sign in.
    changeEngine: !fixed || modes.length > 1 ? toConnect : undefined,
    // Tailscale mode has nothing to sign out of: the identity is the device's tailnet login.
    signOut:
      mode === 'oidc' && auth
        ? () => void auth.signOut()
        : mode === 'apikey'
          ? // The key lives in memory: drop it and ask again on the connect screen.
            toConnect
          : undefined,
  };

  const bearer =
    mode === 'oidc' && auth
      ? () => auth.accessToken()
      : mode === 'apikey'
        ? () => apiKey
        : () => undefined;
  try {
    return await start({
      platform: createWebPlatform({ baseUrl: engine, token: bearer }),
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
