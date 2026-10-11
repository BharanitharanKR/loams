// Writes the hosted console's runtime config.json from the deploy's environment
// (src/runtime-config.ts reads it in the browser):
//
//   LOAMS_AUTH_CLIENT_ID=<your client id> node deploy/config.mjs dist-hosted/config.json
//
//   LOAMS_ENGINE_URL      optional: fixes the engine (https://loams.<tailnet>.ts.net); unset,
//                         the person connects one on first run. LOAMS_CONSOLE_SERVER is
//                         its older name.
//   LOAMS_AUTH_MODE       oidc (default) | tailscale | apikey:
//                           oidc       sign in with Authorization Code + PKCE at the issuer
//                           tailscale  no sign-in; the engine identifies the tailnet user
//                           apikey     ask for an API key, kept in the tab's memory only
//   LOAMS_AUTH_ISSUER     oidc: the issuer, default https://auth.loams.dev (or your own IdP).
//   LOAMS_AUTH_CLIENT_ID  oidc: the console's public client id at the issuer. A console you
//                         deploy yourself registers its own at https://auth.loams.dev/register,
//                         with its exact origin as the redirect URI.
//   LOAMS_AUTH_SCOPE      optional, default "openid profile email offline_access".
//
// stage.mjs calls `hostedConfig` too, so Cloudflare and Vercel deploys get the same file.
import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const LOOPBACK = new Set(['localhost', '127.0.0.1', '[::1]']);

/** An https URL (http only on loopback), or a thrown error naming `what`. */
export function secureUrl(value, what) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${what} must be a valid HTTPS URL, got ${JSON.stringify(value)}`);
  }
  const ok = url.protocol === 'https:' || (url.protocol === 'http:' && LOOPBACK.has(url.hostname));
  if (!ok) throw new Error(`${what} must use HTTPS (plain http only on localhost)`);
  if (url.search || url.hash) throw new Error(`${what} must not carry a query or fragment`);
  return url;
}

const get = (env, name) => {
  const v = env[name];
  return typeof v === 'string' && v.trim() !== '' ? v.trim() : undefined;
};

export const AUTH_MODES = ['oidc', 'tailscale', 'apikey'];
export const DEFAULT_ISSUER = 'https://auth.loams.dev';
export const REGISTER_URL = 'https://auth.loams.dev/register';

/** The config.json object for a hosted deploy, validated. */
export function hostedConfig(env = process.env, { requireAuth = false } = {}) {
  const config = { hosted: true };
  const engine = get(env, 'LOAMS_ENGINE_URL') ?? get(env, 'LOAMS_CONSOLE_SERVER');
  if (engine) config.engineUrl = secureUrl(engine, 'LOAMS_ENGINE_URL').origin;
  const mode = (get(env, 'LOAMS_AUTH_MODE') ?? 'oidc').toLowerCase();
  if (!AUTH_MODES.includes(mode)) {
    throw new Error(`LOAMS_AUTH_MODE must be one of ${AUTH_MODES.join(', ')}, got ${mode}`);
  }
  if (requireAuth && mode !== 'oidc') {
    throw new Error('this deploy requires LOAMS_AUTH_MODE=oidc');
  }
  config.authMode = mode;
  if (mode !== 'oidc') return config;
  const issuer = get(env, 'LOAMS_AUTH_ISSUER') ?? DEFAULT_ISSUER;
  const clientId = get(env, 'LOAMS_AUTH_CLIENT_ID');
  if (!clientId) {
    throw new Error(
      'LOAMS_AUTH_MODE=oidc needs LOAMS_AUTH_CLIENT_ID: register this console at ' +
        `${REGISTER_URL} (or your own IdP), or set LOAMS_AUTH_MODE=tailscale or apikey`,
    );
  }
  const url = secureUrl(issuer, 'LOAMS_AUTH_ISSUER');
  config.authIssuer = `${url.origin}${url.pathname.replace(/\/+$/, '')}`;
  config.authClientId = clientId;
  const scope = get(env, 'LOAMS_AUTH_SCOPE');
  if (scope) config.authScope = scope;
  return config;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const out = process.argv[2];
  if (!out) {
    console.error('usage: node deploy/config.mjs <path/to/config.json>');
    process.exit(2);
  }
  const config = hostedConfig(process.env, {
    requireAuth: process.argv.includes('--require-auth'),
  });
  writeFileSync(out, `${JSON.stringify(config, null, 2)}\n`);
  console.log(`wrote ${out}`);
}
