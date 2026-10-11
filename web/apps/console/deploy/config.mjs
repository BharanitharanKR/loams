// Writes the hosted console's runtime config.json from the deploy's environment
// (src/runtime-config.ts reads it in the browser):
//
//   LOAMS_AUTH_ISSUER=https://auth.loams.dev LOAMS_AUTH_CLIENT_ID=loams-console \
//     node deploy/config.mjs dist-hosted/config.json
//
//   LOAMS_ENGINE_URL      optional: fixes the engine (https://loams.<tailnet>.ts.net); unset,
//                         the person connects one on first run. LOAMS_CONSOLE_SERVER is
//                         its older name.
//   LOAMS_AUTH_ISSUER     the OIDC issuer; with LOAMS_AUTH_CLIENT_ID, the console signs in
//                         with Authorization Code + PKCE. Unset, it offers an API key field.
//   LOAMS_AUTH_CLIENT_ID  the console's public client id at the issuer.
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

/** The config.json object for a hosted deploy, validated. */
export function hostedConfig(env = process.env, { requireAuth = false } = {}) {
  const config = { hosted: true };
  const engine = get(env, 'LOAMS_ENGINE_URL') ?? get(env, 'LOAMS_CONSOLE_SERVER');
  if (engine) config.engineUrl = secureUrl(engine, 'LOAMS_ENGINE_URL').origin;
  const issuer = get(env, 'LOAMS_AUTH_ISSUER');
  const clientId = get(env, 'LOAMS_AUTH_CLIENT_ID');
  if (Boolean(issuer) !== Boolean(clientId)) {
    throw new Error('set LOAMS_AUTH_ISSUER and LOAMS_AUTH_CLIENT_ID together');
  }
  if (requireAuth && !issuer) {
    throw new Error('LOAMS_AUTH_ISSUER and LOAMS_AUTH_CLIENT_ID are required for this deploy');
  }
  if (issuer) {
    const url = secureUrl(issuer, 'LOAMS_AUTH_ISSUER');
    config.authIssuer = `${url.origin}${url.pathname.replace(/\/+$/, '')}`;
    config.authClientId = clientId;
    const scope = get(env, 'LOAMS_AUTH_SCOPE');
    if (scope) config.authScope = scope;
  }
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
