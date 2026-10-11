/**
 * Runtime configuration, read from `<base>config.json` when the console
 * boots (issue #269). Nothing here is baked in at build time: the engine
 * serves its own `config.json` (an empty object, so the console talks to
 * the origin it came from), and a hosted or BYOC deploy (Cloudflare, Vercel,
 * the Compose console) writes one from its environment at deploy time
 * (deploy/config.mjs: LOAMS_ENGINE_URL, LOAMS_AUTH_ISSUER, LOAMS_AUTH_CLIENT_ID).
 */
export type RuntimeConfig = {
  /**
   * The Loams engine's origin, for example `https://loams.tailnet.ts.net`. Unset means
   * same-origin, or, on a hosted console, the engine the person connects on first run.
   * `config.json` spells it `engineUrl` (or `server`, its older name).
   */
  server?: string;
  /**
   * Set on a console that is not served by the engine (Ostrium's, or one deployed on
   * Vercel or Cloudflare): it signs in with OIDC, asks which engine to talk to and
   * sends a bearer token instead of the engine's session cookie.
   */
  hosted?: boolean;
  /** The OIDC provider (`authIssuer`, `authClientId`, optional `authScope`). */
  auth?: { issuer: string; clientId: string; scope: string };
};

/** The scopes asked for when `authScope` is unset. */
export const DEFAULT_SCOPE = 'openid profile email offline_access';

const LOOPBACK = new Set(['localhost', '127.0.0.1', '[::1]']);

/**
 * An `http(s)` URL's origin, or undefined. Plain `http` is accepted only when
 * `allowHttp` says so, or for a loopback host (a console and engine on one machine).
 */
export function originOf(value: unknown, allowHttp = false): string | undefined {
  if (typeof value !== 'string' || value.trim() === '') return undefined;
  let url: URL;
  try {
    url = new URL(value.trim());
  } catch {
    return undefined;
  }
  if (url.protocol === 'https:') return url.origin;
  if (url.protocol === 'http:' && (allowHttp || LOOPBACK.has(url.hostname))) return url.origin;
  return undefined;
}

/** An issuer URL: https (http on loopback), path kept, no trailing slash, no query. */
export function issuerOf(value: unknown): string | undefined {
  const origin = originOf(value);
  if (!origin || typeof value !== 'string') return undefined;
  const url = new URL(value.trim());
  if (url.search || url.hash) return undefined;
  return `${origin}${url.pathname.replace(/\/+$/, '')}`;
}

/** Validates a parsed `config.json`. Unknown keys are ignored; bad values are dropped. */
export function parseRuntimeConfig(raw: unknown): RuntimeConfig {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const r = raw as Record<string, unknown>;
  const config: RuntimeConfig = {};
  // Only the origin: API paths are absolute (`/api/v1/...`, `/v1/...`). The engine-served
  // console has always allowed plain http here (a LAN engine); keep that.
  const server = originOf(r.engineUrl ?? r.server, true);
  if (server) config.server = server;
  const issuer = issuerOf(r.authIssuer);
  const clientId = typeof r.authClientId === 'string' ? r.authClientId.trim() : '';
  if (issuer && clientId) {
    const scope = typeof r.authScope === 'string' && r.authScope.trim() ? r.authScope.trim() : '';
    config.auth = { issuer, clientId, scope: scope || DEFAULT_SCOPE };
  }
  if (r.hosted === true || config.auth) config.hosted = true;
  return config;
}

/** Fetches and parses `<base>config.json`; any failure means the defaults. */
export async function loadRuntimeConfig(
  base: string,
  fetcher: typeof fetch = fetch,
  deadlineMs = 3_000,
): Promise<RuntimeConfig> {
  const controller = new AbortController();
  const deadline = setTimeout(() => controller.abort(), deadlineMs);
  try {
    const res = await fetcher(`${base}config.json`, {
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!res.ok) return {};
    const type = res.headers.get('content-type') ?? '';
    // An SPA fallback answers a missing file with index.html.
    if (!type.includes('json')) return {};
    return parseRuntimeConfig(await res.json());
  } catch {
    return {};
  } finally {
    clearTimeout(deadline);
  }
}
