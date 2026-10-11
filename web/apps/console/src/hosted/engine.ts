// Which engine a hosted console talks to, and whether this browser can reach it.
//
// The engine runs on the person's machine or VM and is reachable only over their
// tailnet (Tailscale, or a self-hosted Headscale): this device must run the
// Tailscale client and be logged in to the same tailnet. The URL is remembered in
// localStorage (it is an address, not a credential); the deploy may fix it instead
// (LOAMS_ENGINE_URL), and then it is not asked for.

import { originOf } from '../runtime-config.js';

const KEY = 'loams.engine.url';

type Store = Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;

function local(): Store | undefined {
  try {
    return globalThis.localStorage;
  } catch {
    return undefined;
  }
}

/** The engine URL this browser connected to last, if any. */
export function savedEngine(store: Store | undefined = local()): string | undefined {
  try {
    return originOf(store?.getItem(KEY) ?? undefined, true);
  } catch {
    return undefined;
  }
}

export function saveEngine(url: string | undefined, store: Store | undefined = local()) {
  try {
    if (url) store?.setItem(KEY, url);
    else store?.removeItem(KEY);
  } catch {
    // Private mode: the person enters it again next time.
  }
}

/** What the person typed, as an engine origin, or why not. */
export function parseEngineUrl(
  input: string,
  pageProtocol = globalThis.location?.protocol ?? 'https:',
): { ok: true; origin: string } | { ok: false; problem: string } {
  const text = input.trim();
  if (!text) return { ok: false, problem: 'Enter the engine URL.' };
  const withScheme = /^[a-z][a-z0-9+.-]*:\/\//i.test(text) ? text : `https://${text}`;
  const origin = originOf(withScheme, true);
  if (!origin) return { ok: false, problem: 'That is not an http(s) URL.' };
  const url = new URL(origin);
  if (pageProtocol === 'https:' && url.protocol === 'http:' && !isLoopback(url.hostname)) {
    return {
      ok: false,
      problem:
        'This console is served over HTTPS, so the browser blocks a plain http engine. ' +
        'Serve the engine over HTTPS on your tailnet (`tailscale serve`, which the Compose ' +
        'file sets up) and use its https://….ts.net address.',
    };
  }
  return { ok: true, origin };
}

function isLoopback(host: string) {
  return host === 'localhost' || host === '127.0.0.1' || host === '[::1]';
}

/** True for a MagicDNS name on Tailscale's own domain. */
export function isTailnetHost(origin: string): boolean {
  return new URL(origin).hostname.endsWith('.ts.net');
}

export type Health =
  | { ok: true; status: number; ms: number }
  | { ok: false; kind: 'unreachable' | 'timeout' | 'http'; status?: number; message: string };

/**
 * `GET <engine>/health`, without credentials. A network error means this device is not
 * on the engine's tailnet, the engine is down, or it does not allow this console's
 * origin (CORS); the browser does not say which.
 */
export async function checkHealth(
  origin: string,
  fetcher: typeof globalThis.fetch = (...a) => globalThis.fetch(...a),
  timeoutMs = 6_000,
  now: () => number = () => performance.now(),
): Promise<Health> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const start = now();
  try {
    const res = await fetcher(`${origin}/health`, {
      method: 'GET',
      mode: 'cors',
      credentials: 'omit',
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!res.ok) {
      return {
        ok: false,
        kind: 'http',
        status: res.status,
        message: `The engine answered ${res.status} on /health.`,
      };
    }
    return { ok: true, status: res.status, ms: Math.round(now() - start) };
  } catch (e) {
    if (controller.signal.aborted) {
      return {
        ok: false,
        kind: 'timeout',
        message: `No answer within ${Math.round(timeoutMs / 1000)} s.`,
      };
    }
    return {
      ok: false,
      kind: 'unreachable',
      message: e instanceof Error ? e.message : String(e),
    };
  } finally {
    clearTimeout(timer);
  }
}
