// Stages the console build as the hosted / BYOC console, for Cloudflare Workers
// static assets (issue #269) or Vercel:
//
//   node deploy/stage.mjs                    # dist/ -> dist-cloudflare/
//   node deploy/stage.mjs --target vercel    # dist/ -> dist-vercel/
//
// The runtime config comes from the environment (deploy/config.mjs:
// LOAMS_ENGINE_URL, LOAMS_AUTH_MODE, LOAMS_AUTH_ISSUER, LOAMS_AUTH_CLIENT_ID) and lands in
// /config.json and /ui/config.json; nothing is baked into the bundle.
//
// The build is the one the engine embeds (base path /ui/), so it lands under
// <out>/ui/. The root page is the cordis console (ui/cordis.html), which the
// SPA fallback also serves for unknown paths; it routes by hash, so the OIDC
// redirect URI is the bare origin. Asset paths resolve to real files and come
// back with their own MIME type, never as the HTML fallback.
import { createHash } from 'node:crypto';
import {
  cpSync,
  existsSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { agentdOrigins, hostedConfig } from './config.mjs';

/** The base path the console is built for (vite.config.ts `base`). */
export const BASE = '/ui/';
/** Tailscale MagicDNS names, where `tailscale serve` publishes the engine over HTTPS. */
export const TAILNET = 'https://*.ts.net';
/** The page every unknown path falls back to. */
export const ENTRY = 'cordis.html';

/** The `sha256-...` CSP sources of every inline `<script>` in a page. */
export function inlineScriptHashes(html) {
  const hashes = [];
  for (const m of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script\b[^>]*>/gi)) {
    if (/\bsrc\s*=/i.test(m[1] ?? '')) continue;
    const body = m[2] ?? '';
    hashes.push(`'sha256-${createHash('sha256').update(body, 'utf8').digest('base64')}'`);
  }
  return hashes;
}

/**
 * Where the hosted console may fetch: itself, the tailnet, the fixed engine, the issuer,
 * and the agentd hosts (LOAMS_AGENTD_ORIGINS, `agentdOrigins`).
 */
export function connectSources(config = {}, agentd = []) {
  const sources = ["'self'", TAILNET];
  for (const url of [config.engineUrl, config.authIssuer, ...agentd]) {
    if (!url) continue;
    const origin = new URL(url).origin;
    if (!sources.includes(origin)) sources.push(origin);
  }
  return sources;
}

/**
 * The console's Content-Security-Policy: no eval, no remote code, fetches only to
 * `connect`, frames only from itself and `frame` (the agentd hosts' previews).
 */
export function contentSecurityPolicy({
  scriptHashes = [],
  connect = ["'self'"],
  frame = [],
} = {}) {
  return [
    "default-src 'self'",
    // WebAssembly compiles (the code viewers' Shiki engine); JavaScript eval does not.
    `script-src ${["'self'", "'wasm-unsafe-eval'", ...scriptHashes].join(' ')}`,
    "style-src 'self' 'unsafe-inline'",
    "img-src 'self' data: blob:",
    "font-src 'self'",
    `connect-src ${connect.join(' ')}`,
    `frame-src ${["'self'", ...frame].join(' ')}`,
    "object-src 'none'",
    "base-uri 'none'",
    "form-action 'self'",
  ].join('; ');
}

/** Adds a CSP once; staging replaces the build's policy with its runtime policy. */
export function injectCsp(html, csp, replaceExisting = false) {
  const meta = `<meta http-equiv="Content-Security-Policy" content="${csp.replaceAll('"', '&quot;')}" />`;
  const existing = /<meta\b[^>]*http-equiv=["']Content-Security-Policy["'][^>]*>/i;
  if (existing.test(html)) return replaceExisting ? html.replace(existing, meta) : html;
  const charset = /<meta charset="utf-8"\s*\/?>/i;
  if (!charset.test(html)) throw new Error('index.html has no <meta charset="utf-8" />');
  return html.replace(charset, (m) => `${m}\n    ${meta}`);
}

/** Cloudflare `_headers` for every path; the CSP rides in each page's `<meta>`. */
export function headersFile() {
  return `/*
  X-Content-Type-Options: nosniff
  Referrer-Policy: strict-origin-when-cross-origin
  Cross-Origin-Opener-Policy: same-origin
  X-Frame-Options: SAMEORIGIN
  Permissions-Policy: camera=(), microphone=(), geolocation=(), payment=()
  Strict-Transport-Security: max-age=31536000; includeSubDomains

${BASE}sandbox/frame.html
  Content-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src data: blob:; connect-src 'none'; form-action 'none'; base-uri 'none'; sandbox allow-scripts

${BASE}assets/*
  Cache-Control: public, max-age=31536000, immutable

${BASE}config.json
  Cache-Control: no-store

/config.json
  Cache-Control: no-store
`;
}

/** Cloudflare `_redirects`: the engine-embedded console's entry has no place here. */
export function redirectsFile() {
  return `${BASE} / 302\n${BASE}index.html / 302\n`;
}

/** Fails when the build was made for another base path (the assets would 404 into the HTML fallback). */
export function checkBase(html) {
  const srcs = [...html.matchAll(/<script\b[^>]*\bsrc="([^"]+)"/gi)].map((m) => m[1] ?? '');
  if (srcs.length === 0) throw new Error('index.html loads no script');
  for (const src of srcs) {
    if (!src.startsWith(`${BASE}assets/`)) {
      throw new Error(
        `index.html loads ${src}, expected ${BASE}assets/...: rebuild with base ${BASE}`,
      );
    }
  }
}

/** vercel.json's static CSP must admit what this deploy's config fetches and frames. */
export function checkVercelCsp(vercelJson, connect, frame = []) {
  // The site-wide rule; the sandbox frame's own, stricter policy overrides it on one path.
  const policies = (vercelJson.headers ?? [])
    .filter((h) => h.source === '/(.*)')
    .flatMap((h) => h.headers ?? [])
    .filter((h) => h.key.toLowerCase() === 'content-security-policy')
    .map((h) => h.value);
  if (policies.length === 0) throw new Error('vercel.json sets no Content-Security-Policy');
  for (const policy of policies) {
    for (const [name, wanted] of [
      ['connect-src', connect],
      ['frame-src', frame],
    ]) {
      const directive = policy
        .split(';')
        .map((d) => d.trim())
        .find((d) => d.startsWith(`${name} `));
      const allowed = new Set(directive?.split(/\s+/).slice(1) ?? []);
      const missing = wanted.filter((s) => !allowed.has(s));
      if (missing.length > 0) {
        throw new Error(
          `vercel.json's Content-Security-Policy ${name} lacks ${missing.join(', ')}: add ` +
            'them there (Vercel headers are static), or the browser blocks those requests',
        );
      }
    }
  }
}

function removeSourceMaps(dir) {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) removeSourceMaps(path);
    else if (name.endsWith('.map')) rmSync(path);
  }
}

/**
 * Stages `dist` into `out` for `target` ('cloudflare' or 'vercel'), with the runtime
 * config from `env`. `vercelJson` is the parsed vercel.json (checked for the Vercel target).
 */
export function stage({
  dist,
  out,
  env = {},
  target = 'cloudflare',
  requireAuth = false,
  vercelJson,
}) {
  if (target !== 'cloudflare' && target !== 'vercel') throw new Error(`unknown target ${target}`);
  // Validate everything before touching `out`.
  const config = hostedConfig(env, { requireAuth });
  const agentd = agentdOrigins(env);
  const connect = connectSources(config, agentd);
  if (target === 'vercel') checkVercelCsp(vercelJson ?? {}, connect, agentd);
  const index = join(dist, 'index.html');
  if (!existsSync(index)) throw new Error(`${index} is missing: run the console build first`);
  checkBase(readFileSync(index, 'utf8'));
  if (!existsSync(join(dist, ENTRY))) throw new Error(`${join(dist, ENTRY)} is missing`);

  rmSync(out, { recursive: true, force: true });
  const ui = join(out, BASE.replaceAll('/', ''));
  cpSync(dist, ui, { recursive: true });

  const json = `${JSON.stringify(config, null, 2)}\n`;
  writeFileSync(join(ui, 'config.json'), json);
  writeFileSync(join(out, 'config.json'), json);

  for (const name of readdirSync(ui)) {
    if (!name.endsWith('.html')) continue;
    const file = join(ui, name);
    const html = readFileSync(file, 'utf8');
    const csp = contentSecurityPolicy({
      scriptHashes: inlineScriptHashes(html),
      connect,
      frame: agentd,
    });
    writeFileSync(file, injectCsp(html, csp, true));
  }
  cpSync(join(ui, ENTRY), join(out, 'index.html'));

  if (target === 'cloudflare') {
    writeFileSync(join(out, '_headers'), headersFile());
    writeFileSync(join(out, '_redirects'), redirectsFile());
    // Source maps stay out of the public deploy.
    writeFileSync(join(out, '.assetsignore'), '*.map\n');
  } else {
    removeSourceMaps(out);
  }
  return config;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
  const at = process.argv.indexOf('--target');
  const target = at > 0 ? process.argv[at + 1] : 'cloudflare';
  const config = stage({
    dist: join(root, 'dist'),
    out: join(root, `dist-${target}`),
    env: process.env,
    target,
    requireAuth: process.argv.includes('--require-auth'),
    vercelJson:
      target === 'vercel' ? JSON.parse(readFileSync(join(root, 'vercel.json'), 'utf8')) : undefined,
  });
  console.log(
    `staged dist-${target}/ (engine: ${config.engineUrl ?? 'chosen on first run'}, ` +
      `issuer: ${config.authIssuer ?? 'none, API key'})`,
  );
}
