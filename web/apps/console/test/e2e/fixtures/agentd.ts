// A real loams-agentd for Studio's end-to-end tests (SD1 Task 29, ruling T8-3).
//
// Each daemon gets a fresh temporary root: its own HOME (with a git identity and
// nothing else), its own daemon directory, worktree root and fixture repository,
// so a run never reads or writes the developer's home directory. The daemon runs
// the Mock harness (`LOAMS_DESKTOP_HARNESS=mock`) with the Task 8 knobs, so a
// turn makes a deterministic edit (`LOAMS_DESKTOP_MOCK_EDIT`) and, when asked,
// waits for an approval (`LOAMS_DESKTOP_MOCK_APPROVE`).
//
// Two ways to reach it:
// - `remote` (the web edition): `serve --remote` on loopback TLS with a
//   self-signed certificate (made with openssl) and the debug-build test issuer
//   (`LOAMS_AGENTD_TEST_ISSUER`, ruling T7-7): a local JWKS file whose Ed25519 key
//   signs the access token the test pastes into Settings › Agentd hosts.
// - `local` (the desktop edition): `run --child` in `<userData>/agentd`, where
//   Electron main finds `agentd.json` and `agentd.token` (T5-5).
//
// The binary is `LOAMS_AGENTD_BIN`, else the workspace's debug build
// (`cargo build -p loams-agentd`). A debug build is required: a release daemon
// ignores the test issuer. Nothing here calls a service outside this machine.

import { type ChildProcess, execFileSync, spawn } from 'node:child_process';
import { generateKeyPairSync, type KeyObject, sign } from 'node:crypto';
import {
  type Dirent,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { createConnection, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

/** The repository root: the nearest directory up from the working directory with pnpm-workspace.yaml. */
export const REPO_ROOT = (() => {
  let d = process.cwd();
  while (!existsSync(join(d, 'pnpm-workspace.yaml'))) {
    const up = dirname(d);
    if (up === d) return process.cwd();
    d = up;
  }
  return d;
})();

/** The audience and subject the test issuer's tokens carry. */
export const AUDIENCE = 'loams-agentd';
export const SUBJECT = 'studio-e2e';
/** A loopback issuer URL: never fetched, because the JWKS comes from a file. */
export const ISSUER = 'http://127.0.0.1:9/application/o/loams-e2e/';

export interface AgentdOptions {
  /** `remote`: `serve --remote` (web edition); `local`: `run --child` (desktop). */
  mode: 'remote' | 'local';
  /** The console's origin, for `--allow-origin` (remote only). */
  allowOrigin?: string;
  /** The daemon directory (local mode: `<userData>/agentd`); default: under the root. */
  dir?: string;
  /** The root to use (default: a new temporary directory, removed by `stop`). */
  root?: string;
  /** Paths the Mock edits on each turn, relative to the run's cwd. */
  mockEdit?: string[];
  /** Ask before each Mock edit (`LOAMS_DESKTOP_MOCK_APPROVE`). */
  mockApprove?: boolean;
  /** Extra environment for the daemon (tests only). */
  env?: Record<string, string>;
}

export interface Agentd {
  readonly mode: 'remote' | 'local';
  /** The temporary root (HOME, the fixture repository, the worktrees). */
  readonly root: string;
  /** The isolated HOME the daemon runs with. */
  readonly home: string;
  /** The daemon directory (`config.toml`, `agentd.json`, `agentd.token`, `store/`). */
  readonly dir: string;
  /** A git repository with one commit on `main`: the project the tests add. */
  readonly repo: string;
  /** The remote listener's URL (`https://127.0.0.1:<port>`); remote only. */
  readonly url?: string;
  /** A valid access token for the remote listener; remote only. */
  readonly token?: string;
  /** The loopback Connect listener's port (from `agentd.json`). */
  readonly connectPort: number;
  /** The daemon's pid. */
  readonly pid: number;
  /** Signs another token, e.g. with a wrong audience or subject (remote only). */
  signToken(claims?: Record<string, unknown>): string;
  /** A unary Connect JSON call (`ProjectService/AddRepo`), over the remote listener or loopback. */
  call<T = Record<string, unknown>>(path: string, body?: unknown, token?: string): Promise<T>;
  /** As `call`, answering the status and body without throwing. */
  raw(
    path: string,
    body?: unknown,
    token?: string | null,
  ): Promise<{ status: number; body: string }>;
  /** `git` in the fixture repository (or `cwd`), answering stdout. */
  git(args: string[], cwd?: string): string;
  /** Every file called `name` in a thread worktree the daemon made. */
  worktreeFiles(name: string): string[];
  /** Whether the daemon process is still running. */
  alive(): boolean;
  /** The daemon's log so far (stdout and stderr). */
  log(): string;
  /** Stops the daemon and removes the root (unless it was given). */
  stop(): Promise<void>;
}

/** The loams-agentd to run: LOAMS_AGENTD_BIN, else the workspace's debug build. */
export function agentdBinary(): string {
  const exe = process.platform === 'win32' ? 'loams-agentd.exe' : 'loams-agentd';
  const explicit = process.env.LOAMS_AGENTD_BIN;
  if (explicit) {
    if (!existsSync(explicit)) throw new Error(`LOAMS_AGENTD_BIN is set but missing: ${explicit}`);
    return explicit;
  }
  let target = process.env.CARGO_TARGET_DIR;
  if (!target) {
    try {
      const meta = JSON.parse(
        execFileSync('cargo', ['metadata', '--format-version', '1', '--no-deps'], {
          cwd: REPO_ROOT,
          encoding: 'utf8',
          maxBuffer: 64 * 1024 * 1024,
        }),
      ) as { target_directory: string };
      target = meta.target_directory;
    } catch {
      target = join(REPO_ROOT, 'target');
    }
  }
  const bin = join(target, 'debug', exe);
  if (!existsSync(bin))
    throw new Error(
      `no debug loams-agentd at ${bin}: run \`cargo build -p loams-agentd\` or set LOAMS_AGENTD_BIN`,
    );
  return bin;
}

/** A free TCP port on 127.0.0.1. */
export function freePort(): Promise<number> {
  return new Promise((ok, fail) => {
    const server = createServer();
    server.once('error', fail);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      const port = typeof address === 'object' && address ? address.port : 0;
      server.close(() => ok(port));
    });
  });
}

function connectable(port: number): Promise<boolean> {
  return new Promise((ok) => {
    const socket = createConnection({ host: '127.0.0.1', port });
    socket.once('connect', () => {
      socket.destroy();
      ok(true);
    });
    socket.once('error', () => ok(false));
  });
}

async function until<T>(
  what: string,
  fn: () => T | undefined | Promise<T | undefined>,
  ms: number,
) {
  const t0 = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined && value !== false) return value as T;
    if (Date.now() - t0 > ms) throw new Error(`timed out after ${ms} ms waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

/** Files named `name` under `dir`, skipping `.git`. */
function findFiles(dir: string, name: string): string[] {
  const found: string[] = [];
  const walk = (d: string) => {
    let entries: Dirent[];
    try {
      entries = readdirSync(d, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      if (e.name === '.git') continue;
      const path = join(d, e.name);
      if (e.isDirectory()) walk(path);
      else if (e.name === name) found.push(path);
    }
  };
  walk(dir);
  return found.sort();
}

const b64url = (data: Buffer | string) => Buffer.from(data).toString('base64url');

/** An EdDSA JWT signed with `key` under `kid`. */
function jwt(key: KeyObject, kid: string, claims: Record<string, unknown>): string {
  const header = b64url(JSON.stringify({ alg: 'EdDSA', typ: 'JWT', kid }));
  const payload = b64url(JSON.stringify(claims));
  const signature = sign(null, Buffer.from(`${header}.${payload}`), key);
  return `${header}.${payload}.${b64url(signature)}`;
}

/**
 * The daemon's PATH: the system directories only, so no agent CLI the developer has
 * installed (claude, codex, opencode, …) counts as an installed harness, and the
 * composer starts runs on the daemon's default, the Mock (which can never be enabled
 * from Settings, so it is never offered by name). LOAMS_E2E_AGENTD_PATH overrides it.
 */
const SYSTEM_PATH =
  process.env.LOAMS_E2E_AGENTD_PATH ??
  '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin';

/** The environment the daemon and git run with: nothing of the caller's. */
function isolatedEnv(home: string, extra: Record<string, string>): Record<string, string> {
  return {
    PATH: SYSTEM_PATH,
    HOME: home,
    USER: 'studio-e2e',
    LANG: 'C.UTF-8',
    XDG_CONFIG_HOME: join(home, '.config'),
    XDG_DATA_HOME: join(home, '.local/share'),
    XDG_STATE_HOME: join(home, '.local/state'),
    XDG_CACHE_HOME: join(home, '.cache'),
    SHELL: '/bin/sh',
    TERM: 'xterm-256color',
    GIT_CONFIG_NOSYSTEM: '1',
    GIT_TERMINAL_PROMPT: '0',
    ...extra,
  };
}

function gitIn(cwd: string, env: Record<string, string>, args: string[]): string {
  return execFileSync('git', args, { cwd, env, encoding: 'utf8' });
}

/** The fixture project: a README and a source file, one commit on `main`. */
function makeRepo(path: string, env: Record<string, string>): void {
  mkdirSync(join(path, 'src'), { recursive: true });
  writeFileSync(join(path, 'README.md'), '# Fixture\n\nA project for Studio end-to-end tests.\n');
  writeFileSync(join(path, 'src', 'main.ts'), 'export const answer = 42;\n');
  writeFileSync(join(path, 'notes.txt'), 'first line\n');
  gitIn(path, env, ['init', '-q', '-b', 'main']);
  gitIn(path, env, ['add', '-A']);
  gitIn(path, env, ['commit', '-q', '-m', 'Initial commit']);
}

export async function startAgentd(options: AgentdOptions): Promise<Agentd> {
  const bin = agentdBinary();
  const given = options.root !== undefined;
  const root = options.root ?? mkdtempSync(join(tmpdir(), 'loams-studio-e2e-'));
  const home = join(root, 'home');
  const dir = options.dir ?? join(root, 'data', 'agentd');
  const repo = join(root, 'repos', 'fixture');
  const worktrees = join(root, 'worktrees');
  for (const d of [home, join(home, '.config'), repo, worktrees, dir])
    mkdirSync(d, { recursive: true });
  // A git identity for the commits the daemon makes; the isolated HOME has nothing else.
  writeFileSync(
    join(home, '.gitconfig'),
    '[user]\n\tname = Studio E2E\n\temail = studio-e2e@example.invalid\n[init]\n\tdefaultBranch = main\n[commit]\n\tgpgsign = false\n',
  );
  const gitEnv = isolatedEnv(home, {
    GIT_AUTHOR_DATE: '2026-10-01T12:00:00Z',
    GIT_COMMITTER_DATE: '2026-10-01T12:00:00Z',
  });
  makeRepo(repo, gitEnv);
  const config = join(dir, 'config.toml');
  if (!existsSync(config)) writeFileSync(config, '');

  const env = isolatedEnv(home, {
    LOAMS_DESKTOP_HARNESS: 'mock',
    LOAMS_DESKTOP_WORKTREES_DIR: worktrees,
    LOAMS_DESKTOP_NO_LOGIN_SHELL: '1',
    LOAMS_DESKTOP_DEVICE_NAME: 'studio-e2e',
    LOAMS_DESKTOP_MOCK_EDIT: (options.mockEdit ?? ['notes.txt']).join(','),
    ...(options.mockApprove ? { LOAMS_DESKTOP_MOCK_APPROVE: '1' } : {}),
    RUST_LOG: process.env.RUST_LOG ?? 'warn',
    ...options.env,
  });

  let url: string | undefined;
  let token: string | undefined;
  let certPem: string | undefined;
  let signToken: (claims?: Record<string, unknown>) => string = () => {
    throw new Error('tokens exist in remote mode only');
  };
  const args: string[] = [];
  if (options.mode === 'remote') {
    const tls = join(root, 'tls');
    mkdirSync(tls);
    // A self-signed P-256 certificate for 127.0.0.1 (rustls takes PKCS#8).
    execFileSync(
      'openssl',
      [
        'req',
        '-x509',
        '-newkey',
        'ec',
        '-pkeyopt',
        'ec_paramgen_curve:prime256v1',
        '-nodes',
        '-keyout',
        join(tls, 'key.pem'),
        '-out',
        join(tls, 'cert.pem'),
        '-days',
        '2',
        '-subj',
        '/CN=127.0.0.1',
        '-addext',
        'subjectAltName=IP:127.0.0.1,DNS:localhost',
      ],
      { stdio: 'ignore' },
    );
    certPem = readFileSync(join(tls, 'cert.pem'), 'utf8');
    // The test issuer: one Ed25519 key in a local JWKS file.
    const { privateKey, publicKey } = generateKeyPairSync('ed25519');
    const kid = 'studio-e2e-1';
    const jwk = publicKey.export({ format: 'jwk' });
    const jwks = join(tls, 'jwks.json');
    writeFileSync(jwks, JSON.stringify({ keys: [{ ...jwk, kid, alg: 'EdDSA', use: 'sig' }] }));
    env.LOAMS_AGENTD_TEST_ISSUER = jwks;
    signToken = (claims = {}) => {
      const now = Math.floor(Date.now() / 1000);
      return jwt(privateKey, kid, {
        iss: ISSUER,
        aud: AUDIENCE,
        sub: SUBJECT,
        iat: now,
        nbf: now - 5,
        exp: now + 3600,
        ...claims,
      });
    };
    token = signToken();
    const port = await freePort();
    url = `https://127.0.0.1:${port}`;
    args.push(
      'serve',
      '--remote',
      '--listen',
      `127.0.0.1:${port}`,
      '--tls-cert',
      join(tls, 'cert.pem'),
      '--tls-key',
      join(tls, 'key.pem'),
      '--issuer',
      ISSUER,
      '--audience',
      AUDIENCE,
      '--subject',
      SUBJECT,
      ...(options.allowOrigin ? ['--allow-origin', options.allowOrigin] : []),
      '--config',
      config,
    );
  } else {
    args.push('run', '--child', '--config', config);
  }

  let output = '';
  const child: ChildProcess = spawn(bin, args, {
    env,
    cwd: root,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout?.on('data', (d) => {
    output += String(d);
  });
  child.stderr?.on('data', (d) => {
    output += String(d);
  });
  let exited: number | null | undefined;
  child.once('exit', (code) => {
    exited = code;
  });
  const alive = () => exited === undefined;

  const stop = async () => {
    if (alive()) {
      child.kill('SIGTERM');
      const gone = await until('the daemon to exit', () => !alive() || undefined, 10_000).catch(
        () => false,
      );
      if (!gone && alive()) child.kill('SIGKILL');
    }
    if (!given) rmSync(root, { recursive: true, force: true });
  };

  let connectPort = 0;
  try {
    const discovery = join(dir, 'agentd.json');
    connectPort = await until(
      'agentd.json',
      () => {
        if (!alive()) throw new Error(`loams-agentd exited (${exited}):\n${output}`);
        if (!existsSync(discovery)) return undefined;
        try {
          const json = JSON.parse(readFileSync(discovery, 'utf8')) as { connectPort?: number };
          return json.connectPort;
        } catch {
          return undefined;
        }
      },
      30_000,
    );
    await until('the loopback listener', () => connectable(connectPort), 15_000);
    if (url) {
      const port = Number(new URL(url).port);
      await until('the remote listener', () => connectable(port), 15_000);
    }
  } catch (e) {
    await stop();
    throw e;
  }

  const raw = (path: string, body: unknown = {}, bearer?: string | null) =>
    new Promise<{ status: number; body: string }>((ok, fail) => {
      const payload = Buffer.from(JSON.stringify(body));
      const headers: Record<string, string | number> = {
        'content-type': 'application/json',
        'connect-protocol-version': '1',
        'content-length': payload.length,
      };
      let req: ReturnType<typeof httpRequest>;
      if (url) {
        const auth = bearer === null ? undefined : (bearer ?? token);
        if (auth) headers.authorization = `Bearer ${auth}`;
        const u = new URL(url);
        req = httpsRequest({
          host: u.hostname,
          port: Number(u.port),
          method: 'POST',
          path,
          headers,
          ca: certPem,
        });
      } else {
        const owner =
          bearer === null
            ? undefined
            : (bearer ?? readFileSync(join(dir, 'agentd.token'), 'utf8').trim());
        if (owner) headers.authorization = `Bearer ${owner}`;
        headers.host = `127.0.0.1:${connectPort}`;
        req = httpRequest({ host: '127.0.0.1', port: connectPort, method: 'POST', path, headers });
      }
      req.on('response', (res) => {
        let text = '';
        res.setEncoding('utf8');
        res.on('data', (d) => {
          text += d;
        });
        res.on('end', () => ok({ status: res.statusCode ?? 0, body: text }));
      });
      req.on('error', fail);
      req.end(payload);
    });

  return {
    mode: options.mode,
    root,
    home,
    dir,
    repo,
    ...(url ? { url } : {}),
    ...(token ? { token } : {}),
    connectPort,
    pid: child.pid ?? 0,
    signToken,
    raw,
    async call<T>(path: string, body?: unknown, bearer?: string) {
      const res = await raw(path, body, bearer);
      if (res.status !== 200) throw new Error(`${path}: ${res.status} ${res.body}`);
      return JSON.parse(res.body || '{}') as T;
    },
    git: (gitArgs, cwd = repo) => gitIn(cwd, gitEnv, gitArgs),
    worktreeFiles: (name) => findFiles(worktrees, name),
    alive,
    log: () => output,
    stop,
  };
}
