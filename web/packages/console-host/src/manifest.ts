// A plugin's manifest: the `loams.plugin` block of its package.json
// (§37 §5.3, D423, AP1a Ruling 3). The JSON Schema of the block is
// `plugin-manifest.schema.json` beside this package; this validator is the
// one the host and the build run, and the tests keep the two in step.

import { isMode, type Mode } from './mode.js';
import { isPermission, type Permission } from './permissions.js';
import { CORE_PLUGINS } from './tiers.js';

export type Tier = 'core' | 'first-party' | 'third-party';
export type Edition = 'oss' | 'desktop' | 'cloud';

export interface PluginManifest {
  /** The package name. */
  package: string;
  version: string;
  kind: 'console';
  /** The ESM export that holds the plugin, for example "./client". */
  entry: string;
  /** What the package claims; the host decides the real tier (`tierOf`). */
  tier: Tier;
  inject: string[];
  provides: string[];
  slots: string[];
  /**
   * The action names this plugin may register in the `actions` registry
   * (§42 §5, D569). Like `slots`, it is a gate: registering an action the
   * manifest does not list fails.
   */
  actions: string[];
  permissions: Permission[];
  /**
   * `console`: a semver range of the console host. `api`: the
   * `loams.<area>.v<N>` packages the plugin needs the engine to serve.
   * `engine`: the oldest engine version it works with (§54 §13.1). A plugin
   * whose requirement fails does not run, so its pages and nav items are
   * not there.
   */
  requires: { console: string; api: string[]; engine?: string };
  editions: Edition[];
  /**
   * The console modes the plugin belongs to (§53 §4.1, D901); `["console"]` when
   * absent. Core plugins load in every mode whatever they declare.
   */
  modes: Mode[];
  config?: string;
  server?: { kind: 'function' | 'connector'; ref: string } | null;
}

export class ManifestError extends Error {
  override name = 'ManifestError';
}

/** Every key a `loams.plugin` block may hold (the schema's `properties`). */
export const MANIFEST_KEYS: ReadonlySet<string> = new Set([
  'kind',
  'entry',
  'tier',
  'inject',
  'provides',
  'slots',
  'actions',
  'permissions',
  'requires',
  'editions',
  'modes',
  'config',
  'server',
]);
const TIERS = new Set<string>(['core', 'first-party', 'third-party']);
const EDITIONS = new Set<string>(['oss', 'desktop', 'cloud']);

function strings(value: unknown, field: string, pkg: string): string[] {
  if (value === undefined) return [];
  if (!Array.isArray(value) || !value.every((v) => typeof v === 'string')) {
    throw new ManifestError(`${pkg}: "${field}" must be a list of strings`);
  }
  return value as string[];
}

/** The id a package's services and events are namespaced under. */
export function pluginId(pkg: string): string {
  return pkg.replace(/^@loams\/(plugin-|platform-|example-plugin-)?/, '').replace(/^@[^/]+\//, '');
}

/** Validates `package.json`'s `loams.plugin` block. */
export function validateManifest(pkg: unknown): PluginManifest {
  if (typeof pkg !== 'object' || pkg === null) throw new ManifestError('not a package.json');
  const p = pkg as Record<string, unknown>;
  const name = typeof p.name === 'string' ? p.name : '';
  if (!name) throw new ManifestError('package.json has no name');
  const loams = p.loams as Record<string, unknown> | undefined;
  const block = loams?.plugin as Record<string, unknown> | undefined;
  if (typeof block !== 'object' || block === null) {
    throw new ManifestError(`${name}: no "loams.plugin" block`);
  }
  for (const key of Object.keys(block)) {
    if (!MANIFEST_KEYS.has(key)) throw new ManifestError(`${name}: unknown manifest key "${key}"`);
  }
  if (block.kind !== 'console') throw new ManifestError(`${name}: "kind" must be "console"`);
  if (typeof block.entry !== 'string') throw new ManifestError(`${name}: "entry" is required`);
  if (typeof block.tier !== 'string' || !TIERS.has(block.tier)) {
    throw new ManifestError(`${name}: unknown tier "${String(block.tier)}"`);
  }
  const tier = block.tier as Tier;
  const id = pluginId(name);
  const provides = strings(block.provides, 'provides', name);
  // The claimed tier is untrusted: only the host's own core packages may
  // provide services outside their namespace.
  if (tier !== 'core' || !CORE_PLUGINS.has(name)) {
    for (const service of provides) {
      if (!service.startsWith(`${id}.`)) {
        throw new ManifestError(
          `${name}: provides "${service}" outside its namespace "${id}.*" (only core plugins may)`,
        );
      }
    }
  }
  // Action names are the intersection both consumers require (the WebMCP tool
  // name rule and MCP's own), so the host refuses a manifest that declares one
  // the console could not possibly expose.
  const actions = strings(block.actions, 'actions', name);
  for (const action of actions) {
    if (!/^[A-Za-z0-9_.-]{1,128}$/.test(action)) {
      throw new ManifestError(
        `${name}: "actions" entry "${action}" is not a usable action name (1-128 of [A-Za-z0-9_.-])`,
      );
    }
  }
  const permissions = strings(block.permissions, 'permissions', name);
  for (const permission of permissions) {
    if (!isPermission(permission)) {
      throw new ManifestError(`${name}: unknown permission "${permission}"`);
    }
  }
  const editions = strings(block.editions, 'editions', name);
  for (const edition of editions) {
    if (!EDITIONS.has(edition)) throw new ManifestError(`${name}: unknown edition "${edition}"`);
  }
  const modes = block.modes === undefined ? ['console'] : strings(block.modes, 'modes', name);
  if (modes.length === 0) throw new ManifestError(`${name}: "modes" needs at least one mode`);
  for (const [i, mode] of modes.entries()) {
    if (!isMode(mode)) throw new ManifestError(`${name}: unknown mode "${mode}"`);
    if (modes.indexOf(mode) !== i) throw new ManifestError(`${name}: mode "${mode}" listed twice`);
  }
  const requires = (block.requires ?? {}) as Record<string, unknown>;
  if (typeof requires !== 'object' || requires === null || Array.isArray(requires)) {
    throw new ManifestError(`${name}: "requires" must be an object`);
  }
  for (const key of Object.keys(requires)) {
    if (key !== 'console' && key !== 'api' && key !== 'engine') {
      throw new ManifestError(`${name}: unknown "requires" key "${key}"`);
    }
  }
  if (requires.console !== undefined && typeof requires.console !== 'string') {
    throw new ManifestError(`${name}: "requires.console" must be a semver range`);
  }
  const api = strings(requires.api, 'requires.api', name);
  for (const pkg of api) {
    if (!/^loams\.[a-z]+\.v[0-9]+$/.test(pkg)) {
      throw new ManifestError(
        `${name}: "requires.api" entry "${pkg}" is not a loams.<area>.v<N> package`,
      );
    }
  }
  if (
    requires.engine !== undefined &&
    (typeof requires.engine !== 'string' ||
      !/^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/.test(requires.engine))
  ) {
    throw new ManifestError(`${name}: "requires.engine" must be an engine version (0.1.0)`);
  }
  const server = block.server ?? null;
  if (server !== null) {
    const ok =
      typeof server === 'object' &&
      !Array.isArray(server) &&
      Object.keys(server).every((k) => k === 'kind' || k === 'ref') &&
      ((server as Record<string, unknown>).kind === 'function' ||
        (server as Record<string, unknown>).kind === 'connector') &&
      typeof (server as Record<string, unknown>).ref === 'string';
    if (!ok) {
      throw new ManifestError(
        `${name}: "server" must be null or {kind: "function" | "connector", ref}`,
      );
    }
  }
  return {
    package: name,
    version: typeof p.version === 'string' ? p.version : '0.0.0',
    kind: 'console',
    entry: block.entry,
    tier,
    inject: strings(block.inject, 'inject', name),
    provides,
    slots: strings(block.slots, 'slots', name),
    actions,
    permissions: permissions as Permission[],
    requires: {
      console: typeof requires.console === 'string' ? requires.console : '*',
      api,
      ...(typeof requires.engine === 'string' ? { engine: requires.engine } : {}),
    },
    editions: editions as Edition[],
    modes: modes as Mode[],
    ...(typeof block.config === 'string' ? { config: block.config } : {}),
    server: server as PluginManifest['server'],
  };
}
