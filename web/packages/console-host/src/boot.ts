// Booting the console (§37 §5.2, AP1a Task 1).
//
// The host is the only code that is not a plugin. It creates a cordis
// Context, provides `slots`, loads the platform plugin (`platform` and
// `transport`), reads GetInstance into `flags`, and loads every catalog row
// as a fiber: core plugins with the raw context, first-party plugins behind
// the guard proxy, third-party plugins as sandboxed frames (only when the
// instance allows them). Then it renders the `root` slot.
//
// Scaffold note (AP1a Rulings E1–E3): bundled plugins come from a module
// table of dynamic imports that Vite code-splits; per-plugin ESM bundles with
// an import map and SRI (Task 2) replace the table without changing this
// API.

import { createClient, type Transport } from '@connectrpc/connect';
import { Context, type Fiber, FiberStates } from '@loams/cordis';
import { instance } from '@loams/proto';
import { SlotRegistry } from '@loams/slots';
import { ActionRegistry, type Admission } from './actions.js';
import { type CatalogEntry, CatalogError } from './catalog.js';
import { guard, injectsAgentd } from './guard.js';
import { type PluginManifest, type Tier, validateManifest } from './manifest.js';
import {
  chooseBootMode,
  MODE_HOME,
  MODES,
  type Mode,
  ModeController,
  type ModeStorage,
  readStoredMode,
} from './mode.js';
import type { Permission } from './permissions.js';
import type { FlagsService, PlatformService } from './services.js';
import { type PluginSource, tierOf } from './tiers.js';

/** What a plugin module exports (its default export, or the module itself). */
export interface PluginModule<C = unknown> {
  name?: string;
  inject?: string[];
  apply(ctx: Context, config: C): unknown;
}

export type ModuleTable = Record<string, () => Promise<PluginModule | { default: PluginModule }>>;

/**
 * `deferred`: the plugin belongs to a mode that has not been entered yet; the first
 * switch into that mode applies it (§53 §4.2).
 */
export type PluginStatus =
  | 'active'
  | 'pending'
  | 'loading'
  | 'failed'
  | 'disabled'
  | 'skipped'
  | 'deferred';

export interface PluginRecord {
  entry: CatalogEntry;
  id: string;
  manifest?: PluginManifest;
  tier?: Tier;
  status: PluginStatus;
  reason?: string;
  fiber?: Fiber;
  /** Third-party only: the script the sandbox frame loads. */
  scriptUrl?: string;
  /** Third-party only: the permissions the bridge enforces. */
  granted?: Permission[];
}

export interface PendingReport {
  id: string;
  /** Injected services that are not available. */
  waitingFor: string[];
  /** Pending only on `rpc.*` services the instance does not serve (Ruling 6). */
  silent: boolean;
}

export interface BootOptions {
  catalog: CatalogEntry[];
  /** The package.json of every plugin the catalog may name. */
  manifests: unknown[];
  /** Bundled plugin code, by package name. */
  modules: ModuleTable;
  /** Provides `platform` and `transport` (@loams/platform-web, or another shell's). */
  platform: PluginModule;
  /** Where each package came from; default: bundled if in `modules`. */
  sources?: Record<string, PluginSource>;
  /** Third-party scripts by package name (paths on the console's origin). */
  sandboxScripts?: Record<string, string>;
  /** The sandbox frame document; default "sandbox/frame.html" beside the page. */
  frameUrl?: string;
  /** Provenance repositories counted as first-party (D428). */
  trustedPublishers?: string[];
  /** The permissions the user grants a third-party plugin; default: none. */
  grant?: (manifest: PluginManifest) => Permission[];
  /** Renders the `root` slot into this element. */
  root?: HTMLElement;
  /** Where the last-used mode is kept; default `localStorage` (§53 §4.2). */
  storage?: ModeStorage;
  /** The route at boot, which may name a mode; default: the window's hash route. */
  initialPath?: () => string;
  /** How the `mode` service changes route; default: the `router` service, else the hash. */
  navigate?: (to: string) => void;
  /**
   * A mode that needs an instance feature (GetInstance `features`) to be available,
   * e.g. `{ studio: 'studio' }` in the browser edition until SD1 Task 32.
   */
  modeFlags?: Partial<Record<Mode, string>>;
}

export interface ConsoleHandle {
  ctx: Context;
  slots: SlotRegistry;
  /** The callable surface plugins registered (the WebMCP tools' source). */
  actions: ActionRegistry;
  plugins(): PluginRecord[];
  /** Plugins still waiting for services (the all-fibers sweep). */
  pending(): PendingReport[];
  disable(id: string): Promise<void>;
  enable(id: string): Promise<void>;
  dispose(): Promise<void>;
}

/** The instance feature that turns sandboxed third-party plugins on (Ruling 9). */
export const THIRD_PARTY_FLAG = 'console.third_party_plugins';

function defaultStorage(): ModeStorage | undefined {
  try {
    return globalThis.localStorage;
  } catch {
    // Reading `localStorage` itself throws when site data is blocked.
    return undefined;
  }
}

function hashPath(): string {
  return globalThis.location?.hash.replace(/^#/, '') || '/';
}

function unwrap(mod: PluginModule | { default: PluginModule }): PluginModule | undefined {
  if (!mod || typeof mod !== 'object') return undefined;
  return 'apply' in mod ? mod : mod.default;
}

/** Reads GetInstance into the `flags` service (a core plugin). */
export const flagsPlugin: PluginModule = {
  name: 'flags',
  inject: ['transport'],
  async apply(ctx) {
    const transport = (ctx as unknown as { transport: Transport }).transport;
    const client = createClient(instance.InstanceService, transport);
    const unknown: FlagsService = {
      edition: 'unknown',
      instanceName: '',
      serverVersion: '',
      features: {},
      apiVersions: [],
      has: () => false,
    };
    /** The instance's flags, or undefined when GetInstance failed. */
    const read = async (): Promise<FlagsService | undefined> => {
      try {
        const info = await client.getInstance({});
        const editions = { 1: 'oss', 2: 'cloud', 3: 'byoc' } as const;
        return {
          edition: editions[info.edition as 1 | 2 | 3] ?? 'unknown',
          instanceName: info.name,
          serverVersion: info.serverVersion,
          features: { ...info.features },
          apiVersions: [...info.apiVersions],
          setupRequired: info.setupRequired,
          has: (api) => info.apiVersions.includes(api),
        };
      } catch (error) {
        console.warn('loams console: GetInstance failed; no API is available', error);
        return undefined;
      }
    };
    let release = ctx.provide('flags', (await read()) ?? unknown);
    let disposed = false;
    ctx.effect(() => () => {
      disposed = true;
    });
    // A platform may say the instance changed (the desktop engine came up): re-read,
    // and re-provide only when the answer differs, so dependent plugins restart once.
    // A failed re-read keeps the current flags, and a read that lands after this
    // plugin was disposed provides nothing.
    const platform = ctx.get('platform', true) as PlatformService | undefined;
    const off = platform?.onInstanceStale?.(() => {
      void read().then((next) => {
        if (!next || disposed) return;
        const cur = ctx.get('flags', true) as FlagsService | undefined;
        if (cur && JSON.stringify(cur.apiVersions) === JSON.stringify(next.apiVersions)) return;
        // Dropping the service restarts the plugins that inject it; they read the new one.
        release();
        release = ctx.provide('flags', next);
      });
    });
    if (off) ctx.effect(() => off);
  },
};

export async function boot(options: BootOptions): Promise<ConsoleHandle> {
  const ctx = new Context();
  const slots = new SlotRegistry();
  const records = new Map<string, PluginRecord>();
  const manifests = new Map<string, PluginManifest>();
  for (const pkg of options.manifests) {
    const manifest = validateManifest(pkg);
    manifests.set(manifest.package, manifest);
  }

  await ctx.plugin({
    name: 'slots',
    apply(c: Context) {
      c.provide('slots', slots);
    },
  });

  // The `actions` service (D569). Its admissions come from the manifests, so a
  // plugin can register only the actions it declared and only call them with
  // the permissions its manifest lists. A third-party row's permissions are the
  // granted subset, the same expression the sandbox bridge uses, computed here
  // rather than off `records` because the service has to exist before any
  // plugin loads.
  const admissions: Record<string, Admission> = {};
  for (const entry of options.catalog) {
    const manifest = manifests.get(entry.name);
    if (!manifest) continue;
    const tier = tierOf(
      manifest,
      options.sources?.[entry.name] ?? {
        bundled: entry.name in options.modules,
      },
      options.trustedPublishers,
    );
    const permissions =
      tier === 'third-party'
        ? (options.grant?.(manifest) ?? []).filter((p) => manifest.permissions.includes(p))
        : manifest.permissions;
    admissions[entry.id] = {
      actions: manifest.actions,
      // The service list admission checks must be the one the plugin fiber
      // actually gets, not the manifest's wider one. A catalog row may narrow
      // `inject` (it can never widen it — see `catalog.ts`), so admitting
      // against `manifest.inject` would authorise a method-backed action for
      // a service this plugin was deliberately denied: the tool would be
      // strictly more capable than the UI beside it, which is the one thing
      // D569's "exactly as the UI would" forbids.
      policy: { services: entry.inject ?? manifest.inject, permissions },
    };
  }
  const actions = new ActionRegistry(admissions);
  await ctx.plugin({
    name: 'actions',
    apply(c: Context) {
      c.provide('actions', actions);
    },
  });
  await ctx.plugin({ name: options.platform.name ?? 'platform', apply: options.platform.apply });
  ctx.plugin({ name: 'flags', inject: flagsPlugin.inject, apply: flagsPlugin.apply });

  // The `mode` service (§53 §4.2, D900–D902). `available` comes from the catalog's
  // manifests; a mode behind an instance flag waits for GetInstance's answer.
  const modeFlags = Object.entries(options.modeFlags ?? {}) as [Mode, string][];
  const flags =
    modeFlags.length > 0
      ? await new Promise<FlagsService>((resolve) => {
          ctx.plugin({
            name: 'mode-flags',
            inject: ['flags'],
            apply: (c: Context) => resolve((c as unknown as { flags: FlagsService }).flags),
          });
        })
      : undefined;
  const modesById = new Map<string, readonly Mode[]>();
  for (const entry of options.catalog) {
    const manifest = manifests.get(entry.name);
    if (manifest && !entry.disabled) modesById.set(entry.id, manifest.modes);
  }
  const declared = new Set([...modesById.values()].flat());
  const available = MODES.filter((m) => {
    if (m !== 'console' && !declared.has(m)) return false;
    const flag = modeFlags.find(([mode]) => mode === m)?.[1];
    return !flag || flags?.features[flag] === true;
  });
  const storage = 'storage' in options ? options.storage : defaultStorage();
  const path = (options.initialPath ?? hashPath)();
  const platformKind = (ctx.get('platform', true) as PlatformService | undefined)?.kind ?? 'web';
  const bootMode = chooseBootMode({
    path,
    stored: readStoredMode(storage),
    kind: platformKind,
    available,
  });
  const navigate =
    options.navigate ??
    ((to: string) => {
      const router = ctx.get('router', true) as { navigate(to: string): void } | undefined;
      if (router) router.navigate(to);
      else globalThis.location.hash = to;
    });
  const deferred: PluginRecord[] = [];
  const mode = new ModeController({
    current: bootMode,
    available,
    modes: modesById,
    storage,
    navigate,
    enter: (m) => {
      for (const record of deferred.filter((r) => r.manifest?.modes.includes(m))) {
        deferred.splice(deferred.indexOf(record), 1);
        record.reason = undefined;
        void load(record);
      }
    },
  });
  await ctx.plugin({
    name: 'mode',
    apply(c: Context) {
      c.provide('mode', mode);
    },
  });
  // A mode whose home is not the shared root opens at its home (the desktop's Studio).
  if ((path.split(/[?#]/, 1)[0] || '/') === '/' && MODE_HOME[bootMode] !== '/') {
    navigate(MODE_HOME[bootMode]);
  }

  const thirdParty: PluginRecord[] = [];

  const load = async (record: PluginRecord) => {
    const { entry, manifest, tier } = record;
    if (!manifest || !tier) return;
    const loader = options.modules[entry.name];
    if (!loader) {
      record.status = 'failed';
      record.reason = `no bundled module for ${entry.name}`;
      return;
    }
    let mod: PluginModule | undefined;
    try {
      mod = unwrap(await loader());
    } catch (error) {
      // A chunk that fails to load or throws at evaluation fails its row only.
      record.status = 'failed';
      record.reason = `could not load ${entry.name}: ${error instanceof Error ? error.message : String(error)}`;
      return;
    }
    if (typeof mod?.apply !== 'function') {
      record.status = 'failed';
      record.reason = `${entry.name} exports no plugin (no apply)`;
      return;
    }
    const plugin = mod;
    const inject = entry.inject ?? manifest.inject;
    record.status = 'loading';
    record.fiber = ctx.plugin(
      {
        name: entry.id,
        inject,
        apply: (c: Context, config: unknown) =>
          plugin.apply(tier === 'core' ? c : guard(c, inject, record.id), config),
      },
      entry.config ?? {},
    );
  };

  for (const entry of options.catalog) {
    const manifest = manifests.get(entry.name);
    const record: PluginRecord = { entry, id: entry.id, manifest, status: 'pending' };
    records.set(entry.id, record);
    if (!manifest) {
      record.status = 'failed';
      record.reason = `no manifest for ${entry.name}`;
      continue;
    }
    const source = options.sources?.[entry.name] ?? {
      bundled: entry.name in options.modules,
    };
    record.tier = tierOf(manifest, source, options.trustedPublishers);
    const widened = (entry.inject ?? []).filter((s) => !manifest.inject.includes(s));
    if (widened.length > 0) {
      record.status = 'failed';
      record.reason = new CatalogError(
        `catalog row ${entry.id} injects ${widened.join(', ')} beyond its manifest`,
      ).message;
      continue;
    }
    if (entry.disabled) {
      record.status = 'disabled';
      continue;
    }
    if (record.tier === 'third-party') {
      const agentd = injectsAgentd(entry.inject ?? manifest.inject);
      if (agentd.length > 0) {
        record.status = 'failed';
        record.reason = `third-party plugins never get ${agentd.join(', ')}`;
        continue;
      }
      record.scriptUrl = options.sandboxScripts?.[entry.name];
      record.granted = (options.grant?.(manifest) ?? []).filter((p) =>
        manifest.permissions.includes(p),
      );
      record.status = 'skipped';
      record.reason = `third-party plugins need the "${THIRD_PARTY_FLAG}" instance feature`;
      thirdParty.push(record);
      continue;
    }
    // Core plugins load in every mode; others wait for their mode's first entry.
    if (record.tier !== 'core' && !manifest.modes.includes(bootMode)) {
      record.status = 'deferred';
      record.reason = `loads when ${manifest.modes.join(' or ')} mode is entered`;
      deferred.push(record);
      continue;
    }
    await load(record);
  }

  if (thirdParty.length > 0) {
    const { sandboxHostPlugin } = await import('./sandbox-host.js');
    ctx.plugin(sandboxHostPlugin, {
      records: thirdParty,
      frameUrl: options.frameUrl ?? 'sandbox/frame.html',
      resolve: (name: string) => ctx.get(name, true),
    });
  }

  let reactRoot: { unmount(): void } | undefined;
  if (options.root) {
    const { renderRoot } = await import('./root.js');
    // The shared server-state cache (console-ui-stack decision 4) runs over the console's transport.
    const transport = ctx.get('transport', true) as Transport | undefined;
    reactRoot = renderRoot(options.root, slots, () => handle, transport);
  }

  const statusOf = (record: PluginRecord): PluginStatus => {
    const state = record.fiber?.state;
    if (state === undefined) return record.status;
    if (state === FiberStates.active) return 'active';
    if (state === FiberStates.failed) return 'failed';
    if (state === FiberStates.disposed) return 'disabled';
    if (state === FiberStates.loading) return 'loading';
    return 'pending';
  };

  const handle: ConsoleHandle = {
    ctx,
    slots,
    actions,
    plugins: () => [...records.values()].map((r) => ({ ...r, status: statusOf(r) })),
    pending: () =>
      [...records.values()]
        .filter((r) => statusOf(r) === 'pending')
        .map((r) => {
          const inject = r.entry.inject ?? r.manifest?.inject ?? [];
          const waitingFor = inject.filter((name) => ctx.get(name, true) === undefined);
          return {
            id: r.id,
            waitingFor,
            silent: waitingFor.length > 0 && waitingFor.every((n) => n.startsWith('rpc.')),
          };
        }),
    async disable(id) {
      const record = records.get(id);
      if (!record?.fiber) return;
      await record.fiber.dispose();
      record.fiber = undefined;
      record.status = 'disabled';
    },
    async enable(id) {
      const record = records.get(id);
      // Only a disabled row can be enabled: a row that failed a boot-time
      // check (for example an inject list wider than its manifest) stays off.
      if (
        !record ||
        record.fiber ||
        record.status !== 'disabled' ||
        record.tier === 'third-party'
      ) {
        return;
      }
      record.entry = { ...record.entry, disabled: false };
      await load(record);
    },
    async dispose() {
      reactRoot?.unmount();
      for (const record of [...records.values()].reverse()) {
        await record.fiber?.dispose();
      }
      for (const runtime of [...ctx.registry.values()]) {
        for (const fiber of [...runtime.fibers]) await fiber.dispose();
      }
    },
  };
  return handle;
}
