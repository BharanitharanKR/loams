// The guard proxy for first-party plugins (§37 §5.6): `ctx` exposes only
// the services in the plugin's `inject` list plus the lifecycle API. This is
// hygiene, not a security boundary (first-party code runs in the console's
// realm); third-party code runs in a sandboxed frame instead.
//
// cordis already refuses `ctx.<service>` for a service that was not
// injected; the guard also closes the side doors: `ctx.get(name)`, the
// registry, `ctx.plugin` (a sub-plugin could inject anything), `ctx.root`.

import type { Context } from '@loams/cordis';
import { gateGitWrites, isAgentdService } from './agentd.js';

/** The cordis API a guarded plugin may use besides its services. */
const LIFECYCLE = new Set(['effect', 'on', 'fiber', 'logger', 'emit']);

export class GuardError extends Error {
  override name = 'GuardError';
}

/**
 * The agentd injection rule (§53 §8.3, D918; SD1 Tasks 6, 15): `agentd`,
 * `agentd.*`, `rpc.agentd.*` and the desktop bridge reach agents, git and files on
 * the person's machine, so only a core or first-party plugin may inject them (and
 * a first-party one only with `agentd:use`, except for `desktop`). Returns the
 * offending services.
 */
export function injectsAgentd(inject: readonly string[]): string[] {
  return inject.filter((s) => s === 'desktop' || isAgentdService(s));
}

/** What the guard needs to know about the plugin besides its `inject` list. */
export interface GuardPolicy {
  /** The plugin's permissions; without `agentd:git-write` its agentd services are gated. */
  permissions?: readonly string[];
}

export function guard(
  ctx: Context,
  inject: readonly string[],
  pluginId: string,
  policy: GuardPolicy = {},
): Context {
  const allowed = new Set(inject);
  const mayWriteGit = policy.permissions?.includes('agentd:git-write') ?? false;
  return new Proxy(ctx, {
    get(target, prop, receiver) {
      if (typeof prop === 'symbol') return Reflect.get(target, prop, receiver);
      if (prop === 'provide') {
        // Non-core plugins provide only `<plugin-id>.*` services (§37 §5.3).
        return (name: string, ...args: unknown[]) => {
          if (!name.startsWith(`${pluginId}.`)) {
            throw new GuardError(`${pluginId} may only provide "${pluginId}.*" services`);
          }
          return (target.provide as (n: string, ...a: unknown[]) => unknown)(name, ...args);
        };
      }
      if (prop === 'emit') {
        // Plugins emit only their own events, `<plugin-id>/...`.
        return (name: string, ...args: unknown[]) => {
          if (!name.startsWith(`${pluginId}/`)) {
            throw new GuardError(`${pluginId} may only emit "${pluginId}/..." events`);
          }
          return (target.emit as (n: string, ...a: unknown[]) => unknown)(name, ...args);
        };
      }
      if (LIFECYCLE.has(prop)) {
        const value = Reflect.get(target, prop, target);
        return typeof value === 'function' ? value.bind(target) : value;
      }
      if (allowed.has(prop)) {
        const value = Reflect.get(target, prop, target);
        // SD1 Task 6 (ruling T6-7): writing to git needs `agentd:git-write`.
        return !mayWriteGit && isAgentdService(prop) ? gateGitWrites(prop, value) : value;
      }
      throw new GuardError(`${pluginId} did not inject "${prop}"`);
    },
    set(_target, prop) {
      throw new GuardError(`${pluginId} cannot set ctx.${String(prop)}`);
    },
  });
}
