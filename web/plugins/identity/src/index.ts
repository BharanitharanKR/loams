// @loams/plugin-identity: the `session` service (§37 §5.4).
//
// Who is signed in comes from two places. `InstanceService/WhoAmI` names the
// principal of a bearer token (an app, an agent, the desktop) and its
// environments. `AuthService/GetSession` (loams.auth.v1, API2 Task 6) is the
// console's own cookie session: the person, their org and role, and the CSRF
// token every mutating call carries. Where the instance serves loams.auth.v1,
// the session also says where sign-in stands (`gate`), which the shell uses to
// send a signed-out visitor to sign-in and a fresh install to setup, as the
// original console's session gate did. The sign-in, setup and consent pages
// are @loams/plugin-account's.

import { create } from '@bufbuild/protobuf';
import { timestampDate } from '@bufbuild/protobuf/wkt';
import { Code, ConnectError, createClient, type Transport } from '@connectrpc/connect';
import {
  type ConsoleAccount,
  type PluginModule,
  type SessionGate,
  type SessionService,
  service,
} from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { auth, instance } from '@loams/proto';
import type { EnvironmentRef } from '@loams/slots';

/** Where sign-in stands, and the console session when there is one. */
export interface AccountState {
  gate: SessionGate;
  account?: ConsoleAccount;
}

/** The console session's lifecycle, when the instance serves loams.auth.v1. */
export interface AccountControl {
  initial: AccountState;
  reload(): Promise<AccountState>;
  end(): Promise<void>;
}

/** Reads the console session: `setup` before first-run setup, `signed-out` without a cookie. */
export async function loadAccount(
  client: Pick<ReturnType<typeof createClient<typeof auth.AuthService>>, 'getSession'>,
  setupRequired: boolean,
): Promise<AccountState> {
  if (setupRequired) return { gate: 'setup' };
  try {
    const res = await client.getSession({});
    return res.session ? { gate: 'ready', account: res.session } : { gate: 'signed-out' };
  } catch (error) {
    if (ConnectError.from(error).code === Code.Unauthenticated) return { gate: 'signed-out' };
    // Unreachable or broken: no gate, so the pages load and say what failed.
    return { gate: 'none' };
  }
}

export function createSession(
  me: instance.WhoAmIResponse | undefined,
  control?: AccountControl,
): SessionService {
  const listeners = new Set<() => void>();
  const notify = () => {
    for (const l of listeners) l();
  };
  const environments: EnvironmentRef[] = (me?.environments ?? []).map((e) => ({
    id: e.id,
    name: e.name,
    namespace: e.namespace,
    protected: e.protected,
  }));
  let selected = environments[0]?.id;
  let state: AccountState = control?.initial ?? { gate: 'none' };
  // A cookie session has no WhoAmI principal: the person comes from the session.
  let fallback: instance.Principal | undefined;
  const principalOf = (account: ConsoleAccount | undefined) =>
    account?.user
      ? create(instance.PrincipalSchema, {
          id: account.user.id,
          kind: instance.PrincipalKind.USER,
          displayName: account.user.name,
          email: account.user.email,
        })
      : undefined;
  fallback = principalOf(state.account);
  const set = (next: AccountState) => {
    state = next;
    fallback = principalOf(next.account);
    notify();
  };
  return {
    principal: () => me?.principal ?? fallback,
    environments: () => environments,
    environment: () => environments.find((e) => e.id === selected),
    select(id) {
      if (!environments.some((e) => e.id === id)) throw new Error(`unknown environment ${id}`);
      selected = id;
      notify();
    },
    authenticatedAt: () => (me?.authenticatedAt ? timestampDate(me.authenticatedAt) : undefined),
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    account: () => state.account,
    gate: () => state.gate,
    ...(control
      ? {
          async signOut() {
            await control.end();
            set({ gate: 'signed-out' });
          },
          async refresh() {
            set(await control.reload());
          },
        }
      : {}),
  };
}

const plugin: PluginModule = {
  name: 'identity',
  inject: ['rpc.instance', 'flags', 'transport', 'platform'],
  async apply(ctx: Context) {
    const client = service(ctx, 'rpc.instance');
    const flags = service(ctx, 'flags');
    const platform = service(ctx, 'platform');
    let me: instance.WhoAmIResponse | undefined;
    try {
      me = await client.whoAmI({});
    } catch {
      // Signed out, a cookie session (no bearer), or an instance without auth (D111).
    }
    let control: AccountControl | undefined;
    if (flags.has('loams.auth.v1')) {
      const accounts = createClient(auth.AuthService, service(ctx, 'transport') as Transport);
      const load = async (setupRequired: boolean) => {
        const next = await loadAccount(accounts, setupRequired);
        platform.setCsrfToken?.(next.account?.csrfToken || undefined);
        return next;
      };
      control = {
        initial: await load(flags.setupRequired === true),
        // After setup or sign-in: the boot-time GetInstance may still say setup is due.
        reload: () => load(false),
        async end() {
          await accounts.deleteSession({});
          platform.setCsrfToken?.(undefined);
        },
      };
    }
    ctx.provide('session', createSession(me, control));
  },
};

export default plugin;
