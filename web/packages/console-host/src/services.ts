// The service contracts plugins cooperate through (§37 §5.4, D424). A value
// import from one plugin into another is a build error; plugins share types
// through this module and @loams/proto, and behaviour only through services.
//
// Plugins add their own services to `Services` by declaration merging:
//
//     declare module '@loams/console-host' {
//       interface Services { 'approvals.inbox': InboxStore }
//     }

import type { Client, Transport } from '@connectrpc/connect';
import type { Context } from '@loams/cordis';
import type {
  admin,
  approvals,
  auth,
  devices,
  instance,
  notifications,
  operations,
} from '@loams/proto';
import type { EnvironmentRef, SlotRegistry } from '@loams/slots';
import type { ComponentType } from 'react';
import type { ActionRegistry } from './actions.js';
import type { AgentdRpcServices, AgentdService } from './agentd.js';
import type { ModeService } from './mode.js';
import type { ThemeChoice } from './theme.js';

/** What differs between the shells the console runs in (today: the browser). */
export interface PlatformService {
  kind: 'web' | 'desktop';
  /** The fetch every request goes through. */
  fetch: typeof globalThis.fetch;
  /** The Connect base URL of the active environment. */
  baseUrl: string;
  /**
   * The server an SDK or another client should talk to, when it is not `baseUrl`:
   * in development the console's origin is the dev server, which proxies to it.
   */
  serverUrl?: string;
  openExternal(url: string): Promise<void>;
  notify(n: { title: string; body: string; route?: string }): Promise<void>;
  clipboardWrite(text: string): Promise<void>;
  /**
   * Optional: the CSRF token of a cookie session (design §19 §6, API2 ruling 6/7.9).
   * The platform sends it as `X-CSRF-Token` on every request that is not a GET;
   * `undefined` stops sending it. A bearer-token platform ignores it.
   */
  setCsrfToken?(token: string | undefined): void;
  /**
   * Optional: the platform calls back when GetInstance may now answer differently
   * (the desktop's local engine became ready). The host re-reads `flags` then.
   */
  onInstanceStale?(callback: () => void): () => void;
}

/** The theme choice and what it resolves to (design 59, D1074); the platform provides it. */
export interface ThemeService {
  readonly choice: ThemeChoice;
  readonly dark: boolean;
  set(choice: ThemeChoice): void;
  subscribe(listener: () => void): () => void;
}

/** An inspector's or the drawer's state (design 59, D1078). */
export interface PanelState {
  open: boolean;
  /** The selected tab's id (its entry's `meta.id`). */
  tab: string | undefined;
  /** What `open({ tab, props })` passed for that tab; not persisted. */
  props: Readonly<Record<string, unknown>>;
  /** Width (inspector) or height (drawer) in pixels. */
  size: number;
}

/** The shell's right inspector or bottom drawer (design 59, D1078); the shell provides both. */
export interface PanelService {
  state(): PanelState;
  /** Opens the panel, on `tab` when given, passing `props` to that tab. */
  open(o?: { tab?: string; props?: Record<string, unknown> }): void;
  close(): void;
  /** Closes the panel when it is open (on `tab`, when given); else opens it. */
  toggle(tab?: string): void;
  resize(size: number): void;
  subscribe(listener: () => void): () => void;
}

/** `GetInstance`'s answer, as plugins gate on it (AP1a Ruling 6). */
export interface FlagsService {
  edition: 'oss' | 'cloud' | 'byoc' | 'unknown';
  instanceName: string;
  serverVersion: string;
  features: Record<string, boolean>;
  apiVersions: string[];
  /** True until first-run setup has created the org and its owner. */
  setupRequired?: boolean;
  /**
   * The instance offers no sign-in (GetInstance lists only SIGN_IN_KIND_NONE): an
   * engine with LOAMS_AUTH_MODE=off, loopback only, where first-run setup needs no
   * token and every caller is the local developer.
   */
  signInOff?: boolean;
  /**
   * Why GetInstance gave no answer: the engine does not have it
   * (`unimplemented`, an engine too old for this console), or it could not
   * be reached (`unreachable`).
   */
  instanceError?: 'unimplemented' | 'unreachable';
  has(api: string): boolean;
}

export interface PageSpec {
  /** The route id, also the `console.page` key. */
  id: string;
  /** A path pattern, for example "/approvals/:id". */
  path: string;
  title: string;
  nav?: { group: string; order: number; label?: string };
  /** The registering plugin's id, for the error boundary and diagnostics. */
  plugin?: string;
  /** A full-screen page without the console's chrome (sign-in, setup, consent). */
  bare?: boolean;
  /** Open without a session: the shell does not send a signed-out visitor away from it. */
  public?: boolean;
}

export interface PageProps {
  params: Record<string, string>;
  environment?: EnvironmentRef;
}

export interface Location {
  path: string;
  pageId?: string;
  params: Record<string, string>;
  /** The route's query (`#/sign-in?next=/teams` → `{ next: '/teams' }`). */
  query?: Record<string, string>;
}

/** The `router` service (@loams/plugin-shell). */
export interface RouterService {
  /** Registers a page; returns its disposer (call inside `ctx.effect`). */
  page(spec: PageSpec, component: ComponentType<PageProps>): () => void;
  /**
   * Goes to a route. `replace` swaps the current history entry instead of adding one,
   * for a redirect, so Back does not land on the redirecting route again.
   */
  navigate(to: string, opts?: { replace?: boolean }): void;
  current(): Location;
  subscribe(listener: () => void): () => void;
  /**
   * A page's own breadcrumbs in place of the shell's (an org, a project, the record
   * it shows); the last crumb is also the tab title. Cleared when the route changes.
   */
  setTrail?(trail: { label: string; href?: string }[] | undefined): void;
}

/**
 * Where the console's sign-in stands, from `AuthService/GetSession` (API2 Task 10):
 * `ready` signed in; `signed-out` and `setup` send the shell to the sign-in or setup
 * page; `none` when the instance has no console sign-in (`loams.auth.v1` not served).
 */
export type SessionGate = 'ready' | 'signed-out' | 'setup' | 'none';

/** The signed-in person's console session (`loams.auth.v1.Session`). */
export type ConsoleAccount = auth.Session;

export interface SessionService {
  principal(): instance.Principal | undefined;
  environment(): EnvironmentRef | undefined;
  environments(): EnvironmentRef[];
  select(environmentId: string): void;
  /** When the session last authenticated the person (step-up, AP0 Ruling 7). */
  authenticatedAt(): Date | undefined;
  subscribe(listener: () => void): () => void;
  /** The console session: the person, their org and role. Unset without one. */
  account?(): ConsoleAccount | undefined;
  /** Where sign-in stands; see `SessionGate`. */
  gate?(): SessionGate;
  /** Ends the console session (`AuthService/DeleteSession`). */
  signOut?(): Promise<void>;
  /** Re-reads the session, after sign-in or setup. */
  refresh?(): Promise<void>;
}

/** The typed Connect clients `@loams/plugin-rpc` provides, gated on `api_versions`. */
export interface RpcServices {
  'rpc.instance': Client<typeof instance.InstanceService>;
  'rpc.approvals': Client<typeof approvals.ApprovalService>;
  'rpc.operations': Client<typeof operations.OperationsService>;
  'rpc.devices': Client<typeof devices.DeviceService>;
  'rpc.notifications': Client<typeof notifications.NotificationService>;
  /** The console API (API2 Tasks 6, 7): gated on `loams.auth.v1` and `loams.admin.v1`. */
  'rpc.auth': Client<typeof auth.AuthService>;
  'rpc.org': Client<typeof admin.OrgService>;
  'rpc.projects': Client<typeof admin.ProjectService>;
  'rpc.agents': Client<typeof admin.AgentService>;
  'rpc.keys': Client<typeof admin.KeyService>;
  'rpc.audit': Client<typeof admin.AuditService>;
}

export interface Services extends RpcServices, AgentdRpcServices {
  platform: PlatformService;
  transport: Transport;
  flags: FlagsService;
  slots: SlotRegistry;
  router: RouterService;
  session: SessionService;
  /** The callable surface plugins register into (§42 §5, D569). */
  actions: ActionRegistry;
  /** Console or Studio (§53 §4.2); the host provides it. */
  mode: ModeService;
  /** System, Dark or Light, per device (design 59, D1074); the platform provides it. */
  theme: ThemeService;
  /** The shell's right inspector and bottom drawer (design 59, D1078). */
  inspector: PanelService;
  drawer: PanelService;
  /**
   * loams-agentd (§53 §6.2, SD1 Task 6; see agentd.ts): `agentd` always, from
   * `@loams/plugin-rpc`; `agentd.transport` and `rpc.agentd.*` while a daemon is
   * reachable; `agentd.endpoint` from the platform, for `@loams/plugin-rpc` only.
   */
  agentd: AgentdService;
  'agentd.transport': Transport;
  'agentd.endpoint': AgentdService;
}

export type ServiceName = keyof Services;

/**
 * A typed service read: `service(ctx, 'rpc.approvals')`. cordis throws if
 * the plugin did not inject the service, and the guard throws for anything
 * outside the plugin's `inject` list.
 */
export function service<K extends ServiceName>(ctx: Context, name: K): Services[K] {
  return (ctx as unknown as Record<string, Services[K]>)[name] as Services[K];
}

/**
 * Opens a server stream tied to the calling fiber: disposing the plugin
 * aborts the stream (Review Focus 2, `dispose_aborts_server_streams`).
 * `open` receives the abort signal to pass as `{ signal }` to the client.
 */
export function watch<T>(
  ctx: Context,
  open: (signal: AbortSignal) => AsyncIterable<T>,
  onMessage: (message: T) => void,
  onError?: (error: unknown) => void,
): void {
  ctx.effect(() => {
    const controller = new AbortController();
    (async () => {
      try {
        for await (const message of open(controller.signal)) onMessage(message);
      } catch (error) {
        if (!controller.signal.aborted) onError?.(error);
      }
    })();
    return () => controller.abort();
  }, 'watch');
}
