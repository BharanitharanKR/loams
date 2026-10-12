// The browser edition's plugin set (hosted console and BYOC deploys): the base
// catalog, web.yml (pages the engine serves), demo.yml under "Demo data", and a
// small header plugin that shows the connected engine, the toggle and sign-out.

import {
  type CatalogPatch,
  type ModuleTable,
  type PluginModule,
  parsePatch,
  service,
} from '@loams/console-host';
import type { Context } from '@loams/cordis';
import gitPkg from '@loams/plugin-git/package.json';
import graphPkg from '@loams/plugin-graph/package.json';
import housePkg from '@loams/plugin-house/package.json';
import livePkg from '@loams/plugin-live/package.json';
import metricsPkg from '@loams/plugin-metrics/package.json';
import pipelinesPkg from '@loams/plugin-pipelines/package.json';
import sqlPkg from '@loams/plugin-sql/package.json';
import { Button } from '@loams/ui';
import demoYml from '../../catalog/demo.yml?raw';
import webYml from '../../catalog/web.yml?raw';
import type { AuthMode } from '../runtime-config.js';
import { DEMO_PAGES } from './demo.js';

export const webModules: ModuleTable = {
  '@loams/plugin-live': () => import('@loams/plugin-live'),
  '@loams/plugin-graph': () => import('@loams/plugin-graph'),
  '@loams/plugin-house': () => import('@loams/plugin-house'),
  '@loams/plugin-sql': () => import('@loams/plugin-sql'),
  '@loams/plugin-metrics': () => import('@loams/plugin-metrics'),
  '@loams/plugin-pipelines': () => import('@loams/plugin-pipelines'),
  '@loams/plugin-git': () => import('@loams/plugin-git'),
};

export const webManifests: unknown[] = [
  livePkg,
  graphPkg,
  housePkg,
  sqlPkg,
  metricsPkg,
  pipelinesPkg,
  gitPkg,
];

/** The patches over base.yml, in order. */
export function webPatches(demo: boolean): CatalogPatch[] {
  return demo ? [parsePatch(webYml), parsePatch(demoYml)] : [parsePatch(webYml)];
}

/** What the header shows and does; the hosted gate provides it. */
export interface HostedSession {
  engine: string;
  /** How this console signs in (LOAMS_AUTH_MODE). */
  authMode: AuthMode;
  /** Who the engine is talking to: the OIDC user, or the tailnet user the engine named. */
  user?: string;
  /** Tailscale mode: why the engine did not say who this is. */
  identityProblem?: string;
  demo: boolean;
  setDemo(on: boolean): void;
  /** Back to the connect screen; unset when the deploy fixes the engine. */
  changeEngine?: () => void;
  signOut?: () => void;
}

export const HOSTED_PACKAGE = '@loams/console-hosted';

export const hostedManifest = {
  name: HOSTED_PACKAGE,
  version: '0.1.0',
  loams: {
    plugin: {
      kind: 'console',
      entry: '.',
      tier: 'first-party',
      inject: ['slots'],
      provides: [],
      slots: ['shell.header.server'],
      permissions: [],
      requires: { console: '^0.1.0' },
      editions: ['oss', 'cloud'],
    },
  },
};

export function HostedHeader({ session }: { session: HostedSession }) {
  const host = new URL(session.engine).host;
  return (
    <div className="lc-hosted">
      {session.changeEngine ? (
        <button
          type="button"
          className="lc-hosted-engine"
          title={`Connected to ${session.engine}. Change engine`}
          onClick={session.changeEngine}
        >
          <span className="lc-hosted-dot" aria-hidden="true" />
          <span>{host}</span>
        </button>
      ) : (
        <span className="lc-hosted-engine" title={`Connected to ${session.engine}`}>
          <span className="lc-hosted-dot" aria-hidden="true" />
          <span>{host}</span>
        </span>
      )}
      <label
        className="lc-hosted-demo"
        title={`Shows pages that run on built-in sample data, not your engine: ${DEMO_PAGES.join(', ')}.`}
      >
        <input
          type="checkbox"
          checked={session.demo}
          onChange={(e) => session.setDemo(e.currentTarget.checked)}
        />
        <span>Demo data</span>
      </label>
      {session.demo && (
        <span className="lc-hosted-demo-on" role="status">
          Sample pages on
        </span>
      )}
      <HostedIdentity session={session} />
      {session.signOut && (
        <Button size="sm" variant="quiet" onClick={session.signOut}>
          {session.authMode === 'apikey' ? 'Forget key' : 'Sign out'}
        </Button>
      )}
    </div>
  );
}

/** Who the console is acting as, and how the engine knows. */
function HostedIdentity({ session }: { session: HostedSession }) {
  if (session.authMode === 'apikey')
    return (
      <span className="lc-hosted-user" title="Signed in to the engine with an API key">
        API key
      </span>
    );
  if (session.authMode === 'tailscale' && !session.user)
    return (
      <span
        className="lc-hosted-user lc-hosted-user-unknown"
        title={session.identityProblem ?? 'The engine did not say who you are.'}
      >
        Tailnet user unknown
      </span>
    );
  if (!session.user) return null;
  return (
    <span
      className="lc-hosted-user"
      title={
        session.authMode === 'tailscale'
          ? `${session.user}, identified by the engine from your tailnet login`
          : `Signed in as ${session.user}`
      }
    >
      {session.user}
      {session.authMode === 'tailscale' && <span className="lc-hosted-via"> via tailnet</span>}
    </span>
  );
}

/** The header plugin; `session` comes from the hosted gate, not from a service. */
export function hostedPlugin(session: HostedSession): PluginModule {
  return {
    name: 'console-hosted',
    inject: ['slots'],
    apply(ctx: Context) {
      const slots = service(ctx, 'slots');
      ctx.effect(() =>
        slots.register({ name: 'shell.header.server', plugin: 'console-hosted' }, () => (
          <HostedHeader session={session} />
        )),
      );
    },
  };
}

/** The catalog row that loads the header plugin. */
export const hostedPatch: CatalogPatch = [
  { insert: [{ id: 'console-hosted', name: HOSTED_PACKAGE }] },
];
