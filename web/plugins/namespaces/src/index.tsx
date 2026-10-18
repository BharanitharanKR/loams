// @loams/plugin-namespaces: the environments the signed-in principal can
// reach, one engine namespace each (§19), from the `session` service, and the
// namespaces to open in the data pages. The engine does not list namespaces, so
// those are `default` and the ones the console's pages opened in this browser.

import { type PluginModule, type SessionService, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import type { EnvironmentRef } from '@loams/slots';
import { Badge, Button, Card, Empty, Input, StatusTag, Table } from '@loams/ui';
import { type FormEvent, useState, useSyncExternalStore } from 'react';

/** The principal an engine with sign-in off (LOAMS_AUTH_MODE=off) names every caller. */
export const DEV_LOOPBACK = 'dev-loopback';

/** Where the console's pages remember the namespaces they opened (newest first). */
const RECENT_KEYS = [
  'loams.data.namespaces',
  'loams.streams.namespaces',
  'loams.graph.namespaces',
  'loams.git.namespaces',
  'loams.jobs.namespaces',
];

/** `default`, the environments' namespaces, then the ones opened in this browser. */
export function knownNamespaces(environments: EnvironmentRef[]): string[] {
  const recent: string[] = [];
  for (const key of RECENT_KEYS) {
    try {
      const raw = JSON.parse(globalThis.localStorage?.getItem(key) ?? '[]');
      if (Array.isArray(raw)) recent.push(...raw.filter((n): n is string => typeof n === 'string'));
    } catch {
      // Storage is a convenience only.
    }
  }
  return [...new Set(['default', ...environments.map((e) => e.namespace), ...recent])];
}

export function NamespacesPage({
  session,
  navigate,
}: {
  session: SessionService;
  /** Opens a route; without it the namespace links are not shown. */
  navigate?: (to: string) => void;
}) {
  useSyncExternalStore(session.subscribe, () => session.environment()?.id);
  const environments = session.environments();
  const selected = session.environment()?.id;
  const who = session.principal();
  const authOff = who?.id === DEV_LOOPBACK;
  return (
    <div className="lc-page">
      <header className="lc-page-head">
        <h1>Namespaces</h1>
        <p>
          {authOff
            ? `${who?.displayName || 'Local developer'}: this engine runs with sign-in off (LOAMS_AUTH_MODE=off, loopback only), so every namespace is open to you.`
            : who
              ? `Signed in as ${who.displayName}`
              : 'Not signed in'}
        </p>
      </header>
      <Card title="Environments" flush>
        <Table<EnvironmentRef>
          caption="Environments and their namespaces"
          rows={environments}
          rowKey={(e) => e.id}
          empty={
            <Empty title="No environments">
              {authOff
                ? 'Environments come from projects, and none grants this principal one. With sign-in off you need none: open any namespace below.'
                : 'No project environment is granted to this principal. You can still open a namespace you have access to below.'}
            </Empty>
          }
          columns={[
            { key: 'name', header: 'Environment', cell: (e) => e.name },
            { key: 'namespace', header: 'Namespace', cell: (e) => <Badge>{e.namespace}</Badge> },
            {
              key: 'protected',
              header: 'Protection',
              cell: (e) =>
                e.protected ? <StatusTag status="progress">protected</StatusTag> : null,
            },
            {
              key: 'select',
              header: '',
              cell: (e) =>
                e.id === selected ? (
                  <StatusTag status="done">selected</StatusTag>
                ) : (
                  <button
                    type="button"
                    className="loams-btn loams-btn-quiet loams-btn-sm"
                    onClick={() => session.select(e.id)}
                  >
                    Select
                  </button>
                ),
            },
          ]}
        />
      </Card>
      {navigate && <OpenNamespace environments={environments} navigate={navigate} />}
    </div>
  );
}

/** The namespaces to open in the Data explorer or Streams & Links, and a field for any other. */
function OpenNamespace({
  environments,
  navigate,
}: {
  environments: EnvironmentRef[];
  navigate: (to: string) => void;
}) {
  const [draft, setDraft] = useState('');
  const rows = knownNamespaces(environments).map((name) => ({ name }));
  const open = (e: FormEvent) => {
    e.preventDefault();
    const ns = draft.trim();
    if (ns) navigate(`/data/${encodeURIComponent(ns)}`);
  };
  const link = (to: string, label: string) => (
    <button
      type="button"
      className="loams-btn loams-btn-quiet loams-btn-sm"
      onClick={() => navigate(to)}
    >
      {label}
    </button>
  );
  return (
    <div style={{ marginTop: 20 }}>
      <Card
        title="Open a namespace"
        flush
        actions={
          <form className="flex items-center gap-2" onSubmit={open}>
            <Input
              aria-label="Namespace"
              placeholder="namespace"
              className={/* mono-ok: id */ 'w-40 font-mono'}
              value={draft}
              spellCheck={false}
              onChange={(e) => setDraft(e.target.value)}
            />
            <Button size="sm" type="submit" disabled={!draft.trim()}>
              Open
            </Button>
          </form>
        }
      >
        <Table<{ name: string }>
          caption="Namespaces to open"
          rows={rows}
          rowKey={(r) => r.name}
          columns={[
            { key: 'name', header: 'Namespace', cell: (r) => <Badge>{r.name}</Badge> },
            {
              key: 'open',
              header: '',
              cell: (r) => (
                <span className="flex justify-end gap-1">
                  {link(`/data/${encodeURIComponent(r.name)}`, 'Data explorer')}
                  {link(`/streams/${encodeURIComponent(r.name)}`, 'Streams & Links')}
                </span>
              ),
            },
          ]}
        />
      </Card>
      <p className="text-small text-muted" style={{ marginTop: 8 }}>
        The engine does not list namespaces: these are <code>default</code>, your environments'
        namespaces, and the ones opened in this browser. Open any other by name.
      </p>
    </div>
  );
}

const plugin: PluginModule = {
  name: 'namespaces',
  inject: ['session', 'router'],
  apply(ctx: Context) {
    const session = service(ctx, 'session');
    const router = service(ctx, 'router');
    ctx.effect(() =>
      router.page(
        {
          id: 'namespaces',
          path: '/namespaces',
          title: 'Namespaces',
          plugin: 'namespaces',
          nav: { group: 'Instance', order: 10 },
        },
        () => <NamespacesPage session={session} navigate={(to) => router.navigate(to)} />,
      ),
    );
  },
};

export default plugin;
