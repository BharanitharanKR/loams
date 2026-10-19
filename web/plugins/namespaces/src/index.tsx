// @loams/plugin-namespaces: the environments the signed-in principal can
// reach, one engine namespace each (§19), from the `session` service, and the
// namespaces to open in the data pages. Those come from the engine's
// `NamespaceService/ListNamespaces` (B7); an engine older than it answers
// `unimplemented`, and then they are `default`, the environments' namespaces and
// the ones the console's pages opened in this browser.

import { Code, ConnectError, createClient, type Transport } from '@connectrpc/connect';
import { type PluginModule, type SessionService, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { collection } from '@loams/proto';
import type { EnvironmentRef } from '@loams/slots';
import { Badge, Button, Card, Empty, Input, StatusTag, Table } from '@loams/ui';
import { type FormEvent, useEffect, useState, useSyncExternalStore } from 'react';

/** One namespace the engine lists. */
export interface NamespaceRow {
  name: string;
  collections?: number;
  streams?: number;
}

/**
 * Every namespace the caller reaches, from `ListNamespaces` (all pages), or
 * `undefined` when the engine predates it (`unimplemented`, or a 404 from a
 * proxy that does not know the path). Any other failure is thrown.
 */
export async function listNamespaces(transport: Transport): Promise<NamespaceRow[] | undefined> {
  const client = createClient(collection.NamespaceService, transport);
  const rows: NamespaceRow[] = [];
  let pageToken = '';
  try {
    for (let page = 0; page < 100; page++) {
      const res = await client.listNamespaces({ pageSize: 500, pageToken });
      for (const ns of res.namespaces) {
        rows.push({ name: ns.namespace, collections: ns.collectionCount, streams: ns.streamCount });
      }
      if (!res.nextPageToken) break;
      pageToken = res.nextPageToken;
    }
  } catch (err) {
    const code = ConnectError.from(err).code;
    if (code === Code.Unimplemented || code === Code.NotFound) return undefined;
    throw err;
  }
  return rows;
}

/** The engine's list, `undefined` until it answers or when the engine has none to give. */
function useEngineNamespaces(load?: () => Promise<NamespaceRow[] | undefined>) {
  const [state, setState] = useState<{
    rows?: NamespaceRow[];
    error?: string;
    done: boolean;
  }>({ done: !load });
  useEffect(() => {
    if (!load) return;
    let live = true;
    load()
      .then((rows) => live && setState({ rows, done: true }))
      .catch((err: unknown) => live && setState({ error: String(err), done: true }));
    return () => {
      live = false;
    };
  }, [load]);
  return state;
}

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
  load,
}: {
  session: SessionService;
  /** Opens a route; without it the namespace links are not shown. */
  navigate?: (to: string) => void;
  /** The engine's namespaces ({@link listNamespaces}); without it, the browser's own list. */
  load?: () => Promise<NamespaceRow[] | undefined>;
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
      {navigate && <OpenNamespace environments={environments} navigate={navigate} load={load} />}
    </div>
  );
}

/** The namespaces to open in the Data explorer or Streams & Links, and a field for any other. */
function OpenNamespace({
  environments,
  navigate,
  load,
}: {
  environments: EnvironmentRef[];
  navigate: (to: string) => void;
  load?: () => Promise<NamespaceRow[] | undefined>;
}) {
  const [draft, setDraft] = useState('');
  const engine = useEngineNamespaces(load);
  const listed = engine.rows !== undefined;
  const rows: NamespaceRow[] =
    engine.rows ?? knownNamespaces(environments).map((name) => ({ name }));
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
        <Table<NamespaceRow>
          caption="Namespaces to open"
          rows={engine.done ? rows : []}
          rowKey={(r) => r.name}
          empty={
            <Empty title={engine.done ? 'No namespaces' : 'Loading namespaces…'}>
              {engine.done
                ? 'This engine has no namespace you can reach yet. Writing a collection or a stream creates one; open any by name above.'
                : null}
            </Empty>
          }
          columns={[
            { key: 'name', header: 'Namespace', cell: (r) => <Badge>{r.name}</Badge> },
            ...(listed
              ? [
                  {
                    key: 'collections',
                    header: 'Collections',
                    cell: (r: NamespaceRow) => r.collections ?? 0,
                  },
                  { key: 'streams', header: 'Streams', cell: (r: NamespaceRow) => r.streams ?? 0 },
                ]
              : []),
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
      {engine.error && (
        <p className="text-small text-muted" style={{ marginTop: 8 }}>
          The engine's namespace list did not load ({engine.error}); showing the ones this browser
          knows.
        </p>
      )}
      {engine.done && !listed && (
        <p className="text-small text-muted" style={{ marginTop: 8 }}>
          This engine does not list namespaces: these are <code>default</code>, your environments'
          namespaces, and the ones opened in this browser. Open any other by name.
        </p>
      )}
    </div>
  );
}

const plugin: PluginModule = {
  name: 'namespaces',
  inject: ['session', 'router', 'transport'],
  apply(ctx: Context) {
    const session = service(ctx, 'session');
    const router = service(ctx, 'router');
    const transport = service(ctx, 'transport');
    const load = () => listNamespaces(transport);
    ctx.effect(() =>
      router.page(
        {
          id: 'namespaces',
          path: '/namespaces',
          title: 'Namespaces',
          plugin: 'namespaces',
          nav: { group: 'Instance', order: 10 },
        },
        () => (
          <NamespacesPage session={session} navigate={(to) => router.navigate(to)} load={load} />
        ),
      ),
    );
  },
};

export default plugin;
