import { Code } from '@connectrpc/connect';
import { useMutation, useQuery } from '@connectrpc/connect-query';
import {
  ACTIONS,
  ActivityChart,
  errorText,
  Form,
  iso,
  LoadError,
  Loading,
  num,
  Page,
  PageHead,
  Scoped,
  useAccount,
  useNotify,
  useTrail,
} from '@loams/admin-ui';
import type { RouterService, SessionService } from '@loams/console-host';
import { admin, collection } from '@loams/proto';
import {
  Badge,
  Button,
  Card,
  Checkbox,
  ComingSoon,
  DataTable,
  Dialog,
  Empty,
  Field,
  formatBytes,
  formatDate,
  formatNumber,
  formatRelative,
  Input,
  isNotServed,
  Notice,
  Primary,
  SelectMenu,
  Snippet,
  Stat,
  Stats,
  StatusTag,
} from '@loams/ui';
import { KeyRound, Lock } from 'lucide-react';
import { type FormEvent, useEffect, useState } from 'react';

/** The SDK example on the Connect card: the configured server, this environment's namespace. */
export function connectSnippet(server: string, namespace: string): string {
  return [
    "import { Loams, apiKey } from '@loams/client';",
    '',
    'const loams = new Loams({',
    `  endpoint: '${server}',`,
    '  auth: apiKey(process.env.LOAMS_API_KEY!),',
    '});',
    '',
    `// This environment's data lives in the namespace ${namespace}.`,
    'const me = await loams.instance.whoAmI({});',
  ].join('\n');
}

const title = (s: string) => (s ? s[0]?.toUpperCase() + s.slice(1) : s);

/** Fields, vectors and sparse vectors from a collection's schema (the REST schema JSON). */
function schemaDetail(info: collection.CollectionInfo): string {
  const schema = (info.schema ?? {}) as Record<string, unknown>;
  const list = (k: string) =>
    Array.isArray(schema[k]) ? (schema[k] as Record<string, unknown>[]) : [];
  return [
    ...list('vectors').map((v) => `${String(v.name ?? '')} ${String(v.dim ?? '')}d`),
    ...list('sparse_vectors').map((s) => `${String(s.name ?? '')} sparse`),
    `${list('fields').length} fields`,
  ].join(' · ');
}

function HotTier({ info }: { info: collection.CollectionInfo }) {
  const state = info.hot?.vectors?.state;
  if (state === collection.HotStateKind.READY) return <StatusTag status="done">Ready</StatusTag>;
  if (state === collection.HotStateKind.BUILDING)
    return <StatusTag status="progress">Building</StatusTag>;
  return <StatusTag status="neutral">Off</StatusTag>;
}

function Collections({ namespace }: { namespace: string }) {
  const cols = useQuery(
    collection.CollectionService.method.listCollections,
    { namespace, pageSize: 100 },
    // A server without loams.collection.v1 will not start serving it on a retry.
    { retry: (count, error) => error.code !== Code.Unimplemented && count < 1 },
  );
  if (cols.isPending) return <Loading />;
  if (cols.error) {
    const unserved = cols.error.code === Code.Unimplemented;
    return (
      <div className="pad">
        {isNotServed({ code: unserved ? 'unimplemented' : undefined }) ? (
          <ComingSoon feature="the collections API (loams.collection.v1)" />
        ) : (
          <LoadError error={cols.error} retry={() => void cols.refetch()} />
        )}
      </div>
    );
  }
  return (
    <DataTable<collection.CollectionInfo>
      rows={cols.data?.collections ?? []}
      rowKey={(c) => String(c.id)}
      empty={
        <div className="pad">
          <Empty title="No collections yet">
            Create one with the SDK or the native API; it appears here.
          </Empty>
        </div>
      }
      columns={[
        {
          key: 'name',
          header: 'Collection',
          sortValue: (c) => c.name,
          cell: (c) => <Primary title={c.name} detail={schemaDetail(c)} />,
        },
        {
          key: 'docs',
          header: 'Documents',
          numeric: true,
          sortValue: (c) => num(c.liveDocCount),
          cell: (c) => formatNumber(num(c.liveDocCount)),
        },
        {
          key: 'size',
          header: 'Size',
          numeric: true,
          sortValue: (c) => num(c.sizeBytes),
          cell: (c) => formatBytes(num(c.sizeBytes)),
        },
        {
          key: 'ver',
          header: 'Manifest',
          numeric: true,
          cell: (c) => <Badge>v{num(c.manifestVersion)}</Badge>,
        },
        { key: 'hot', header: 'Hot tier', cell: (c) => <HotTier info={c} /> },
        {
          key: 'lag',
          header: 'Link lag',
          numeric: true,
          cell: (c) =>
            c.linkLagRecords ? (
              `${num(c.linkLagRecords)} records`
            ) : (
              <span className="muted">none</span>
            ),
        },
      ]}
    />
  );
}

export function EnvironmentPage({
  project,
  environment,
  router,
  session,
  server,
}: {
  project: string;
  environment: string;
  router: RouterService;
  session: SessionService;
  /** The server an SDK talks to, for the Connect snippet. */
  server: string;
}) {
  const orgName = useAccount(session)?.org?.name ?? 'Organization';
  const projects = useQuery(admin.ProjectService.method.listProjects, {});
  const projectName = projects.data?.projects.find((p) => p.slug === project)?.name ?? project;
  const env = useQuery(admin.ProjectService.method.getEnvironment, { project, environment });
  const activity = useQuery(admin.ProjectService.method.getEnvironmentActivity, {
    project,
    environment,
  });
  const keys = useQuery(admin.KeyService.method.listKeys, { project, environment });
  const revoke = useMutation(admin.KeyService.method.revokeKey);
  const notify = useNotify();
  const [newKey, setNewKey] = useState(false);
  const e = env.data?.environment;
  useTrail(router, [
    { label: orgName, href: '/' },
    { label: projectName, href: `/projects/${project}` },
    { label: e?.name ?? title(environment) },
  ]);

  // The data pages (Namespaces, Data) follow the open environment where the session has it.
  useEffect(() => {
    if (!e) return;
    const match = session.environments().find((x) => x.namespace === e.namespace);
    if (match && session.environment()?.id !== match.id) session.select(match.id);
  }, [e, session]);

  if (env.error) {
    return (
      <Page>
        <LoadError error={env.error} retry={() => void env.refetch()} />
      </Page>
    );
  }
  if (!e) return <Loading />;

  return (
    <Page>
      <PageHead
        eyebrow="Environment"
        title={
          <span className="title-row">
            <span className="env-dot lg" data-slug={e.slug} aria-hidden="true" />
            {e.name}
            {e.protected && (
              <StatusTag status="progress">
                <Lock size={11} aria-hidden="true" /> Protected
              </StatusTag>
            )}
          </span>
        }
        actions={
          <Button variant="primary" onClick={() => setNewKey(true)}>
            <KeyRound size={15} aria-hidden="true" /> Create API key
          </Button>
        }
      >
        Namespace <Badge>{e.namespace}</Badge> in <Badge>{e.bucket}</Badge> ({e.region})
      </PageHead>

      <Stats>
        <Stat
          label="Documents"
          value={formatNumber(num(e.stats?.documents))}
          detail={`${num(e.stats?.collections)} collections`}
        />
        <Stat
          label="Storage"
          value={formatBytes(num(e.stats?.storageBytes))}
          detail="Lance and Tantivy in the bucket"
        />
        <Stat label="Queries, 24 h" value={formatNumber(num(e.stats?.queries24h))} />
        <Stat label="Writes, 24 h" value={formatNumber(num(e.stats?.writes24h))} />
      </Stats>

      <Card title="Queries, last 30 days" style={{ marginTop: 16 }}>
        {activity.data ? (
          <ActivityChart series={activity.data.series} />
        ) : activity.error ? (
          <LoadError error={activity.error} />
        ) : (
          <Loading />
        )}
      </Card>

      <Card title="Collections" flush style={{ marginTop: 16 }}>
        <Collections namespace={e.namespace} />
      </Card>

      <div className="grid-2" style={{ marginTop: 16 }}>
        <Card title="API keys" flush>
          {keys.error ? (
            <div className="pad">
              <LoadError error={keys.error} retry={() => void keys.refetch()} />
            </div>
          ) : (
            <DataTable<admin.ApiKey>
              rows={keys.data?.keys ?? []}
              rowKey={(k) => k.id}
              empty={
                <div className="pad">
                  <Empty title="No API keys" grain="silt" seed={21}>
                    Agents never need one: they get short-lived tokens. Keys are for SDKs and
                    service accounts.
                  </Empty>
                </div>
              }
              columns={[
                {
                  key: 'name',
                  header: 'Key',
                  sortValue: (k) => k.name,
                  cell: (k) => <Primary title={k.name} detail={<code>{k.prefix}…</code>} />,
                },
                { key: 'who', header: 'Principal', cell: (k) => k.principal?.name },
                {
                  key: 'used',
                  header: 'Last used',
                  cell: (k) => {
                    const at = iso(k.lastUsedAt);
                    return at ? formatRelative(at) : <span className="muted">never</span>;
                  },
                },
                {
                  key: 'exp',
                  header: 'Expires',
                  cell: (k) => {
                    const at = iso(k.expiresAt);
                    return at ? formatDate(at) : <span className="muted">never</span>;
                  },
                },
                {
                  key: 'revoke',
                  header: '',
                  cell: (k) => (
                    <Button
                      variant="danger"
                      size="sm"
                      onClick={async () => {
                        try {
                          await revoke.mutateAsync({ key: k.id });
                          notify(`Revoked ${k.name}`);
                          void keys.refetch();
                        } catch (err) {
                          notify(errorText(err), 'danger');
                        }
                      }}
                    >
                      Revoke
                    </Button>
                  ),
                },
              ]}
            />
          )}
        </Card>
        <Card title="Connect">
          <p>
            Connect with the SDK (<code>@loams/client</code>) and an API key from this environment.
            It speaks Connect on the server's one port:
          </p>
          <Snippet prompt={false}>{connectSnippet(server, e.namespace)}</Snippet>
          <p>
            Agents exchange their workload identity for a token scoped to it; see an agent's page.
          </p>
        </Card>
      </div>

      <NewKey
        project={project}
        environment={environment}
        protectedEnv={e.protected}
        open={newKey}
        onClose={() => setNewKey(false)}
        onCreated={() => void keys.refetch()}
      />
    </Page>
  );
}

const EXPIRY = ['7', '30', '90', '180', '365'] as const;
/** The key's owner when no service account is picked: the signed-in person. */
const ME = 'me';

function NewKey({
  project,
  environment,
  protectedEnv,
  open,
  onClose,
  onCreated,
}: {
  project: string;
  environment: string;
  protectedEnv: boolean;
  open: boolean;
  onCreated: () => void;
  onClose: () => void;
}) {
  const sas = useQuery(admin.AgentService.method.listServiceAccounts, { project });
  const create = useMutation(admin.KeyService.method.createKey);
  const [name, setName] = useState('');
  const [sa, setSa] = useState('');
  const [days, setDays] = useState<(typeof EXPIRY)[number]>('90');
  const [scopes, setScopes] = useState<string[]>(['query']);
  const [secret, setSecret] = useState<string>();
  const [error, setError] = useState<string>();

  const close = () => {
    setSecret(undefined);
    setName('');
    setError(undefined);
    onClose();
  };
  const submit = async (ev: FormEvent) => {
    ev.preventDefault();
    if (!name.trim()) return setError('Name the key after what uses it.');
    if (!scopes.length) return setError('Pick at least one action.');
    try {
      const r = await create.mutateAsync({
        project,
        environment,
        name: name.trim(),
        serviceAccount: sa,
        scopes,
        expiresInDays: Number(days),
        // The one keyed RPC (API2 ruling 6/7.7): its secret is answered once per key.
        idempotencyKey: crypto.randomUUID(),
      });
      setSecret(r.secret);
      onCreated();
    } catch (e) {
      setError(errorText(e));
    }
  };

  return (
    <Dialog
      open={open}
      onClose={close}
      title={secret ? 'Copy your key now' : 'Create API key'}
      footer={
        secret ? (
          <Button variant="primary" onClick={close}>
            I've stored it
          </Button>
        ) : (
          <>
            <Button onClick={close}>Cancel</Button>
            <Button variant="primary" type="submit" form="new-key" disabled={create.isPending}>
              Create key
            </Button>
          </>
        )
      }
    >
      {secret ? (
        <Scoped>
          <Notice tone="warn" title="This is the only time the secret is shown">
            Loams keeps only its hash. If you lose it, revoke the key and create another.
          </Notice>
          <Snippet prompt={false}>{secret}</Snippet>
        </Scoped>
      ) : (
        <Form id="new-key" onSubmit={submit}>
          {protectedEnv && (
            <Notice tone="warn" title="Protected environment">
              Creating a key here needs the project admin role.
            </Notice>
          )}
          <Field label="Name" error={error}>
            {(p) => (
              <Input
                {...p}
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="ci-deploy"
                autoFocus
              />
            )}
          </Field>
          <Field label="For" hint="A service account owns the key; without one, you do.">
            {() => (
              <SelectMenu
                label="For"
                value={sa || ME}
                onValueChange={(v) => setSa(v === ME ? '' : v)}
                options={[
                  { value: ME, label: 'Me' },
                  ...(sas.data?.serviceAccounts ?? []).map((s) => ({ value: s.id, label: s.name })),
                ]}
              />
            )}
          </Field>
          <fieldset className="checks">
            <legend>Actions</legend>
            {ACTIONS.filter((a) => a.id !== 'mcp:tools').map((a) => (
              <Checkbox
                key={a.id}
                label={a.label}
                checked={scopes.includes(a.id)}
                onChange={(e) =>
                  setScopes((s) => (e.target.checked ? [...s, a.id] : s.filter((x) => x !== a.id)))
                }
              />
            ))}
          </fieldset>
          <Field label="Expires after" hint="Keys expire; 90 days is the default.">
            {() => (
              <SelectMenu
                label="Expires after"
                value={days}
                onValueChange={setDays}
                options={EXPIRY.map((d) => ({ value: d, label: `${d} days` }))}
              />
            )}
          </Field>
        </Form>
      )}
    </Dialog>
  );
}
