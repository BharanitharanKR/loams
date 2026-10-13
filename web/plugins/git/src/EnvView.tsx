// Env secrets as code (§19.13): the repository's environments and their live revisions,
// each key's promotion status per environment (pinned on the branch vs live, D842), its
// rotation policy and status (D843), and the audit log (D844). Key names, fingerprints and
// versions only: this view has no way to show a value, and the client has no call that
// returns one.

import {
  Badge,
  Button,
  Field,
  formatRelative,
  Notice,
  Select,
  type Status,
  StatusTag,
} from '@loams/ui';
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  type AuditEvent,
  type Environment,
  type EnvKey,
  type GitClient,
  type GitFailure,
  isAbort,
  type KeyEnvStatus,
  type KeyInEnv,
  type PromotionStatus,
  type RotationStatus,
  shortOid,
  toFailure,
} from './client.js';

const KEY_STATUS: Record<KeyEnvStatus, { tag: Status; text: string }> = {
  live: { tag: 'done', text: 'live' },
  pending: { tag: 'progress', text: 'pending promotion' },
  rotated: { tag: 'neutral', text: 'rotated' },
  absent: { tag: 'planned', text: 'not set' },
};

const PROMOTION: Record<PromotionStatus, Status> = {
  promoted: 'done',
  pending: 'progress',
  failed: 'failed',
};

const ROTATION: Record<RotationStatus, { tag: Status; text: string }> = {
  ok: { tag: 'done', text: 'ok' },
  due: { tag: 'progress', text: 'due' },
  rotating: { tag: 'progress', text: 'rotating' },
  failed: { tag: 'failed', text: 'failed' },
};

export function keyStatusText(k: KeyInEnv): string {
  const base = KEY_STATUS[k.status].text;
  if (k.status === 'absent') return base;
  if (k.status === 'rotated') return `${base}: pinned v${k.pinnedVersion}, live v${k.liveVersion}`;
  if (k.status === 'pending') return `${base}: v${k.pinnedVersion} (live v${k.liveVersion ?? '-'})`;
  return k.liveVersion !== undefined ? `${base} v${k.liveVersion}` : base;
}

export function EnvView(p: { client: GitClient; namespace: string; repoId: string }) {
  const { client, namespace, repoId } = p;
  const [envs, setEnvs] = useState<Environment[]>();
  const [keys, setKeys] = useState<EnvKey[]>();
  const [error, setError] = useState<GitFailure>();

  useEffect(() => {
    const ac = new AbortController();
    setEnvs(undefined);
    setKeys(undefined);
    setError(undefined);
    const ref = { namespace, repoId };
    Promise.all([client.listEnvironments(ref, ac.signal), client.listEnvKeys(ref, ac.signal)]).then(
      ([e, k]) => {
        setEnvs(e);
        setKeys(k);
      },
      (e) => {
        if (!isAbort(e)) setError(toFailure(e));
      },
    );
    return () => ac.abort();
  }, [client, namespace, repoId]);

  if (error) {
    return (
      <Notice tone="danger" title="Could not read the environments">
        {error.message}
      </Notice>
    );
  }
  if (!envs || !keys) {
    return (
      <p className="m-0 text-sm text-muted" role="status">
        Loading environments…
      </p>
    );
  }
  if (envs.length === 0) {
    return (
      <p className="m-0 text-sm text-muted">
        This repository has no env/ folder. Commit env/dev.env or env/main.env from a Loams
        workspace to seal secrets into the vault.
      </p>
    );
  }

  return (
    <div className="flex min-w-0 flex-col gap-5">
      <Notice title="Values are never shown here">
        Env files hold references to the vault. This page lists key names, fingerprints and
        versions. Use <code>loams env show</code> with an env:read grant to read a value; the read
        is audited.
      </Notice>
      <section aria-labelledby="git-env-envs" className="flex flex-col gap-2">
        <h3 id="git-env-envs" className="m-0 text-sm">
          Environments
        </h3>
        <ul className="m-0 grid list-none gap-3 p-0 md:grid-cols-2">
          {envs.map((e) => (
            <li key={e.name} className="rounded-md border border-solid border-rule p-3">
              <p className="m-0 flex items-center gap-2">
                <span className="font-mono text-ink">{e.name}</span>
                <Badge>rev {e.revision}</Badge>
                {e.lastPromotion && (
                  <StatusTag status={PROMOTION[e.lastPromotion.status]}>
                    {e.lastPromotion.status}
                  </StatusTag>
                )}
              </p>
              <p className="m-0 text-xs text-muted">
                {e.branch} · {e.files.join(', ')}
              </p>
              {e.lastPromotion && (
                <p className="m-0 mt-1 text-xs text-muted">
                  Last promotion: {shortOid(e.lastPromotion.commit)} at seq{' '}
                  {e.lastPromotion.seq.toLocaleString('en-US')},{' '}
                  {formatRelative(e.lastPromotion.at)}.{' '}
                  {[
                    e.lastPromotion.added.length && `added ${e.lastPromotion.added.join(', ')}`,
                    e.lastPromotion.changed.length &&
                      `changed ${e.lastPromotion.changed.join(', ')}`,
                    e.lastPromotion.removed.length &&
                      `removed ${e.lastPromotion.removed.join(', ')}`,
                  ]
                    .filter(Boolean)
                    .join('; ')}
                </p>
              )}
            </li>
          ))}
        </ul>
      </section>

      <section aria-labelledby="git-env-keys" className="flex flex-col gap-2">
        <h3 id="git-env-keys" className="m-0 text-sm">
          Keys
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full border-collapse text-sm">
            <caption className="sr-only">
              Env keys with their status in each environment and their rotation
            </caption>
            <thead>
              <tr className="text-left text-muted">
                <th scope="col" className="py-1 pr-3 font-normal">
                  Key
                </th>
                <th scope="col" className="py-1 pr-3 font-normal">
                  Value
                </th>
                {envs.map((e) => (
                  <th key={e.name} scope="col" className="py-1 pr-3 font-normal">
                    {e.name}
                  </th>
                ))}
                <th scope="col" className="py-1 font-normal">
                  Rotation
                </th>
              </tr>
            </thead>
            <tbody>
              {keys.map((k) => (
                <tr key={k.key} className="border-0 border-t border-solid border-rule align-top">
                  <th scope="row" className="py-1.5 pr-3 text-left font-normal">
                    <span className="font-mono text-ink">{k.key}</span>
                    <span className="block text-xs text-muted">{k.file}</span>
                  </th>
                  <td className="py-1.5 pr-3 font-mono text-xs text-muted">
                    {k.plain ? (
                      <Badge title="Non-secret configuration kept in git (# loams:plain)">
                        plain
                      </Badge>
                    ) : (
                      <span title="Redacted; the fingerprint identifies the value without revealing it">
                        &lt;redacted fp={k.fingerprint}&gt;
                      </span>
                    )}
                  </td>
                  {envs.map((e) => {
                    const s = k.environments[e.name] ?? { status: 'absent' as const };
                    return (
                      <td key={e.name} className="py-1.5 pr-3">
                        <StatusTag status={KEY_STATUS[s.status].tag}>{keyStatusText(s)}</StatusTag>
                      </td>
                    );
                  })}
                  <td className="py-1.5 text-xs">
                    {k.rotation ? (
                      <span className="flex flex-col gap-0.5">
                        <span>
                          <StatusTag status={ROTATION[k.rotation.status].tag}>
                            {ROTATION[k.rotation.status].text}
                          </StatusTag>
                        </span>
                        <span className="text-muted">
                          {k.rotation.provider}
                          {k.rotation.every !== '-' && `, every ${k.rotation.every}`}
                          {k.rotation.lastRotatedAt &&
                            `, last ${formatRelative(k.rotation.lastRotatedAt)}`}
                          {k.rotation.nextDueAt && `, next ${formatRelative(k.rotation.nextDueAt)}`}
                        </span>
                      </span>
                    ) : (
                      <span className="text-muted">none</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <AuditLog client={client} namespace={namespace} repoId={repoId} environments={envs} />
    </div>
  );
}

function AuditLog(p: {
  client: GitClient;
  namespace: string;
  repoId: string;
  environments: Environment[];
}) {
  const { client, namespace, repoId } = p;
  const [environment, setEnvironment] = useState('');
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [next, setNext] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<GitFailure>();
  // One request at a time: a new page or a different filter aborts the one in flight, so
  // an older page for the previous filter never lands in the new list.
  const inflight = useRef<AbortController | null>(null);

  const load = useCallback(
    async (token: string) => {
      inflight.current?.abort();
      const ac = new AbortController();
      inflight.current = ac;
      setLoading(true);
      setError(undefined);
      try {
        const res = await client.listEnvAudit(
          { namespace, repoId, environment: environment || undefined, pageToken: token },
          ac.signal,
        );
        setEvents((prev) => (token ? [...prev, ...res.items] : res.items));
        setNext(res.nextPageToken);
      } catch (e) {
        if (isAbort(e)) return;
        setError(toFailure(e));
      } finally {
        if (inflight.current === ac) {
          inflight.current = null;
          setLoading(false);
        }
      }
    },
    [client, namespace, repoId, environment],
  );

  useEffect(() => {
    setEvents([]);
    setNext('');
    void load('');
    return () => inflight.current?.abort();
  }, [load]);

  return (
    <section aria-labelledby="git-env-audit" className="flex flex-col gap-2">
      <div className="flex flex-wrap items-end justify-between gap-2">
        <h3 id="git-env-audit" className="m-0 text-sm">
          Audit log
        </h3>
        <Field label="Environment">
          {(f) => (
            <Select {...f} value={environment} onChange={(e) => setEnvironment(e.target.value)}>
              <option value="">All</option>
              {p.environments.map((e) => (
                <option key={e.name} value={e.name}>
                  {e.name}
                </option>
              ))}
            </Select>
          )}
        </Field>
      </div>
      {error && (
        <Notice tone="danger" title="Could not read the audit log">
          {error.message}
        </Notice>
      )}
      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-sm">
          <caption className="sr-only">Env audit events, newest first; never values</caption>
          <thead>
            <tr className="text-left text-muted">
              <th scope="col" className="py-1 pr-3 font-normal">
                When
              </th>
              <th scope="col" className="py-1 pr-3 font-normal">
                Event
              </th>
              <th scope="col" className="py-1 pr-3 font-normal">
                Environment
              </th>
              <th scope="col" className="py-1 pr-3 font-normal">
                Keys
              </th>
              <th scope="col" className="py-1 pr-3 font-normal">
                Principal
              </th>
              <th scope="col" className="py-1 font-normal">
                Where
              </th>
            </tr>
          </thead>
          <tbody>
            {events.map((e) => (
              <tr key={e.id} className="border-0 border-t border-solid border-rule">
                <td className="py-1 pr-3 text-xs text-muted">
                  <time dateTime={e.at}>{formatRelative(e.at)}</time>
                </td>
                <td className="py-1 pr-3">
                  <Badge>{e.kind}</Badge>
                </td>
                <td className="py-1 pr-3 font-mono">{e.environment}</td>
                <td className="py-1 pr-3 font-mono text-xs">
                  {e.keys.map((k, i) => `${k}@v${e.versions[i] ?? '?'}`).join(', ')}
                </td>
                <td className="py-1 pr-3 font-mono text-xs">{e.principal}</td>
                <td className="py-1 font-mono text-xs text-muted">{e.subject || '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {loading && (
        <p className="m-0 text-sm text-muted" role="status">
          Loading events…
        </p>
      )}
      {!loading && !error && events.length === 0 && (
        <p className="m-0 text-sm text-muted">No audit events yet.</p>
      )}
      {next && !loading && (
        <Button size="sm" variant="quiet" onClick={() => void load(next)}>
          Older events
        </Button>
      )}
    </section>
  );
}
