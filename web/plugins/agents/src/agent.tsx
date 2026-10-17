import { useMutation, useQuery } from '@connectrpc/connect-query';
import {
  AuditTable,
  errorText,
  FLOW_LABEL,
  Form,
  iso,
  LoadError,
  Loading,
  Page,
  PageHead,
  RouteLink,
  Scoped,
  useAccount,
  useNotify,
  useTrail,
} from '@loams/admin-ui';
import type { RouterService, SessionService } from '@loams/console-host';
import { admin } from '@loams/proto';
import {
  Badge,
  Button,
  Card,
  DataTable,
  Dialog,
  Empty,
  Field,
  formatDuration,
  formatRelative,
  Input,
  Notice,
  Primary,
  SelectMenu,
  Snippet,
  Stat,
  Stats,
} from '@loams/ui';
import { Bot, Fingerprint, Pause, Play, Plus } from 'lucide-react';
import { type FormEvent, useEffect, useState } from 'react';
import { AgentStatusTag } from './agents.js';

/** Re-renders every `ms`, for countdowns. */
export function useNow(ms = 1000): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(t);
  }, [ms]);
  return now;
}

/** The token exchange a runtime with workload identity runs (RFC 8693; an HTTP endpoint, D602). */
export function exchangeSnippet(server: string, project: string, rule?: admin.PolicyRule): string {
  return `curl -X POST ${server}/api/v1/oauth/token \\
  -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \\
  -d subject_token="$WORKLOAD_OIDC_TOKEN" \\
  -d subject_token_type=urn:ietf:params:oauth:token-type:id_token \\
  -d audience=${project}/${rule?.environment || 'development'} \\
  -d scope="${(rule?.actions.length ? rule.actions : ['query']).join(' ')}"`;
}

export function AgentPage({
  project,
  agent,
  router,
  session,
  server,
}: {
  project: string;
  agent: string;
  router: RouterService;
  session: SessionService;
  server: string;
}) {
  const orgName = useAccount(session)?.org?.name ?? 'Organization';
  const projects = useQuery(admin.ProjectService.method.listProjects, {});
  const projectName = projects.data?.projects.find((p) => p.slug === project)?.name ?? project;
  const a = useQuery(admin.AgentService.method.getAgent, { agent });
  const tokens = useQuery(admin.AgentService.method.listAgentTokens, { agent });
  const trust = useQuery(admin.AgentService.method.listTrustPolicies, { agent });
  const audit = useQuery(admin.AuditService.method.listAuditEvents, { actor: agent, pageSize: 6 });
  const suspend = useMutation(admin.AgentService.method.suspendAgent);
  const resume = useMutation(admin.AgentService.method.resumeAgent);
  const revoke = useMutation(admin.KeyService.method.revokeToken);
  const notify = useNotify();
  const [status, setStatus] = useState<admin.AgentStatus>();
  const [confirm, setConfirm] = useState(false);
  const [addTrust, setAddTrust] = useState(false);
  const now = useNow();
  const data = a.data?.agent;
  useTrail(router, [
    { label: orgName, href: '/' },
    { label: projectName, href: `/projects/${project}` },
    { label: 'Agents', href: `/projects/${project}/agents` },
    { label: data?.name ?? '…' },
  ]);

  if (a.error) {
    return (
      <Page>
        <LoadError error={a.error} retry={() => void a.refetch()} />
      </Page>
    );
  }
  if (!data) return <Loading />;
  const current = status ?? data.status;
  const active = current !== admin.AgentStatus.SUSPENDED;
  const live = (tokens.data?.tokens ?? []).filter(
    (t) => new Date(iso(t.expiresAt) ?? 0).getTime() > now,
  );
  const lastActive = iso(data.lastActiveAt);

  const toggle = async () => {
    setConfirm(false);
    try {
      const r = active ? await suspend.mutateAsync({ agent }) : await resume.mutateAsync({ agent });
      const next = r.agent?.status ?? current;
      setStatus(next);
      notify(
        next === admin.AgentStatus.SUSPENDED
          ? `Suspended ${data.name}; its tokens are revoked`
          : `Resumed ${data.name}`,
      );
    } catch (e) {
      notify(errorText(e), 'danger');
    }
  };

  return (
    <Page>
      <PageHead
        eyebrow={
          <RouteLink to={`/projects/${project}/agents`} className="eyebrow-link">
            <Bot size={13} aria-hidden="true" /> Agents
          </RouteLink>
        }
        title={
          <span className="title-row">
            {data.name}
            <AgentStatusTag status={current} />
          </span>
        }
        actions={
          active ? (
            <Button variant="danger" onClick={() => setConfirm(true)}>
              <Pause size={15} aria-hidden="true" /> Suspend
            </Button>
          ) : (
            <Button variant="primary" onClick={toggle}>
              <Play size={15} aria-hidden="true" /> Resume
            </Button>
          )
        }
      >
        {data.description} Owned by {data.owner?.name}.
      </PageHead>

      <Stats>
        <Stat label="Live tokens" value={active ? live.length : 0} detail="Unexpired, right now" />
        <Stat
          label="Token lifetime cap"
          value={formatDuration(data.maxTokenTtlS)}
          detail="No refresh tokens"
        />
        <Stat
          label="Rate limit"
          value={`${data.limits?.requestsPerS ?? 0}/s`}
          detail={`${data.limits?.concurrentQueries ?? 0} concurrent queries`}
        />
        <Stat label="Last active" value={lastActive ? formatRelative(lastActive, now) : 'never'} />
      </Stats>

      <div className="grid-2" style={{ marginTop: 16 }}>
        <Card title="Policy">
          {data.policy.map((r) => (
            <div key={r.environment} className="rule">
              <span className="env-chip">
                <span className="env-dot" data-slug={r.environment} aria-hidden="true" />
                {r.environment}
              </span>
              <span className="chips">
                {r.actions.map((x) => (
                  <Badge key={x}>{x}</Badge>
                ))}
              </span>
              <span className="muted small">
                {r.collections.length ? `Only ${r.collections.join(', ')}` : 'All collections'}
              </span>
            </div>
          ))}
        </Card>
        <Card
          title="Trust policies"
          actions={
            <Button size="sm" onClick={() => setAddTrust(true)}>
              <Plus size={13} aria-hidden="true" /> Trust an identity
            </Button>
          }
        >
          {(trust.data?.trustPolicies ?? []).length === 0 ? (
            <p>
              None. This agent gets tokens only by user delegation (OAuth with PKCE, for example
              from an MCP client) or from a parent token.
            </p>
          ) : (
            (trust.data?.trustPolicies ?? []).map((t) => (
              <div key={t.id} className="trust">
                <Fingerprint size={16} aria-hidden="true" />
                <div>
                  <strong>{t.name}</strong>
                  <code className="small">{t.issuer}</code>
                  <span className="small">
                    subject <code>{t.subjectPattern}</code> → {t.environments.join(', ')}
                  </span>
                </div>
              </div>
            ))
          )}
        </Card>
      </div>

      <Card title="Tokens" flush style={{ marginTop: 16 }}>
        {!active ? (
          <div className="pad">
            <Notice tone="danger" title="Suspended">
              Every token was revoked, and exchanges are refused until the agent is resumed.
            </Notice>
          </div>
        ) : (
          <DataTable<admin.AgentToken>
            rows={live}
            rowKey={(t) => t.jti}
            empty={
              <div className="pad">
                <Empty title="No live tokens" grain="clay" seed={7}>
                  Tokens appear here while they are valid, and expire on their own.
                </Empty>
              </div>
            }
            columns={[
              { key: 'jti', header: 'Token', cell: (t) => <Badge>{t.jti}</Badge> },
              {
                key: 'flow',
                header: 'How',
                cell: (t) => (
                  <Primary
                    title={FLOW_LABEL[t.flow] ?? 'Token'}
                    detail={<code>{t.subject}</code>}
                  />
                ),
              },
              {
                key: 'for',
                header: 'Acting for',
                cell: (t) => t.actingFor?.name || <span className="muted">itself</span>,
              },
              { key: 'env', header: 'Environment', cell: (t) => t.environment },
              {
                key: 'scopes',
                header: 'Scopes',
                cell: (t) => (
                  <span className="chips">
                    {t.scopes.map((s) => (
                      <Badge key={s}>{s}</Badge>
                    ))}
                  </span>
                ),
              },
              {
                key: 'exp',
                header: 'Expires',
                sortValue: (t) => iso(t.expiresAt) ?? '',
                cell: (t) => (
                  <Countdown
                    until={iso(t.expiresAt) ?? ''}
                    issued={iso(t.issuedAt) ?? ''}
                    now={now}
                  />
                ),
              },
              {
                key: 'revoke',
                header: '',
                cell: (t) => (
                  <Button
                    variant="danger"
                    size="sm"
                    onClick={async () => {
                      try {
                        await revoke.mutateAsync({ jti: t.jti });
                        notify(`Revoked ${t.jti}`);
                        void tokens.refetch();
                      } catch (e) {
                        notify(errorText(e), 'danger');
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

      <Card title="Get a token" style={{ marginTop: 16 }}>
        <p>
          From a runtime with workload identity (GitHub Actions, Kubernetes, a cloud role), exchange
          its OIDC token:
        </p>
        <Snippet>{exchangeSnippet(server, project, data.policy[0])}</Snippet>
        <p>
          The answer is a token for {formatDuration(data.maxTokenTtlS)} at most; exchange again when
          it expires.
        </p>
      </Card>
      <Card title="Activity" flush style={{ marginTop: 16 }}>
        <AuditTable events={audit.data?.events ?? []} showProject={false} />
      </Card>

      <Dialog
        open={confirm}
        onClose={() => setConfirm(false)}
        title={`Suspend ${data.name}?`}
        footer={
          <>
            <Button onClick={() => setConfirm(false)}>Keep it running</Button>
            <Button variant="danger" onClick={toggle}>
              Suspend and revoke tokens
            </Button>
          </>
        }
      >
        <Scoped>
          <p className="muted">
            Its {live.length} live tokens stop working within seconds, and new exchanges are
            refused. You can resume it later.
          </p>
        </Scoped>
      </Dialog>
      <AddTrust
        agent={agent}
        open={addTrust}
        onClose={() => setAddTrust(false)}
        onCreated={() => void trust.refetch()}
      />
    </Page>
  );
}

function Countdown({ until, issued, now }: { until: string; issued: string; now: number }) {
  const end = new Date(until).getTime();
  const start = new Date(issued).getTime();
  const left = Math.max(0, Math.round((end - now) / 1000));
  const ratio = Math.max(0, Math.min(1, (end - now) / Math.max(1, end - start)));
  return (
    <span className="countdown" title={new Date(until).toLocaleString()}>
      <span
        className="countdown-bar"
        style={{ transform: `scaleX(${ratio})` }}
        data-low={ratio < 0.2 || undefined}
      />
      <span className="nowrap">
        {Math.floor(left / 60)}:{String(left % 60).padStart(2, '0')}
      </span>
    </span>
  );
}

const PRESETS = [
  {
    name: 'GitHub Actions',
    issuer: 'https://token.actions.githubusercontent.com',
    subject: 'repo:org/repo:ref:refs/heads/main',
  },
  {
    name: 'Kubernetes service account',
    issuer: 'https://oidc.eks.<region>.amazonaws.com/id/<cluster>',
    subject: 'system:serviceaccount:<ns>:<name>',
  },
  {
    name: 'Google Cloud',
    issuer: 'https://accounts.google.com',
    subject: '<service-account-unique-id>',
  },
] as const;

function AddTrust({
  agent,
  open,
  onClose,
  onCreated,
}: {
  agent: string;
  open: boolean;
  onClose: () => void;
  onCreated: () => void;
}) {
  const [preset, setPreset] = useState('0');
  const chosen = PRESETS[Number(preset)] ?? PRESETS[0];
  const [issuer, setIssuer] = useState<string>(chosen.issuer);
  const [subject, setSubject] = useState<string>(chosen.subject);
  const [audience, setAudience] = useState('loams');
  const [envs, setEnvs] = useState('development');
  const [error, setError] = useState<string>();
  const create = useMutation(admin.AgentService.method.createTrustPolicy);
  const notify = useNotify();

  const submit = async (ev: FormEvent) => {
    ev.preventDefault();
    if (!issuer.startsWith('https://')) return setError('The issuer is an https URL.');
    try {
      const r = await create.mutateAsync({
        agent,
        name: chosen.name,
        issuer,
        audience,
        subjectPattern: subject,
        environments: envs
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean),
      });
      notify(`Trusted ${r.trustPolicy?.name ?? chosen.name}`);
      onCreated();
      onClose();
    } catch (e) {
      setError(errorText(e));
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Trust a workload identity"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="new-trust" disabled={create.isPending}>
            Add trust policy
          </Button>
        </>
      }
    >
      <Form id="new-trust" onSubmit={submit}>
        <Field label="Runtime">
          {() => (
            <SelectMenu
              label="Runtime"
              value={preset}
              onValueChange={(i) => {
                setPreset(i);
                const next = PRESETS[Number(i)];
                setIssuer(next?.issuer ?? '');
                setSubject(next?.subject ?? '');
              }}
              options={PRESETS.map((x, i) => ({ value: String(i), label: x.name }))}
            />
          )}
        </Field>
        <Field label="Issuer" error={error}>
          {(p) => <Input {...p} value={issuer} onChange={(e) => setIssuer(e.target.value)} />}
        </Field>
        <Field
          label="Subject"
          hint="A glob over the token's sub claim; * matches any run of characters."
        >
          {(p) => (
            <Input
              {...p}
              value={subject}
              onChange={(e) => setSubject(e.target.value)}
              className={/* mono-ok: id */ 'loams-mono'}
            />
          )}
        </Field>
        <Field label="Audience">
          {(p) => <Input {...p} value={audience} onChange={(e) => setAudience(e.target.value)} />}
        </Field>
        <Field label="Environments" hint="Comma-separated; the agent's policy still applies.">
          {(p) => <Input {...p} value={envs} onChange={(e) => setEnvs(e.target.value)} />}
        </Field>
      </Form>
    </Dialog>
  );
}
