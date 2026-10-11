import type { AgentdStatus } from '@loams/desktop/contracts';
import { Button, Card, Empty, formatRelative, Notice, StatusTag } from '@loams/ui';
import { useId, useState } from 'react';
import { ConsentDialog, StopDialog } from './consent-dialog.js';
import type { AgentdApi } from './mock.js';
import {
  backgroundLabel,
  modeLabel,
  SESSION_LABEL,
  SESSION_TONE,
  STATE_LABEL,
  STATE_TONE,
  useAgentdStatus,
  useBackground,
  useSessions,
} from './state.js';

type Pending = null | 'start' | 'stop' | 'consent' | `reattach:${string}`;

/**
 * Settings › Background agents: the loams-agentd daemon's state, version and
 * data directory, its sessions with reattach, the background-mode choice, and
 * an explicit start and stop.
 */
export function AgentdSection({
  agentd,
  navigate,
}: {
  agentd: AgentdApi;
  navigate: (path: string) => void;
}) {
  const status = useAgentdStatus(agentd);
  const [bg, setBg] = useBackground(agentd);
  const sessions = useSessions(agentd, status);
  const [pending, setPending] = useState<Pending>(null);
  const [error, setError] = useState<string | null>(null);
  const [consentOpen, setConsentOpen] = useState(false);
  const [stopOpen, setStopOpen] = useState(false);
  const sessionsHeading = useId();

  const start = async () => {
    setError(null);
    if (bg === 'ask') {
      setConsentOpen(true);
      return;
    }
    setPending('start');
    const r = await agentd.start();
    setPending(null);
    if (!r.ok) {
      if (r.code === 'consent_required') setConsentOpen(true);
      else setError(r.message);
    }
  };

  const answer = async (v: 'on' | 'off') => {
    setPending('consent');
    setError(null);
    const r = await agentd.background.set(v);
    if (!r.ok) {
      setPending(null);
      setError(r.message);
      return;
    }
    setBg(v);
    setConsentOpen(false);
    const s = await agentd.start();
    setPending(null);
    if (!s.ok) setError(s.message);
  };

  const stop = async () => {
    setPending('stop');
    setError(null);
    const r = await agentd.stop({ confirmed: true });
    setPending(null);
    setStopOpen(false);
    if (!r.ok) setError(r.message);
  };

  const reattach = async (id: string) => {
    setPending(`reattach:${id}`);
    setError(null);
    const r = await agentd.reattach(id);
    setPending(null);
    if (r.ok) navigate(r.value.route);
    else setError(r.message);
  };

  if (!status) {
    return (
      <Card title="Background agents">
        <p className="lc-muted" role="status">
          Reading the agent daemon's state…
        </p>
      </Card>
    );
  }
  const running = status.state === 'running';

  return (
    <div className="lc-agentd">
      <Card
        title="Background agents"
        actions={
          <StatusTag status={STATE_TONE[status.state]}>
            {running && status.draining ? 'Stopping' : STATE_LABEL[status.state]}
          </StatusTag>
        }
      >
        <StatusDetails status={status} />
        {status.state === 'unavailable' && (
          <Notice tone="danger" title="The agent daemon is unavailable">
            {status.reason}
          </Notice>
        )}
        {error && (
          <div role="alert">
            <Notice tone="danger" title="That did not work">
              {error}
            </Notice>
          </div>
        )}
        <dl className="lc-agentd-facts">
          <div>
            <dt>Background mode</dt>
            <dd>
              {bg === null ? '…' : backgroundLabel(bg)}{' '}
              {bg !== null && (
                <Button
                  variant="quiet"
                  onClick={() => setConsentOpen(true)}
                  aria-haspopup="dialog"
                >
                  Change
                </Button>
              )}
            </dd>
          </div>
        </dl>
        <div className="lc-actions">
          {running ? (
            <Button
              onClick={() => setStopOpen(true)}
              disabled={status.draining || pending !== null}
              aria-haspopup="dialog"
            >
              Stop background agents
            </Button>
          ) : (
            <Button
              variant="primary"
              onClick={() => void start()}
              disabled={pending !== null || status.state === 'unavailable'}
            >
              {pending === 'start' ? 'Starting…' : 'Start background agents'}
            </Button>
          )}
        </div>
      </Card>

      <section aria-labelledby={sessionsHeading}>
        <h3 id={sessionsHeading}>Agent sessions</h3>
        {!running ? (
          <Empty title="No daemon running">Start background agents to see their sessions.</Empty>
        ) : sessions.kind === 'loading' ? (
          <p className="lc-muted" role="status">
            Loading sessions…
          </p>
        ) : sessions.kind === 'error' ? (
          <Notice tone="danger" title="Could not list sessions">
            {sessions.message}
          </Notice>
        ) : sessions.sessions.length === 0 ? (
          <Empty title="No sessions">Agent sessions appear here while they run.</Empty>
        ) : (
          <ul className="lc-agentd-sessions" aria-label="Agent sessions">
            {sessions.sessions.map((s) => (
              <li key={s.id}>
                <div>
                  <strong>{s.title || s.id}</strong>
                  <span className="lc-muted">
                    {' '}
                    {s.harness} · updated{' '}
                    {formatRelative(new Date(s.updatedAtMs).toISOString())}
                  </span>
                </div>
                <StatusTag status={SESSION_TONE[s.state]}>{SESSION_LABEL[s.state]}</StatusTag>
                {s.pendingApprovals > 0 && (
                  <span className="lc-muted">
                    {s.pendingApprovals} approval{s.pendingApprovals === 1 ? '' : 's'} waiting
                  </span>
                )}
                <Button
                  size="sm"
                  disabled={!s.resumable || pending !== null || status.draining}
                  aria-label={`Reattach to ${s.title || s.id}`}
                  title={s.resumable ? undefined : 'This session cannot be resumed'}
                  onClick={() => void reattach(s.id)}
                >
                  {pending === `reattach:${s.id}` ? 'Reattaching…' : 'Reattach'}
                </Button>
              </li>
            ))}
          </ul>
        )}
      </section>

      <ConsentDialog
        open={consentOpen}
        busy={pending === 'consent'}
        onAnswer={(v) => void answer(v)}
        onClose={() => setConsentOpen(false)}
      />
      <StopDialog
        open={stopOpen}
        busy={pending === 'stop'}
        runningTurns={running ? status.runningTurns : 0}
        service={running && status.mode === 'service'}
        onConfirm={() => void stop()}
        onClose={() => setStopOpen(false)}
      />
    </div>
  );
}

function StatusDetails({ status }: { status: AgentdStatus }) {
  if (status.state === 'unavailable') return null;
  const rows: [string, string][] =
    status.state === 'running'
      ? [
          ['Mode', modeLabel(status.mode)],
          ['Version', `${status.version} (protocol ${status.protocol})`],
          ['Process', `pid ${status.pid}`],
          ['Started', formatRelative(new Date(status.startedAtMs).toISOString())],
          ['Running turns', String(status.runningTurns)],
          ['Pending approvals', String(status.pendingApprovals)],
          ['Data directory', status.dataDir],
        ]
      : [
          ...(status.version ? ([['Version', status.version]] as [string, string][]) : []),
          ['Data directory', status.dataDir],
        ];
  return (
    <dl className="lc-agentd-facts">
      {rows.map(([k, v]) => (
        <div key={k}>
          <dt>{k}</dt>
          <dd>{k === 'Data directory' ? <code>{v}</code> : v}</dd>
        </div>
      ))}
    </dl>
  );
}
