// What Studio shows when it cannot reach a daemon yet (§53 §5.1, §6.3): on the
// desktop, "Start the daemon"; on the web, "Connect an agentd host".

import { Button, Empty } from '@loams/ui';
import { useState } from 'react';
import type { Reach, StudioAgentd } from '../agentd.js';
import { studioHref } from '../routes.js';

export function NotConnected({ agentd, reach }: { agentd: StudioAgentd; reach: Reach }) {
  const [starting, setStarting] = useState(false);
  const [failure, setFailure] = useState<string>();
  if (reach.state === 'checking') {
    return <Empty title="Looking for the daemon…" />;
  }
  if (reach.state === 'no-host') {
    return (
      <Empty
        title="Connect an agentd host"
        actions={
          <a className="loams-btn loams-btn-primary" href={`#${studioHref({ kind: 'settings' })}`}>
            Agentd hosts
          </a>
        }
      >
        Studio in the browser works with a <code>loams-agentd</code> running on a machine you
        control, reached over TLS with your sign-in. Studio settings says how to run one.
      </Empty>
    );
  }
  if (reach.state === 'stopped' && agentd.start) {
    const start = async () => {
      setStarting(true);
      setFailure(undefined);
      try {
        await agentd.start?.();
      } catch (e) {
        setFailure(e instanceof Error ? e.message : String(e));
      } finally {
        setStarting(false);
      }
    };
    return (
      <Empty
        title="Start the daemon"
        actions={
          <Button variant="primary" onClick={start} disabled={starting}>
            {starting ? 'Starting…' : 'Start'}
          </Button>
        }
      >
        Studio runs agents, git and terminals through <code>loams-agentd</code> on this machine. It
        is not running.
        {failure && (
          <span role="alert" className="mt-2 block text-danger">
            {failure}
          </span>
        )}
      </Empty>
    );
  }
  const reason = reach.state === 'unavailable' ? reach.reason : 'stopped';
  return (
    <Empty title="The daemon is unavailable">
      Studio could not reach <code>loams-agentd</code> ({reason}). Check Settings › Background
      agents.
    </Empty>
  );
}
