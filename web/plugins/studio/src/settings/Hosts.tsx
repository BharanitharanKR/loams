// Agentd hosts for Studio in the browser (§53 §6.3, D909). A host is a
// `loams-agentd serve --remote` the person runs and the console reaches over TLS
// with their own short-lived token. The registry and its transport are SD1 Task 7's
// and move here when they land; until then this section says how to run a host.

import { Badge, Card, Empty, Snippet } from '@loams/ui';
import type { StudioAgentd } from '../agentd.js';

export function Hosts({ agentd }: { agentd: StudioAgentd }) {
  const host = agentd.host();
  return (
    <Card title="Agentd hosts">
      {host ? (
        <p className="m-0 flex items-center gap-2">
          <span className="text-ink">{host.name}</span>
          <Badge>connected</Badge>
        </p>
      ) : (
        <Empty title="No agentd hosts">
          This console cannot register a remote <code>loams-agentd</code> yet. When it can, a host
          runs:
          <Snippet copy={false}>
            {'loams-agentd serve --remote --listen 0.0.0.0:8443 --audience loams-agentd …'}
          </Snippet>
        </Empty>
      )}
    </Card>
  );
}
