// A thread's usage in its header (SD1 Task 14; §53 §6.5 G20): the tokens and time of its
// turns so far, from `ThreadService.GetThreadUsage`, with each turn in the tooltip. It is
// informational, never billing: a cost appears only when the harness itself reported one.
// Nothing shows while the thread has no finished turn, or when the daemon does not serve
// usage (or fails): the header is no place for an error.

import { useAgentdQuery } from '@loams/console-host/query';
import { type GetThreadUsageResponse, ThreadService, type UsageTotals } from '@loams/proto/agentd';
import { formatNumber } from '@loams/ui';
import { Gauge } from 'lucide-react';

/** How often the header refreshes while a thread is open. */
const REFRESH_MS = 15_000;

/** `95 s` → `1m 35s`; under a minute, seconds; over an hour, hours and minutes. */
export function formatElapsed(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

/** `0.0125` → `$0.013`; whole cents above a dollar. */
export function formatCost(usd: number): string {
  if (usd === 0) return '$0';
  return usd >= 1 ? `$${usd.toFixed(2)}` : `$${usd.toPrecision(2)}`;
}

const n = (v: bigint | undefined) => (v === undefined ? undefined : Number(v));

/** The header's parts: tokens in, out and cached, the cost if reported, the time. */
export function usageParts(
  totals: Pick<
    UsageTotals,
    'inputTokens' | 'outputTokens' | 'cacheTokens' | 'costUsd' | 'durationMs'
  >,
): string[] {
  const parts: string[] = [];
  const input = n(totals.inputTokens);
  const output = n(totals.outputTokens);
  const cache = n(totals.cacheTokens);
  if (input !== undefined) parts.push(`${formatNumber(input)} in`);
  if (output !== undefined) parts.push(`${formatNumber(output)} out`);
  if (cache !== undefined && cache > 0) parts.push(`${formatNumber(cache)} cached`);
  if (totals.costUsd !== undefined) parts.push(formatCost(totals.costUsd));
  parts.push(formatElapsed(Number(totals.durationMs)));
  return parts;
}

function tooltip(usage: GetThreadUsageResponse): string {
  const lines = usage.turns.map((turn, i) => {
    return `Turn ${i + 1}: ${usageParts(turn).join(' · ')}`;
  });
  return [
    `${usage.turns.length} turn${usage.turns.length === 1 ? '' : 's'}, as the harness reported them`,
    ...lines,
  ].join('\n');
}

export function ThreadUsage({ threadId }: { threadId: string }) {
  const usage = useAgentdQuery(
    ThreadService.method.getThreadUsage,
    { chatId: threadId },
    { refetchInterval: REFRESH_MS, retry: false },
  );
  const data = usage.data;
  if (!data || data.turns.length === 0 || !data.totals) return null;
  return (
    <span
      role="note"
      aria-label="Thread usage"
      className="inline-flex h-7 shrink-0 items-center gap-1.5 rounded-pill border border-rule bg-bg px-2.5 text-[0.76rem] text-muted tabular-nums max-[720px]:hidden"
      title={tooltip(data)}
      data-thread-usage
    >
      <Gauge size={13} aria-hidden="true" />
      <span>{usageParts(data.totals).join(' · ')}</span>
    </span>
  );
}
