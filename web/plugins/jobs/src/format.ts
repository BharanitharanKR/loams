// Pure helpers for the Jobs page: labels, rates, times and the payload preview.

import type { Status } from '@loams/ui';
import type { AttemptOutcome, JobState, Payload } from './client.js';

const STATE_LABEL: Record<JobState, string> = {
  waiting: 'Waiting',
  prioritized: 'Prioritized',
  delayed: 'Delayed',
  active: 'Active',
  waiting_children: 'Waiting for children',
  completed: 'Completed',
  failed: 'Failed',
  dead_lettered: 'Dead-lettered',
  canceled: 'Canceled',
};

export const stateLabel = (s: JobState): string => STATE_LABEL[s];

/** A state's colour in the shared status palette. */
export function stateStatus(s: JobState): Status {
  switch (s) {
    case 'completed':
      return 'done';
    case 'active':
      return 'progress';
    case 'failed':
    case 'dead_lettered':
      return 'failed';
    case 'canceled':
      return 'neutral';
    default:
      return 'planned';
  }
}

const OUTCOME_LABEL: Record<AttemptOutcome, string> = {
  running: 'Running',
  ok: 'Succeeded',
  retry: 'Retried',
  fail: 'Failed',
  stalled: 'Stalled',
  released: 'Released',
};

export const outcomeLabel = (o: AttemptOutcome): string => OUTCOME_LABEL[o];

export function outcomeStatus(o: AttemptOutcome): Status {
  if (o === 'ok') return 'done';
  if (o === 'running') return 'progress';
  if (o === 'fail' || o === 'stalled') return 'failed';
  return 'planned';
}

/** "12.3/s", "0.4/s", "0/s". */
export function rateText(perSecond: number): string {
  if (!Number.isFinite(perSecond) || perSecond <= 0) return '0/s';
  if (perSecond >= 100) return `${Math.round(perSecond)}/s`;
  return `${perSecond.toFixed(1)}/s`;
}

/** "45 s", "12 min", "3.5 h", "2.0 d". */
export function ageText(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return '—';
  if (seconds < 60) return `${Math.round(seconds)} s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} min`;
  if (seconds < 86_400) return `${(seconds / 3600).toFixed(1)} h`;
  return `${(seconds / 86_400).toFixed(1)} d`;
}

/** An RFC 3339 time as "2026-10-10 12:00:05 UTC": stable across locales and in tests. */
export function timeText(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return `${d.toISOString().slice(0, 19).replace('T', ' ')} UTC`;
}

/** Seconds as "1.2 s", "3 min 4 s", "2 h 5 min". */
export function secondsText(seconds: number): string {
  if (seconds < 60) return `${seconds < 10 ? seconds.toFixed(1) : Math.round(seconds)} s`;
  const m = Math.floor(seconds / 60);
  if (m < 60) return `${m} min ${Math.round(seconds % 60)} s`;
  return `${Math.floor(m / 60)} h ${m % 60} min`;
}

/** The longest preview the page renders, in characters. */
export const PREVIEW_LIMIT = 4096;

export type Preview =
  | { kind: 'text'; text: string; truncated: boolean }
  | { kind: 'external'; blobRef: string }
  | { kind: 'binary' };

/** What the payload panel shows: pretty JSON, plain text, an object-store reference, or nothing. */
export function payloadPreview(p: Payload): Preview {
  if (p.text === undefined)
    return p.blobRef ? { kind: 'external', blobRef: p.blobRef } : { kind: 'binary' };
  let text = p.text;
  if (/^application\/(.+\+)?json\b/.test(p.contentType)) {
    try {
      text = JSON.stringify(JSON.parse(p.text), null, 2);
    } catch {
      // Not valid JSON after all: show it as it came.
    }
  }
  const truncated = text.length > PREVIEW_LIMIT;
  return { kind: 'text', text: truncated ? text.slice(0, PREVIEW_LIMIT) : text, truncated };
}

/** The sum of the counts that are still to run. */
export function backlog(counts: Record<JobState, number>): number {
  return counts.waiting + counts.prioritized + counts.delayed + counts.waiting_children;
}
