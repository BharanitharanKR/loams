// Pure rules the page and the mock share: applying stream events, durations, what a
// stage's state means for its deployment, and when a rollback may be asked for.

import type {
  DeployEvent,
  Deployment,
  DeploymentState,
  PromotionStep,
  SecretEvent,
  Stage,
  StageKind,
  StageState,
} from './types.js';

export const STAGE_LABEL: Record<StageKind, string> = {
  build: 'Build',
  upload: 'Upload',
  validate: 'Validate',
  activate: 'Activate',
  rollback: 'Rollback',
};

export const STATE_LABEL: Record<StageState, string> = {
  pending: 'Pending',
  running: 'Running',
  succeeded: 'Succeeded',
  failed: 'Failed',
  skipped: 'Skipped',
};

export const DEPLOYMENT_STATE_LABEL: Record<DeploymentState, string> = {
  running: 'Deploying',
  succeeded: 'Live',
  failed: 'Failed',
  rolling_back: 'Rolling back',
  rolled_back: 'Rolled back',
};

/** The @loams/ui StatusTag colour for a state. */
export function statusTone(state: StageState | DeploymentState) {
  switch (state) {
    case 'succeeded':
      return 'done' as const;
    case 'running':
    case 'rolling_back':
      return 'progress' as const;
    case 'failed':
      return 'failed' as const;
    case 'rolled_back':
      return 'neutral' as const;
    default:
      return 'planned' as const;
  }
}

/** Applies one stream event to the list, newest deployment first. Never mutates. */
export function applyEvent(list: readonly Deployment[], ev: DeployEvent): Deployment[] {
  switch (ev.kind) {
    case 'snapshot':
      return sortNewest(ev.deployments);
    case 'deployment': {
      const rest = list.filter((d) => d.id !== ev.deployment.id);
      return sortNewest([ev.deployment, ...rest]);
    }
    case 'stage':
      return list.map((d) =>
        d.id !== ev.deploymentId
          ? d
          : {
              ...d,
              state: ev.state,
              stages: d.stages.map((s) => (s.kind === ev.stage.kind ? ev.stage : s)),
            },
      );
  }
}

function sortNewest(list: readonly Deployment[]): Deployment[] {
  return [...list].sort((a, b) => b.createdAt - a.createdAt);
}

/** Milliseconds a stage has taken (so far, while it runs). */
export function stageDuration(stage: Stage, now: number): number | undefined {
  if (stage.startedAt === undefined) return undefined;
  return Math.max(0, (stage.finishedAt ?? now) - stage.startedAt);
}

/** "850 ms", "12.4 s", "3 min 05 s". */
export function formatMs(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)} s`;
  const m = Math.floor(s / 60);
  const rest = Math.floor(s % 60);
  return `${m} min ${String(rest).padStart(2, '0')} s`;
}

/** A rollback needs somewhere to go back to, and nothing else in flight. */
export function canRollback(d: Deployment): boolean {
  if (!d.previousVersion) return false;
  return d.state === 'succeeded' || d.state === 'failed';
}

/** The deployment's state, read off its stages. */
export function deriveState(stages: readonly Stage[]): DeploymentState {
  const rollback = stages.find((s) => s.kind === 'rollback');
  if (rollback?.state === 'running') return 'rolling_back';
  if (rollback?.state === 'succeeded') return 'rolled_back';
  const main = stages.filter((s) => s.kind !== 'rollback');
  if (main.some((s) => s.state === 'failed')) return 'failed';
  if (main.every((s) => s.state === 'succeeded')) return 'succeeded';
  return 'running';
}

/** The secret and env events attached to one promotion step. */
export function eventsFor(step: PromotionStep, events: readonly SecretEvent[]): SecretEvent[] {
  return events.filter((e) => e.step === step.kind);
}

/** "2 promoted, 1 rotated": a per-kind count, in a fixed order. */
export function summariseSecrets(events: readonly SecretEvent[]): string {
  const order: SecretEvent['kind'][] = ['promoted', 'rotated', 'added', 'removed'];
  return order
    .map((k) => [k, events.filter((e) => e.kind === k).length] as const)
    .filter(([, n]) => n > 0)
    .map(([k, n]) => `${n} ${k}`)
    .join(', ');
}
