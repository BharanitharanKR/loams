// The deployments client contract. There is no `loams.deployments.v1` proto yet, so the
// page talks to this typed interface; `mock.ts` implements it from fixtures until the
// service exists, and a Connect-backed client replaces it without touching the page.

import type { SeriesPoint } from '@loams/charts';

/** A deployment's stages, in pipeline order. `rollback` runs only when asked for. */
export const STAGE_KINDS = ['build', 'upload', 'validate', 'activate', 'rollback'] as const;
export type StageKind = (typeof STAGE_KINDS)[number];

export type StageState = 'pending' | 'running' | 'succeeded' | 'failed' | 'skipped';

export interface Stage {
  kind: StageKind;
  state: StageState;
  /** 0..1; only meaningful while running. */
  progress: number;
  /** Epoch milliseconds. */
  startedAt?: number;
  finishedAt?: number;
  /** One line from the stage's runner, e.g. why it failed. */
  message?: string;
}

export type DeploymentState = 'running' | 'succeeded' | 'failed' | 'rolling_back' | 'rolled_back';

export interface Deployment {
  id: string;
  service: string;
  environment: EnvName;
  version: string;
  /** The version a rollback returns to; absent for a first deploy. */
  previousVersion?: string;
  commit: string;
  author: string;
  createdAt: number;
  state: DeploymentState;
  stages: Stage[];
}

/** What `watchDeployments` streams: a full snapshot first, then granular updates. */
export type DeployEvent =
  | { kind: 'snapshot'; deployments: Deployment[] }
  | { kind: 'deployment'; deployment: Deployment }
  | { kind: 'stage'; deploymentId: string; stage: Stage; state: DeploymentState };

export interface LogLine {
  at: number;
  level: 'info' | 'warn' | 'error';
  text: string;
}

// ---- Git environment promotion -------------------------------------------------------

export type EnvName = 'dev' | 'main';

export const PROMOTION_STEPS = [
  'workspace',
  'review',
  'merge_dev',
  'promote_dev',
  'merge_main',
  'promote_main',
] as const;
export type PromotionStepKind = (typeof PROMOTION_STEPS)[number];

export interface PromotionStep {
  kind: PromotionStepKind;
  state: StageState;
  at?: number;
  actor?: string;
  /** A ref the step produced: a review number, a merge commit. Never a URL. */
  ref?: string;
}

/**
 * A change to an environment's secrets or env vars that rode along with a promotion.
 * Only the key's name is ever carried; values never reach the console.
 */
export interface SecretEvent {
  id: string;
  kind: 'promoted' | 'rotated' | 'added' | 'removed';
  key: string;
  env: EnvName;
  step: PromotionStepKind;
  at: number;
}

export interface Promotion {
  id: string;
  workspace: string;
  title: string;
  steps: PromotionStep[];
  secretEvents: SecretEvent[];
}

// ---- Metrics ---------------------------------------------------------------------------

export type DeployMetric = 'frequency' | 'failure_rate';

export interface DeployClient {
  /** Snapshot then live updates until `signal` aborts. */
  watchDeployments(signal: AbortSignal): AsyncIterable<DeployEvent>;
  /** Starts the rollback stage; progress arrives on `watchDeployments`. */
  rollback(deploymentId: string): Promise<void>;
  stageLogs(deploymentId: string, stage: StageKind): Promise<LogLine[]>;
  /** Snapshot then live updates until `signal` aborts. */
  watchPromotions(signal: AbortSignal): AsyncIterable<Promotion[]>;
  /** The window the timeline opens with. */
  metricHistory(metric: DeployMetric, windowMs: number): Promise<SeriesPoint[]>;
  /** Each subscription is independent; ending the iteration stops it. */
  metricStream(metric: DeployMetric): AsyncIterable<SeriesPoint[]>;
}
