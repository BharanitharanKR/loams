// What the canvas knows about each service kind: its name, the console page it deep-links
// to, and the one metric its node's sparkline plots.

import type { Service, ServiceKind, ServiceStatus } from './client.js';

export interface KindInfo {
  kind: ServiceKind;
  /** Singular, for a node's kicker and the add palette. */
  label: string;
  /** The add palette's one-line description. */
  blurb: string;
  /** The product page's console path. */
  page: string;
  /** The product page's name, for "Open in ...". */
  product: string;
  metric: { name: string; unit: string };
  /** A short glyph shown on the node; text, so it needs no icon set and reads in any theme. */
  glyph: string;
}

export const KINDS: Readonly<Record<ServiceKind, KindInfo>> = {
  'postgres-branch': {
    kind: 'postgres-branch',
    label: 'Postgres branch',
    blurb: 'A branch compute on Loams Postgres.',
    page: '/postgres',
    product: 'Postgres',
    metric: { name: 'CPU', unit: '%' },
    glyph: 'PG',
  },
  'sql-database': {
    kind: 'sql-database',
    label: 'Loams SQL database',
    blurb: 'A MySQL-compatible Loams SQL database.',
    page: '/wesql',
    product: 'Loams SQL',
    metric: { name: 'Queries', unit: '/s' },
    glyph: 'SQL',
  },
  'live-deployment': {
    kind: 'live-deployment',
    label: 'Live deployment',
    blurb: 'A Live app deployment serving requests.',
    page: '/live',
    product: 'Live',
    metric: { name: 'Requests', unit: '/s' },
    glyph: 'LV',
  },
  graph: {
    kind: 'graph',
    label: 'Graph',
    blurb: 'A stored property graph, queried with GQL.',
    page: '/graph',
    product: 'Graph',
    metric: { name: 'Queries', unit: '/s' },
    glyph: 'GR',
  },
  house: {
    kind: 'house',
    label: 'House',
    blurb: 'An analytical House warehouse.',
    page: '/house',
    product: 'House',
    metric: { name: 'Scanned', unit: 'MB/s' },
    glyph: 'HS',
  },
  'jobs-queue': {
    kind: 'jobs-queue',
    label: 'Jobs queue',
    blurb: 'A durable queue of background jobs.',
    page: '/durable',
    product: 'Durable',
    metric: { name: 'Backlog', unit: 'jobs' },
    glyph: 'JQ',
  },
  connector: {
    kind: 'connector',
    label: 'Connector',
    blurb: 'A source or sink connector moving data between services.',
    page: '/connectors',
    product: 'Connectors',
    metric: { name: 'Throughput', unit: 'rec/s' },
    glyph: 'CN',
  },
  'git-repo': {
    kind: 'git-repo',
    label: 'Git repository',
    blurb: 'A Loams Git repository that deployments build from.',
    page: '/factory',
    product: 'Software Factory',
    metric: { name: 'Pushes', unit: '/h' },
    glyph: 'GIT',
  },
};

/** The palette's order: data first, then compute, then integration. */
export const KIND_ORDER: readonly ServiceKind[] = [
  'postgres-branch',
  'sql-database',
  'graph',
  'house',
  'live-deployment',
  'jobs-queue',
  'connector',
  'git-repo',
];

export const kindInfo = (kind: ServiceKind): KindInfo => KINDS[kind];

/** The console path the side panel's deep link opens. */
export function serviceHref(s: Service): string {
  if (s.href) return s.href;
  if (s.kind === 'connector') return `/connectors/${encodeURIComponent(s.id)}`;
  return KINDS[s.kind].page;
}

export const STATUS_LABEL: Readonly<Record<ServiceStatus, string>> = {
  healthy: 'Healthy',
  deploying: 'Deploying',
  degraded: 'Degraded',
  failed: 'Failed',
  stopped: 'Stopped',
};

/** The shared StatusTag colour for each live status. */
export const STATUS_TONE: Readonly<
  Record<ServiceStatus, 'done' | 'progress' | 'planned' | 'failed' | 'neutral'>
> = {
  healthy: 'done',
  deploying: 'progress',
  degraded: 'progress',
  failed: 'failed',
  stopped: 'planned',
};
