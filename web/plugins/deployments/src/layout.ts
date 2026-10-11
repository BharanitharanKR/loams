// Deployment and promotion pipelines -> React Flow nodes and edges. Positions come from
// @loams-core/flows' `compileGraph` (the same longest-path ranking the dashboard graph
// tile uses); this module only swaps the stock node for the `stage` node and paints
// the edges by state.

import { compileGraph } from '@loams-core/flows';
import type { Edge, Node } from '@xyflow/react';
import { eventsFor, formatMs, STAGE_LABEL, STATE_LABEL, stageDuration } from './model.js';
import type {
  Deployment,
  Promotion,
  PromotionStepKind,
  SecretEvent,
  StageKind,
  StageState,
} from './types.js';

export const NODE_WIDTH = 188;
export const NODE_HEIGHT = 112;
/** Promotion steps carry their key events under the bar, so they stand taller. */
export const PROMOTION_NODE_HEIGHT = 168;

export const PROMOTION_LABEL: Record<PromotionStepKind, string> = {
  workspace: 'Workspace',
  review: 'Review',
  merge_dev: 'Merge to dev',
  promote_dev: 'Env promoted (dev)',
  merge_main: 'Merge to main',
  promote_main: 'Env promoted (main)',
};

/** What a `stage` node draws. Shared by deployment stages and promotion steps. */
export type StageNodeData = {
  label: string;
  state: StageState;
  progress: number;
  /** Shown under the label: a duration, an actor, a ref. */
  detail?: string;
  /** Secret and env key events (names only), for promotion steps. */
  secrets?: SecretEvent[];
  /** A deployment stage that has logs to open. */
  logs?: { deploymentId: string; stage: StageKind };
  /** Drawn dashed and apart: the rollback branch. */
  branch?: boolean;
  reducedMotion: boolean;
};

export type StageNode = Node<StageNodeData, 'stage'>;

const LAYOUT = {
  direction: 'LR',
  nodeWidth: NODE_WIDTH,
  nodeHeight: NODE_HEIGHT,
  rankSep: 40,
  nodeSep: 24,
} as const;

function edgeStyle(state: StageState, branch: boolean) {
  const stroke =
    state === 'failed'
      ? 'var(--op-danger)'
      : state === 'succeeded'
        ? 'var(--op-grow)'
        : state === 'running'
          ? 'var(--op-accent)'
          : 'var(--op-rule)';
  return { stroke, strokeWidth: 2, ...(branch ? { strokeDasharray: '6 4' } : {}) };
}

function chain(
  ids: string[],
  extra: { source: string; target: string }[] = [],
): { nodes: { id: string; label: string }[]; edges: { source: string; target: string }[] } {
  const edges = ids.slice(1).map((id, i) => ({ source: ids[i] as string, target: id }));
  return { nodes: ids.map((id) => ({ id, label: id })), edges: [...edges, ...extra] };
}

function positions(
  spec: ReturnType<typeof chain>,
  nodeHeight: number = NODE_HEIGHT,
): Map<string, { x: number; y: number }> {
  const compiled = compileGraph({ ...spec, layout: { ...LAYOUT, nodeHeight } }, []);
  return new Map(compiled.nodes.map((n) => [n.id, n.position]));
}

/**
 * A deployment's pipeline: build -> upload -> validate -> activate, and the rollback
 * branch off activate. The branch sits below the main line rather than after it.
 */
export function deploymentGraph(
  d: Deployment,
  now: number,
  reducedMotion: boolean,
): { nodes: StageNode[]; edges: Edge[] } {
  const main = d.stages.filter((s) => s.kind !== 'rollback').map((s) => s.kind);
  const spec = chain(main);
  const pos = positions(spec);
  const activate = pos.get('activate') ?? { x: 0, y: 0 };
  const rollbackStage = d.stages.find((s) => s.kind === 'rollback');
  const showRollback = !!rollbackStage && rollbackStage.state !== 'pending';
  const nodes: StageNode[] = d.stages
    .filter((s) => s.kind !== 'rollback' || showRollback)
    .map((s) => {
      const ms = stageDuration(s, now);
      const branch = s.kind === 'rollback';
      return {
        id: s.kind,
        type: 'stage',
        position: branch
          ? { x: activate.x, y: activate.y + NODE_HEIGHT + LAYOUT.nodeSep * 2 }
          : (pos.get(s.kind) ?? { x: 0, y: 0 }),
        width: NODE_WIDTH,
        height: NODE_HEIGHT,
        draggable: false,
        connectable: false,
        ariaLabel: `${STAGE_LABEL[s.kind]}: ${STATE_LABEL[s.state]}${ms === undefined ? '' : `, ${formatMs(ms)}`}${s.message ? `. ${s.message}` : ''}`,
        data: {
          label: STAGE_LABEL[s.kind],
          state: s.state,
          progress: s.progress,
          detail: ms === undefined ? undefined : formatMs(ms),
          logs: s.startedAt === undefined ? undefined : { deploymentId: d.id, stage: s.kind },
          branch,
          reducedMotion,
        },
      };
    });
  const byKind = new Map(d.stages.map((s) => [s.kind, s]));
  const edges: Edge[] = spec.edges.map((e) => {
    const state = byKind.get(e.target as StageKind)?.state ?? 'pending';
    return {
      id: `${e.source}->${e.target}`,
      source: e.source,
      target: e.target,
      animated: state === 'running' && !reducedMotion,
      style: edgeStyle(state, false),
    };
  });
  if (showRollback && rollbackStage) {
    edges.push({
      id: 'activate->rollback',
      source: 'activate',
      target: 'rollback',
      sourceHandle: 'branch',
      targetHandle: 'branch-in',
      animated: rollbackStage.state === 'running' && !reducedMotion,
      style: edgeStyle(rollbackStage.state, true),
    });
  }
  return { nodes, edges };
}

/** A promotion: workspace -> review -> merge to dev -> promoted (dev) -> merge to main -> promoted (main). */
export function promotionGraph(
  p: Promotion,
  reducedMotion: boolean,
): { nodes: StageNode[]; edges: Edge[] } {
  const spec = chain(p.steps.map((s) => s.kind));
  const pos = positions(spec, PROMOTION_NODE_HEIGHT);
  const nodes: StageNode[] = p.steps.map((s) => {
    const secrets = eventsFor(s, p.secretEvents);
    const detail = [s.actor, s.ref].filter(Boolean).join(' · ') || undefined;
    return {
      id: s.kind,
      type: 'stage',
      position: pos.get(s.kind) ?? { x: 0, y: 0 },
      width: NODE_WIDTH,
      height: PROMOTION_NODE_HEIGHT,
      draggable: false,
      connectable: false,
      ariaLabel: `${PROMOTION_LABEL[s.kind]}: ${STATE_LABEL[s.state]}${detail ? `, ${detail}` : ''}${
        secrets.length ? `. Keys: ${secrets.map((e) => `${e.key} ${e.kind}`).join(', ')}` : ''
      }`,
      data: {
        label: PROMOTION_LABEL[s.kind],
        state: s.state,
        progress: s.state === 'succeeded' ? 1 : s.state === 'running' ? 0.5 : 0,
        detail,
        secrets,
        reducedMotion,
      },
    };
  });
  const byKind = new Map(p.steps.map((s) => [s.kind, s]));
  const edges: Edge[] = spec.edges.map((e) => {
    const state = byKind.get(e.target as PromotionStepKind)?.state ?? 'pending';
    return {
      id: `${e.source}->${e.target}`,
      source: e.source,
      target: e.target,
      animated: state === 'running' && !reducedMotion,
      style: edgeStyle(state, false),
    };
  });
  return { nodes, edges };
}
