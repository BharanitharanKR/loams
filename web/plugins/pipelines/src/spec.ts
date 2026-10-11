// The builder graph <-> the pipeline spec the client deploys.
//
// The spec carries two views of the same pipeline:
//   - `graph`: a GraphSpec exactly as @loams-core/flows' `compileGraph` reads it
//     (nodes with id/label/position/className, edges with id/source/target/label,
//     the flows layout), so a dashboard `graph` widget can draw a deployed pipeline
//     with no translation;
//   - `steps`: what the flow runtime runs, one per node, with config and secret refs.
// Secret values never enter a spec: a secret field is a `secretRef` the deploy call
// resolves from the values it is handed alongside, once.

import { DEFAULT_GRAPH_LAYOUT, positionNodes } from '@loams-core/flows/src/compiler.ts';
import type {
  ConfigValue,
  FieldMapping,
  FieldType,
  NodeKind,
  PipelineGraph,
  PipelineNode,
} from './model.js';

export const PIPELINE_API_VERSION = 'loams.flow/v1';

/** A GraphSpec node as @loams-core/flows reads it. */
export interface FlowsGraphNode {
  id: string;
  label: string;
  position: { x: number; y: number };
  className: string;
}

/** A GraphSpec edge as @loams-core/flows reads it. */
export interface FlowsGraphEdge {
  id: string;
  source: string;
  target: string;
  type: 'smoothstep';
  animated?: boolean;
}

export interface FlowsGraphSpec {
  nodes: FlowsGraphNode[];
  edges: FlowsGraphEdge[];
  layout: typeof DEFAULT_GRAPH_LAYOUT;
  fitView: true;
  pannable: true;
  zoomable: true;
}

export interface SecretRef {
  secretRef: string;
}

export interface PipelineStep {
  id: string;
  kind: NodeKind;
  /** `connector:<manifest id>` or `transform:<kind>`. */
  uses: string;
  inputs: string[];
  config: Record<string, ConfigValue>;
  secrets: Record<string, SecretRef>;
  mappings?: FieldMapping[];
  casts?: Record<string, FieldType>;
}

export interface PipelineSpec {
  apiVersion: typeof PIPELINE_API_VERSION;
  kind: 'Pipeline';
  metadata: { name: string };
  graph: FlowsGraphSpec;
  steps: PipelineStep[];
}

/** The reference a secret is stored under: pipeline, node and field, never its value. */
export const secretRef = (pipeline: string, nodeId: string, key: string) =>
  `pipelines/${pipeline}/${nodeId}/${key}`;

const finite = (n: number) => (Number.isFinite(n) ? n : 0);

export function toPipelineSpec(
  graph: PipelineGraph,
  opts: { running?: boolean } = {},
): PipelineSpec {
  return {
    apiVersion: PIPELINE_API_VERSION,
    kind: 'Pipeline',
    metadata: { name: graph.name },
    graph: {
      nodes: graph.nodes.map((n) => ({
        id: n.id,
        label: n.label,
        // flows' GraphNodeSchema refuses a non-finite position.
        position: { x: finite(n.position.x), y: finite(n.position.y) },
        className: `pipeline-${n.kind}`,
      })),
      edges: graph.edges.map((e) => ({
        id: e.id,
        source: e.source,
        target: e.target,
        type: 'smoothstep' as const,
        ...(opts.running ? { animated: true } : {}),
      })),
      layout: { ...DEFAULT_GRAPH_LAYOUT },
      fitView: true,
      pannable: true,
      zoomable: true,
    },
    steps: graph.nodes.map((n) => ({
      id: n.id,
      kind: n.kind,
      uses: n.kind === 'transform' ? `transform:${n.ref}` : `connector:${n.ref}`,
      inputs: graph.edges.filter((e) => e.target === n.id).map((e) => e.source),
      config: { ...n.config },
      secrets: Object.fromEntries(
        Object.entries(n.secrets)
          .filter(([, set]) => set)
          .map(([key]) => [key, { secretRef: secretRef(graph.name, n.id, key) }]),
      ),
      ...(n.mappings ? { mappings: n.mappings.map((m) => ({ ...m })) } : {}),
      ...(n.casts ? { casts: { ...n.casts } } : {}),
    })),
  };
}

/** The inverse, for opening a deployed pipeline. Unknown `uses` prefixes are refused. */
export function fromPipelineSpec(spec: PipelineSpec): PipelineGraph {
  const positions = new Map(spec.graph.nodes.map((n) => [n.id, n]));
  const nodes: PipelineNode[] = spec.steps.map((s) => {
    const [prefix, ref] = s.uses.split(':', 2);
    if ((prefix !== 'connector' && prefix !== 'transform') || !ref) {
      throw new Error(`Step ${s.id} uses ${s.uses}; expected connector:<id> or transform:<kind>.`);
    }
    const g = positions.get(s.id);
    return {
      id: s.id,
      kind: s.kind,
      ref,
      label: g?.label ?? s.id,
      position: g?.position ?? { x: 0, y: 0 },
      config: { ...s.config },
      secrets: Object.fromEntries(Object.keys(s.secrets).map((k) => [k, true])),
      ...(s.mappings ? { mappings: s.mappings } : {}),
      ...(s.casts ? { casts: s.casts } : {}),
    };
  });
  return {
    name: spec.metadata.name,
    nodes,
    edges: spec.graph.edges.map((e) => ({ id: e.id, source: e.source, target: e.target })),
  };
}

/** Lays the graph out with flows' own ranker, so the builder and a dashboard agree. */
export function autoLayout(graph: PipelineGraph): PipelineGraph {
  const positions = positionNodes(
    graph.nodes.map((n) => ({ id: n.id })),
    graph.edges.map((e) => ({ source: e.source, target: e.target })),
    { ...DEFAULT_GRAPH_LAYOUT, nodeWidth: 220, nodeHeight: 96 },
  );
  return {
    ...graph,
    nodes: graph.nodes.map((n) => ({ ...n, position: positions.get(n.id) ?? n.position })),
  };
}
