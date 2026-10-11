// Local validation, run before the server's: the page refuses to deploy a graph with any
// `error` issue. Each issue names the node or edge it is about, so the issues list can
// move focus to it.

import type { PipelineGraph, PipelineNode } from './model.js';
import { checkEdges, type Lookup, topoOrder } from './schema.js';

export interface Issue {
  level: 'error' | 'warning';
  message: string;
  nodeId?: string;
  edgeId?: string;
}

const isBlank = (v: unknown) => v === undefined || v === null || v === '';

function configIssues(node: PipelineNode, lookup: Lookup): Issue[] {
  const issues: Issue[] = [];
  if (node.kind === 'transform') {
    if (node.ref === 'filter' && isBlank(node.config.expression)) {
      issues.push({
        level: 'error',
        nodeId: node.id,
        message: `${node.label}: the filter has no expression.`,
      });
    }
    if (node.ref === 'map' && (node.mappings ?? []).filter((m) => m.to).length === 0) {
      issues.push({
        level: 'warning',
        nodeId: node.id,
        message: `${node.label}: no fields mapped; records pass through unchanged.`,
      });
    }
    return issues;
  }
  const def = lookup(node.ref);
  if (!def) {
    issues.push({
      level: 'error',
      nodeId: node.id,
      message: `${node.label}: unknown connector ${node.ref}.`,
    });
    return issues;
  }
  for (const f of def.config) {
    if (!f.required) continue;
    const missing = f.kind === 'secret' ? !node.secrets[f.key] : isBlank(node.config[f.key]);
    if (missing) {
      issues.push({
        level: 'error',
        nodeId: node.id,
        message: `${node.label}: ${f.label} is required.`,
      });
    }
  }
  return issues;
}

/** Every problem with the graph, errors first. An empty list means it may deploy. */
export function validateGraph(graph: PipelineGraph, lookup: Lookup): Issue[] {
  const issues: Issue[] = [];
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(graph.name)) {
    issues.push({
      level: 'error',
      message:
        'The pipeline name must be lowercase letters, digits and dashes, starting with a letter or digit.',
    });
  }
  if (!graph.nodes.some((n) => n.kind === 'source'))
    issues.push({ level: 'error', message: 'Add a source.' });
  if (!graph.nodes.some((n) => n.kind === 'sink'))
    issues.push({ level: 'error', message: 'Add a sink.' });

  const ordered = new Set(topoOrder(graph));
  if (ordered.size < graph.nodes.length)
    issues.push({ level: 'error', message: 'The pipeline has a cycle.' });

  for (const node of graph.nodes) {
    const hasIn = graph.edges.some((e) => e.target === node.id);
    const hasOut = graph.edges.some((e) => e.source === node.id);
    if (node.kind !== 'source' && !hasIn) {
      issues.push({ level: 'error', nodeId: node.id, message: `${node.label} has no input.` });
    }
    if (node.kind !== 'sink' && !hasOut) {
      issues.push({
        level: 'error',
        nodeId: node.id,
        message: `${node.label} goes nowhere; connect it to a sink.`,
      });
    }
    issues.push(...configIssues(node, lookup));
  }

  const labels = new Map(graph.nodes.map((n) => [n.id, n.label]));
  for (const [edgeId, check] of checkEdges(graph, lookup)) {
    if (check.ok) continue;
    const edge = graph.edges.find((e) => e.id === edgeId);
    const name = edge ? `${labels.get(edge.source)} → ${labels.get(edge.target)}` : edgeId;
    for (const p of check.problems)
      issues.push({ level: 'error', edgeId, nodeId: edge?.target, message: `${name}: ${p}` });
  }

  return issues.sort((a, b) => (a.level === b.level ? 0 : a.level === 'error' ? -1 : 1));
}
