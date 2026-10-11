// Schema propagation and port type checks. Every node's output schema is computed from
// its inputs (a source emits its connector's schema; a filter passes its input through;
// a map renames and projects; a schema mapping casts). An edge is checked against the
// target's accepted schema: a missing required field, or a type that does not convert
// without loss, makes it an error edge. The rules mirror CN2 Task 5: widening is fine
// (int -> float, anything -> string, anything -> json), narrowing needs a schema mapping.

import type {
  ConnectorDef,
  FieldType,
  PipelineEdge,
  PipelineGraph,
  PipelineNode,
  Schema,
  SchemaField,
} from './model.js';

/** Whether a value of type `from` can be written to a field of type `to` without loss. */
export function convertible(from: FieldType, to: FieldType): boolean {
  if (from === to) return true;
  if (to === 'json' || to === 'string') return from !== 'bytes';
  if (from === 'int' && to === 'float') return true;
  return false;
}

/** Whether a schema mapping may cast `from` to `to` (a lossy cast, chosen on purpose). */
export function castable(from: FieldType, to: FieldType): boolean {
  if (convertible(from, to)) return true;
  if (from === 'string') return to !== 'bytes';
  if (from === 'float' && to === 'int') return true;
  if (from === 'timestamp' && to === 'int') return true;
  if (from === 'int' && to === 'timestamp') return true;
  if (from === 'json') return to !== 'bytes';
  return false;
}

export type Lookup = (ref: string) => ConnectorDef | undefined;

export interface EdgeCheck {
  ok: boolean;
  /** One line, for the edge label and the issues list. */
  message?: string;
  /** Each problem, field by field. */
  problems: string[];
}

const fieldsOf = (s: Schema | undefined): readonly SchemaField[] | undefined =>
  s === undefined || s.open ? undefined : s.fields;

/** Merges several inputs into one: fields by name, first type wins. */
function union(inputs: readonly Schema[]): Schema {
  if (inputs.length === 0) return { fields: [] };
  if (inputs.some((s) => s.open)) return { open: true };
  const seen = new Map<string, SchemaField>();
  for (const s of inputs) {
    for (const f of fieldsOf(s) ?? []) if (!seen.has(f.name)) seen.set(f.name, f);
  }
  return { fields: [...seen.values()] };
}

/** The schema a node emits, given what reaches it. Sinks emit nothing. */
export function outputSchema(
  node: PipelineNode,
  inputs: readonly Schema[],
  lookup: Lookup,
): Schema | undefined {
  if (node.kind === 'source') return lookup(node.ref)?.emits ?? { open: true };
  if (node.kind === 'sink') return undefined;
  const input = union(inputs);
  switch (node.ref) {
    case 'filter':
      return input;
    case 'map': {
      if (input.open) {
        // An open input still has the mapped names; their types are unknown, so json.
        return {
          fields: (node.mappings ?? [])
            .filter((m) => m.to)
            .map((m) => ({ name: m.to, type: 'json' as const })),
        };
      }
      if (!node.mappings || node.mappings.length === 0) return input;
      const byName = new Map(input.fields.map((f) => [f.name, f]));
      const out: SchemaField[] = [];
      for (const m of node.mappings) {
        const f = byName.get(m.from);
        if (f && m.to) out.push({ name: m.to, type: f.type });
      }
      return { fields: out };
    }
    case 'schema': {
      if (input.open) return input;
      const casts = node.casts ?? {};
      return { fields: input.fields.map((f) => ({ name: f.name, type: casts[f.name] ?? f.type })) };
    }
    default:
      return input;
  }
}

/** Topological order (Kahn). Nodes on a cycle are left out and reported by `validate`. */
export function topoOrder(graph: PipelineGraph): string[] {
  const indeg = new Map(graph.nodes.map((n) => [n.id, 0]));
  for (const e of graph.edges) {
    if (indeg.has(e.target) && indeg.has(e.source)) indeg.set(e.target, (indeg.get(e.target) ?? 0) + 1);
  }
  const queue = graph.nodes.filter((n) => indeg.get(n.id) === 0).map((n) => n.id);
  const order: string[] = [];
  while (queue.length) {
    const id = queue.shift() as string;
    order.push(id);
    for (const e of graph.edges) {
      if (e.source !== id || !indeg.has(e.target)) continue;
      const d = (indeg.get(e.target) ?? 0) - 1;
      indeg.set(e.target, d);
      if (d === 0) queue.push(e.target);
    }
  }
  return order;
}

/** Every node's output schema, in one pass over the topological order. */
export function propagate(graph: PipelineGraph, lookup: Lookup): Map<string, Schema | undefined> {
  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  const out = new Map<string, Schema | undefined>();
  for (const id of topoOrder(graph)) {
    const node = byId.get(id);
    if (!node) continue;
    const inputs = graph.edges
      .filter((e) => e.target === id)
      .map((e) => out.get(e.source))
      .filter((s): s is Schema => s !== undefined);
    out.set(id, outputSchema(node, inputs, lookup));
  }
  return out;
}

/** What a node accepts on its input port. Transforms take anything. */
export function acceptedSchema(node: PipelineNode, lookup: Lookup): Schema {
  if (node.kind === 'sink') return lookup(node.ref)?.accepts ?? { open: true };
  return { open: true };
}

/** Checks one edge: does what the source side emits fit what the target accepts? */
export function checkSchemas(emitted: Schema | undefined, accepted: Schema): EdgeCheck {
  if (accepted.open) return { ok: true, problems: [] };
  if (emitted === undefined) return { ok: false, message: 'No output', problems: ['The upstream node emits nothing.'] };
  // An open stream cannot be proven to fit a fixed schema: map it first.
  if (emitted.open) {
    return {
      ok: false,
      message: 'Untyped input',
      problems: ['The input has no declared schema; add a schema mapping before this sink.'],
    };
  }
  const have = new Map(emitted.fields.map((f) => [f.name, f.type]));
  const problems: string[] = [];
  for (const want of accepted.fields) {
    const got = have.get(want.name);
    if (got === undefined) {
      if (want.required) problems.push(`Missing required field ${want.name} (${want.type}).`);
      continue;
    }
    if (!convertible(got, want.type)) {
      problems.push(`Field ${want.name} is ${got}; the sink needs ${want.type}.`);
    }
  }
  return problems.length === 0
    ? { ok: true, problems }
    : {
        ok: false,
        message: problems.length === 1 ? 'Schema mismatch' : `${problems.length} schema mismatches`,
        problems,
      };
}

/** Checks every edge of the graph against the propagated schemas. */
export function checkEdges(graph: PipelineGraph, lookup: Lookup): Map<string, EdgeCheck> {
  const schemas = propagate(graph, lookup);
  const byId = new Map(graph.nodes.map((n) => [n.id, n]));
  const result = new Map<string, EdgeCheck>();
  for (const e of graph.edges) {
    const target = byId.get(e.target);
    if (!target) continue;
    result.set(e.id, checkSchemas(schemas.get(e.source), acceptedSchema(target, lookup)));
  }
  return result;
}

/**
 * Whether a connection is allowed at all (port direction, not schema): a source has no
 * input, a sink no output, no self loops, no duplicates, and no cycles. A schema
 * mismatch is still allowed: it draws as an error edge so the person sees why.
 */
export function canConnect(
  graph: PipelineGraph,
  sourceId: string,
  targetId: string,
): { ok: true } | { ok: false; reason: string } {
  if (sourceId === targetId) return { ok: false, reason: 'A node cannot feed itself.' };
  const source = graph.nodes.find((n) => n.id === sourceId);
  const target = graph.nodes.find((n) => n.id === targetId);
  if (!source || !target) return { ok: false, reason: 'Unknown node.' };
  if (source.kind === 'sink') return { ok: false, reason: 'A sink has no output.' };
  if (target.kind === 'source') return { ok: false, reason: 'A source has no input.' };
  if (graph.edges.some((e) => e.source === sourceId && e.target === targetId)) {
    return { ok: false, reason: 'These nodes are already connected.' };
  }
  if (reaches(graph.edges, targetId, sourceId)) {
    return { ok: false, reason: 'That connection would make a cycle.' };
  }
  return { ok: true };
}

function reaches(edges: readonly PipelineEdge[], from: string, to: string): boolean {
  const seen = new Set<string>();
  const stack = [from];
  while (stack.length) {
    const id = stack.pop() as string;
    if (id === to) return true;
    if (seen.has(id)) continue;
    seen.add(id);
    for (const e of edges) if (e.source === id) stack.push(e.target);
  }
  return false;
}

/** A schema, one line: `id:int, name:string` or `any record`. */
export function schemaText(s: Schema | undefined): string {
  if (s === undefined) return 'none';
  if (s.open) return 'any record';
  if (s.fields.length === 0) return 'no fields';
  return s.fields.map((f) => `${f.name}:${f.type}`).join(', ');
}
