// The Search tab's state, how it travels in the URL, and how it becomes the engine's
// SearchRequest (proto JSON). Pure, so it is tested without React.

import type { JsonObject } from '@bufbuild/protobuf';
import { type Compiled, compileQuery, type FieldInfo, type KqlError } from './kql.js';

export type Mode = 'hybrid' | 'text' | 'vector';
export type FusionMethod = 'rrf' | 'weighted';
export type View = 'cards' | 'table';

/** A filter pill: one query-language clause, ANDed with the query. */
export interface Pill {
  id: string;
  q: string;
  negate: boolean;
  disabled: boolean;
}

export interface SearchState {
  q: string;
  pills: Pill[];
  mode: Mode;
  /** Top-k: hits shown, and each retriever's k. */
  k: number;
  fusion: FusionMethod;
  /** RRF's k constant. */
  rrfK: number;
  /** Weighted sum: the text retriever's weight; the vector's is 1 - this. */
  textWeight: number;
  vectorField: string;
  /** "Find similar": the id of the document whose vector is the query (shareable). */
  similarTo?: string;
  view: View;
  /** Table columns. */
  columns: string[];
}

export const K_CHOICES = [10, 20, 50, 100] as const;

export function defaultState(fields: FieldInfo[]): SearchState {
  const vector = fields.find((f) => f.type === 'vector');
  return {
    q: '',
    pills: [],
    mode: vector ? 'hybrid' : 'text',
    k: 20,
    fusion: 'rrf',
    rrfK: 60,
    textWeight: 0.5,
    vectorField: vector?.name ?? '',
    view: 'cards',
    columns: [],
  };
}

const MODES: Mode[] = ['hybrid', 'text', 'vector'];

/** Reads the state from the route's query parameters over the defaults. */
export function fromParams(
  params: Record<string, string | undefined>,
  fields: FieldInfo[],
): SearchState {
  const s = defaultState(fields);
  if (params.q !== undefined) s.q = params.q;
  if (params.mode && MODES.includes(params.mode as Mode)) s.mode = params.mode as Mode;
  const k = Number(params.k);
  if (Number.isInteger(k) && k >= 1 && k <= 1000) s.k = k;
  if (params.fusion === 'weighted' || params.fusion === 'rrf') s.fusion = params.fusion;
  const rk = Number(params.rrfk);
  if (Number.isInteger(rk) && rk >= 1) s.rrfK = rk;
  const tw = Number(params.tw);
  if (params.tw !== undefined && Number.isFinite(tw) && tw >= 0 && tw <= 1) s.textWeight = tw;
  if (params.vf && fields.some((f) => f.type === 'vector' && f.name === params.vf))
    s.vectorField = params.vf;
  if (params.sim) s.similarTo = params.sim;
  if (params.view === 'table' || params.view === 'cards') s.view = params.view;
  if (params.cols) s.columns = params.cols.split(',').filter(Boolean);
  if (params.f) {
    try {
      const list = JSON.parse(params.f) as unknown;
      if (Array.isArray(list)) {
        s.pills = list.flatMap((p, i) => {
          if (!Array.isArray(p) || typeof p[0] !== 'string') return [];
          const flags = String(p[1] ?? '');
          return [
            { id: `p${i}`, q: p[0], negate: flags.includes('n'), disabled: flags.includes('d') },
          ];
        });
      }
    } catch {
      // A hand-edited link with a broken filter list: drop it.
    }
  }
  if (!fields.some((f) => f.type === 'vector') && s.mode !== 'text') s.mode = 'text';
  return s;
}

/** The query string that reproduces the state (defaults left out). */
export function toParams(s: SearchState, fields: FieldInfo[]): string {
  const d = defaultState(fields);
  const p = new URLSearchParams();
  if (s.q) p.set('q', s.q);
  if (s.pills.length > 0) {
    p.set(
      'f',
      JSON.stringify(
        s.pills.map((x) => {
          const flags = `${x.negate ? 'n' : ''}${x.disabled ? 'd' : ''}`;
          return flags ? [x.q, flags] : [x.q];
        }),
      ),
    );
  }
  if (s.mode !== d.mode) p.set('mode', s.mode);
  if (s.k !== d.k) p.set('k', String(s.k));
  if (s.fusion !== d.fusion) p.set('fusion', s.fusion);
  if (s.rrfK !== d.rrfK) p.set('rrfk', String(s.rrfK));
  if (s.textWeight !== d.textWeight) p.set('tw', String(s.textWeight));
  if (s.vectorField && s.vectorField !== d.vectorField) p.set('vf', s.vectorField);
  if (s.similarTo) p.set('sim', s.similarTo);
  if (s.view !== d.view) p.set('view', s.view);
  if (s.columns.length > 0) p.set('cols', s.columns.join(','));
  return p.toString();
}

export const HIGHLIGHT_PRE = '';
export const HIGHLIGHT_POST = '';

export interface BuiltRequest {
  /** The SearchRequest, without namespace and collection. */
  ir: JsonObject;
  /** The same request with one retriever each, for the per-signal ranks (hybrid only). */
  signals?: { text: JsonObject; vector: JsonObject };
  /** Words to highlight in the browser when the engine returns no highlight. */
  terms: string[];
  /** What the request does, for the status line: "BM25 + vector (RRF k=60)". */
  plan: string;
  /** Things the user should know: an ignored vector, words used as a filter. */
  notes: string[];
}

export type BuildResult =
  | { ok: true; request: BuiltRequest }
  | { ok: false; error: KqlError; source: 'query' | string };

/**
 * Builds the request. `vector` is the query vector (pasted, or the similar document's);
 * pills are ANDed with the query as filters.
 */
export function buildRequest(s: SearchState, fields: FieldInfo[], vector?: number[]): BuildResult {
  const notes: string[] = [];
  const useVector = s.mode !== 'text' && !!vector && !!s.vectorField;
  // Vector mode: words narrow the candidates instead of ranking.
  const scored = s.mode !== 'vector';
  const main = compileQuery(s.q, fields, { scored });
  if (!main.ok) return { ok: false, error: main.error, source: 'query' };
  const filters: JsonObject[] = [];
  if (main.compiled.filter) filters.push(main.compiled.filter);
  for (const p of s.pills) {
    if (p.disabled) continue;
    const c = compileQuery(p.q, fields, { scored: false });
    if (!c.ok) return { ok: false, error: c.error, source: p.id };
    if (!c.compiled.filter) continue;
    filters.push(p.negate ? { bool: { mustNot: [c.compiled.filter] } } : c.compiled.filter);
  }
  const compiled: Compiled = main.compiled;
  const filter =
    filters.length === 0
      ? undefined
      : filters.length === 1
        ? filters[0]
        : { bool: { must: filters } };

  const textRetriever = compiled.text ? { text: { query: compiled.text, k: s.k } } : undefined;
  const vectorRetriever =
    useVector && vector ? { vector: { field: s.vectorField, query: vector, k: s.k } } : undefined;

  const retrievers: JsonObject[] = [];
  if (textRetriever && s.mode !== 'vector') retrievers.push(textRetriever);
  if (vectorRetriever) retrievers.push(vectorRetriever);

  if (s.mode === 'vector' && !vector)
    notes.push('Vector mode needs a vector: paste one, or use Find similar on a result.');
  if (s.mode === 'vector' && vector && !s.vectorField)
    notes.push('This collection has no vector field.');
  if (s.mode === 'vector' && compiled.terms.length > 0 && vector)
    notes.push('In Vector mode, words filter the results; they do not rank them.');
  if (s.mode === 'hybrid' && !vector && textRetriever)
    notes.push(
      'Hybrid without a vector is a text search: paste a vector or use Find similar to fuse both.',
    );
  if (s.mode === 'text' && vector) notes.push('Text mode ignores the vector.');

  const ir: JsonObject = { limit: s.k, trackTotalHits: 'exact' };
  if (retrievers.length > 0) ir.retrievers = retrievers;
  let plan = 'Filter only, in id order';
  if (retrievers.length === 2) {
    ir.fusion =
      s.fusion === 'rrf'
        ? { rrf: { k: s.rrfK } }
        : { weightedSum: { weights: [round(s.textWeight), round(1 - s.textWeight)] } };
    plan =
      s.fusion === 'rrf'
        ? `BM25 + vector, RRF k=${s.rrfK}`
        : `BM25 × ${round(s.textWeight)} + vector × ${round(1 - s.textWeight)}`;
  } else if (textRetriever && retrievers[0] === textRetriever) plan = 'BM25';
  else if (vectorRetriever) plan = `Vector (${s.vectorField})`;
  if (filter) ir.filter = filter;
  if (compiled.highlightFields.length > 0 && s.mode !== 'vector') {
    ir.highlight = {
      fields: compiled.highlightFields.map((field) => ({
        field,
        preTag: HIGHLIGHT_PRE,
        postTag: HIGHLIGHT_POST,
        fragmentSize: 180,
        numberOfFragments: 2,
      })),
    };
  }
  const request: BuiltRequest = { ir, terms: compiled.terms, plan, notes };
  if (retrievers.length === 2 && textRetriever && vectorRetriever) {
    const base: JsonObject = { limit: s.k };
    if (filter) base.filter = filter;
    request.signals = {
      text: { ...base, retrievers: [textRetriever] },
      vector: { ...base, retrievers: [vectorRetriever] },
    };
  }
  return { ok: true, request };
}

function round(n: number): number {
  return Math.round(n * 100) / 100;
}

/** Parses a pasted vector: a JSON array of numbers of the field's dimension. */
export function parseVector(text: string, dim?: number): number[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text.trim());
  } catch {
    throw new Error('A vector is a JSON array of numbers, like [0.1, 0.2, 0.3].');
  }
  if (
    !Array.isArray(parsed) ||
    parsed.length === 0 ||
    parsed.some((n) => typeof n !== 'number' || !Number.isFinite(n))
  ) {
    throw new Error('A vector is a JSON array of numbers, like [0.1, 0.2, 0.3].');
  }
  if (dim && parsed.length !== dim) {
    throw new Error(`This vector has ${parsed.length} numbers; the field takes ${dim}.`);
  }
  return parsed as number[];
}

/** Splits an engine highlight fragment into plain and marked runs. */
export function highlightRuns(fragment: string): { text: string; hit: boolean }[] {
  const out: { text: string; hit: boolean }[] = [];
  let i = 0;
  while (i < fragment.length) {
    const open = fragment.indexOf(HIGHLIGHT_PRE, i);
    if (open < 0) {
      out.push({ text: fragment.slice(i), hit: false });
      break;
    }
    if (open > i) out.push({ text: fragment.slice(i, open), hit: false });
    const close = fragment.indexOf(HIGHLIGHT_POST, open + 1);
    const end = close < 0 ? fragment.length : close;
    out.push({ text: fragment.slice(open + 1, end), hit: true });
    i = close < 0 ? fragment.length : close + 1;
  }
  return out.filter((r) => r.text.length > 0);
}

/** Marks the query's words in a value, for fields the engine did not highlight. */
export function markTerms(text: string, terms: string[]): { text: string; hit: boolean }[] {
  const words = [
    ...new Set(
      terms
        .flatMap((t) => t.split(/\s+/))
        .map((w) => w.replace(/[^\p{L}\p{N}_-]/gu, ''))
        .filter((w) => w.length > 1),
    ),
  ];
  if (words.length === 0) return [{ text, hit: false }];
  const re = new RegExp(
    `(?<![\\p{L}\\p{N}])(${words.map((w) => w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})(?![\\p{L}\\p{N}])`,
    'giu',
  );
  const out: { text: string; hit: boolean }[] = [];
  let last = 0;
  for (const m of text.matchAll(re)) {
    const at = m.index ?? 0;
    if (at > last) out.push({ text: text.slice(last, at), hit: false });
    out.push({ text: m[0], hit: true });
    last = at + m[0].length;
  }
  if (last < text.length) out.push({ text: text.slice(last), hit: false });
  return out;
}
