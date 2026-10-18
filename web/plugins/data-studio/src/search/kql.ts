// The Search tab's query language: a small KQL. It parses to an AST with source
// positions, and compiles against the collection's schema to the engine's native
// query (loams.collection.v1 `Query` in proto JSON):
//
//   french river                 free text: BM25 over the text fields (words are ORed, ranked)
//   "french river"               a phrase
//   title:river                  a field clause; on a text field it is a scored match
//   type:PushEvent               keyword: exact term;  type:Push*  prefix;  repo:*kube*  wildcard
//   type:(ForkEvent OR WatchEvent)  one of several values
//   body_chars >= 200            ranges on numbers, dates and keywords:  >  >=  <  <=
//   created_at:2024-01-15        a date without a time is that whole day
//   org:*                        the field has a value
//   a AND b, a OR b, NOT a, (…)  boolean structure; juxtaposition is AND
//
// The top-level AND parts that only match text (free text, or `field:words` on a text
// field) become the text retriever, so they rank by BM25 and get highlights; every
// other part becomes the filter. Anything the engine cannot do is a KqlError with the
// span it is about, never a silently different query.

import type { JsonObject, JsonValue } from '@bufbuild/protobuf';

export type FieldType =
  | 'text'
  | 'keyword'
  | 'i64'
  | 'f64'
  | 'bool'
  | 'date'
  | 'uuid'
  | 'json'
  | 'vector'
  | 'unknown';

export interface FieldInfo {
  name: string;
  type: FieldType;
  /** A fast (columnar) field: it can be aggregated, so its top values can be listed. */
  fast: boolean;
  indexed: boolean;
  /** Vectors: the dimension and the distance. */
  dim?: number;
  distance?: string;
}

/** The fields of a collection's REST schema JSON, vectors last. */
export function schemaFields(schema: JsonObject): FieldInfo[] {
  const out: FieldInfo[] = [];
  const fields = Array.isArray(schema.fields) ? schema.fields : [];
  for (const f of fields) {
    if (typeof f !== 'object' || f === null || Array.isArray(f)) continue;
    const kind = f.kind;
    let type: FieldType = 'unknown';
    if (typeof kind === 'string') type = kindType(kind);
    else if (kind && typeof kind === 'object' && !Array.isArray(kind)) {
      type = kindType(Object.keys(kind)[0] ?? '');
    }
    out.push({
      name: String(f.name ?? ''),
      type,
      fast: f.fast === true,
      indexed: f.indexed !== false,
    });
  }
  const vectors = Array.isArray(schema.vectors) ? schema.vectors : [];
  for (const v of vectors) {
    if (typeof v !== 'object' || v === null || Array.isArray(v)) continue;
    out.push({
      name: String(v.name ?? ''),
      type: 'vector',
      fast: false,
      indexed: true,
      dim: Number(v.dim ?? 0),
      distance: typeof v.distance === 'string' ? v.distance : undefined,
    });
  }
  return out;
}

function kindType(k: string): FieldType {
  const t = k.toLowerCase();
  return (['text', 'keyword', 'i64', 'f64', 'bool', 'date', 'uuid', 'json'] as const).includes(
    t as never,
  )
    ? (t as FieldType)
    : 'unknown';
}

// ---------------------------------------------------------------------------
// Lexer

export type TokenKind =
  | 'ws'
  | 'lparen'
  | 'rparen'
  | 'colon'
  | 'cmp'
  | 'and'
  | 'or'
  | 'not'
  | 'word'
  | 'quoted';

export interface Token {
  kind: TokenKind;
  /** The source text of the token. */
  raw: string;
  /** For words and quoted strings: the value (quotes and escapes removed). */
  value: string;
  from: number;
  to: number;
  /** A quoted string with no closing quote. */
  open?: boolean;
  /** A word read in value position (after `:` or a comparison). */
  isValue?: boolean;
}

const SPECIAL = new Set(['(', ')', ':', '<', '>', '"', '=']);

/** Splits the text into tokens; never throws (an open quote is marked `open`). */
export function lex(text: string): Token[] {
  const out: Token[] = [];
  let i = 0;
  // After `:` or a comparison, the next word is a value and may hold `:` (a time).
  let valueNext = false;
  // Inside `field:( … )`: values until the closing parenthesis.
  let listDepth = 0;
  while (i < text.length) {
    const c = text[i] as string;
    const start = i;
    if (/\s/.test(c)) {
      while (i < text.length && /\s/.test(text[i] as string)) i++;
      out.push({ kind: 'ws', raw: text.slice(start, i), value: '', from: start, to: i });
      continue;
    }
    if (c === '(') {
      i++;
      if (valueNext) listDepth++;
      out.push({ kind: 'lparen', raw: c, value: '', from: start, to: i });
      valueNext = false;
      continue;
    }
    if (c === ')') {
      i++;
      if (listDepth > 0) listDepth--;
      out.push({ kind: 'rparen', raw: c, value: '', from: start, to: i });
      valueNext = false;
      continue;
    }
    if (c === ':' || c === '=') {
      i++;
      out.push({ kind: 'colon', raw: c, value: c, from: start, to: i });
      valueNext = true;
      continue;
    }
    if (c === '<' || c === '>') {
      i++;
      if (text[i] === '=') i++;
      const raw = text.slice(start, i);
      out.push({ kind: 'cmp', raw, value: raw, from: start, to: i });
      valueNext = true;
      continue;
    }
    if (c === '"') {
      i++;
      let value = '';
      let closed = false;
      while (i < text.length) {
        const d = text[i] as string;
        if (d === '\\' && i + 1 < text.length) {
          value += text[i + 1];
          i += 2;
          continue;
        }
        if (d === '"') {
          i++;
          closed = true;
          break;
        }
        value += d;
        i++;
      }
      out.push({
        kind: 'quoted',
        raw: text.slice(start, i),
        value,
        from: start,
        to: i,
        open: !closed,
        isValue: valueNext || listDepth > 0,
      });
      valueNext = false;
      continue;
    }
    const inValue = valueNext || listDepth > 0;
    while (i < text.length) {
      const d = text[i] as string;
      if (/\s/.test(d) || d === '(' || d === ')' || d === '"') break;
      if (!valueNext && SPECIAL.has(d)) break;
      i++;
    }
    const raw = text.slice(start, i);
    const upper = raw.toUpperCase();
    let kind: TokenKind = 'word';
    if (!valueNext && (upper === 'AND' || upper === 'OR' || upper === 'NOT')) {
      kind = upper.toLowerCase() as TokenKind;
    }
    out.push({ kind, raw, value: raw, from: start, to: i, isValue: inValue && kind === 'word' });
    valueNext = false;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Parser

export interface KqlValue {
  text: string;
  quoted: boolean;
  from: number;
  to: number;
}

export type Comparison = ':' | '>' | '>=' | '<' | '<=';

export type Node =
  | { kind: 'and' | 'or'; items: Node[]; from: number; to: number }
  | { kind: 'not'; item: Node; from: number; to: number }
  | { kind: 'text'; text: string; phrase: boolean; from: number; to: number }
  | {
      kind: 'field';
      field: string;
      fieldFrom: number;
      fieldTo: number;
      op: Comparison;
      values: KqlValue[];
      from: number;
      to: number;
    };

export class KqlError extends Error {
  constructor(
    message: string,
    readonly from: number,
    readonly to: number,
  ) {
    super(message);
    this.name = 'KqlError';
  }
}

export type ParseResult = { ok: true; ast: Node | undefined } | { ok: false; error: KqlError };

/** Parses a query. An empty query parses to `undefined` (match everything). */
export function parse(text: string): ParseResult {
  try {
    return { ok: true, ast: new Parser(text).parseAll() };
  } catch (e) {
    if (e instanceof KqlError) return { ok: false, error: e };
    throw e;
  }
}

class Parser {
  private readonly toks: Token[];
  private i = 0;

  constructor(private readonly text: string) {
    this.toks = lex(text).filter((t) => t.kind !== 'ws');
  }

  parseAll(): Node | undefined {
    if (this.toks.length === 0) return undefined;
    const node = this.parseOr();
    const t = this.peek();
    if (t) {
      if (t.kind === 'rparen') throw new KqlError('This ) has no matching (.', t.from, t.to);
      throw new KqlError(`Unexpected ${describe(t)}.`, t.from, t.to);
    }
    return node;
  }

  private peek(o = 0): Token | undefined {
    return this.toks[this.i + o];
  }

  private next(): Token {
    return this.toks[this.i++] as Token;
  }

  private end(): number {
    return this.text.length;
  }

  private parseOr(): Node {
    const items = [this.parseAnd()];
    while (this.peek()?.kind === 'or') {
      const op = this.next();
      if (!this.startsClause(this.peek())) {
        throw new KqlError('OR needs a clause after it.', op.from, op.to);
      }
      items.push(this.parseAnd());
    }
    return items.length === 1 ? (items[0] as Node) : group('or', items);
  }

  private startsClause(t: Token | undefined): boolean {
    return (
      !!t && (t.kind === 'word' || t.kind === 'quoted' || t.kind === 'lparen' || t.kind === 'not')
    );
  }

  private parseAnd(): Node {
    const first = this.peek();
    if (!this.startsClause(first)) {
      if (!first)
        throw new KqlError('The query ends where a clause was expected.', this.end(), this.end());
      throw new KqlError(`Expected a clause, found ${describe(first)}.`, first.from, first.to);
    }
    const items: Node[] = [this.parseNot()];
    // Consecutive bare words joined by nothing are one free-text query: BM25 ranks them.
    const implicit: boolean[] = [];
    for (;;) {
      const t = this.peek();
      if (t?.kind === 'and') {
        this.next();
        if (!this.startsClause(this.peek())) {
          throw new KqlError('AND needs a clause after it.', t.from, t.to);
        }
        items.push(this.parseNot());
        implicit.push(false);
      } else if (this.startsClause(t)) {
        items.push(this.parseNot());
        implicit.push(true);
      } else break;
    }
    const merged: Node[] = [items[0] as Node];
    for (let k = 1; k < items.length; k++) {
      const prev = merged[merged.length - 1] as Node;
      const cur = items[k] as Node;
      if (
        implicit[k - 1] &&
        prev.kind === 'text' &&
        !prev.phrase &&
        cur.kind === 'text' &&
        !cur.phrase
      ) {
        merged[merged.length - 1] = {
          kind: 'text',
          text: `${prev.text} ${cur.text}`,
          phrase: false,
          from: prev.from,
          to: cur.to,
        };
      } else merged.push(cur);
    }
    return merged.length === 1 ? (merged[0] as Node) : group('and', merged);
  }

  private parseNot(): Node {
    const t = this.peek();
    if (t?.kind === 'not') {
      this.next();
      if (!this.startsClause(this.peek())) {
        throw new KqlError('NOT needs a clause after it.', t.from, t.to);
      }
      const item = this.parseNot();
      return { kind: 'not', item, from: t.from, to: item.to };
    }
    return this.parsePrimary();
  }

  private parsePrimary(): Node {
    const t = this.next();
    if (t.kind === 'lparen') {
      if (this.peek()?.kind === 'rparen') {
        throw new KqlError('Empty parentheses.', t.from, (this.peek() as Token).to);
      }
      const inner = this.parseOr();
      const close = this.peek();
      if (close?.kind !== 'rparen') {
        throw new KqlError('This ( is not closed.', t.from, t.to);
      }
      this.next();
      return inner;
    }
    if (t.kind === 'quoted') {
      if (t.open) throw new KqlError('This quote is not closed.', t.from, t.from + 1);
      if (!t.value.trim()) throw new KqlError('An empty phrase.', t.from, t.to);
      return { kind: 'text', text: t.value, phrase: true, from: t.from, to: t.to };
    }
    // A word: a field clause when an operator follows.
    const op = this.peek();
    if (op && (op.kind === 'colon' || op.kind === 'cmp')) {
      this.next();
      return this.parseFieldRest(t, op);
    }
    return { kind: 'text', text: t.value, phrase: false, from: t.from, to: t.to };
  }

  private parseFieldRest(field: Token, op: Token): Node {
    const cmp = (op.kind === 'colon' ? ':' : op.value) as Comparison;
    const v = this.peek();
    const base = {
      kind: 'field' as const,
      field: field.value,
      fieldFrom: field.from,
      fieldTo: field.to,
      op: cmp,
    };
    if (cmp === ':' && v?.kind === 'lparen') {
      this.next();
      const values: KqlValue[] = [];
      for (;;) {
        const w = this.peek();
        if (!w || (w.kind !== 'word' && w.kind !== 'quoted')) {
          const at = w ?? { from: this.end(), to: this.end() };
          throw new KqlError(`Expected a value in ${field.value}:( … ).`, at.from, at.to);
        }
        if (w.kind === 'quoted' && w.open)
          throw new KqlError('This quote is not closed.', w.from, w.from + 1);
        this.next();
        values.push({ text: w.value, quoted: w.kind === 'quoted', from: w.from, to: w.to });
        const sep = this.peek();
        if (sep?.kind === 'or') {
          this.next();
          continue;
        }
        if (sep?.kind === 'rparen') {
          this.next();
          return { ...base, values, from: field.from, to: sep.to };
        }
        if (sep?.kind === 'and') {
          throw new KqlError(
            `A field is one value at a time: use OR inside ${field.value}:( … ), or two clauses.`,
            sep.from,
            sep.to,
          );
        }
        if (!sep) throw new KqlError('This ( is not closed.', v.from, v.to);
        throw new KqlError(`Expected OR or ) here, found ${describe(sep)}.`, sep.from, sep.to);
      }
    }
    if (!v || (v.kind !== 'word' && v.kind !== 'quoted')) {
      const at = v ?? { from: this.end(), to: this.end() };
      throw new KqlError(`${field.value}${op.raw} needs a value.`, op.from, Math.max(op.to, at.to));
    }
    if (v.kind === 'quoted' && v.open)
      throw new KqlError('This quote is not closed.', v.from, v.from + 1);
    this.next();
    return {
      ...base,
      values: [{ text: v.value, quoted: v.kind === 'quoted', from: v.from, to: v.to }],
      from: field.from,
      to: v.to,
    };
  }
}

function group(kind: 'and' | 'or', items: Node[]): Node {
  return {
    kind,
    items,
    from: (items[0] as Node).from,
    to: (items[items.length - 1] as Node).to,
  };
}

function describe(t: Token): string {
  switch (t.kind) {
    case 'rparen':
      return ')';
    case 'lparen':
      return '(';
    case 'colon':
    case 'cmp':
      return `"${t.raw}"`;
    case 'and':
    case 'or':
    case 'not':
      return t.raw.toUpperCase();
    default:
      return `"${t.raw}"`;
  }
}

// ---------------------------------------------------------------------------
// Compiler

export interface Compiled {
  /** The scored part: the text retriever's query, or undefined. */
  text?: JsonObject;
  /** The filter: everything else, or undefined. */
  filter?: JsonObject;
  /** The words and phrases of the query, for client-side highlighting. */
  terms: string[];
  /** The text fields the scored part reads, for the engine's highlights. */
  highlightFields: string[];
}

export interface CompileOptions {
  /**
   * `false` puts the scored part into the filter as well (Vector mode: the words then
   * narrow the candidates instead of ranking them).
   */
  scored?: boolean;
}

/** Compiles a parsed query against the schema's fields; throws KqlError. */
export function compile(
  ast: Node | undefined,
  fields: FieldInfo[],
  opts: CompileOptions = {},
): Compiled {
  const c = new Compiler(fields);
  const out: Compiled = { terms: [], highlightFields: [] };
  if (!ast) return out;
  const parts = ast.kind === 'and' ? ast.items : [ast];
  const scored: JsonObject[] = [];
  const filters: JsonObject[] = [];
  for (const p of parts) {
    if (opts.scored !== false && c.isScorable(p)) scored.push(c.node(p, true));
    else filters.push(c.node(p, false));
  }
  if (scored.length > 0)
    out.text = scored.length === 1 ? (scored[0] as JsonObject) : { bool: { must: scored } };
  if (filters.length > 0)
    out.filter = filters.length === 1 ? (filters[0] as JsonObject) : { bool: { must: filters } };
  out.terms = c.terms;
  out.highlightFields = [...c.highlight];
  return out;
}

/** Parses and compiles in one step; returns the error instead of throwing. */
export function compileQuery(
  text: string,
  fields: FieldInfo[],
  opts?: CompileOptions,
): { ok: true; compiled: Compiled; ast: Node | undefined } | { ok: false; error: KqlError } {
  const parsed = parse(text);
  if (!parsed.ok) return parsed;
  try {
    return { ok: true, compiled: compile(parsed.ast, fields, opts), ast: parsed.ast };
  } catch (e) {
    if (e instanceof KqlError) return { ok: false, error: e };
    throw e;
  }
}

const DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/;

/** The document id, a clause on every collection: `_id:42`. */
export const ID_FIELD = '_id';

class Compiler {
  readonly terms: string[] = [];
  readonly highlight = new Set<string>();
  private readonly byName: Map<string, FieldInfo>;
  private readonly textFields: string[];

  constructor(private readonly fields: FieldInfo[]) {
    this.byName = new Map(fields.map((f) => [f.name, f]));
    this.textFields = fields.filter((f) => f.type === 'text' && f.indexed).map((f) => f.name);
  }

  isScorable(n: Node): boolean {
    if (n.kind === 'text') return true;
    if (n.kind === 'field') {
      const f = this.byName.get(n.field);
      return (
        f?.type === 'text' && n.op === ':' && !n.values.some((v) => !v.quoted && v.text === '*')
      );
    }
    if (n.kind === 'or') return n.items.every((i) => this.isScorable(i));
    return false;
  }

  node(n: Node, scored: boolean): JsonObject {
    switch (n.kind) {
      case 'and':
        return { bool: { must: n.items.map((i) => this.node(i, scored)) } };
      case 'or':
        return { bool: { should: n.items.map((i) => this.node(i, scored)) } };
      case 'not':
        return { bool: { mustNot: [this.node(n.item, false)] } };
      case 'text':
        return this.freeText(n);
      case 'field':
        return this.field(n, scored);
    }
  }

  private freeText(n: Extract<Node, { kind: 'text' }>): JsonObject {
    if (this.textFields.length === 0) {
      throw new KqlError(
        'This collection has no text fields, so free text has nothing to match. Use field:value.',
        n.from,
        n.to,
      );
    }
    this.terms.push(n.text);
    for (const f of this.textFields) this.highlight.add(f);
    const one = (field: string): JsonObject =>
      n.phrase ? { matchPhrase: { field, text: n.text } } : { match: { field, text: n.text } };
    if (this.textFields.length === 1) return one(this.textFields[0] as string);
    return { bool: { should: this.textFields.map(one) } };
  }

  private resolve(n: Extract<Node, { kind: 'field' }>): { info: FieldInfo; path: string } {
    const exact = this.byName.get(n.field);
    if (exact) return { info: exact, path: n.field };
    // A path under a JSON field: `payload.user.id`.
    const dot = n.field.indexOf('.');
    if (dot > 0) {
      const root = this.byName.get(n.field.slice(0, dot));
      if (root?.type === 'json') return { info: { ...root, type: 'json' }, path: n.field };
    }
    const near = closest(
      n.field,
      this.fields.map((f) => f.name),
    );
    throw new KqlError(
      `There is no field "${n.field}" in this collection.${near ? ` Did you mean ${near}?` : ''}`,
      n.fieldFrom,
      n.fieldTo,
    );
  }

  private field(n: Extract<Node, { kind: 'field' }>, scored: boolean): JsonObject {
    if (n.field === ID_FIELD && !this.byName.has(ID_FIELD)) return this.ids(n);
    const { info, path } = this.resolve(n);
    const at = (v: KqlValue) => [v.from, v.to] as const;
    if (info.type === 'vector') {
      throw new KqlError(
        `${n.field} is a vector field: search it with Vector or Hybrid mode, not a clause.`,
        n.fieldFrom,
        n.fieldTo,
      );
    }
    if (!info.indexed) {
      throw new KqlError(
        `${n.field} is not indexed, so it cannot be searched.`,
        n.fieldFrom,
        n.fieldTo,
      );
    }
    // field:* — has a value.
    if (
      n.op === ':' &&
      n.values.length === 1 &&
      !n.values[0]?.quoted &&
      n.values[0]?.text === '*'
    ) {
      return { exists: { field: path } };
    }
    if (n.op !== ':') {
      const v = n.values[0] as KqlValue;
      if (info.type === 'text' || info.type === 'bool' || info.type === 'json') {
        throw new KqlError(
          `${n.field} is a ${info.type} field: ranges work on numbers, dates and keywords.`,
          n.fieldFrom,
          v.to,
        );
      }
      const value = this.scalar(info, n.field, v);
      const key = { '>': 'gt', '>=': 'gte', '<': 'lt', '<=': 'lte' }[n.op];
      return { range: { field: path, [key]: value } };
    }
    const one = (v: KqlValue): JsonObject => {
      const wild = !v.quoted && /[*?]/.test(v.text);
      switch (info.type) {
        case 'text':
          if (wild) {
            throw new KqlError(
              `Wildcards work on keyword fields; ${n.field} is a text field.`,
              ...at(v),
            );
          }
          this.terms.push(v.text);
          if (scored) this.highlight.add(path);
          return v.quoted
            ? { matchPhrase: { field: path, text: v.text } }
            : { match: { field: path, text: v.text } };
        case 'keyword':
        case 'uuid':
        case 'json':
          this.terms.push(v.text.replace(/[*?]/g, ''));
          if (wild) {
            const star = v.text.indexOf('*');
            if (star === v.text.length - 1 && !v.text.includes('?') && star > 0) {
              return { prefix: { field: path, value: v.text.slice(0, -1) } };
            }
            return { wildcard: { field: path, pattern: v.text } };
          }
          return { term: { field: path, value: v.text } };
        case 'date':
          if (wild)
            throw new KqlError(`Wildcards do not work on ${n.field}, a date field.`, ...at(v));
          if (DATE_ONLY.test(v.text)) {
            const day = Date.parse(`${v.text}T00:00:00Z`);
            if (Number.isNaN(day)) throw new KqlError(`"${v.text}" is not a date.`, ...at(v));
            const next = new Date(day + 86_400_000).toISOString().slice(0, 10);
            return { range: { field: path, gte: `${v.text}T00:00:00Z`, lt: `${next}T00:00:00Z` } };
          }
          return { term: { field: path, value: this.scalar(info, n.field, v) } };
        default:
          if (wild)
            throw new KqlError(
              `Wildcards do not work on ${n.field}, a ${info.type} field.`,
              ...at(v),
            );
          return { term: { field: path, value: this.scalar(info, n.field, v) } };
      }
    };
    if (n.values.length === 1) return one(n.values[0] as KqlValue);
    const plain = n.values.every((v) => v.quoted || !/[*?]/.test(v.text));
    if (plain && info.type !== 'text' && info.type !== 'date') {
      for (const v of n.values)
        if (info.type === 'keyword' || info.type === 'uuid') this.terms.push(v.text);
      return { terms: { field: path, values: n.values.map((v) => this.scalar(info, n.field, v)) } };
    }
    return { bool: { should: n.values.map(one) } };
  }

  /** `_id:42` or `_id:(1 OR 2)`: the documents with these ids. */
  private ids(n: Extract<Node, { kind: 'field' }>): JsonObject {
    if (n.op !== ':') {
      throw new KqlError('_id takes exact ids: _id:42 or _id:(1 OR 2).', n.fieldFrom, n.to);
    }
    const ids = n.values.map((v): JsonValue => {
      if (!v.quoted && /[*?]/.test(v.text)) {
        throw new KqlError('_id takes exact ids, not wildcards.', v.from, v.to);
      }
      // A whole number is a uint id; anything else (or a quoted number) a string or UUID id.
      return !v.quoted && /^\d+$/.test(v.text) && Number.isSafeInteger(Number(v.text))
        ? Number(v.text)
        : v.text;
    });
    return { ids };
  }

  private scalar(info: FieldInfo, name: string, v: KqlValue): JsonValue {
    switch (info.type) {
      case 'i64':
      case 'f64': {
        const num = Number(v.text);
        if (v.text.trim() === '' || !Number.isFinite(num)) {
          throw new KqlError(
            `${name} is a number field; "${v.text}" is not a number.`,
            v.from,
            v.to,
          );
        }
        if (info.type === 'i64' && !Number.isInteger(num)) {
          throw new KqlError(`${name} holds whole numbers; "${v.text}" is not one.`, v.from, v.to);
        }
        return num;
      }
      case 'bool': {
        const t = v.text.toLowerCase();
        if (t !== 'true' && t !== 'false') {
          throw new KqlError(`${name} is true or false, not "${v.text}".`, v.from, v.to);
        }
        return t === 'true';
      }
      case 'date':
        if (Number.isNaN(Date.parse(v.text))) {
          throw new KqlError(
            `"${v.text}" is not a date. Use 2024-01-15 or 2024-01-15T15:00:00Z.`,
            v.from,
            v.to,
          );
        }
        return v.text;
      default:
        return v.text;
    }
  }
}

/** The name nearest to `name` by edit distance, if any is close. */
export function closest(name: string, names: string[]): string | undefined {
  let best: string | undefined;
  let bestD = Number.POSITIVE_INFINITY;
  const lower = name.toLowerCase();
  for (const n of names) {
    const d = distance(lower, n.toLowerCase());
    if (d < bestD) {
      bestD = d;
      best = n;
    }
  }
  return best !== undefined && bestD <= Math.max(2, Math.floor(name.length / 3)) ? best : undefined;
}

function distance(a: string, b: string): number {
  const row = Array.from({ length: b.length + 1 }, (_, j) => j);
  for (let i = 1; i <= a.length; i++) {
    let prev = row[0] as number;
    row[0] = i;
    for (let j = 1; j <= b.length; j++) {
      const tmp = row[j] as number;
      row[j] = Math.min(
        (row[j] as number) + 1,
        (row[j - 1] as number) + 1,
        prev + (a[i - 1] === b[j - 1] ? 0 : 1),
      );
      prev = tmp;
    }
  }
  return row[b.length] as number;
}

/** A value as the query language writes it: quoted when it is not a plain word. */
export function quoteValue(v: string): string {
  if (v !== '' && /^[^\s()":<>=\\]+$/.test(v) && !/^(and|or|not)$/i.test(v)) return v;
  return `"${v.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

/** A field name as the query language writes it (names are plain words in schemas). */
export function clause(field: string, value: string, op: Comparison = ':'): string {
  return op === ':' ? `${field}:${quoteValue(value)}` : `${field} ${op} ${quoteValue(value)}`;
}
