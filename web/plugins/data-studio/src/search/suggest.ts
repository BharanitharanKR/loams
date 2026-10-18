// Suggestions for the query bar, by cursor position: field names (with their types),
// the operators a field's type allows, values (from the caller's cache of top values),
// AND / OR / NOT, and recent queries. Pure: the caller fetches values and passes them in.

import { type FieldInfo, type FieldType, ID_FIELD, lex, quoteValue, type Token } from './kql.js';

export type SuggestionKind = 'field' | 'operator' | 'value' | 'conjunction' | 'recent' | 'hint';

export interface Suggestion {
  kind: SuggestionKind;
  /** What the list shows. */
  label: string;
  /** A second line: the field type, a count, what an operator does. */
  detail?: string;
  /** The text that replaces [from, to). A `hint` has none and cannot be picked. */
  insert?: string;
  from: number;
  to: number;
  type?: FieldType;
}

/** What the cursor is on, for the caller to fetch values for. */
export type SuggestContext =
  | { kind: 'empty' }
  | { kind: 'field'; prefix: string }
  | { kind: 'operator'; field: string }
  | { kind: 'value'; field: string; prefix: string; type: FieldType | undefined }
  | { kind: 'after-clause' };

export interface SuggestInput {
  text: string;
  cursor: number;
  fields: FieldInfo[];
  /** Top values of a field for a prefix, when the caller has them (keyword fields). */
  values?: (field: string, prefix: string) => { value: string; count?: number }[] | undefined;
  recent?: string[];
  /** The most items returned. */
  limit?: number;
}

export interface SuggestResult {
  context: SuggestContext;
  items: Suggestion[];
}

const OPS: Record<string, { op: string; label: string; detail: string }[]> = {
  text: [
    { op: ':', label: ':', detail: 'contains these words' },
    { op: ':*', label: ':*', detail: 'has a value' },
  ],
  keyword: [
    { op: ':', label: ':', detail: 'is' },
    { op: ':*', label: ':*', detail: 'has a value' },
    { op: ' > ', label: '>', detail: 'after, alphabetically' },
    { op: ' < ', label: '<', detail: 'before, alphabetically' },
  ],
  number: [
    { op: ':', label: ':', detail: 'is' },
    { op: ' >= ', label: '>=', detail: 'at least' },
    { op: ' > ', label: '>', detail: 'greater than' },
    { op: ' <= ', label: '<=', detail: 'at most' },
    { op: ' < ', label: '<', detail: 'less than' },
    { op: ':*', label: ':*', detail: 'has a value' },
  ],
  date: [
    { op: ':', label: ':', detail: 'on (a day, or an exact time)' },
    { op: ' >= ', label: '>=', detail: 'on or after' },
    { op: ' < ', label: '<', detail: 'before' },
    { op: ' > ', label: '>', detail: 'after' },
    { op: ' <= ', label: '<=', detail: 'on or before' },
    { op: ':*', label: ':*', detail: 'has a value' },
  ],
  bool: [
    { op: ':', label: ':', detail: 'is' },
    { op: ':*', label: ':*', detail: 'has a value' },
  ],
};

function opsFor(type: FieldType): { op: string; label: string; detail: string }[] {
  if (type === 'i64' || type === 'f64') return OPS.number ?? [];
  if (type === 'date') return OPS.date ?? [];
  if (type === 'bool') return OPS.bool ?? [];
  if (type === 'text') return OPS.text ?? [];
  return OPS.keyword ?? [];
}

export const TYPE_LABEL: Record<FieldType, string> = {
  text: 'text',
  keyword: 'keyword',
  i64: 'integer',
  f64: 'number',
  bool: 'boolean',
  date: 'date',
  uuid: 'uuid',
  json: 'json',
  vector: 'vector',
  unknown: 'field',
};

/** Ranks names against a prefix: prefix matches first, then substring matches. */
function rankNames(fields: FieldInfo[], prefix: string): FieldInfo[] {
  const p = prefix.toLowerCase();
  const starts: FieldInfo[] = [];
  const contains: FieldInfo[] = [];
  for (const f of fields) {
    const n = f.name.toLowerCase();
    if (!p || n.startsWith(p)) starts.push(f);
    else if (n.includes(p)) contains.push(f);
  }
  return [...starts, ...contains];
}

/** The suggestions at the cursor. */
export function suggest(input: SuggestInput): SuggestResult {
  const { text, fields } = input;
  const cursor = Math.max(0, Math.min(input.cursor, text.length));
  const limit = input.limit ?? 12;
  const searchable = fields.filter((f) => f.type !== 'vector' && f.indexed);
  // `_id` is a clause on every collection (last, unless the schema has its own).
  if (!searchable.some((f) => f.name === ID_FIELD)) {
    searchable.push({ name: ID_FIELD, type: 'keyword', fast: false, indexed: true });
  }
  const sig = lex(text).filter((t) => t.kind !== 'ws');
  // The word or conjunction the cursor is in or at the end of.
  const at = sig.find(
    (t) =>
      (t.kind === 'word' || t.kind === 'and' || t.kind === 'or' || t.kind === 'not') &&
      t.from < cursor &&
      cursor <= t.to,
  );
  const before = sig.filter((t) => t.to <= (at ? at.from : cursor));
  const prev = before[before.length - 1];
  const prev2 = before[before.length - 2];

  const fieldItems = (prefix: string, from: number, to: number): Suggestion[] =>
    rankNames(searchable, prefix).map((f) => ({
      kind: 'field',
      label: f.name,
      detail: TYPE_LABEL[f.type],
      type: f.type,
      insert: `${f.name} `,
      from,
      to,
    }));

  // A value position: after `field:` / `field >=`, or inside `field:( … )`.
  const valueOwner = valueField(sig, cursor);
  if (valueOwner) {
    const { field, from, to, prefix } = valueOwner;
    const info = fields.find((f) => f.name === field);
    const items: Suggestion[] = [];
    if (info?.type === 'bool') {
      for (const v of ['true', 'false']) {
        if (v.startsWith(prefix.toLowerCase()))
          items.push({ kind: 'value', label: v, insert: `${v} `, from, to });
      }
    } else {
      const vals = input.values?.(field, prefix) ?? [];
      for (const v of vals) {
        items.push({
          kind: 'value',
          label: v.value,
          detail: v.count === undefined ? undefined : v.count.toLocaleString('en-US'),
          insert: `${quoteValue(v.value)} `,
          from,
          to,
        });
      }
      if (items.length === 0) {
        const hint =
          field === ID_FIELD && !info
            ? 'A document id, like _id:42 or _id:(1 OR 2)'
            : valueHint(info?.type, field, info?.fast ?? false);
        if (hint) items.push({ kind: 'hint', label: hint, from, to });
      }
    }
    return {
      context: { kind: 'value', field, prefix, type: info?.type },
      items: items.slice(0, limit),
    };
  }

  // In a word: a field name (or free text) being typed.
  if (at && at.kind === 'word') {
    const prefix = text.slice(at.from, cursor);
    const items = fieldItems(prefix, at.from, at.to);
    const upper = prefix.toUpperCase();
    if (prev && completesClause(prev)) {
      for (const c of ['AND', 'OR', 'NOT']) {
        if (c.startsWith(upper) && upper.length > 0) {
          items.push({ kind: 'conjunction', label: c, insert: `${c} `, from: at.from, to: at.to });
        }
      }
    }
    return { context: { kind: 'field', prefix }, items: items.slice(0, limit) };
  }
  if (at && (at.kind === 'and' || at.kind === 'or' || at.kind === 'not')) {
    // The cursor is at the end of a conjunction: offer the others, completing it.
    const prefix = text.slice(at.from, cursor).toUpperCase();
    const items: Suggestion[] = ['AND', 'OR', 'NOT']
      .filter((c) => c.startsWith(prefix))
      .map((c) => ({
        kind: 'conjunction' as const,
        label: c,
        insert: `${c} `,
        from: at.from,
        to: at.to,
      }));
    items.push(...fieldItems(text.slice(at.from, cursor), at.from, at.to));
    return { context: { kind: 'field', prefix }, items: items.slice(0, limit) };
  }

  // After whitespace (or at the start).
  if (!prev) {
    const items: Suggestion[] = [];
    if (!text.trim()) {
      for (const r of (input.recent ?? []).slice(0, 4)) {
        items.push({
          kind: 'recent',
          label: r,
          detail: 'recent',
          insert: r,
          from: 0,
          to: text.length,
        });
      }
    }
    items.push(...fieldItems('', cursor, cursor));
    return { context: { kind: 'empty' }, items: items.slice(0, limit) };
  }
  // `field ` then the cursor: operators for its type.
  if (
    prev.kind === 'word' &&
    !prev.isValue &&
    !(prev2 && (prev2.kind === 'colon' || prev2.kind === 'cmp'))
  ) {
    const info = searchable.find((f) => f.name === prev.value);
    if (info) {
      const items: Suggestion[] = opsFor(info.type).map((o) => ({
        kind: 'operator',
        label: `${info.name} ${o.label}`,
        detail: o.detail,
        // Replaces the gap after the name, so `field :` becomes `field:`.
        insert: o.op === ':*' ? ':* ' : o.op,
        from: prev.to,
        to: cursor,
      }));
      return { context: { kind: 'operator', field: info.name }, items: items.slice(0, limit) };
    }
  }
  if (completesClause(prev)) {
    const items: Suggestion[] = ['AND', 'OR', 'NOT'].map((c) => ({
      kind: 'conjunction',
      label: c,
      insert: `${c} `,
      from: cursor,
      to: cursor,
    }));
    items.push(...fieldItems('', cursor, cursor));
    return { context: { kind: 'after-clause' }, items: items.slice(0, limit) };
  }
  // After AND / OR / NOT / `(`: a new clause.
  return {
    context: { kind: 'field', prefix: '' },
    items: fieldItems('', cursor, cursor).slice(0, limit),
  };
}

function completesClause(t: Token): boolean {
  return t.kind === 'word' || t.kind === 'quoted' || t.kind === 'rparen';
}

function valueHint(type: FieldType | undefined, field: string, fast: boolean): string | undefined {
  switch (type) {
    case 'i64':
      return `A whole number, like ${field} >= 100`;
    case 'f64':
      return 'A number, like 0.5';
    case 'date':
      return 'A date, like 2024-01-15 or 2024-01-15T15:00:00Z';
    case 'text':
      return 'Words to match, or a "quoted phrase"';
    case 'keyword':
    case 'uuid':
      return fast ? 'No matching values' : 'An exact value; Push* matches a prefix';
    default:
      return undefined;
  }
}

/** The field whose value the cursor is on, the replace span and the typed prefix. */
function valueField(
  sig: Token[],
  cursor: number,
): { field: string; from: number; to: number; prefix: string } | undefined {
  const valueTok = sig.find(
    (t) =>
      (t.kind === 'word' || t.kind === 'quoted') && t.isValue && t.from < cursor && cursor <= t.to,
  );
  let opIdx: number;
  if (valueTok) opIdx = sig.indexOf(valueTok) - 1;
  else {
    opIdx = -1;
    for (let k = 0; k < sig.length; k++) if ((sig[k] as Token).to <= cursor) opIdx = k;
  }
  const op = sig[opIdx];
  if (!op) return undefined;
  let span: { from: number; to: number; prefix: string } = { from: cursor, to: cursor, prefix: '' };
  if (valueTok) {
    const raw = valueTok.raw.slice(0, cursor - valueTok.from);
    span = {
      from: valueTok.from,
      to: valueTok.to,
      prefix: valueTok.kind === 'quoted' ? raw.replace(/^"/, '').replace(/"$/, '') : raw,
    };
  }
  if (op.kind === 'colon' || op.kind === 'cmp') {
    const field = sig[opIdx - 1];
    return field?.kind === 'word' ? { field: field.value, ...span } : undefined;
  }
  // Inside `field:( a OR b`: walk back to the `(` that follows `field:`.
  if (op.kind === 'or' || op.kind === 'lparen') {
    for (let k = opIdx; k >= 0; k--) {
      const t = sig[k] as Token;
      if (t.kind === 'lparen') {
        const colon = sig[k - 1];
        const field = sig[k - 2];
        return colon?.kind === 'colon' && field?.kind === 'word'
          ? { field: field.value, ...span }
          : undefined;
      }
      if (t.kind !== 'or' && t.kind !== 'word' && t.kind !== 'quoted') return undefined;
    }
  }
  return undefined;
}

/** Applies a suggestion: the new text and where the cursor goes. */
export function applySuggestion(text: string, s: Suggestion): { text: string; cursor: number } {
  if (s.insert === undefined) return { text, cursor: s.to };
  let insert = s.insert;
  const rest = text.slice(s.to);
  // Do not double the space before what follows.
  if (insert.endsWith(' ') && /^\s/.test(rest)) insert = insert.slice(0, -1);
  const next = text.slice(0, s.from) + insert + rest;
  return { text: next, cursor: s.from + insert.length + (insert !== s.insert ? 1 : 0) };
}
