// The SQL editor's languages (CodeMirror 6, MIT): the SQL dialects each page speaks, a small
// GQL/Cypher mode for the Graph page, the highlight style (classes painted from tokens.css),
// schema-aware completion and the signature hint for function calls.

import {
  type Completion,
  type CompletionContext,
  type CompletionResult,
  type CompletionSource,
  completeFromList,
} from '@codemirror/autocomplete';
import {
  MySQL,
  PostgreSQL,
  type SQLDialect,
  type SQLNamespace,
  SQLDialect as SqlDialectClass,
  StandardSQL,
  sql,
} from '@codemirror/lang-sql';
import {
  HighlightStyle,
  LanguageSupport,
  StreamLanguage,
  syntaxHighlighting,
} from '@codemirror/language';
import { type EditorState, type Extension, StateField } from '@codemirror/state';
import { showTooltip, type Tooltip } from '@codemirror/view';
import { tags as t } from '@lezer/highlight';

/** The query languages the editor knows. */
export type SqlDialectName = 'generic' | 'datafusion' | 'mysql' | 'postgres' | 'clickhouse' | 'gql';

export interface SqlColumn {
  name: string;
  /** The engine's type name (`Utf8`, `Int64`, `LowCardinality(String)`, `varchar(255)`). */
  type?: string;
}

export interface SqlTable {
  name: string;
  /** The schema or database the table lives in, when it is not the default one. */
  schema?: string;
  /** Shown beside the name in completions: `collection`, `table`, `view`. */
  kind?: string;
  columns: SqlColumn[];
}

export interface SqlFunction {
  name: string;
  /** Parameter names in order; `?` marks an optional one, `...` a repeating one. */
  params?: string[];
  /** What the call returns, for table functions its columns. */
  returns?: string;
  kind?: 'table' | 'scalar' | 'aggregate' | 'window';
  /** One sentence for the completion's info panel. */
  doc?: string;
}

/** What the editor completes beyond keywords: tables with their typed columns, and functions. */
export interface SqlSchema {
  tables: SqlTable[];
  functions?: SqlFunction[];
  /** The schema or database that unqualified names resolve in. */
  defaultSchema?: string;
}

export const EMPTY_SCHEMA: SqlSchema = { tables: [] };

// ---------- dialects ----------

/** DataFusion speaks PostgreSQL-flavoured SQL; the engine adds its search and graph functions. */
const DataFusion = SqlDialectClass.define({
  ...PostgreSQL.spec,
  keywords: `${PostgreSQL.spec.keywords ?? ''} explain analyze describe show tables columns unnest qualify`,
});

const CLICKHOUSE_KEYWORDS =
  'select from where prewhere group by order limit offset having with totals rollup cube as on ' +
  'using join inner left right full outer cross any all anti semi asof global array distinct ' +
  'union intersect except case when then else end and or not in is null like ilike between ' +
  'exists interval insert into values format settings final sample create table database view ' +
  'materialized dictionary drop truncate alter add column modify rename optimize describe desc ' +
  'show tables databases processlist explain asc nulls first last collate fill step to if ' +
  'engine partition primary key ttl system kill query mutation attach detach exchange window ' +
  'over rows range unbounded preceding following current row qualify true false';
const CLICKHOUSE_TYPES =
  'UInt8 UInt16 UInt32 UInt64 UInt128 UInt256 Int8 Int16 Int32 Int64 Int128 Int256 Float32 ' +
  'Float64 Decimal Decimal32 Decimal64 Decimal128 Decimal256 Bool String FixedString UUID Date ' +
  'Date32 DateTime DateTime64 Enum8 Enum16 Array Tuple Map Nullable LowCardinality IPv4 IPv6 ' +
  'JSON Dynamic Variant Nested';

const ClickHouse = SqlDialectClass.define({
  keywords: CLICKHOUSE_KEYWORDS,
  types: CLICKHOUSE_TYPES.toLowerCase(),
  builtin: '',
  backslashEscapes: true,
  hashComments: true,
  identifierQuotes: '`"',
  operatorChars: '*+-%<>!=&|/^?:',
});

function sqlDialect(name: SqlDialectName): SQLDialect {
  switch (name) {
    case 'mysql':
      return MySQL;
    case 'postgres':
      return PostgreSQL;
    case 'clickhouse':
      return ClickHouse;
    case 'datafusion':
      return DataFusion;
    default:
      return StandardSQL;
  }
}

// ---------- GQL / Cypher ----------

export const GQL_KEYWORDS = [
  'MATCH',
  'OPTIONAL',
  'WHERE',
  'RETURN',
  'WITH',
  'ORDER',
  'BY',
  'LIMIT',
  'SKIP',
  'OFFSET',
  'ASC',
  'DESC',
  'DISTINCT',
  'AS',
  'AND',
  'OR',
  'XOR',
  'NOT',
  'IN',
  'IS',
  'NULL',
  'TRUE',
  'FALSE',
  'CREATE',
  'MERGE',
  'DELETE',
  'DETACH',
  'SET',
  'REMOVE',
  'UNWIND',
  'CALL',
  'YIELD',
  'INSERT',
  'FILTER',
  'LET',
  'FOR',
  'NEXT',
  'USE',
  'GRAPH',
  'EXISTS',
  'CASE',
  'WHEN',
  'THEN',
  'ELSE',
  'END',
  'UNION',
  'ALL',
  'COUNT',
  'STARTS',
  'ENDS',
  'CONTAINS',
];
const GQL_KEYWORD_SET = new Set(GQL_KEYWORDS.map((k) => k.toLowerCase()));

interface GqlState {
  comment: boolean;
}

/** A stream mode for GQL and Cypher: keywords, labels, strings, numbers, parameters, comments. */
const gqlLanguage = StreamLanguage.define<GqlState>({
  name: 'gql',
  startState: () => ({ comment: false }),
  token(stream, state) {
    if (state.comment) {
      if (stream.skipTo('*/')) {
        stream.next();
        stream.next();
        state.comment = false;
      } else stream.skipToEnd();
      return 'blockComment';
    }
    if (stream.eatSpace()) return null;
    if (stream.match('//') || stream.match('--')) {
      stream.skipToEnd();
      return 'lineComment';
    }
    if (stream.match('/*')) {
      state.comment = true;
      return 'blockComment';
    }
    const ch = stream.peek();
    if (ch === '"' || ch === "'") {
      stream.next();
      let escaped = false;
      for (let c = stream.next(); c !== undefined; c = stream.next()) {
        if (c === ch && !escaped) break;
        escaped = !escaped && c === '\\';
      }
      return 'string';
    }
    if (ch === '`') {
      stream.next();
      stream.skipTo('`') ? stream.next() : stream.skipToEnd();
      return 'variableName';
    }
    if (stream.match(/^\$[A-Za-z_][\w]*/)) return 'variableName.special';
    if (stream.match(/^-?\d+(\.\d+)?([eE][-+]?\d+)?/)) return 'number';
    if (stream.match(/^:[A-Za-z_][\w]*/)) return 'typeName';
    if (stream.match(/^[A-Za-z_][\w]*/)) {
      const word = stream.current().toLowerCase();
      if (GQL_KEYWORD_SET.has(word)) return 'keyword';
      if (stream.match(/^\s*\(/, false)) return 'function';
      return 'variableName';
    }
    if (stream.match(/^(->|<-|-|=|<>|<=|>=|<|>|\+|\*|\/|%|\|)/)) return 'operator';
    if (stream.match(/^[()[\]{},.;]/)) return 'punctuation';
    stream.next();
    return null;
  },
  languageData: { commentTokens: { line: '//', block: { open: '/*', close: '*/' } } },
});

// ---------- highlighting ----------

/** Token classes; tokens.css paints them (`--op-code-*`), so light and dark follow the theme. */
export const highlightStyle = HighlightStyle.define([
  { tag: [t.keyword, t.operatorKeyword, t.modifier, t.controlKeyword], class: 'loams-tok-keyword' },
  { tag: [t.string, t.special(t.string), t.regexp], class: 'loams-tok-string' },
  { tag: [t.number, t.bool, t.null, t.atom], class: 'loams-tok-number' },
  { tag: [t.lineComment, t.blockComment, t.comment], class: 'loams-tok-comment' },
  {
    tag: [t.function(t.variableName), t.standard(t.name), t.function(t.name)],
    class: 'loams-tok-function',
  },
  { tag: [t.typeName, t.className, t.labelName], class: 'loams-tok-type' },
  { tag: [t.special(t.name), t.special(t.variableName)], class: 'loams-tok-param' },
  { tag: [t.operator, t.compareOperator, t.arithmeticOperator], class: 'loams-tok-operator' },
  { tag: [t.punctuation, t.paren, t.bracket, t.separator], class: 'loams-tok-punct' },
  { tag: [t.propertyName, t.attributeName], class: 'loams-tok-property' },
]);

// ---------- functions ----------

/** The engine's search table functions (crates/loams-query/src/sql/udtf.rs). */
export const ENGINE_SEARCH_FUNCTIONS: SqlFunction[] = [
  {
    name: 'vector_search',
    kind: 'table',
    params: ['collection', 'query_vector', 'field?', 'k?', 'filter_json?', 'exact?'],
    returns: 'the collection’s columns and _score',
    doc: 'Nearest neighbours of a vector in a collection’s vector field.',
  },
  {
    name: 'text_search',
    kind: 'table',
    params: ['collection', 'text', 'field?', 'k?', 'filter_json?'],
    returns: 'the collection’s columns and _score',
    doc: 'BM25 full-text search over a text field.',
  },
  {
    name: 'hybrid_search',
    kind: 'table',
    params: [
      'collection',
      'vector_field',
      'query_vector',
      'text_field',
      'text',
      'k',
      'fusion?',
      'rrf_k?',
      'filter_json?',
    ],
    returns: 'the collection’s columns and _score',
    doc: 'Vector and text search fused in one call (rrf or dbsf).',
  },
  {
    name: 'rrf',
    kind: 'table',
    params: ['retriever', 'retriever', '...', 'k?', 'limit?'],
    returns: 'the collection’s columns and _score',
    doc: 'Reciprocal rank fusion of nested vector_search and text_search calls on one collection.',
  },
];

/** The engine's graph table functions (graph_udtf.rs, graph_algos.rs). */
export const ENGINE_GRAPH_FUNCTIONS: SqlFunction[] = [
  {
    name: 'graph_expand',
    kind: 'table',
    params: [
      'graph',
      'seeds',
      'seed_label?',
      'hops?',
      'direction?',
      'edge_types?',
      'edge_filter?',
      'vertex_filter?',
      'limit_per_seed?',
    ],
    doc: 'Walks out from seed keys up to `hops` edges. NULL takes an argument’s default.',
  },
  {
    name: 'graph_neighbors',
    kind: 'table',
    params: [
      'graph',
      'keys',
      'label?',
      'direction?',
      'edge_types?',
      'edge_filter?',
      'limit_per_key?',
    ],
    doc: 'The direct neighbours of each key.',
  },
  {
    name: 'graph_degree',
    kind: 'table',
    params: ['graph', 'keys', 'label?', 'direction?', 'edge_types?'],
    doc: 'Edge counts per key.',
  },
  {
    name: 'graph_shortest_path',
    kind: 'table',
    params: ['graph', 'from', 'to', 'label?', 'direction?', 'edge_types?', 'max_hops?'],
    doc: 'The shortest path between two keys.',
  },
  {
    name: 'pagerank',
    kind: 'table',
    params: ['graph', 'label?', 'edge_types?', 'damping?', 'max_iterations?', 'tolerance?'],
    returns: '(label, key, rank)',
    doc: 'PageRank over the graph’s adjacency index.',
  },
  {
    name: 'wcc',
    kind: 'table',
    params: ['graph', 'edge_types?'],
    returns: '(label, key, component)',
    doc: 'Weakly connected components.',
  },
  {
    name: 'leiden',
    kind: 'table',
    params: [
      'graph',
      'edge_types?',
      'weight?',
      'resolution?',
      'max_levels?',
      'max_cluster_size?',
      'seed?',
    ],
    returns: '(label, key, level, community, parent_community)',
    doc: 'Leiden community detection.',
  },
];

export const ENGINE_FUNCTIONS: SqlFunction[] = [
  ...ENGINE_SEARCH_FUNCTIONS,
  ...ENGINE_GRAPH_FUNCTIONS,
];

/** `name(a, b [, c])` for a function. */
export function signatureText(f: SqlFunction): string {
  if (!f.params) return `${f.name}(…)`;
  let out = '';
  let optional = 0;
  f.params.forEach((p, i) => {
    const opt = p.endsWith('?');
    const name = opt ? p.slice(0, -1) : p;
    const sep = i === 0 ? '' : ', ';
    if (opt) {
      out += ` [${sep}${name}`;
      optional++;
    } else out += `${sep}${name}`;
  });
  return `${f.name}(${out.trim()}${']'.repeat(optional)})`;
}

// ---------- completion ----------

/** Table names after FROM / JOIN in the statement around the cursor, with aliases. */
export function tablesInScope(text: string): { name: string; alias?: string }[] {
  const out: { name: string; alias?: string }[] = [];
  const re =
    /\b(?:from|join)\s+((?:[`"]?[\w$]+[`"]?\.)?[`"]?[\w$]+[`"]?)(?:\s+(?:as\s+)?([A-Za-z_][\w]*))?/gi;
  const reserved = new Set([
    'where',
    'on',
    'using',
    'join',
    'left',
    'right',
    'inner',
    'outer',
    'full',
    'cross',
    'group',
    'order',
    'limit',
    'having',
    'union',
    'prewhere',
    'final',
    'sample',
    'settings',
    'format',
    'window',
    'qualify',
  ]);
  // A search table function reads the collection named by its first argument.
  for (const m of text.matchAll(
    /\b(?:vector_search|text_search|hybrid_search)\s*\(\s*'([^']+)'/gi,
  )) {
    if (m[1]) out.push({ name: m[1] });
  }
  for (const m of text.matchAll(re)) {
    const raw = (m[1] ?? '').replace(/[`"]/g, '');
    const name = raw.includes('.') ? (raw.split('.').pop() ?? raw) : raw;
    const alias = m[2] && !reserved.has(m[2].toLowerCase()) ? m[2] : undefined;
    out.push({ name, alias });
  }
  return out;
}

function columnOption(c: SqlColumn, table?: string): Completion {
  return {
    label: c.name,
    type: 'property',
    detail: c.type,
    info: table ? `${table}.${c.name}${c.type ? ` : ${c.type}` : ''}` : undefined,
    boost: c.name.startsWith('_') ? -1 : 2,
  };
}

function functionOption(f: SqlFunction): Completion {
  const sig = f.params ? signatureText(f) : undefined;
  return {
    label: f.name,
    type: f.kind === 'table' ? 'class' : 'function',
    detail: f.kind === 'table' ? 'table function' : f.kind,
    info:
      sig || f.doc
        ? [sig, f.returns && `→ ${f.returns}`, f.doc].filter(Boolean).join('\n')
        : undefined,
    apply: f.params ? `${f.name}(` : undefined,
    boost: f.params ? 1 : -2,
  };
}

/** Columns of the tables the statement reads, offered without a qualifier; functions. */
function scopeSource(schema: SqlSchema, functions: SqlFunction[]): CompletionSource {
  const byName = new Map(schema.tables.map((tb) => [tb.name.toLowerCase(), tb]));
  const fnOptions = functions.map((f) => ({ f, option: functionOption(f) }));
  return (ctx: CompletionContext): CompletionResult | null => {
    const word = ctx.matchBefore(/[\w$]+/);
    const before = ctx.state.sliceDoc(
      Math.max(0, (word?.from ?? ctx.pos) - 1),
      word?.from ?? ctx.pos,
    );
    // After a dot, lang-sql's schema source completes `table.column`.
    if (before === '.') return null;
    if (!word && !ctx.explicit) return null;
    if (word && word.from === word.to && !ctx.explicit) return null;
    const text = ctx.state.doc.toString();
    const options: Completion[] = [];
    const seen = new Set<string>();
    for (const { name } of tablesInScope(text)) {
      const tb = byName.get(name.toLowerCase());
      if (!tb) continue;
      for (const c of tb.columns) {
        if (seen.has(c.name)) continue;
        seen.add(c.name);
        options.push(columnOption(c, tb.name));
      }
    }
    // After FROM or JOIN a table function is what fits; elsewhere a column or a scalar.
    const lead = ctx.state.sliceDoc(
      Math.max(0, (word?.from ?? ctx.pos) - 40),
      word?.from ?? ctx.pos,
    );
    const afterFrom = /\b(from|join)\s+$/i.test(lead);
    for (const { f, option } of fnOptions) {
      const table = f.kind === 'table';
      options.push({ ...option, boost: table === afterFrom ? (option.boost ?? 0) + 2 : -4 });
    }
    return { from: word?.from ?? ctx.pos, options, validFor: /^[\w$]*$/ };
  };
}

function namespaceOf(schema: SqlSchema): SQLNamespace {
  // Prototype-free: schema and table names come from the server (`__proto__` is a valid name).
  const top: Record<string, SQLNamespace> = Object.create(null);
  const bySchema: Record<string, Record<string, SQLNamespace>> = Object.create(null);
  for (const tb of schema.tables) {
    const entry: SQLNamespace = {
      self: { label: tb.name, type: 'type', detail: tb.kind ?? 'table', boost: 1 },
      children: tb.columns.map((c) => columnOption(c)),
    };
    if (!tb.schema || tb.schema === schema.defaultSchema) top[tb.name] = entry;
    if (tb.schema) {
      bySchema[tb.schema] ??= Object.create(null);
      (bySchema[tb.schema] as Record<string, SQLNamespace>)[tb.name] = entry;
    }
  }
  for (const [s, tables] of Object.entries(bySchema)) {
    if (!(s in top))
      top[s] = { self: { label: s, type: 'namespace', detail: 'schema' }, children: tables };
  }
  return top;
}

// ---------- signature hint ----------

/** The call the cursor is inside: its function name, where `(` is, and the argument index. */
export function callAt(
  text: string,
  pos: number,
): { name: string; open: number; arg: number } | undefined {
  const start = Math.max(0, pos - 4000);
  const stack: { name: string; open: number; arg: number }[] = [];
  let quote: string | undefined;
  let line = false;
  let block = false;
  for (let i = start; i < pos; i++) {
    const c = text[i] as string;
    const next = text[i + 1];
    if (line) {
      if (c === '\n') line = false;
      continue;
    }
    if (block) {
      if (c === '*' && next === '/') {
        block = false;
        i++;
      }
      continue;
    }
    if (quote) {
      if (c === '\\') i++;
      else if (c === quote) quote = undefined;
      continue;
    }
    if (c === "'" || c === '"' || c === '`') quote = c;
    else if (c === '-' && next === '-') line = true;
    else if (c === '/' && next === '*') block = true;
    else if (c === '(') {
      const m = /([A-Za-z_][\w]*)\s*$/.exec(text.slice(Math.max(start, i - 64), i));
      stack.push({ name: m?.[1] ?? '', open: i, arg: 0 });
    } else if (c === ')') stack.pop();
    else if (c === ',' || c === '[') {
      const top = stack[stack.length - 1];
      if (c === ',' && top) top.arg++;
      // A `[` opens an array literal: its commas are not arguments.
      if (c === '[') stack.push({ name: '', open: i, arg: 0 });
    } else if (c === ']') stack.pop();
  }
  for (let k = stack.length - 1; k >= 0; k--) {
    const s = stack[k] as { name: string; open: number; arg: number };
    if (s.name) return s;
    // inside an array literal: report the enclosing call
  }
  return undefined;
}

function signatureTooltip(state: EditorState, fns: Map<string, SqlFunction>): Tooltip | null {
  const sel = state.selection.main;
  if (!sel.empty) return null;
  const call = callAt(state.doc.toString(), sel.head);
  const f = call && fns.get(call.name.toLowerCase());
  if (!call || !f?.params) return null;
  return {
    pos: call.open,
    above: true,
    strictSide: true,
    arrow: false,
    create() {
      const dom = document.createElement('div');
      dom.className = 'loams-cm-signature';
      dom.setAttribute('role', 'status');
      const params = f.params ?? [];
      const at = Math.min(call.arg, params.length - 1);
      const head = document.createElement('span');
      head.className = 'loams-tok-function';
      head.textContent = f.name;
      dom.append(head, '(');
      params.forEach((p, i) => {
        if (i) dom.append(', ');
        const span = document.createElement('span');
        span.textContent = p.endsWith('?') ? `${p.slice(0, -1)}?` : p;
        if (i === at) span.className = 'loams-cm-signature-active';
        dom.append(span);
      });
      dom.append(')');
      if (f.doc) {
        const doc = document.createElement('div');
        doc.className = 'loams-cm-signature-doc';
        doc.textContent = f.doc;
        dom.append(doc);
      }
      return { dom };
    },
  };
}

function signatureHint(functions: SqlFunction[]): Extension {
  const fns = new Map(functions.filter((f) => f.params).map((f) => [f.name.toLowerCase(), f]));
  if (fns.size === 0) return [];
  return StateField.define<Tooltip | null>({
    create: (s) => signatureTooltip(s, fns),
    update: (v, tr) => (tr.docChanged || tr.selection ? signatureTooltip(tr.state, fns) : v),
    provide: (f) => showTooltip.from(f),
  });
}

/** Every function the editor knows for a dialect: the schema's, plus the engine's for DataFusion. */
export function functionsFor(dialect: SqlDialectName, schema: SqlSchema): SqlFunction[] {
  const own = schema.functions ?? [];
  const base = dialect === 'datafusion' ? ENGINE_FUNCTIONS : [];
  const seen = new Set<string>();
  const out: SqlFunction[] = [];
  // The documented signatures win over a bare name from the server's function list.
  for (const f of [...base, ...own.filter((f) => f.params), ...own]) {
    const k = f.name.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(f);
  }
  return out;
}

/** The language extension for a dialect and schema: highlighting, completion, signature hints. */
export function languageExtension(dialect: SqlDialectName, schema: SqlSchema): Extension {
  const functions = functionsFor(dialect, schema);
  if (dialect === 'gql') {
    const labels = schema.tables.map((tb) => ({ label: tb.name, type: 'type', detail: tb.kind }));
    const keywords = GQL_KEYWORDS.map((k) => ({ label: k, type: 'keyword' }));
    return [
      new LanguageSupport(gqlLanguage, [
        gqlLanguage.data.of({
          autocomplete: completeFromList([
            ...keywords,
            ...labels,
            ...functions.map(functionOption),
          ]),
        }),
      ]),
      syntaxHighlighting(highlightStyle),
      signatureHint(functions),
    ];
  }
  const d = sqlDialect(dialect);
  const support = sql({
    dialect: d,
    schema: namespaceOf(schema),
    defaultSchema: schema.defaultSchema,
    upperCaseKeywords: true,
  });
  return [
    support,
    d.language.data.of({ autocomplete: scopeSource(schema, functions) }),
    syntaxHighlighting(highlightStyle),
    signatureHint(functions),
  ];
}
