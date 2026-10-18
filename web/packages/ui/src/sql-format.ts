// A small, dependency-free SQL formatter for the editor's Format button: keywords upper-cased,
// each major clause on its own line, select lists and AND/OR conditions one per line, and
// sub-queries indented. Strings, quoted identifiers and comments pass through unchanged.

const CLAUSES = [
  'WITH',
  'SELECT',
  'FROM',
  'PREWHERE',
  'WHERE',
  'GROUP BY',
  'HAVING',
  'QUALIFY',
  'WINDOW',
  'ORDER BY',
  'LIMIT',
  'OFFSET',
  'SETTINGS',
  'FORMAT',
  'UNION ALL',
  'UNION',
  'INTERSECT',
  'EXCEPT',
  'INSERT INTO',
  'VALUES',
  'UPDATE',
  'SET',
  'DELETE FROM',
  'RETURNING',
  'LEFT OUTER JOIN',
  'RIGHT OUTER JOIN',
  'FULL OUTER JOIN',
  'LEFT JOIN',
  'RIGHT JOIN',
  'FULL JOIN',
  'INNER JOIN',
  'CROSS JOIN',
  'ARRAY JOIN',
  'JOIN',
  // GQL / Cypher
  'MATCH',
  'OPTIONAL MATCH',
  'RETURN',
  'UNWIND',
  'MERGE',
  'CREATE',
  'DETACH DELETE',
];

const KEYWORDS = new Set(
  (
    'select from where group by order having limit offset as and or not in is null like ilike ' +
    'between exists case when then else end on using join left right full outer inner cross ' +
    'union all distinct asc desc with insert into values update set delete returning true false ' +
    'interval cast over partition rows range preceding following current row window qualify ' +
    'prewhere settings format array any match optional return unwind merge create detach nulls ' +
    'first last filter except intersect lateral recursive'
  ).split(' '),
);

type Token = { kind: 'word' | 'string' | 'comment' | 'punct' | 'space' | 'other'; text: string };

const RULES: [RegExp, Token['kind']][] = [
  [/^\s+/, 'space'],
  [/^--[^\n]*/, 'comment'],
  [/^\/\*[\s\S]*?(\*\/|$)/, 'comment'],
  [/^[A-Za-z_$][\w$]*/, 'word'],
  [/^\d+(\.\d+)?([eE][-+]?\d+)?/, 'other'],
  [/^(::|->>|->|<=|>=|<>|!=|\|\||[(),;.[\]])/, 'punct'],
];

/** The end of a quoted string or identifier that opens at `i`. */
function quotedEnd(text: string, i: number): number {
  const q = text[i];
  let j = i + 1;
  while (j < text.length) {
    if (text[j] === '\\') j += 2;
    else if (text[j] === q) {
      if (text[j + 1] === q) j += 2;
      else break;
    } else j++;
  }
  return Math.min(text.length, j + 1);
}

export function tokenizeSql(text: string): Token[] {
  const out: Token[] = [];
  let i = 0;
  while (i < text.length) {
    const c = text[i] as string;
    if (c === "'" || c === '"' || c === '`') {
      const end = quotedEnd(text, i);
      out.push({ kind: 'string', text: text.slice(i, end) });
      i = end;
      continue;
    }
    const rest = text.slice(i);
    let tok: Token = { kind: 'other', text: c };
    for (const [re, kind] of RULES) {
      const m = re.exec(rest);
      if (m) {
        tok = { kind, text: m[0] };
        break;
      }
    }
    out.push(tok);
    i += tok.text.length || 1;
  }
  return out;
}

/** Formats one or more statements. Never changes what a statement means. */
export function formatSql(text: string, indent = '  '): string {
  const tokens = tokenizeSql(text).filter((t) => t.kind !== 'space');
  const lines: string[] = [];
  let line = '';
  let depth = 0;
  // A stack of paren kinds: true when the paren opened a sub-query (its body is indented).
  const parens: boolean[] = [];
  let clause = '';
  const pad = () => indent.repeat(depth);
  const newline = (extra = 0) => {
    if (line.trim()) lines.push(line.replace(/\s+$/, ''));
    line = pad() + indent.repeat(extra);
  };
  const emit = (s: string, space = true) => {
    const needsSpace = space && line.trim() !== '' && !/[ (.[]$/.test(line) && !/^[),.;\]]/.test(s);
    line += (needsSpace ? ' ' : '') + s;
  };

  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i] as Token;
    if (tok.kind === 'word') {
      const upper = tok.text.toUpperCase();
      const next = tokens[i + 1];
      const next2 = tokens[i + 2];
      const three = [upper, next?.text.toUpperCase(), next2?.text.toUpperCase()].join(' ');
      const two = [upper, next?.text.toUpperCase()].join(' ');
      const match =
        CLAUSES.find((c) => c === three) ??
        CLAUSES.find((c) => c === two) ??
        CLAUSES.find((c) => c === upper);
      // A word followed by `(` is a function call, not a clause (`format(…)`, `values(…)`).
      const isCall =
        next?.text === '(' && !['VALUES', 'IN', 'EXISTS', 'AS', 'FROM', 'JOIN'].includes(upper);
      if (match && !isCall && tokens[i - 1]?.text !== '.') {
        newline();
        line = pad();
        emit(match);
        i += match.split(' ').length - 1;
        clause = match;
        if (['SELECT', 'GROUP BY', 'ORDER BY', 'RETURN', 'WITH', 'SET'].includes(match)) {
          newline(1);
        }
        continue;
      }
      if (
        (upper === 'AND' || upper === 'OR') &&
        ['WHERE', 'HAVING', 'PREWHERE', 'QUALIFY'].includes(clause) &&
        !inBetween(tokens, i)
      ) {
        newline(1);
        emit(upper);
        continue;
      }
      emit(KEYWORDS.has(tok.text.toLowerCase()) ? upper : tok.text);
      continue;
    }
    if (tok.kind === 'punct') {
      if (tok.text === '(') {
        const sub = /^(select|with)$/i.test(tokens[i + 1]?.text ?? '');
        parens.push(sub);
        const prev = tokens[i - 1];
        emit(
          '(',
          !(prev && (prev.kind === 'word' || prev.text === ')' || prev.text === ']')) ||
            KEYWORDS.has(prev.text.toLowerCase()),
        );
        if (sub) depth++;
        continue;
      }
      if (tok.text === ')') {
        const sub = parens.pop();
        if (sub) {
          depth = Math.max(0, depth - 1);
          newline();
          line = pad();
          line += ')';
        } else emit(')', false);
        continue;
      }
      if (tok.text === ',') {
        line += ',';
        if (parens.length === 0 || parens[parens.length - 1]) {
          if (['SELECT', 'GROUP BY', 'ORDER BY', 'RETURN', 'WITH', 'SET'].includes(clause))
            newline(1);
        }
        continue;
      }
      if (tok.text === ';') {
        line += ';';
        lines.push(line);
        lines.push('');
        line = '';
        depth = 0;
        parens.length = 0;
        clause = '';
        continue;
      }
      if (tok.text === '.' || tok.text === '::') {
        line += tok.text;
        continue;
      }
      emit(tok.text, tok.text !== '[' && tok.text !== ']');
      continue;
    }
    if (tok.kind === 'comment') {
      if (tok.text.startsWith('--')) {
        emit(tok.text);
        newline();
      } else emit(tok.text);
      continue;
    }
    const prev = tokens[i - 1];
    emit(tok.text, !(prev?.text === '.' || prev?.text === '::'));
  }
  if (line.trim()) lines.push(line.replace(/\s+$/, ''));
  while (lines.length && lines[lines.length - 1] === '') lines.pop();
  return lines.join('\n');
}

/** The AND of `BETWEEN a AND b` stays on its line. */
function inBetween(tokens: Token[], i: number): boolean {
  for (let k = i - 1; k >= Math.max(0, i - 4); k--) {
    const w = tokens[k]?.text.toUpperCase();
    if (w === 'BETWEEN') return true;
    if (w === 'AND' || w === 'OR') return false;
  }
  return false;
}
