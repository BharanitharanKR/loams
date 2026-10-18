// Where in the statement an engine's error points, read from its message. Each engine says it
// differently: DataFusion's parser `at Line: 1, Column: 8`, ClickHouse `(line 1, col 8)` or
// `at position 8`, MySQL/TiDB `near "…" at line 1`, PostgreSQL `at character 8` or `LINE 1:`.

/** A 1-based line and column, with the span's length in characters when known. */
export interface SqlPosition {
  line: number;
  column: number;
  length?: number;
}

/** The 1-based line and column of a 0-based offset. */
export function positionOfOffset(text: string, offset: number): SqlPosition {
  const before = text.slice(0, Math.max(0, Math.min(offset, text.length)));
  const lines = before.split('\n');
  return { line: lines.length, column: (lines[lines.length - 1]?.length ?? 0) + 1 };
}

/** The 0-based offset of a 1-based position, clamped to the text. */
export function offsetOfPosition(text: string, pos: SqlPosition): number {
  const lines = text.split('\n');
  const line = Math.min(Math.max(1, pos.line), lines.length);
  let at = 0;
  for (let i = 0; i < line - 1; i++) at += (lines[i]?.length ?? 0) + 1;
  return Math.min(text.length, at + Math.max(0, pos.column - 1));
}

/** The word starting at `offset`, for a span when the engine gives only a point. */
function wordLength(text: string, offset: number): number {
  const m = /^[\w$.]+|^\S/.exec(text.slice(offset));
  return m ? m[0].length : 1;
}

/** The error's position in `statement`, or undefined when the message names none. */
export function sqlErrorPosition(message: string, statement: string): SqlPosition | undefined {
  const at = (line: number, column: number): SqlPosition => {
    const off = offsetOfPosition(statement, { line, column });
    return { line, column, length: wordLength(statement, off) };
  };
  const fromOffset = (oneBased: number): SqlPosition => {
    const p = positionOfOffset(statement, oneBased - 1);
    return at(p.line, p.column);
  };
  const firstUse = (qualified: string): SqlPosition | undefined => {
    const name = qualified.replace(/\.+$/, '').split('.').pop() ?? '';
    const idx = name ? statement.search(new RegExp(`\\b${name.replace(/\$/g, '\\$')}\\b`)) : -1;
    return idx >= 0 ? { ...positionOfOffset(statement, idx), length: name.length } : undefined;
  };
  const near = (quoted: string, line: number): SqlPosition => {
    const first = quoted.split('\n')[0] ?? '';
    const text = statement.split('\n')[line - 1] ?? '';
    const col = first ? text.indexOf(first) : -1;
    if (col < 0) return { line, column: 1, length: text.length || 1 };
    const off = offsetOfPosition(statement, { line, column: col + 1 });
    return { line, column: col + 1, length: wordLength(statement, off) };
  };
  const rules: [RegExp, (m: RegExpExecArray) => SqlPosition | undefined][] = [
    // DataFusion / sqlparser-rs: "found: SELEC at Line: 1, Column: 1"
    [/Line:\s*(\d+),\s*Column:?\s*(\d+)/i, (m) => at(Number(m[1]), Number(m[2]))],
    // ClickHouse: "(line 1, col 8)"
    [/\(line\s+(\d+),\s*col\s+(\d+)\)/i, (m) => at(Number(m[1]), Number(m[2]))],
    // ClickHouse: "failed at position 8 (FRM)" (1-based)
    [/at position\s+(\d+)/i, (m) => fromOffset(Number(m[1]))],
    // PostgreSQL: "at character 8" (1-based)
    [/at character\s+(\d+)/i, (m) => fromOffset(Number(m[1]))],
    // MySQL / TiDB: `near "FRM t" at line 1`: the quoted text on that line.
    [/near ["'`]([\s\S]*?)["'`]\s+at line\s+(\d+)/i, (m) => near(m[1] ?? '', Number(m[2]))],
    // A schema error naming a column or table: its first use.
    [/No field named\s+["'`]?([\w.]+)["'`]?/i, (m) => firstUse(m[1] ?? '')],
    [
      /table ['"`]?([\w.]+)['"`]? (?:not found|doesn't exist|does not exist)/i,
      (m) => firstUse(m[1] ?? ''),
    ],
  ];
  for (const [re, f] of rules) {
    const m = re.exec(message);
    if (m) return f(m);
  }
  return undefined;
}

/** Strips the wrappers engines put around the real message (`invalid argument: sql: SQL error: ParserError("…")`). */
export function cleanSqlError(message: string): string {
  let m = message.trim();
  m = m.replace(/^invalid argument:\s*/i, '').replace(/^sql:\s*/i, '');
  const inner =
    /^(?:SQL error|Error during planning|Schema error|Execution error|Arrow error):\s*(?:\w+Error\("([\s\S]*)"\))?/i.exec(
      m,
    );
  if (inner?.[1]) return inner[1].replace(/\\"/g, '"');
  return m;
}
