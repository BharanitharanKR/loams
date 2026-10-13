#!/usr/bin/env node
// The T3 Code upstream drift report (design §53 §9.4, D915; SD1 Task 1).
//
//   upstream-report.mjs --from <old scratch tree> --to <new scratch tree> [--package <dir>] [--json]
//
// For every file in provenance.json it compares the upstream file at the recorded commit (in
// --from) with the same path in --to, and prints `unchanged`, `changed upstream` (with the
// unified diff) or `gone upstream`. Both trees are made by `vendor.mjs fetch`; they are only read.
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DEFAULT_PACKAGE, loadProvenance, safeRelative } from './vendor.mjs';

/** A line-based unified diff (Myers' O(ND) algorithm) with three lines of context. */
export function unifiedDiff(a, b, labelA = 'a', labelB = 'b', context = 3) {
  const x = a.split('\n');
  const y = b.split('\n');
  const n = x.length;
  const m = y.length;
  const max = n + m;
  const v = new Array(2 * max + 2).fill(0);
  const trace = [];
  outer: for (let d = 0; d <= max; d++) {
    trace.push(v.slice());
    for (let k = -d; k <= d; k += 2) {
      let i =
        k === -d || (k !== d && v[k - 1 + max] < v[k + 1 + max])
          ? v[k + 1 + max]
          : v[k - 1 + max] + 1;
      let j = i - k;
      while (i < n && j < m && x[i] === y[j]) {
        i++;
        j++;
      }
      v[k + max] = i;
      if (i >= n && j >= m) break outer;
    }
  }
  // Backtrack into an edit script of [op, line] with op ' ', '-' or '+'.
  const ops = [];
  let i = n;
  let j = m;
  for (let d = trace.length - 1; d > 0; d--) {
    const vv = trace[d];
    const k = i - j;
    const prevK = k === -d || (k !== d && vv[k - 1 + max] < vv[k + 1 + max]) ? k + 1 : k - 1;
    const prevI = vv[prevK + max];
    const prevJ = prevI - prevK;
    while (i > prevI && j > prevJ) ops.push([' ', x[--i], --j]);
    if (i === prevI) ops.push(['+', y[--j]]);
    else ops.push(['-', x[--i]]);
  }
  while (i > 0 && j > 0) ops.push([' ', x[--i], --j]);
  ops.reverse();
  // Group into hunks.
  const out = [`--- ${labelA}`, `+++ ${labelB}`];
  let ai = 0;
  let bi = 0;
  const rows = ops.map(([op, line]) => {
    const row = { op, line, a: ai, b: bi };
    if (op !== '+') ai++;
    if (op !== '-') bi++;
    return row;
  });
  let idx = 0;
  while (idx < rows.length) {
    if (rows[idx].op === ' ') {
      idx++;
      continue;
    }
    const start = Math.max(0, idx - context);
    let end = idx;
    let lastChange = idx;
    while (end < rows.length && end - lastChange <= 2 * context) {
      if (rows[end].op !== ' ') lastChange = end;
      end++;
    }
    end = Math.min(rows.length, lastChange + context + 1);
    const hunk = rows.slice(start, end);
    const aLen = hunk.filter((r) => r.op !== '+').length;
    const bLen = hunk.filter((r) => r.op !== '-').length;
    out.push(`@@ -${hunk[0].a + 1},${aLen} +${hunk[0].b + 1},${bLen} @@`);
    for (const r of hunk) out.push(`${r.op}${r.line}`);
    idx = end;
  }
  return `${out.join('\n')}\n`;
}

export function classify(fromDir, toDir, prov) {
  return prov.files.map((row) => {
    const rel = safeRelative(row.upstreamPath);
    if (!rel) throw new Error(`unsafe upstream path in provenance.json: ${row.upstreamPath}`);
    const before = resolve(fromDir, rel);
    const after = resolve(toDir, rel);
    const base = { path: row.path, upstreamPath: row.upstreamPath, group: row.group };
    if (!existsSync(after)) return { ...base, status: 'gone upstream' };
    const a = existsSync(before) ? readFileSync(before, 'utf8') : '';
    const b = readFileSync(after, 'utf8');
    if (a === b) return { ...base, status: 'unchanged' };
    return {
      ...base,
      status: 'changed upstream',
      diff: unifiedDiff(a, b, `a/${rel}`, `b/${rel}`),
    };
  });
}

function parse(argv) {
  const opts = {};
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    if (flag === '--json') opts.json = true;
    else if (['--from', '--to', '--package'].includes(flag) && argv[i + 1])
      opts[flag.slice(2)] = argv[++i];
    else throw new Error(`unexpected argument ${flag}`);
  }
  if (!opts.from || !opts.to)
    throw new Error(
      'usage: upstream-report.mjs --from <old> --to <new> [--package <dir>] [--json]',
    );
  return opts;
}

function main(argv) {
  const opts = parse(argv);
  const prov = loadProvenance(resolve(opts.package ?? DEFAULT_PACKAGE));
  const rows = classify(resolve(opts.from), resolve(opts.to), prov);
  if (opts.json) {
    process.stdout.write(`${JSON.stringify(rows, null, 2)}\n`);
    return;
  }
  for (const r of rows) {
    process.stdout.write(`${r.status.padEnd(17)} ${r.upstreamPath} -> ${r.path} (${r.group})\n`);
    if (r.diff) process.stdout.write(r.diff);
  }
  const count = (s) => rows.filter((r) => r.status === s).length;
  process.stdout.write(
    `\n${count('unchanged')} unchanged, ${count('changed upstream')} changed upstream, ${count('gone upstream')} gone upstream\n`,
  );
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    main(process.argv.slice(2));
  } catch (err) {
    process.stderr.write(`upstream-report: ${err.message}\n`);
    process.exit(1);
  }
}
