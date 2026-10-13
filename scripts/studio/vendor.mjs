#!/usr/bin/env node
// The T3 Code vendoring tool (design §53 §9.1, D912; SD1 Task 1).
//
//   fetch  --commit <sha> --out <scratch> [--archive <zip> | --url <url>] [--expect-sha256 <hex>]
//          Downloads (or copies) the upstream source archive into a new, empty directory outside
//          the repository, records its SHA-256 and extracts it. Extraction refuses absolute paths,
//          `..`, drive letters, encrypted entries and zip64, validates every entry before writing
//          any, writes files without an executable bit and skips symbolic links.
//   import --from <scratch>/<top> --group <g> --map <upstream>=<local> … [--verdict as-is|adapt|rewrite]
//          [--rewrite <specifier>=<specifier> …] [--change <what changed>] [--note <note>]
//          [--commit <sha>] [--package <dir>]
//          Copies upstream files into the package (web/packages/studio-ui by default, or
//          web/packages/ui for the primitives), writes the §53 §9.2 header naming the package's
//          own LICENSE.t3code, rewrites import
//          specifiers (as-is files only, by the explicit --rewrite map) and upserts provenance rows.
//   check  [--package <dir>]
//          Every file under src/ carrying the header has a provenance row; every row's file exists
//          and its header names its upstream path and commit; as-is rows' localSha256 match.
//
// Upstream is untrusted data. This tool only reads and copies bytes: it never imports, requires,
// spawns or evaluates anything from the upstream tree, and it never installs its dependencies.
import { createHash } from 'node:crypto';
import {
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from 'node:fs';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { crc32, inflateRawSync } from 'node:zlib';

const HERE = dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = resolve(HERE, '..', '..');
export const DEFAULT_PACKAGE = join(REPO_ROOT, 'web', 'packages', 'studio-ui');
export const UPSTREAM = 'https://github.com/pingdotgg/t3code';
export const COPYRIGHT = 'Copyright (c) 2026 T3 Tools Inc.';
export const HEADER_MARK =
  '// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),';
const VERDICTS = new Set(['as-is', 'adapt', 'rewrite']);
const MAX_TOTAL_BYTES = 2 * 1024 ** 3;

class VendorError extends Error {}
const fail = (message) => {
  throw new VendorError(message);
};

export const sha256 = (data) => createHash('sha256').update(data).digest('hex');
const isSha = (s) => /^[0-9a-f]{40}$/.test(s ?? '');
const shortSha = (commit) => commit.slice(0, 8);

// ---------- arguments ----------

function parseArgs(argv) {
  const [command, ...rest] = argv;
  const opts = { _multi: {} };
  for (let i = 0; i < rest.length; i++) {
    const flag = rest[i];
    if (!flag.startsWith('--')) fail(`unexpected argument ${flag}`);
    const key = flag.slice(2);
    const value = rest[i + 1];
    if (value === undefined || value.startsWith('--')) fail(`${flag} needs a value`);
    i++;
    if (key === 'map' || key === 'rewrite') opts._multi[key] = [...(opts._multi[key] ?? []), value];
    else opts[key] = value;
  }
  return { command, opts };
}

function splitPair(pair, flag) {
  const at = pair.indexOf('=');
  if (at <= 0 || at === pair.length - 1) fail(`${flag} expects <from>=<to>, got ${pair}`);
  return [pair.slice(0, at), pair.slice(at + 1)];
}

// ---------- paths ----------

/** A relative, forward-slash path with no `..`, no drive letter, no NUL and no backslash. */
export function safeRelative(name) {
  if (
    name.length === 0 ||
    name.includes('\0') ||
    name.includes('\\') ||
    name.startsWith('/') ||
    /^[A-Za-z]:/.test(name)
  ) {
    return null;
  }
  const parts = name.split('/');
  if (parts.some((p) => p === '..' || p === '.')) return null;
  return parts.filter((p) => p !== '').join('/');
}

function within(base, target) {
  const rel = relative(base, target);
  return rel !== '' && !rel.startsWith('..') && !isAbsolute(rel);
}

function realOrResolved(path) {
  try {
    return realpathSync(path);
  } catch {
    return resolve(path);
  }
}

// ---------- zip ----------

/**
 * Reads a zip's central directory. Returns the entries and the archive comment (GitHub writes the
 * commit id there). Refuses zip64, encryption and unsafe names before anything is written.
 */
export function readZip(buf) {
  const minEnd = Math.max(0, buf.length - 22 - 0xffff);
  let end = -1;
  for (let i = buf.length - 22; i >= minEnd; i--) {
    if (buf.readUInt32LE(i) === 0x06054b50) {
      end = i;
      break;
    }
  }
  if (end < 0) fail('not a zip archive (no end of central directory)');
  const count = buf.readUInt16LE(end + 10);
  const cdSize = buf.readUInt32LE(end + 12);
  const cdOffset = buf.readUInt32LE(end + 16);
  const commentLen = buf.readUInt16LE(end + 20);
  if (count === 0xffff || cdOffset === 0xffffffff || cdSize === 0xffffffff) {
    fail('zip64 archives are not supported');
  }
  const comment = buf.subarray(end + 22, end + 22 + commentLen).toString('utf8');
  const entries = [];
  let p = cdOffset;
  let total = 0;
  for (let n = 0; n < count; n++) {
    if (p + 46 > buf.length || buf.readUInt32LE(p) !== 0x02014b50)
      fail('corrupt central directory');
    const flags = buf.readUInt16LE(p + 8);
    const method = buf.readUInt16LE(p + 10);
    const crc = buf.readUInt32LE(p + 16);
    const compSize = buf.readUInt32LE(p + 20);
    const size = buf.readUInt32LE(p + 24);
    const nameLen = buf.readUInt16LE(p + 28);
    const extraLen = buf.readUInt16LE(p + 30);
    const cmtLen = buf.readUInt16LE(p + 32);
    const madeBy = buf.readUInt16LE(p + 4) >> 8;
    const external = buf.readUInt32LE(p + 38);
    const localOffset = buf.readUInt32LE(p + 42);
    const rawName = buf.subarray(p + 46, p + 46 + nameLen).toString('utf8');
    p += 46 + nameLen + extraLen + cmtLen;
    const isDir = rawName.endsWith('/');
    const name = safeRelative(isDir ? rawName.slice(0, -1) : rawName);
    if (name === null || name === '') fail(`unsafe path in archive: ${JSON.stringify(rawName)}`);
    if (flags & 1) fail(`encrypted entry: ${rawName}`);
    if (method !== 0 && method !== 8) fail(`unsupported compression ${method}: ${rawName}`);
    const unixMode = madeBy === 3 ? external >>> 16 : 0;
    const isLink = (unixMode & 0o170000) === 0o120000;
    total += size;
    if (total > MAX_TOTAL_BYTES) fail('archive expands beyond the size limit');
    entries.push({ name, isDir, isLink, method, crc, compSize, size, localOffset });
  }
  return { entries, comment };
}

function entryData(buf, entry) {
  const at = entry.localOffset;
  if (buf.readUInt32LE(at) !== 0x04034b50) fail(`corrupt local header: ${entry.name}`);
  const start = at + 30 + buf.readUInt16LE(at + 26) + buf.readUInt16LE(at + 28);
  const raw = buf.subarray(start, start + entry.compSize);
  const data =
    entry.method === 0 ? raw : inflateRawSync(raw, { maxOutputLength: Math.max(1, entry.size) });
  if (data.length !== entry.size) fail(`size mismatch: ${entry.name}`);
  if (crc32(data) !== entry.crc) fail(`CRC mismatch: ${entry.name}`);
  return data;
}

/** Extracts a validated archive into `out`. Every entry is checked before the first write. */
export function extractZip(buf, out) {
  const { entries, comment } = readZip(buf);
  const base = resolve(out);
  for (const e of entries) {
    if (!within(base, resolve(base, e.name))) fail(`unsafe path in archive: ${e.name}`);
  }
  const skipped = [];
  for (const e of entries) {
    const target = resolve(base, e.name);
    if (e.isLink) {
      skipped.push(e.name);
      continue;
    }
    if (e.isDir) {
      mkdirSync(target, { recursive: true, mode: 0o755 });
      continue;
    }
    mkdirSync(dirname(target), { recursive: true, mode: 0o755 });
    writeFileSync(target, entryData(buf, e), { flag: 'wx', mode: 0o644 });
  }
  const tops = [...new Set(entries.map((e) => e.name.split('/')[0]))];
  return { comment, skipped, top: tops.length === 1 ? tops[0] : null, count: entries.length };
}

// ---------- provenance ----------

export function loadProvenance(pkg) {
  const path = join(pkg, 'provenance.json');
  if (!existsSync(path)) fail(`no provenance.json in ${pkg}`);
  const prov = JSON.parse(readFileSync(path, 'utf8'));
  prov.archives ??= [];
  prov.files ??= [];
  return prov;
}

function saveProvenance(pkg, prov) {
  prov.files.sort((a, b) => a.path.localeCompare(b.path));
  writeFileSync(join(pkg, 'provenance.json'), `${JSON.stringify(prov, null, 2)}\n`);
}

// ---------- header ----------

/**
 * Where a package's copy of the MIT text lives, as the header names it: the package's own
 * LICENSE.t3code when it is in the repository (`@loams/studio-ui`, `@loams/ui`), else
 * Studio's (a package outside the repository, as in the tool's own tests).
 */
export function licencePathFor(pkg = DEFAULT_PACKAGE) {
  const dir = resolve(pkg);
  const base = within(REPO_ROOT, dir) ? dir : DEFAULT_PACKAGE;
  return `${relative(REPO_ROOT, base).split(sep).join('/')}/LICENSE.t3code`;
}

export function headerFor({ upstreamPath, commit, change, licence = licencePathFor() }) {
  return [
    HEADER_MARK,
    `// ${upstreamPath} at commit ${shortSha(commit)}, ${COPYRIGHT}, MIT licence`,
    `// (see ${licence}). Modified by The Loams Authors:`,
    `// ${change}. Loams' changes are Apache-2.0.`,
    '',
  ].join('\n');
}

export function headerProblems(text, row) {
  const head = text.split('\n').slice(0, 6).join('\n');
  const problems = [];
  if (!text.startsWith(HEADER_MARK)) problems.push('does not start with the T3 Code header');
  if (!head.includes(`// ${row.upstreamPath} at commit ${shortSha(row.commit)},`)) {
    problems.push('header does not name its upstream path and commit');
  }
  if (!head.includes('LICENSE.t3code')) problems.push('header does not point at LICENSE.t3code');
  return problems;
}

/** Rewrites only the exact module specifiers named in the map, in import/export/import() forms. */
export function rewriteSpecifiers(text, map) {
  let out = text;
  for (const [from, to] of map) {
    const esc = from.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(`((?:\\bfrom\\s*|\\bimport\\s*\\(?\\s*))(["'])${esc}\\2`, 'g');
    const next = out.replace(re, (_m, lead, q) => `${lead}${q}${to}${q}`);
    if (next === out) fail(`--rewrite ${from}: specifier not found`);
    out = next;
  }
  return out;
}

// ---------- commands ----------

async function cmdFetch(opts) {
  const commit = opts.commit;
  if (!isSha(commit)) fail('--commit must be a full 40-hex commit id');
  if (!opts.out) fail('--out is required');
  const out = resolve(opts.out);
  const repo = realOrResolved(REPO_ROOT);
  if (realOrResolved(out) === repo || within(repo, realOrResolved(out)) || within(REPO_ROOT, out)) {
    fail(`--out must be outside the repository (${out})`);
  }
  if (existsSync(out) && readdirSync(out).length > 0) fail(`--out must be new or empty (${out})`);
  // A local --archive is recorded as such unless --url says where it was downloaded from.
  const url =
    opts.url ??
    (opts.archive ? `local:${basename(opts.archive)}` : `${UPSTREAM}/archive/${commit}.zip`);
  let buf;
  if (opts.archive) {
    buf = readFileSync(opts.archive);
  } else {
    const res = await fetch(url, { redirect: 'follow' });
    if (!res.ok) fail(`download failed: ${res.status} ${url}`);
    buf = Buffer.from(await res.arrayBuffer());
  }
  const digest = sha256(buf);
  if (opts['expect-sha256'] && opts['expect-sha256'] !== digest) {
    fail(`SHA-256 mismatch: expected ${opts['expect-sha256']}, got ${digest}`);
  }
  const pkg = resolve(opts.package ?? DEFAULT_PACKAGE);
  if (existsSync(join(pkg, 'provenance.json'))) {
    const known = loadProvenance(pkg).archives.find((a) => a.commit === commit && a.url === url);
    if (known?.sha256 && known.sha256 !== digest) {
      fail(`SHA-256 mismatch with provenance.json: expected ${known.sha256}, got ${digest}`);
    }
  }
  // Validate the whole archive before creating anything.
  readZip(buf);
  mkdirSync(out, { recursive: true, mode: 0o700 });
  const res = extractZip(buf, out);
  if (res.comment && isSha(res.comment.trim()) && res.comment.trim() !== commit) {
    fail(`archive comment names commit ${res.comment.trim()}, not ${commit}`);
  }
  const meta = {
    commit,
    url,
    sha256: digest,
    top: res.top,
    entries: res.count,
    skipped: res.skipped,
  };
  writeFileSync(join(out, 'fetch.json'), `${JSON.stringify(meta, null, 2)}\n`, { flag: 'wx' });
  process.stdout.write(
    `fetched ${url}\nsha256 ${digest}\nextracted ${res.count} entries into ${out}/${res.top ?? ''}\n` +
      (res.skipped.length ? `skipped symbolic links: ${res.skipped.join(', ')}\n` : ''),
  );
}

function cmdImport(opts) {
  if (!opts.from) fail('--from is required');
  if (!opts.group) fail('--group is required');
  const maps = opts._multi.map ?? [];
  if (maps.length === 0) fail('at least one --map <upstream>=<local> is required');
  const verdict = opts.verdict ?? 'as-is';
  if (!VERDICTS.has(verdict)) fail(`--verdict must be as-is, adapt or rewrite`);
  const rewrites = (opts._multi.rewrite ?? []).map((r) => splitPair(r, '--rewrite'));
  if (rewrites.length && verdict !== 'as-is') fail('--rewrite applies to as-is files only');
  const from = resolve(opts.from);
  const meta = existsSync(join(dirname(from), 'fetch.json'))
    ? JSON.parse(readFileSync(join(dirname(from), 'fetch.json'), 'utf8'))
    : null;
  const commit = opts.commit ?? meta?.commit;
  if (!isSha(commit)) fail('no commit: pass --commit or import from a tree made by `fetch`');
  if (meta && opts.commit && meta.commit !== opts.commit) fail('--commit differs from fetch.json');
  const pkg = resolve(opts.package ?? DEFAULT_PACKAGE);
  const prov = loadProvenance(pkg);
  if (meta) {
    const known = prov.archives.find((a) => a.commit === commit);
    if (known && known.sha256 !== meta.sha256 && known.url === meta.url) {
      fail(`archive SHA-256 ${meta.sha256} differs from provenance.json (${known.sha256})`);
    }
    if (!known) prov.archives.push({ commit, url: meta.url, sha256: meta.sha256 });
  } else if (!prov.archives.some((a) => a.commit === commit)) {
    fail(`commit ${commit} has no archive row in provenance.json; use \`fetch\` first`);
  }
  const change =
    opts.change ??
    (verdict === 'as-is'
      ? rewrites.length
        ? 'this header; import paths rewritten'
        : 'this header only'
      : 'ported to the Loams UI stack');
  for (const pair of maps) {
    const [upRaw, localRaw] = splitPair(pair, '--map');
    const upstreamPath = safeRelative(upRaw);
    const localPath = safeRelative(localRaw);
    if (!upstreamPath) fail(`unsafe upstream path ${upRaw}`);
    if (!localPath?.startsWith('src/')) fail(`local path must be under src/: ${localRaw}`);
    const source = resolve(from, upstreamPath);
    if (!within(from, source)) fail(`upstream path escapes --from: ${upRaw}`);
    const st = lstatSync(source, { throwIfNoEntry: false });
    if (!st?.isFile()) fail(`not a regular file upstream: ${upstreamPath}`);
    const upstreamBytes = readFileSync(source);
    let body = upstreamBytes.toString('utf8');
    if (body.startsWith(HEADER_MARK)) fail(`${upstreamPath} already carries a header`);
    const fileRewrites = rewrites.filter(([spec]) => body.includes(spec));
    if (fileRewrites.length) body = rewriteSpecifiers(body, fileRewrites);
    const text = headerFor({ upstreamPath, commit, change, licence: licencePathFor(pkg) }) + body;
    const target = join(pkg, localPath);
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, text);
    const row = {
      path: localPath,
      upstreamPath,
      commit,
      upstreamSha256: sha256(upstreamBytes),
      localSha256: sha256(text),
      verdict,
      group: opts.group,
      ...(opts.note ? { note: opts.note } : {}),
    };
    const at = prov.files.findIndex((f) => f.path === localPath);
    if (at >= 0) prov.files[at] = row;
    else prov.files.push(row);
    process.stdout.write(`${verdict} ${upstreamPath} -> ${localPath}\n`);
  }
  saveProvenance(pkg, prov);
}

function walk(dir, acc = []) {
  if (!existsSync(dir)) return acc;
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    const st = lstatSync(p);
    if (st.isDirectory()) walk(p, acc);
    else if (st.isFile()) acc.push(p);
  }
  return acc;
}

/** Returns the list of problems; empty means the package passes. */
export function checkPackage(pkg) {
  const problems = [];
  const prov = loadProvenance(pkg);
  if (prov.upstream !== UPSTREAM) problems.push(`provenance.json: upstream must be ${UPSTREAM}`);
  if (prov.license !== 'MIT') problems.push('provenance.json: license must be MIT');
  if (prov.copyright !== COPYRIGHT)
    problems.push(`provenance.json: copyright must be ${COPYRIGHT}`);
  const commits = new Set(prov.archives.map((a) => a.commit));
  for (const a of prov.archives) {
    if (!isSha(a.commit) || !/^[0-9a-f]{64}$/.test(a.sha256 ?? '') || !a.url) {
      problems.push(`provenance.json: incomplete archive row ${JSON.stringify(a)}`);
    }
  }
  const rows = new Map();
  for (const row of prov.files) {
    if (rows.has(row.path)) problems.push(`${row.path}: duplicate provenance row`);
    rows.set(row.path, row);
    const file = join(pkg, row.path);
    if (!safeRelative(row.path) || !row.path.startsWith('src/')) {
      problems.push(`${row.path}: provenance path must be under src/`);
      continue;
    }
    if (!VERDICTS.has(row.verdict)) problems.push(`${row.path}: bad verdict ${row.verdict}`);
    if (!commits.has(row.commit))
      problems.push(`${row.path}: commit ${row.commit} has no archive row`);
    if (!existsSync(file)) {
      problems.push(`${row.path}: file in provenance.json does not exist`);
      continue;
    }
    const text = readFileSync(file, 'utf8');
    for (const p of headerProblems(text, row)) problems.push(`${row.path}: ${p}`);
    if (row.verdict === 'as-is' && sha256(text) !== row.localSha256) {
      problems.push(`${row.path}: localSha256 does not match the file (as-is files are hashed)`);
    }
  }
  for (const file of walk(join(pkg, 'src'))) {
    const rel = relative(pkg, file).split(sep).join('/');
    if (rows.has(rel)) continue;
    if (readFileSync(file, 'utf8').includes('Portions of this file are from T3 Code')) {
      problems.push(`${rel}: carries the T3 Code header but has no provenance row`);
    }
  }
  return problems;
}

function cmdCheck(opts) {
  const pkg = resolve(opts.package ?? DEFAULT_PACKAGE);
  const problems = checkPackage(pkg);
  if (problems.length) fail(`vendor check failed for ${pkg}:\n  ${problems.join('\n  ')}`);
  const n = loadProvenance(pkg).files.length;
  process.stdout.write(
    `vendor check passed: ${n} vendored file(s) in ${relative(REPO_ROOT, pkg) || pkg}\n`,
  );
}

export async function main(argv) {
  const { command, opts } = parseArgs(argv);
  if (command === 'fetch') return cmdFetch(opts);
  if (command === 'import') return cmdImport(opts);
  if (command === 'check') return cmdCheck(opts);
  fail('usage: vendor.mjs fetch|import|check [options] (see the header of this file)');
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).catch((err) => {
    process.stderr.write(`vendor: ${err instanceof VendorError ? err.message : err.stack}\n`);
    process.exit(1);
  });
}
