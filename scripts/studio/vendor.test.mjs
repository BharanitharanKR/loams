// Tests for the T3 Code vendoring tool and the upstream report (SD1 Task 1, design §53 §9).
// Run with `node --test scripts/studio/vendor.test.mjs`.
import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { crc32 } from 'node:zlib';

const here = dirname(fileURLToPath(import.meta.url));
const vendor = join(here, 'vendor.mjs');
const report = join(here, 'upstream-report.mjs');
const COMMIT = 'a'.repeat(40);

const sha256 = (data) => createHash('sha256').update(data).digest('hex');
const scratch = (name) => mkdtempSync(join(tmpdir(), `loams-vendor-${name}-`));

// A minimal zip writer (stored entries only), so the fixtures need no zip library.
function makeZip(entries, comment = '') {
  const locals = [];
  const centrals = [];
  let offset = 0;
  for (const { name, data = '', mode = 0o100644 } of entries) {
    const nameBuf = Buffer.from(name, 'utf8');
    const body = Buffer.from(data);
    const crc = crc32(body);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt16LE(0, 8);
    local.writeUInt32LE(crc, 14);
    local.writeUInt32LE(body.length, 18);
    local.writeUInt32LE(body.length, 22);
    local.writeUInt16LE(nameBuf.length, 26);
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE((3 << 8) | 20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(body.length, 20);
    central.writeUInt32LE(body.length, 24);
    central.writeUInt16LE(nameBuf.length, 28);
    central.writeUInt32LE((mode << 16) >>> 0, 38);
    central.writeUInt32LE(offset, 42);
    locals.push(local, nameBuf, body);
    centrals.push(central, nameBuf);
    offset += local.length + nameBuf.length + body.length;
  }
  const cd = Buffer.concat(centrals);
  const commentBuf = Buffer.from(comment, 'utf8');
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(cd.length, 12);
  end.writeUInt32LE(offset, 16);
  end.writeUInt16LE(commentBuf.length, 20);
  return Buffer.concat([...locals, cd, end, commentBuf]);
}

function run(script, args, opts = {}) {
  return spawnSync(process.execPath, [script, ...args], { encoding: 'utf8', ...opts });
}

// A package directory with an empty provenance file, outside the repository.
function makePackage() {
  const pkg = scratch('pkg');
  mkdirSync(join(pkg, 'src'));
  writeFileSync(
    join(pkg, 'provenance.json'),
    `${JSON.stringify(
      {
        upstream: 'https://github.com/pingdotgg/t3code',
        license: 'MIT',
        copyright: 'Copyright (c) 2026 T3 Tools Inc.',
        archives: [],
        files: [],
      },
      null,
      2,
    )}\n`,
  );
  return pkg;
}

test('vendor_refuses_path_traversal', () => {
  const work = scratch('traversal');
  const outside = join(work, 'x');
  for (const evil of ['top/../../x', '/x', '../x', 'top/..\\..\\x', 'C:/x']) {
    const zip = join(work, 'evil.zip');
    writeFileSync(
      zip,
      makeZip([
        { name: 'top/', mode: 0o40755 },
        { name: 'top/ok.ts', data: 'export {};\n' },
        { name: evil, data: 'pwned' },
      ]),
    );
    const out = join(work, `out-${sha256(evil).slice(0, 8)}`);
    const res = run(vendor, ['fetch', '--commit', COMMIT, '--archive', zip, '--out', out]);
    assert.notEqual(res.status, 0, `${evil} must be refused`);
    assert.match(res.stderr, /unsafe path/);
    assert.equal(existsSync(outside), false, 'nothing outside the output directory');
    assert.equal(existsSync('/x'), false);
    // Validation happens before any write: not even the safe entry was extracted.
    assert.equal(existsSync(join(out, 'top', 'ok.ts')), false);
  }
});

test('vendor_refuses_out_dir_inside_repository', () => {
  const work = scratch('inrepo');
  const zip = join(work, 'a.zip');
  writeFileSync(zip, makeZip([{ name: 'top/a.ts', data: 'export {};\n' }]));
  const res = run(vendor, [
    'fetch',
    '--commit',
    COMMIT,
    '--archive',
    zip,
    '--out',
    join(here, 'never-here'),
  ]);
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /outside the repository/);
  assert.equal(existsSync(join(here, 'never-here')), false);
});

test('vendor_checks_expected_sha256', () => {
  const work = scratch('sha');
  const zip = join(work, 'a.zip');
  writeFileSync(zip, makeZip([{ name: 'top/a.ts', data: 'export {};\n' }]));
  const res = run(vendor, [
    'fetch',
    '--commit',
    COMMIT,
    '--archive',
    zip,
    '--out',
    join(work, 'out'),
    '--expect-sha256',
    '0'.repeat(64),
  ]);
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /SHA-256 mismatch/);
  assert.equal(existsSync(join(work, 'out', 'top')), false);
});

test('vendor_never_executes', () => {
  const work = scratch('noexec');
  const marker = join(work, 'MARKER');
  const evil = `require('fs').writeFileSync(${JSON.stringify(marker)}, 'ran')`;
  const pkgJson = JSON.stringify({
    name: 'evil',
    scripts: { preinstall: `node -e "${evil}"`, postinstall: `node -e "${evil}"` },
  });
  const zip = join(work, 'up.zip');
  writeFileSync(
    zip,
    makeZip(
      [
        { name: 'top/package.json', data: pkgJson },
        { name: 'top/.npmrc', data: 'ignore-scripts=false\n' },
        { name: 'top/apps/web/src/lib/a.ts', data: `export const a = 1;\n${evil};\n` },
        { name: 'top/run.sh', data: `#!/bin/sh\ntouch ${marker}\n`, mode: 0o100755 },
      ],
      COMMIT,
    ),
  );
  const out = join(work, 'out');
  const fetched = run(vendor, ['fetch', '--commit', COMMIT, '--archive', zip, '--out', out]);
  assert.equal(fetched.status, 0, fetched.stderr);
  const pkg = makePackage();
  const imported = run(vendor, [
    'import',
    '--from',
    join(out, 'top'),
    '--package',
    pkg,
    '--group',
    'lib',
    '--map',
    'apps/web/src/lib/a.ts=src/lib/a.ts',
    '--verdict',
    'as-is',
  ]);
  assert.equal(imported.status, 0, imported.stderr);
  const checked = run(vendor, ['check', '--package', pkg]);
  assert.equal(checked.status, 0, checked.stderr);
  assert.equal(existsSync(marker), false, 'no upstream code ran');
  // Extracted files never carry an executable bit.
  const mode = execFileSync('stat', ['-c', '%a', join(out, 'top', 'run.sh')], {
    encoding: 'utf8',
  }).trim();
  assert.equal(mode, '644');
});

test('vendor_import_writes_header_and_provenance', () => {
  const work = scratch('import');
  const upstream = 'import { observeResize } from "../lib/observeResize";\nexport const x = 1;\n';
  const zip = join(work, 'up.zip');
  writeFileSync(zip, makeZip([{ name: 'top/apps/web/src/components/X.tsx', data: upstream }]));
  const out = join(work, 'out');
  assert.equal(
    run(vendor, ['fetch', '--commit', COMMIT, '--archive', zip, '--out', out]).status,
    0,
  );
  const pkg = makePackage();
  const res = run(vendor, [
    'import',
    '--from',
    join(out, 'top'),
    '--package',
    pkg,
    '--group',
    'lib',
    '--map',
    'apps/web/src/components/X.tsx=src/lib/X.tsx',
    '--rewrite',
    '../lib/observeResize=./observeResize',
    '--verdict',
    'as-is',
  ]);
  assert.equal(res.status, 0, res.stderr);
  const local = readFileSync(join(pkg, 'src/lib/X.tsx'), 'utf8');
  assert.match(local, /^\/\/ Portions of this file are from T3 Code/);
  assert.match(local, /apps\/web\/src\/components\/X\.tsx at commit aaaaaaaa/);
  assert.match(local, /from "\.\/observeResize"/);
  const prov = JSON.parse(readFileSync(join(pkg, 'provenance.json'), 'utf8'));
  assert.equal(prov.archives.length, 1);
  assert.equal(prov.archives[0].sha256, sha256(readFileSync(zip)));
  assert.equal(prov.files.length, 1);
  const row = prov.files[0];
  assert.equal(row.path, 'src/lib/X.tsx');
  assert.equal(row.upstreamSha256, sha256(upstream));
  assert.equal(row.localSha256, sha256(local));
  assert.equal(row.verdict, 'as-is');
  assert.equal(row.commit, COMMIT);
  // Re-importing updates the row instead of appending a second one.
  assert.equal(
    run(vendor, [
      'import',
      '--from',
      join(out, 'top'),
      '--package',
      pkg,
      '--group',
      'lib',
      '--map',
      'apps/web/src/components/X.tsx=src/lib/X.tsx',
      '--verdict',
      'adapt',
    ]).status,
    0,
  );
  const again = JSON.parse(readFileSync(join(pkg, 'provenance.json'), 'utf8'));
  assert.equal(again.files.length, 1);
  assert.equal(again.files[0].verdict, 'adapt');
});

test('vendor_check_fails_on_drift_and_missing_rows', () => {
  const work = scratch('check');
  const zip = join(work, 'up.zip');
  writeFileSync(zip, makeZip([{ name: 'top/a.ts', data: 'export const a = 1;\n' }]));
  const out = join(work, 'out');
  run(vendor, ['fetch', '--commit', COMMIT, '--archive', zip, '--out', out]);
  const pkg = makePackage();
  run(vendor, [
    'import',
    '--from',
    join(out, 'top'),
    '--package',
    pkg,
    '--group',
    'lib',
    '--map',
    'a.ts=src/a.ts',
  ]);
  assert.equal(run(vendor, ['check', '--package', pkg]).status, 0);
  // An as-is file edited without updating provenance fails.
  writeFileSync(join(pkg, 'src/a.ts'), `${readFileSync(join(pkg, 'src/a.ts'), 'utf8')}// edit\n`);
  let res = run(vendor, ['check', '--package', pkg]);
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /localSha256/);
  // A file carrying the header without a provenance row fails.
  const prov = JSON.parse(readFileSync(join(pkg, 'provenance.json'), 'utf8'));
  prov.files = [];
  writeFileSync(join(pkg, 'provenance.json'), JSON.stringify(prov));
  res = run(vendor, ['check', '--package', pkg]);
  assert.notEqual(res.status, 0);
  assert.match(res.stderr, /no provenance row/);
});

test('upstream_report_classifies', () => {
  const work = scratch('report');
  const oldTree = join(work, 'old');
  const newTree = join(work, 'new');
  const files = {
    'apps/web/src/same.ts': ['export const same = 1;\n', 'export const same = 1;\n'],
    'apps/web/src/changed.ts': ['export const v = 1;\n', 'export const v = 2;\n'],
    'apps/web/src/gone.ts': ['export const gone = 1;\n', null],
  };
  for (const [path, [before, after]] of Object.entries(files)) {
    mkdirSync(dirname(join(oldTree, path)), { recursive: true });
    writeFileSync(join(oldTree, path), before);
    if (after !== null) {
      mkdirSync(dirname(join(newTree, path)), { recursive: true });
      writeFileSync(join(newTree, path), after);
    }
  }
  const pkg = makePackage();
  const prov = JSON.parse(readFileSync(join(pkg, 'provenance.json'), 'utf8'));
  prov.files = Object.keys(files).map((upstreamPath) => ({
    path: `src/${upstreamPath.split('/').pop()}`,
    upstreamPath,
    commit: COMMIT,
    upstreamSha256: sha256(files[upstreamPath][0]),
    localSha256: '',
    verdict: 'adapt',
    group: 'lib',
  }));
  writeFileSync(join(pkg, 'provenance.json'), JSON.stringify(prov));
  const res = run(report, ['--from', oldTree, '--to', newTree, '--package', pkg, '--json']);
  assert.equal(res.status, 0, res.stderr);
  const rows = Object.fromEntries(JSON.parse(res.stdout).map((r) => [r.upstreamPath, r]));
  assert.equal(rows['apps/web/src/same.ts'].status, 'unchanged');
  assert.equal(rows['apps/web/src/changed.ts'].status, 'changed upstream');
  assert.match(rows['apps/web/src/changed.ts'].diff, /^-export const v = 1;$/m);
  assert.match(rows['apps/web/src/changed.ts'].diff, /^\+export const v = 2;$/m);
  assert.equal(rows['apps/web/src/gone.ts'].status, 'gone upstream');
  // The text report names each class too.
  const text = run(report, ['--from', oldTree, '--to', newTree, '--package', pkg]);
  assert.match(text.stdout, /unchanged/);
  assert.match(text.stdout, /changed upstream/);
  assert.match(text.stdout, /gone upstream/);
  assert.deepEqual(readdirSync(work).sort(), ['new', 'old']);
});
