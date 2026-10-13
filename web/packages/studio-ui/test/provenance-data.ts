// Shared loader for provenance.json and the package's files (SD1 Task 1).
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

export const PACKAGE_DIR = join(dirname(fileURLToPath(import.meta.url)), '..');
export const WEB_DIR = join(PACKAGE_DIR, '..', '..');

export interface ProvenanceRow {
  path: string;
  upstreamPath: string;
  commit: string;
  upstreamSha256: string;
  localSha256: string;
  verdict: 'as-is' | 'adapt' | 'rewrite';
  group: string;
  note?: string;
}

export interface Provenance {
  upstream: string;
  license: string;
  copyright: string;
  archives: { commit: string; url: string; sha256: string }[];
  files: ProvenanceRow[];
}

export function loadProvenance(): Provenance {
  return JSON.parse(readFileSync(join(PACKAGE_DIR, 'provenance.json'), 'utf8')) as Provenance;
}

export function readPackageFile(path: string): string {
  return readFileSync(join(PACKAGE_DIR, path), 'utf8');
}

/** Every regular file under `dir`, skipping node_modules and dist, as forward-slash paths. */
export function listFiles(dir: string, root = dir): string[] {
  const out: string[] = [];
  let names: string[];
  try {
    names = readdirSync(dir);
  } catch {
    return out;
  }
  for (const name of names) {
    if (name === 'node_modules' || name === 'dist') continue;
    const path = join(dir, name);
    if (statSync(path).isDirectory()) out.push(...listFiles(path, root));
    else out.push(relative(root, path).split(sep).join('/'));
  }
  return out;
}
