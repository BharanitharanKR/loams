// @vitest-environment node
// Colours come only from tokens.css (docs/design/console-ui-stack.md, decision 1):
// a raw hex, rgb(), hsl() or oklch() literal anywhere else in the console's plugins
// and packages fails here. Use a `var(--op-*)` token or a Tailwind token utility
// (`bg-surface`, `text-muted`, ...) instead, or add a token to tokens.css.

import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const WEB = fileURLToPath(new URL('../../..', import.meta.url));
const ROOTS = ['plugins', 'packages'].map((d) => join(WEB, d));

/** Where colour literals are allowed: the token sheet, generated code and tests. */
const SKIP_DIRS = new Set(['node_modules', 'dist', 'gen', 'test', 'tests', '__tests__']);
const ALLOWED = new Set([join(WEB, 'packages/ui/src/tokens.css')]);
const SOURCE = /\.(css|ts|tsx|js|jsx|mjs|html)$/;
const TEST = /\.(test|spec)\.[jt]sx?$/;

const LITERAL =
  /(?<![\w&/-])#(?:[0-9a-f]{8}|[0-9a-f]{6}|[0-9a-f]{3,4})(?![\w-])|\b(?:rgba?|hsla?|hwb|lab|lch|oklab|oklch)\(\s*[-\d.]/gi;

function* files(dir: string): Generator<string> {
  for (const name of readdirSync(dir)) {
    if (SKIP_DIRS.has(name) || name.startsWith('.')) continue;
    const path = join(dir, name);
    if (statSync(path).isDirectory()) yield* files(path);
    else if (SOURCE.test(name) && !TEST.test(name) && !ALLOWED.has(path)) yield path;
  }
}

/** Every colour literal outside tokens.css, as `path:line: match`. */
function colourLiterals(): string[] {
  const found: string[] = [];
  for (const root of ROOTS) {
    for (const path of files(root)) {
      const lines = readFileSync(path, 'utf8').split('\n');
      lines.forEach((line, i) => {
        // A `colour-literal-ok` comment marks a line whose match is not a colour.
        if (line.includes('colour-literal-ok')) return;
        for (const m of line.matchAll(LITERAL)) {
          found.push(`${relative(WEB, path)}:${i + 1}: ${m[0]}`);
        }
      });
    }
  }
  return found;
}

describe('colour tokens', () => {
  it('has no raw colour literal in web/plugins or web/packages outside tokens.css', () => {
    expect(colourLiterals()).toEqual([]);
  });

  it('catches the literals it is meant to', () => {
    const sample = "color: #1a1511; fill: '#fff'; rgb(0 0 0 / 0.5); oklch(0.7 0.1 80)";
    expect(sample.match(LITERAL)).toHaveLength(4);
    expect('href="#/streams" #add-on rgba(var(--op-ink))'.match(LITERAL)).toBeNull();
  });
});
