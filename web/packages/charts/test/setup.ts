// The token sheet, as the console loads it: the chart theme reads every colour from it.
// Read from disk: Vitest does not process CSS, so a `?raw` import comes back empty.
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';

const tokens = readFileSync(createRequire(import.meta.url).resolve('@loams/ui/tokens.css'), 'utf8');

const style = document.createElement('style');
style.textContent = tokens;
document.head.append(style);

/** A custom property's value in one block of tokens.css (`:root` or `.dark`). */
export function token(block: ':root' | '.dark', name: string): string {
  const body = tokens.slice(tokens.indexOf(`${block} {`));
  const m = body.slice(0, body.indexOf('\n}')).match(new RegExp(`${name}:\\s*([^;]+);`));
  if (!m?.[1]) throw new Error(`no ${name} in ${block}`);
  return m[1].trim();
}

export const palette = (block: ':root' | '.dark') =>
  Array.from({ length: 8 }, (_, i) => token(block, `--op-chart-${i + 1}`));
