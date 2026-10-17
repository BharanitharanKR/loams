// The diff viewer's colours (SD1 Task 20; console-ui-stack decision 1, tokens only).
// @pierre/diffs paints inside an open shadow root; custom properties inherit across it, so
// every colour the renderer reads is pointed at an @loams/ui token here and follows the
// console's light and dark values. Syntax colours are Task 21's `loams-tokens` Shiki theme
// (files/codeTheme.ts). T3 Code's `--diffs-*` theming (its index.css and
// DIFF_SURFACE_THEME_UNSAFE_CSS) reads `--code-background`, `--code-foreground`,
// `--diff-addition` and `--diff-deletion`, which are tokens here as well.
//
// This is our own static CSS, never text from a patch or a file (T0-4).

import { DIFF_SURFACE_THEME_UNSAFE_CSS } from "./diffRendering";

/** The token each colour hook of the renderer takes. */
export const DIFF_TOKEN_VARIABLES: Readonly<Record<string, string>> = {
  "--code-background": "var(--op-bg)",
  "--code-foreground": "var(--op-ink)",
  "--diff-addition": "var(--op-grow)",
  "--diff-deletion": "var(--op-danger)",
  "--diffs-addition-color-override": "var(--op-grow)",
  "--diffs-deletion-color-override": "var(--op-danger)",
  "--diffs-modified-color-override": "var(--op-info)",
  "--diffs-fg-number-override": "var(--op-faint)",
  "--diffs-bg-selection-override": "var(--op-accent-soft)",
  "--diffs-bg-selection-number-override": "var(--op-accent-soft)",
  /* mono-ok: code */ "--diffs-font-family": "var(--loams-font-mono)",
  "--diffs-header-font-family": "var(--loams-font-sans)",
  /* mono-ok: code */ "--font-mono": "var(--loams-font-mono)",
  "--font-sans": "var(--loams-font-sans)",
};

/**
 * The stylesheet set inside each file's shadow root. `light-dark()` in the vendored surface
 * rules resolves against `color-scheme`, which follows the console's `.dark` class.
 */
export function diffSurfaceCss(scheme: "light" | "dark"): string {
  const variables = Object.entries(DIFF_TOKEN_VARIABLES)
    .map(([name, value]) => `  ${name}: ${value};`)
    .join("\n");
  return `
:host {
  display: block;
  color-scheme: ${scheme};
${variables}
}
${DIFF_SURFACE_THEME_UNSAFE_CSS}
[data-line-annotation] {
  font-family: var(--loams-font-sans);
}
`;
}
