// The file viewer's highlighting theme (SD1 Task 21; console-ui-stack decision 1, tokens
// only). @pierre/diffs highlights with Shiki; Shiki's CSS-variables theme names a
// variable for each kind of token, and this registers it with each variable defaulting to
// an @loams/ui token. Custom properties inherit into the viewer's shadow root, so the
// colours follow the console's light and dark tokens with no colour of our own.

import { onThemeChange } from "@loams/ui";
import { registerCustomCSSVariableTheme } from "@pierre/diffs";
import { useSyncExternalStore } from "react";

export const CODE_THEME = "loams-tokens";

const TOKEN_COLOURS: Record<string, string> = {
  foreground: "var(--op-ink)",
  background: "var(--op-bg)",
  "token-keyword": "var(--op-accent)",
  "token-string": "var(--op-grow)",
  "token-string-expression": "var(--op-grow)",
  "token-comment": "var(--op-faint)",
  "token-constant": "var(--op-info)",
  "token-function": "var(--op-chart-7)",
  "token-parameter": "var(--op-ink)",
  "token-punctuation": "var(--op-muted)",
  "token-link": "var(--op-chart-1)",
};

let registered = false;

/** Registers the theme once per page; @pierre/diffs keeps one shared highlighter. */
export function ensureCodeTheme(): string {
  if (!registered) {
    registered = true;
    registerCustomCSSVariableTheme(CODE_THEME, TOKEN_COLOURS, true);
  }
  return CODE_THEME;
}

/**
 * The viewer's own CSS inside its shadow root: our static rules only, never text from a
 * file (T0-4: no `{html}` path). Fonts and the selected-line tint are tokens too.
 */
export const CODE_SURFACE_CSS = `
  :host {
    display: block;
    min-height: 100%;
    --diffs-font-family: var(--loams-font-mono);
    --diffs-bg-selection-override: var(--op-accent-soft);
    --diffs-bg-selection-number-override: var(--op-accent-soft);
    --diffs-fg-number-override: var(--op-faint);
  }
`;

/** `light` or `dark`, as the console's `.dark` class on <html> says. */
export function useColourScheme(): "light" | "dark" {
  return useSyncExternalStore(
    (listener) => onThemeChange(listener),
    () => (document.documentElement.classList.contains("dark") ? "dark" : "light"),
    () => "light",
  );
}
