// The theme before the console renders (design 59, D1074): the desktop's saved choice (the
// preload passes it), else this browser's, else System, which is dark when the OS states no
// preference. main.tsx imports this first; the CSP allows no inline script, so it runs as the
// entry's first module. The `theme` service takes over once the platform plugin is applied.

import {
  applyTheme,
  MOTION_KEY,
  parseThemeChoice,
  resolveDark,
  THEME_KEY,
  type ThemeChoice,
} from '@loams/console-host/theme';

export { THEME_KEY };

export function initialChoice(): ThemeChoice {
  const desktop = (globalThis as { loamsDesktop?: { theme?: { initial?: unknown } } }).loamsDesktop
    ?.theme?.initial;
  if (desktop !== undefined) return parseThemeChoice(desktop);
  try {
    return parseThemeChoice(localStorage.getItem(THEME_KEY));
  } catch {
    return 'system';
  }
}

export function applySavedTheme(root: HTMLElement = document.documentElement): void {
  applyTheme(resolveDark(initialChoice()), root);
  let motion = false;
  try {
    motion = localStorage.getItem(MOTION_KEY) === 'on';
  } catch {
    // Storage blocked: panels snap.
  }
  if (motion) root.dataset.motion = 'on';
  else delete root.dataset.motion;
}

applySavedTheme();
