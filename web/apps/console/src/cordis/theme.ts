// The theme before the console renders: the saved choice (the key the original
// console and the shell's toggle share), else light. main.tsx imports this
// first; the CSP allows no inline script, so it runs as the entry's first module.

export const THEME_KEY = 'loams-theme';

export function applySavedTheme(root: HTMLElement = document.documentElement): void {
  let dark = false;
  try {
    dark = localStorage.getItem(THEME_KEY) === 'dark';
  } catch {
    // Storage blocked: light.
  }
  root.classList.toggle('dark', dark);
}

applySavedTheme();
