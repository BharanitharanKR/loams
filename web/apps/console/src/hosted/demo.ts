// The "Demo data" toggle of the browser console: off by default, remembered per
// browser. On, the console also loads the pages that still run on built-in sample
// data (catalog/demo.yml); off, every page talks to the connected engine.

const KEY = 'loams.console.demo-data';

/** The pages demo.yml adds or turns on, as their nav labels, for the toggle's explanation. */
export const DEMO_PAGES = ['Deployments', 'Live deployments', 'SQL', 'Metrics', 'Pipelines', 'Git'];

export function demoEnabled(): boolean {
  try {
    return globalThis.localStorage?.getItem(KEY) === '1';
  } catch {
    return false;
  }
}

export function setDemoEnabled(on: boolean) {
  try {
    if (on) globalThis.localStorage?.setItem(KEY, '1');
    else globalThis.localStorage?.removeItem(KEY);
  } catch {
    // Private mode: the toggle lasts for this page only.
  }
}
