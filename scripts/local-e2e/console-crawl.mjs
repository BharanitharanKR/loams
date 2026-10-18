// Visits every page the console's navigation offers (plus EXTRA routes),
// against the console dev server on the real engine, and records for each:
// a full-page screenshot (1440 wide, dark theme), console errors, failed
// requests, whether the page shows the "Sample data" notice, and error text on
// the page. Writes report.json beside the screenshots.
//
//   node scripts/local-e2e/console-crawl.mjs [--base http://127.0.0.1:5190/] [--out DIR] [--only /path,...]
//
// Playwright comes from web/apps/console's devDependencies (pnpm install).

import { mkdirSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const require = createRequire(join(here, '../../web/apps/console/package.json'));
const { chromium } = require('@playwright/test');

const args = Object.fromEntries(
  process.argv.slice(2).reduce((acc, a, i, all) => (a.startsWith('--') ? [...acc, [a.slice(2), all[i + 1]]] : acc), []),
);
const BASE = (args.base ?? 'http://127.0.0.1:5190/').replace(/\/?$/, '/');
const OUT = args.out ?? join(process.env.E2E_SHOTS ?? '/mnt/Projects/loams-e2e/shots', 'console');
const EXTRA = (process.env.E2E_EXTRA_ROUTES ?? '').split(',').filter(Boolean);
mkdirSync(OUT, { recursive: true });

const slug = (r) => (r.replace(/^#?\/?/, '').replace(/[^a-z0-9]+/gi, '-').replace(/^-|-$/g, '') || 'home');

const browser = await chromium.launch({ executablePath: process.env.E2E_CHROME || undefined });
const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: 'dark' });
await context.addInitScript(() => {
  try {
    localStorage.setItem('loams-theme', 'dark');
  } catch {}
});
const page = await context.newPage();
let log = [];
page.on('console', (m) => {
  if (m.type() === 'error' || m.type() === 'warning') log.push({ kind: `console.${m.type()}`, text: m.text().slice(0, 500) });
});
page.on('pageerror', (e) => log.push({ kind: 'pageerror', text: String(e).slice(0, 500) }));
page.on('response', (r) => {
  if (r.status() >= 400) log.push({ kind: 'http', status: r.status(), url: r.url().replace(BASE, '/').slice(0, 200) });
});
page.on('requestfailed', (r) => log.push({ kind: 'requestfailed', url: r.url().slice(0, 200), text: r.failure()?.errorText }));

await page.goto(BASE, { waitUntil: 'networkidle', timeout: 60_000 }).catch(() => {});
await page.waitForTimeout(3000);
// The hosted edition asks which engine to use first: its config.json names it, so Connect.
if (await page.getByText('Connect your engine').isVisible().catch(() => false)) {
  await page.getByRole('button', { name: 'Connect', exact: true }).click();
  await page.waitForLoadState('networkidle').catch(() => {});
  await page.waitForTimeout(3000);
  console.log('connected the hosted console to its engine');
}

// The sidebar lists pinned pages only (D1080): pin every page first, through
// the "Edit pages" menu, so their links are on the page.
for (let i = 0; i < 60; i++) {
  const edit = page.getByRole('button', { name: 'Edit pages' });
  if (!(await edit.isVisible().catch(() => false))) break;
  await edit.click();
  const item = page.locator('[role="menuitemcheckbox"][aria-checked="false"]').first();
  if (!(await item.waitFor({ state: 'visible', timeout: 1500 }).then(() => true, () => false))) {
    await page.keyboard.press('Escape');
    break;
  }
  await item.click();
  await page.keyboard.press('Escape').catch(() => {});
  await page.waitForTimeout(150);
}

// The routes: every in-app link in the navigation (hash routes), then EXTRA.
let routes = await page.$$eval('a[href]', (as) =>
  [...new Set(as.map((a) => a.getAttribute('href')).filter((h) => h && (h.startsWith('#/') || h.startsWith('/')) && !h.startsWith('//')))],
);
// Settings pages are listed in the sidebar once Settings is open.
const settings = page.getByRole('link', { name: 'Settings', exact: true }).first();
if (await settings.isVisible().catch(() => false)) {
  await settings.click();
  await page.waitForTimeout(1500);
  routes.push(...(await page.$$eval('a[href]', (as) =>
    as.map((a) => a.getAttribute('href')).filter((h) => h && (h.startsWith('#/') || h.startsWith('/')) && !h.startsWith('//')))));
}
routes = [...new Set(['#/', ...routes, ...EXTRA])];
if (args.only) routes = args.only.split(',');
console.log(`routes (${routes.length}):`, routes.join(' '));

const report = [];
for (const route of routes) {
  log = [];
  const url = route.startsWith('#') ? `${BASE}${route}` : new URL(route.replace(/^\//, ''), BASE).href;
  const t0 = Date.now();
  await page.goto(url, { waitUntil: 'networkidle', timeout: 45_000 }).catch((e) => log.push({ kind: 'goto', text: String(e).slice(0, 200) }));
  await page.waitForTimeout(2500);
  const info = await page.evaluate(() => {
    const text = document.body?.innerText ?? '';
    const errs = [...document.querySelectorAll('[role=alert], .lc-error, [data-error], .loams-notice-error')]
      .map((e) => e.textContent?.trim()).filter(Boolean).slice(0, 5);
    const title = document.querySelector('h1, [data-page-title]')?.textContent?.trim() ?? document.title;
    return {
      title,
      sample: !!document.querySelector('[data-sample-data]'),
      alerts: errs,
      errorWords: (text.match(/[^\n]*(error|failed|not configured|unavailable|not_in_variant|unimplemented|denied|refused)[^\n]*/gi) ?? []).slice(0, 6),
      textLength: text.length,
    };
  });
  const file = join(OUT, `${slug(route)}.png`);
  await page.screenshot({ path: file, fullPage: true }).catch(() => {});
  report.push({ route, url, ms: Date.now() - t0, screenshot: file, ...info, log: log.slice(0, 40) });
  console.log(`${route}: ${info.title} | sample=${info.sample} | ${log.length} issues${info.errorWords.length ? ` | ${info.errorWords[0]}` : ''}`);
}
writeFileSync(join(OUT, 'report.json'), JSON.stringify(report, null, 2));
await browser.close();
console.log(`report: ${join(OUT, 'report.json')}`);
