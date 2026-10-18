// Screens every page of Loams Desktop (the built app, apps/desktop-electron/out)
// with Playwright's Electron driver, in a userData of its own
// ($E2E_HOME/desktop-crawl-userdata: the running desktop.sh instance keeps its
// single-instance lock), with the same servers.json (the compose stack active)
// and the desktop agentd's discovery files. For each nav link (and EXTRA routes):
// a screenshot (1440 wide, dark), console errors, failed requests, the Sample
// data notice. Writes report.json.
//
//   node scripts/local-e2e/desktop-crawl.mjs --out DIR [--server local|e2e-stack]
import { copyFileSync, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '../..');
const { _electron: electron } = createRequire(join(root, 'apps/desktop-electron/package.json'))('@playwright/test');
const args = Object.fromEntries(process.argv.slice(2).reduce((a, x, i, all) => (x.startsWith('--') ? [...a, [x.slice(2), all[i + 1]]] : a), []));
const E2E = process.env.E2E_HOME ?? '/mnt/Projects/loams-e2e';
const OUT = args.out ?? join(E2E, 'shots', 'desktop');
const server = args.server ?? 'e2e-stack';
mkdirSync(OUT, { recursive: true });
const ud = join(E2E, 'desktop-crawl-userdata');
mkdirSync(join(ud, 'agentd'), { recursive: true, mode: 0o700 });
writeFileSync(join(ud, 'servers.json'), JSON.stringify({ v: 1, servers: [{ id: 'e2e-stack', name: 'Local compose stack (e2e)', kind: 'remote', url: 'http://127.0.0.1:18080' }], activeId: server }));
// Dark, through the desktop's own setting (settings.json `ui.theme`, D1074).
writeFileSync(join(ud, 'settings.json'), JSON.stringify({ 'ui.theme': 'dark' }));
for (const f of ['agentd.json', 'agentd.token']) {
  const src = join(E2E, 'desktop-userdata', 'agentd', f);
  if (existsSync(src)) copyFileSync(src, join(ud, 'agentd', f));
}
const app = await electron.launch({
  args: [join(root, 'apps/desktop-electron')],
  cwd: join(root, 'apps/desktop-electron'),
  env: { ...process.env, LOAMS_DESKTOP_USER_DATA: ud, LOAMS_BIN: join(E2E, 'bin/loams-engine'), LOAMS_DESKTOP_NO_UPDATE_CHECK: '1' },
});
const page = await app.firstWindow();
await page.setViewportSize({ width: 1440, height: 900 }).catch(() => {});
await page.evaluate(() => { try { localStorage.setItem('loams-theme', 'dark'); } catch {} });
let log = [];
page.on('console', (m) => ['error', 'warning'].includes(m.type()) && log.push({ kind: `console.${m.type()}`, text: m.text().slice(0, 400) }));
page.on('pageerror', (e) => log.push({ kind: 'pageerror', text: String(e).slice(0, 400) }));
page.on('response', (r) => r.status() >= 400 && log.push({ kind: 'http', status: r.status(), url: r.url().slice(0, 200) }));
await page.waitForTimeout(8000);
// The desktop opens in Studio on first run: switch to Console for the nav.
await page.getByRole('button', { name: /^Console$/ }).first().click().catch(() => {});
await page.waitForTimeout(3000);
const base = page.url().split('#')[0];
let routes = await page.$$eval('a[href]', (as) => [...new Set(as.map((a) => a.getAttribute('href')).filter((h) => h?.startsWith('#/')))]);
routes = [...new Set(['#/', ...routes, ...(process.env.E2E_EXTRA_ROUTES ?? '').split(',').filter(Boolean)])];
console.log(`desktop base ${base}; routes (${routes.length}): ${routes.join(' ')}`);
const report = [];
for (const route of routes) {
  log = [];
  await page.goto(base + route).catch((e) => log.push({ kind: 'goto', text: String(e).slice(0, 200) }));
  await page.waitForTimeout(4000);
  const info = await page.evaluate(() => {
    const text = document.body?.innerText ?? '';
    return {
      title: document.querySelector('h1')?.textContent?.trim() ?? document.title,
      sample: !!document.querySelector('[data-sample-data]'),
      notFound: /Nothing here|No plugin serves/.test(text),
      errorWords: (text.match(/[^\n]*(error|failed|not configured|unavailable|not_in_variant|unimplemented|denied|refused)[^\n]*/gi) ?? []).slice(0, 6),
    };
  });
  const file = join(OUT, `${route.replace(/^#\/?/, '').replace(/[^a-z0-9]+/gi, '-') || 'home'}.png`);
  await page.screenshot({ path: file, fullPage: true }).catch(() => {});
  report.push({ route, screenshot: file, ...info, log: log.slice(0, 30) });
  console.log(`${route}: ${info.title} | sample=${info.sample} | notFound=${info.notFound} | ${log.length} issues${info.errorWords[0] ? ` | ${info.errorWords[0].slice(0, 120)}` : ''}`);
}
writeFileSync(join(OUT, 'report.json'), JSON.stringify(report, null, 2));
await app.close();
