// Drives the hosted web console (console.sh hosted, :5192) through the pages
// that take input, against the seeded stack, with a screenshot after each step:
// Analytics (House) runs a query on nyc.yellow_trips, Graph opens namespace e2e
// and queries `github`, Live's Live query and Deployments tabs, and Studio adds
// the web agentd host (agentd.sh web) and opens a thread. Best effort: a step
// that fails is logged and the next one runs.
//
//   node scripts/local-e2e/console-interact.mjs [--base URL] [--out DIR]
import { readFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const { chromium } = createRequire(join(here, '../../web/apps/console/package.json'))('@playwright/test');
const args = Object.fromEntries(process.argv.slice(2).reduce((a, x, i, all) => (x.startsWith('--') ? [...a, [x.slice(2), all[i + 1]]] : a), []));
const BASE = (args.base ?? 'http://127.0.0.1:5192/').replace(/\/?$/, '/');
const E2E = process.env.E2E_HOME ?? '/mnt/Projects/loams-e2e';
const OUT = args.out ?? join(E2E, 'shots', 'interact');
mkdirSync(OUT, { recursive: true });

const browser = await chromium.launch();
const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: 'dark', ignoreHTTPSErrors: true });
await ctx.addInitScript(() => { try { localStorage.setItem('loams-theme', 'dark'); } catch {} });
const page = await ctx.newPage();
const log = [];
page.on('response', (r) => r.status() >= 400 && log.push(`${r.status()} ${r.url().replace(BASE, '/').slice(0, 160)}`));
page.on('pageerror', (e) => log.push(`pageerror ${String(e).slice(0, 200)}`));
const results = [];
async function step(name, fn) {
  const before = log.length;
  try {
    const detail = await fn();
    results.push({ step: name, ok: true, detail: String(detail ?? '').slice(0, 300), http: log.slice(before) });
    console.log(`PASS ${name} ${String(detail ?? '').slice(0, 160)}`);
  } catch (e) {
    results.push({ step: name, ok: false, detail: String(e).slice(0, 300), http: log.slice(before) });
    console.log(`FAIL ${name} ${String(e).split('\n')[0].slice(0, 200)}`);
  }
  await page.screenshot({ path: join(OUT, `${name.replace(/[^a-z0-9]+/gi, '-')}.png`), fullPage: true }).catch(() => {});
}
const text = async () => (await page.locator('body').innerText()).replace(/\s+/g, ' ');

await page.goto(BASE, { waitUntil: 'networkidle' });
if (await page.getByText('Connect your engine').isVisible().catch(() => false)) {
  await page.getByRole('button', { name: 'Connect', exact: true }).click();
  await page.waitForTimeout(3000);
}

await step('analytics-query', async () => {
  await page.goto(`${BASE}#/analytics`);
  await page.waitForTimeout(2500);
  const editor = page.locator('textarea, [contenteditable=true]').first();
  await editor.click();
  await page.keyboard.press('Control+A');
  await page.keyboard.type('SELECT payment_type, count() AS trips, round(avg(tip_amount), 2) AS avg_tip FROM nyc.yellow_trips GROUP BY payment_type ORDER BY trips DESC');
  // The completion popup stays open over the Run button: close it, run with the keyboard.
  await page.keyboard.press('Escape');
  await page.keyboard.press('Control+Enter');
  await page.waitForTimeout(5000);
  const t = await text();
  if (!/rows in .* rows read/.test(t)) throw new Error(t.slice(t.indexOf('Allow writes'), t.indexOf('Allow writes') + 300));
  return t.slice(t.indexOf('Allow writes'), t.indexOf('Allow writes') + 200);
});

await step('graph-github-query', async () => {
  await page.goto(`${BASE}#/graph`);
  await page.waitForTimeout(2000);
  await page.getByLabel('Namespace').fill('e2e');
  await page.getByRole('button', { name: 'Open', exact: true }).click();
  await page.waitForTimeout(2500);
  await page.getByText('github', { exact: true }).first().click();
  await page.waitForTimeout(2500);
  const editor = page.locator('textarea, [contenteditable=true]').first();
  await editor.click();
  await page.keyboard.press('Control+A');
  await page.keyboard.type('MATCH (a:Actor)-[:OPENED_PR]->(r:Repo) RETURN a.login, r.name LIMIT 10');
  await page.keyboard.press('Escape');
  await page.keyboard.press('Control+Enter');
  await page.waitForTimeout(5000);
  return (await text()).slice(0, 300);
});

await step('live-live-query-tab', async () => {
  await page.goto(`${BASE}#/live`);
  await page.waitForTimeout(2500);
  await page.getByRole('tab', { name: 'Live query' }).click();
  await page.waitForTimeout(2000);
  return 'opened';
});

await step('live-deployments', async () => {
  await page.goto(`${BASE}#/live`);
  await page.waitForTimeout(2000);
  await page.getByRole('button', { name: 'Deployments and functions' }).click();
  await page.waitForTimeout(3000);
  return (await text()).slice(0, 300);
});

await step('studio-add-agentd-host', async () => {
  const token = readFileSync(join(E2E, 'agentd/web/token'), 'utf8').trim();
  await page.goto(`${BASE}#/studio/settings`);
  await page.waitForTimeout(2500);
  // As web/apps/console/test/e2e/studio.spec.ts addHost does.
  const form = page.getByRole('form', { name: 'Add an agentd host' });
  await form.getByLabel('Name').fill('local-e2e');
  await form.getByLabel('URL', { exact: true }).fill('https://127.0.0.1:17443');
  await form.getByRole('button', { name: 'Add host' }).click();
  const row = page.getByRole('list', { name: 'Registered hosts' }).getByRole('listitem', { name: 'local-e2e' });
  await row.getByLabel('Access token for local-e2e').fill(token);
  await row.getByRole('button', { name: 'Set token' }).click();
  await page.waitForTimeout(3000);
  return (await row.innerText()).replace(/\s+/g, ' ');
});

await step('studio-home', async () => {
  // In-app navigation: the pasted token lives in this tab's memory only, a reload drops it.
  await page.getByRole('link', { name: 'Home', exact: true }).first().click();
  await page.waitForTimeout(4000);
  return (await text()).slice(0, 300);
});

await step('studio-thread', async () => {
  await page.getByRole('region', { name: 'Projects' }).getByRole('link', { name: /e2e-sample/ }).click();
  await page.waitForTimeout(2500);
  const box = page.getByRole('textbox', { name: 'New thread prompt' });
  await box.click();
  await box.fill('local-e2e: add a line to notes.txt');
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  await page.waitForTimeout(10000);
  return (await page.getByRole('log', { name: 'Thread timeline' }).innerText()).replace(/\s+/g, ' ').slice(0, 300);
});

writeFileSync(join(OUT, 'interact.json'), JSON.stringify(results, null, 2));
await browser.close();
