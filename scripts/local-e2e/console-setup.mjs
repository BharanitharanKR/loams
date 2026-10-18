// First-run setup through the console's own screen (the engine answers
// setupRequired until an organization and its owner exist). With
// LOAMS_AUTH_MODE=off the engine's setup gate is open, so the token field is
// left empty. The owner's password comes from $E2E_HOME/console-owner.env.
//
//   node scripts/local-e2e/console-setup.mjs [base] [screenshot-dir]
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const { chromium } = createRequire(join(here, '../../web/apps/console/package.json'))('@playwright/test');
const base = process.argv[2] ?? 'http://127.0.0.1:5190/';
const out = process.argv[3] ?? '/mnt/Projects/loams-e2e/shots';
const env = Object.fromEntries(
  readFileSync(join(process.env.E2E_HOME ?? '/mnt/Projects/loams-e2e', 'console-owner.env'), 'utf8')
    .split('\n').filter(Boolean).map((l) => l.split(/=(.*)/s).slice(0, 2)),
);
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, colorScheme: 'dark' });
page.on('response', (r) => r.url().includes('loams.auth') && console.log(r.status(), r.url().replace(base, '/')));
await page.goto(base, { waitUntil: 'networkidle' });
const form = page.getByText('Set up this install');
if (!(await form.isVisible().catch(() => false))) {
  console.log('no setup screen: already set up');
} else {
  // The field is required in the form; an open gate (auth off) ignores its value.
  await page.getByLabel('Setup token').fill(process.env.LOAMS_SETUP_TOKEN ?? 'auth-off');
  await page.getByLabel('Organization name').fill('Ostrium e2e');
  await page.getByLabel('Your name').fill('E2E Owner');
  await page.getByLabel('Email').fill(env.E2E_CONSOLE_EMAIL);
  await page.getByLabel('Password').fill(env.E2E_CONSOLE_PASSWORD);
  await page.getByRole('button', { name: 'Create organization' }).click();
  await page.waitForTimeout(4000);
  await page.screenshot({ path: join(out, 'setup-after.png'), fullPage: true });
  console.log('after setup:', (await page.locator('body').innerText()).slice(0, 400).replace(/\n+/g, ' | '));
}
await browser.close();
