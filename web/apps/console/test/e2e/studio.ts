// Shared steps for Studio's end-to-end specs (SD1 Task 29): the web edition's
// spec and the desktop's both drive the same Studio, so the scenarios are written
// once against a Playwright `Page` and the edition only says how to get there.

import { expect, type Locator, type Page } from '@playwright/test';

/** The page's `location` (the desktop's tsconfig has no DOM types). */
interface PageGlobal {
  location: { hash: string };
}

/** The hash route the page shows, without the `#`. */
export const hashOf = (page: Page) =>
  page.evaluate(() => (globalThis as unknown as PageGlobal).location.hash.slice(1));

/** Moves the hash router without a reload (a reload would drop a pasted token). */
export async function go(page: Page, path: string): Promise<void> {
  await page.evaluate((p) => {
    (globalThis as unknown as PageGlobal).location.hash = p;
  }, path);
  await expect.poll(() => hashOf(page)).toBe(path);
}

export const projectHref = (repo: string) => `/studio/p/${encodeURIComponent(repo)}`;

/** The mode switcher's buttons. */
export function modes(page: Page) {
  const group = page.getByRole('group', { name: 'Mode' });
  return {
    console: group.getByRole('button', { name: 'Console' }),
    studio: group.getByRole('button', { name: 'Studio' }),
  };
}

/**
 * Chooses Settings › Studio's approval mode, which a new thread takes before its first
 * run (`SetThreadApprovalMode`). The specs never lean on the daemon's own default.
 */
export async function chooseApprovalMode(
  page: Page,
  mode: 'supervised' | 'full-access',
): Promise<void> {
  await go(page, '/studio/settings');
  const select = page.getByRole('combobox', { name: 'Approvals' });
  await select.selectOption(mode);
  await expect(select).toHaveValue(mode);
}

/** Types into the Tiptap editor (a contenteditable textbox) and sends. */
export async function sendPrompt(page: Page, box: Locator, text: string): Promise<void> {
  await box.click();
  await page.keyboard.type(text);
  // The page's own Send: the desktop's inspector can show the Agent tab, whose chat has one too.
  const send = page.getByRole('main').getByRole('button', { name: 'Send', exact: true });
  await expect(send).toBeEnabled();
  await send.click();
}

/** Starts a thread from the project page's composer; answers the thread id. */
export async function startThread(page: Page, repo: string, prompt: string): Promise<string> {
  await go(page, projectHref(repo));
  const box = page.getByRole('textbox', { name: 'New thread prompt' });
  await expect(box).toBeVisible();
  // The daemon's default harness is the Mock (LOAMS_DESKTOP_HARNESS=mock), listed first.
  await expect(page.getByRole('combobox', { name: 'Harness' })).toHaveText(/Mock/);
  await sendPrompt(page, box, prompt);
  await expect.poll(() => hashOf(page), { timeout: 30_000 }).toMatch(/^\/studio\/t\//);
  const hash = await hashOf(page);
  return decodeURIComponent(hash.replace(/^\/studio\/t\//, '').split('?')[0] ?? '');
}

/** The thread timeline. */
export const timeline = (page: Page) => page.getByRole('log', { name: 'Thread timeline' });

/** The shell's inspector (design 59): the thread's panels are its tabs. */
export const inspector = (page: Page) => page.getByRole('complementary', { name: 'Inspector' });

/** Opens one of the thread's panels in the inspector (a thread opens it on Diff, T9-6; the person may have closed it). */
export async function openInspectorTab(page: Page, tab: string): Promise<void> {
  const show = page.getByRole('button', { name: 'Show inspector' });
  if (await show.isVisible()) await show.click();
  await inspector(page).getByRole('tab', { name: tab }).click();
}

/** The inspector Diff tab's diff of one file. */
export const diffOf = (page: Page, path: string) =>
  inspector(page).getByRole('region', { name: path });

/** Waits until the turn's Mock edit is in the timeline and the diff. */
export async function expectMockEdit(page: Page, n: number): Promise<void> {
  await expect(
    timeline(page)
      .getByRole('button', { name: 'Edited notes.txt' })
      .nth(n - 1),
  ).toBeVisible({
    timeout: 60_000,
  });
  await expect(diffOf(page, 'notes.txt')).toContainText(`mock edit ${n}`, { timeout: 30_000 });
}

/**
 * Adds a review comment on a diff line: hovering a line shows the gutter's "+",
 * which opens a draft under it (@pierre/diffs, in an open shadow root).
 */
export async function commentOnDiff(page: Page, path: string, text: string): Promise<void> {
  const file = diffOf(page, path);
  const line = file.locator('[data-line]').filter({ hasText: 'mock edit' }).first();
  const plus = file.locator('[data-utility-button], [data-gutter-utility-slot] button').first();
  const draft = file.getByRole('textbox', { name: /^Comment on line/ });
  // The viewer re-renders while the diff settles (the daemon's diff sync), which drops
  // the hover that shows the "+": hover and click again until the draft opens. A draft
  // only opens while comments are possible (the diff has a checkout and a checksum).
  await expect(async () => {
    if (await draft.isVisible()) return;
    await line.hover();
    await plus.click({ timeout: 2_000 });
    await expect(draft).toBeVisible({ timeout: 2_000 });
  }).toPass({ timeout: 30_000 });
  await draft.fill(text);
  await file.getByRole('button', { name: /^(Comment|Save|Add comment)$/ }).click();
  await expect(file).toContainText(text);
}
