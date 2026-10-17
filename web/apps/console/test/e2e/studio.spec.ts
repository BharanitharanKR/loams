// Loams Studio end to end on the web edition (SD1 Task 29; §53 §10): the
// console's dev server in demo mode against a real `loams-agentd serve
// --remote` on loopback TLS, with the debug-build test issuer and the Mock
// harness (fixtures/agentd.ts). The scenarios run in order on one page, so each
// builds on the last: a project, a thread whose turn edits a file, its diff and a
// review sent back to the agent, a restore, a commit, files, a terminal, the
// palette, notifications, and a run group compared and merged.
//
// The approval scenario has its own daemon, started with
// `LOAMS_DESKTOP_MOCK_APPROVE=1` (Task 13's contract: the Mock asks through
// `RunControls::request_approval` in a supervised thread).

import { readFileSync } from 'node:fs';
import { type BrowserContext, expect, type Page, test } from '@playwright/test';
import { type Agentd, startAgentd } from './fixtures/agentd';
import {
  chooseApprovalMode,
  commentOnDiff,
  diffOf,
  expectMockEdit,
  go,
  hashOf,
  inspector,
  modes,
  openInspectorTab,
  projectHref,
  startThread,
  timeline,
} from './studio';

/** Registers the daemon in Settings › Agentd hosts; pastes its token unless told not to. */
async function addHost(page: Page, agentd: Agentd, opts: { token?: boolean } = {}) {
  await go(page, '/studio/settings');
  const form = page.getByRole('form', { name: 'Add an agentd host' });
  await form.getByLabel('Name').fill('E2E host');
  await form.getByLabel('URL', { exact: true }).fill(agentd.url ?? '');
  await form.getByRole('button', { name: 'Add host' }).click();
  const row = page.getByRole('list', { name: 'Registered hosts' }).getByRole('listitem', {
    name: 'E2E host',
  });
  await expect(row).toContainText('no token');
  if (opts.token === false) return;
  await row.getByLabel('Access token for E2E host').fill(agentd.token ?? '');
  await row.getByRole('button', { name: 'Set token' }).click();
  await expect(row).toContainText('signed in');
}

/** Opens the console in demo mode at Studio (the dev server's demo has `studio` on). */
async function openStudio(context: BrowserContext): Promise<Page> {
  const page = await context.newPage();
  await page.goto('/?demo#/studio');
  await expect(modes(page).studio).toHaveAttribute('aria-pressed', 'true', { timeout: 60_000 });
  return page;
}

test.describe('studio on the web edition', () => {
  test.describe.configure({ mode: 'serial' });

  let agentd: Agentd;
  let context: BrowserContext;
  let page: Page;
  let threadId = '';

  test.beforeAll(async ({ browser, baseURL }) => {
    agentd = await startAgentd({
      mode: 'remote',
      allowOrigin: new URL(baseURL ?? 'http://127.0.0.1:5191').origin,
      mockEdit: ['notes.txt'],
    });
    context = await browser.newContext();
    page = await openStudio(context);
  });

  test.afterAll(async () => {
    await context?.close();
    if (agentd) {
      if (test.info().status !== 'passed') console.log(agentd.log().slice(-4000));
      await agentd.stop();
    }
  });

  test('web_remote_requires_token', async () => {
    // No host yet: Studio asks for one.
    await expect(page.getByText('Connect an agentd host')).toBeVisible();
    // A host without a token is still not connected: the transport never calls without one.
    await addHost(page, agentd, { token: false });
    await go(page, '/studio');
    await expect(page.getByText('Connect an agentd host')).toBeVisible();
    // The daemon refuses a call without a token (and with someone else's).
    expect((await agentd.raw('/loams.agentd.v1.StudioService/GetHost', {}, null)).status).toBe(401);
    expect(
      (
        await agentd.raw(
          '/loams.agentd.v1.StudioService/GetHost',
          {},
          agentd.signToken({ sub: 'x' }),
        )
      ).status,
    ).toBe(403);
    // With the token Studio reaches the daemon: the empty projects state.
    const row = await (async () => {
      await go(page, '/studio/settings');
      return page.getByRole('list', { name: 'Registered hosts' }).getByRole('listitem', {
        name: 'E2E host',
      });
    })();
    await row.getByLabel('Access token for E2E host').fill(agentd.token ?? '');
    await row.getByRole('button', { name: 'Set token' }).click();
    await expect(row).toContainText('signed in');
    await go(page, '/studio');
    await expect(page.getByText('No projects yet').first()).toBeVisible();
  });

  test('add_project_from_fixture_repo', async () => {
    // On the web, Add clones a remote URL onto the host; a local path is refused here.
    await page.getByRole('button', { name: 'Add project' }).click();
    const dialog = page.getByRole('dialog', { name: 'Add a project' });
    await dialog.getByLabel('Repository URL').fill(agentd.repo);
    await dialog.getByRole('button', { name: 'Clone' }).click();
    await expect(dialog).toContainText('Use an https, ssh or user@host:path git URL');
    await dialog.getByRole('button', { name: 'Cancel' }).click();
    // The fixture repository is on the daemon's own disk: it is registered there
    // (AddRepo, as the desktop's folder picker does), and Studio lists it.
    await agentd.call('/loams.agentd.v1.ProjectService/AddRepo', { path: agentd.repo });
    await page.reload();
    // The reload drops the pasted token (memory only, T7-11): paste it again.
    await expect(page.getByText('Connect an agentd host')).toBeVisible({ timeout: 30_000 });
    await go(page, '/studio/settings');
    const row = page.getByRole('list', { name: 'Registered hosts' }).getByRole('listitem', {
      name: 'E2E host',
    });
    await row.getByLabel('Access token for E2E host').fill(agentd.token ?? '');
    await row.getByRole('button', { name: 'Set token' }).click();
    await go(page, '/studio');
    const projects = page.getByRole('navigation', { name: 'Projects' });
    await expect(projects.getByRole('link', { name: /^fixture/ })).toBeVisible();
    await page
      .getByRole('region', { name: 'Projects' })
      .getByRole('link', { name: /fixture/ })
      .click();
    await expect.poll(() => hashOf(page)).toBe(projectHref(agentd.repo));
    await expect(page.getByRole('heading', { name: 'fixture', level: 1 })).toBeVisible();
  });

  test('mode_switch_keeps_studio', async () => {
    // A draft typed in Studio survives a trip to Console and back (Tasks 3, 19).
    const box = page.getByRole('textbox', { name: 'New thread prompt' });
    await box.click();
    await page.keyboard.type('draft that survives');
    await modes(page).console.click();
    await expect.poll(() => hashOf(page)).toBe('/');
    await expect(modes(page).console).toHaveAttribute('aria-pressed', 'true');
    await modes(page).studio.click();
    await expect.poll(() => hashOf(page)).toBe('/studio');
    await go(page, projectHref(agentd.repo));
    await expect(page.getByRole('textbox', { name: 'New thread prompt' })).toContainText(
      'draft that survives',
    );
    // Clear the draft for the next scenario.
    await page.getByRole('textbox', { name: 'New thread prompt' }).click();
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.press('Backspace');
  });

  test('thread_runs_and_shows_diff', async () => {
    // The thread's mode is Settings › Studio's choice, not the daemon's default: full
    // access here, against a remote daemon whose default is supervised.
    await chooseApprovalMode(page, 'full-access');
    threadId = await startThread(page, agentd.repo, 'Please edit the notes');
    expect(threadId).not.toBe('');
    await expect(timeline(page)).toContainText('Please edit the notes');
    // The diff is the shell inspector's Diff tab (design 59).
    await openInspectorTab(page, 'Diff');
    await expectMockEdit(page, 1);
    // The header names the Mock and the thread's approval mode, as Settings › Studio chose.
    await expect(page.getByRole('main')).toContainText(/Mock\s*full-access/);
    // The diff is the thread's worktree, not the project's checkout.
    expect(agentd.git(['status', '--porcelain'])).toBe('');
  });

  test('comment_and_send_review_to_agent', async () => {
    await commentOnDiff(page, 'notes.txt', 'Please add another line');
    const send = page.getByRole('button', { name: 'Send 1 comment to the agent' });
    await expect(send).toBeVisible();
    await send.click();
    // The review becomes the thread's next turn: the Mock edits again.
    await expectMockEdit(page, 2);
    await expect(timeline(page)).toContainText('Please add another line');
  });

  test('restore_turn', async () => {
    const restore = timeline(page).getByRole('button', { name: 'Restore to before this turn' });
    await expect(restore).toHaveCount(2);
    await restore.last().click();
    const confirm = page.getByRole('alertdialog', { name: 'Restore to before this turn?' });
    await confirm.getByRole('button', { name: 'Restore' }).click();
    await expect(page.getByText(/Restored the checkout to [0-9a-f]{12}/)).toBeVisible();
    // Back to the first turn's edit only.
    await expect(diffOf(page, 'notes.txt')).toContainText('mock edit 1');
    await expect(diffOf(page, 'notes.txt')).not.toContainText('mock edit 2');
  });

  test('commit_from_toolbar', async () => {
    const toolbar = page.getByRole('toolbar', { name: 'Branch and git' });
    await toolbar.getByRole('button', { name: 'Git action options' }).click();
    await page.getByRole('menuitem', { name: /^Commit$/ }).click();
    const dialog = page.getByRole('dialog', { name: 'Commit changes' });
    await expect(dialog).toContainText('notes.txt');
    await dialog.getByLabel('Commit message').fill('studio e2e: the mock edit');
    await dialog.getByRole('button', { name: 'Commit', exact: true }).click();
    await expect(dialog).toBeHidden();
    // The commit is on the thread's branch, signed off, and the worktree is clean.
    await expect
      .poll(() => agentd.git(['log', '--all', '--format=%s%n%b']))
      .toContain('studio e2e: the mock edit');
    expect(agentd.git(['log', '--all', '--format=%b', '-1', '--grep=studio e2e'])).toContain(
      'Signed-off-by: Studio E2E',
    );
    await expect(diffOf(page, 'notes.txt')).toBeHidden();
  });

  test('files_tab_reads_the_checkout', async () => {
    // The inspector's tab strip is the shell's: it switches tabs without a `?panel=` link.
    await openInspectorTab(page, 'Files');
    const panel = inspector(page);
    await panel.getByText('README.md', { exact: true }).click();
    await expect(panel).toContainText('A project for Studio end-to-end tests.');
  });

  test('terminal_round_trip', async () => {
    // The terminal is the shell drawer's tab (design 59).
    await page.getByRole('button', { name: 'Show drawer' }).click();
    const terminal = page.locator('.xterm').first();
    await expect(terminal).toBeVisible({ timeout: 30_000 });
    await terminal.click();
    await page.keyboard.type('echo studio-$((40 + 2))');
    await page.keyboard.press('Enter');
    await expect(page.locator('.xterm-rows').first()).toContainText('studio-42', {
      timeout: 30_000,
    });
  });

  test('palette_opens_a_thread', async () => {
    await go(page, '/studio');
    await page.keyboard.press('ControlOrMeta+K');
    const palette = page.getByRole('dialog', { name: 'Command palette' });
    await expect(palette).toBeVisible();
    await palette.getByRole('combobox').fill('Please edit');
    await palette
      .getByRole('option', { name: /Please edit the notes/ })
      .first()
      .click();
    await expect.poll(() => hashOf(page)).toContain(`/studio/t/${threadId}`);
  });

  test('notifications_lead_to_the_thread', async () => {
    await go(page, '/studio');
    const bell = page.getByRole('button', { name: /^Notifications/ });
    await expect(bell).toBeVisible();
    await bell.click();
    const list = page.getByRole('list', { name: 'Notifications' });
    await expect(list.getByRole('listitem').first()).toBeVisible();
    await list
      .getByRole('listitem')
      .filter({ hasText: /Please edit the notes/ })
      .first()
      .click();
    await expect.poll(() => hashOf(page)).toContain(`/studio/t/${threadId}`);
  });

  test('run_group_of_three_compare_and_merge', async () => {
    await go(page, projectHref(agentd.repo));
    const box = page.getByRole('textbox', { name: 'New thread prompt' });
    await box.click();
    await page.keyboard.type('Three ways to edit the notes');
    await page.getByRole('button', { name: 'Run in parallel…' }).click();
    const dialog = page.getByRole('dialog', { name: 'Run in parallel' });
    while ((await dialog.getByLabel(/^Variant \d+ harness$/).count()) < 3)
      await dialog.getByRole('button', { name: 'Add a variant' }).click();
    // The dialog offers a second harness for variant 2; all three run the Mock here.
    for (const n of [1, 2, 3])
      await dialog.getByLabel(`Variant ${n} harness`).selectOption({ label: 'Mock' });
    await dialog.getByRole('button', { name: 'Start 3 runs' }).click();
    await expect.poll(() => hashOf(page), { timeout: 30_000 }).toMatch(/^\/studio\/g\//);
    // Every variant finishes; the comparison lists the file each changed.
    await expect(page.getByText('Ready to resolve')).toBeVisible({ timeout: 90_000 });
    const matrix = page.getByRole('table', { name: 'Files changed by each variant' });
    await expect(matrix)
      .toContainText('notes.txt')
      .catch(async (error) => {
        // What the page and the daemon say, for a failure in CI.
        const groupId = (await hashOf(page)).split('/').pop();
        const compare = page.getByRole('region', { name: 'Compare' });
        console.log('compare section:', await compare.textContent().catch(() => '?'));
        console.log(
          'comparison:',
          JSON.stringify(
            await agentd
              .call('/loams.agentd.v1.RunGroupService/GetRunGroupComparison', { groupId })
              .catch((e) => String(e)),
          ),
        );
        throw error;
      });
    // Resolve: merge the first variant into main, confirmed in one dialog.
    await page.getByRole('button', { name: 'Resolve…' }).click();
    const resolve = page.getByRole('dialog', { name: 'Resolve the run group' });
    await resolve.getByRole('radio', { name: /Merge into the base/ }).check();
    await resolve.getByRole('button', { name: 'Merge into main' }).click();
    await expect(page.getByText(/^Resolved: /).first()).toBeVisible({ timeout: 60_000 });
    expect(agentd.git(['show', 'main:notes.txt'])).toContain('mock edit 1');
  });
});

test.describe('supervised approvals on the web edition', () => {
  let agentd: Agentd;
  let context: BrowserContext;

  test.beforeAll(async ({ baseURL }) => {
    agentd = await startAgentd({
      mode: 'remote',
      allowOrigin: new URL(baseURL ?? 'http://127.0.0.1:5191').origin,
      mockEdit: ['notes.txt'],
      mockApprove: true,
    });
    await agentd.call('/loams.agentd.v1.ProjectService/AddRepo', { path: agentd.repo });
  });

  test.afterAll(async () => {
    await context?.close();
    if (agentd) {
      if (test.info().status !== 'passed') console.log(agentd.log().slice(-4000));
      await agentd.stop();
    }
  });

  test('supervised_approval_blocks_then_runs', async ({ browser }) => {
    context = await browser.newContext();
    const page = await openStudio(context);
    await addHost(page, agentd);
    // Supervised by choice (Settings › Studio, then SetThreadApprovalMode), whatever the
    // daemon's default is.
    await chooseApprovalMode(page, 'supervised');
    await startThread(page, agentd.repo, 'Edit the notes, with my approval');
    await expect(page.getByRole('main')).toContainText(/Mock\s*supervised/);
    // The run waits on the approval (Task 13): a card in the timeline, the edit not made.
    const card = page.getByRole('button', { name: 'Allow once' });
    await expect(card).toBeVisible({ timeout: 60_000 });
    // Nothing is written while the approval waits: the worktree's notes are untouched.
    const notes = () => agentd.worktreeFiles('notes.txt').map((f) => readFileSync(f, 'utf8'));
    await expect.poll(notes).toHaveLength(1);
    await page.waitForTimeout(1_000);
    expect(notes()).toEqual(['first line\n']);
    // Allowed once, it runs: the edit lands.
    await card.click();
    await openInspectorTab(page, 'Diff');
    await expectMockEdit(page, 1);
    await expect(page.getByRole('button', { name: 'Allow once' })).toHaveCount(0);
    expect(notes()).toEqual(['first line\nmock edit 1\n']);
    // The project's own checkout is never touched.
    expect(readFileSync(`${agentd.repo}/notes.txt`, 'utf8')).toBe('first line\n');
  });
});
