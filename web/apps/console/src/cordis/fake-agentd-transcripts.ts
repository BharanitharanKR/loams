// DEV ONLY: the demo threads' transcripts for the fake agentd (fake-agentd.ts), so a thread
// opened in the `?desktop` preview shows a conversation instead of "could not be loaded".
// `ThreadService.WatchTranscript` sends a reset (the turn so far), then a few deltas a
// moment apart, as the daemon does: the person's follow-up, the agent's reply with a tool
// call, and the call's result. A thread with a pending approval (`pending`) stops at the
// call, which waits in `ApprovalService.WatchApprovals`.

import type { ServiceImpl } from '@connectrpc/connect';
import { MessageRole, MessageStatus, type ThreadService } from '@loams/proto/agentd';

// The init shapes the router takes, read off the service (no @bufbuild/protobuf import).
type Frame =
  ReturnType<ServiceImpl<typeof ThreadService>['watchTranscript']> extends AsyncIterable<infer F>
    ? F
    : never;
type Reset = Extract<NonNullable<NonNullable<Frame['update']>['frame']>, { case: 'reset' }>;
type Entry = NonNullable<Reset['value']['entries']>[number];

const exec = (command: string) => ({ case: 'exec' as const, value: { command } });
const read = (path: string) => ({ case: 'readFile' as const, value: { path } });
const write = (path: string) => ({ case: 'writeFile' as const, value: { path } });
const search = (pattern: string, path?: string) => ({
  case: 'search' as const,
  value: { pattern, ...(path ? { path } : {}) },
});
const edit = (path: string, oldString: string, newString: string) => ({
  case: 'editFile' as const,
  value: { path, oldString, newString },
});
type Call =
  | ReturnType<typeof exec>
  | ReturnType<typeof read>
  | ReturnType<typeof write>
  | ReturnType<typeof search>
  | ReturnType<typeof edit>;

/** One demo thread's conversation. */
export interface DemoScript {
  prompt: string;
  reasoning: string;
  look: { call: Call; output: string };
  reply: string;
  followUp: string;
  /** What the agent says before its second tool call. */
  intro: string;
  act: { call: Call; output: string };
  /** The last words; absent with `pending` (the call waits for an approval). */
  done?: string;
  /** The second call waits for an approval: `ApprovalService` lists it for this thread. */
  pending?: { tool: string; command: string };
  /** The turn failed (the errored thread). */
  error?: string;
}

export const DEMO_SCRIPTS: Record<string, DemoScript> = {
  'demo-login': {
    prompt:
      'Signing in with an expired session loops between /login and /callback. Find the cause and fix it.',
    reasoning:
      'The loop means the callback sends the person back to /login. The callback probably reads the old session cookie before it is replaced.',
    look: {
      call: search('redirect\\(', 'web/apps/console/src/hosted'),
      output:
        'web/apps/console/src/hosted/auth.ts:88:    return redirect(loginUrl(next));\nweb/apps/console/src/hosted/auth.ts:131:  if (expired(session)) return redirect(loginUrl(location));',
    },
    reply:
      "Found it. `auth.ts:131` checks the **old** session before the callback stores the new one, so an expired cookie sends you back to `/login`, which starts the flow again. I'll store the new session first and only then check it.",
    followUp: 'Sounds right. Make the change and run the auth tests.',
    intro: 'Storing the new session before the expiry check, then running the auth tests.',
    act: {
      call: exec('pnpm --filter @loams/console test -- auth'),
      output: '',
    },
    pending: { tool: 'Bash', command: 'pnpm --filter @loams/console test -- auth' },
  },
  'demo-build': {
    prompt: 'The console build takes 48 seconds on CI. Make it faster without dropping checks.',
    reasoning:
      'Measure first: the time is probably in type checking or in the connector catalog step, not in Vite itself.',
    look: {
      call: exec('pnpm --filter @loams/console build --profile'),
      output: 'catalog  14.2s\ntsc       21.7s\nvite      11.9s\ntotal     47.8s',
    },
    reply:
      'Most of the time is `tsc` (21.7s) and the catalog step (14.2s), which runs again even when `connectors.json` is current. Vite itself is 11.9s.',
    followUp: 'Skip the catalog when it is current, and keep tsc.',
    intro: 'Making the catalog step check the input hash before it rebuilds.',
    act: {
      call: edit(
        'apps/desktop-electron/scripts/connectors-catalog.mjs',
        'await build(catalog);',
        'if (!(await upToDate(catalog))) await build(catalog);',
      ),
      output: 'Edited apps/desktop-electron/scripts/connectors-catalog.mjs (+6 -1)',
    },
    done: 'The build is now 33.4s on a warm cache (catalog 0.3s). `tsc` still runs on every build.',
  },
  'demo-readme': {
    prompt: 'Draft a README for Studio: what it is, how to run it, and where its code lives.',
    reasoning: 'Read the plugin entry and the design doc first so the README matches what ships.',
    look: {
      call: read('web/plugins/studio/src/index.tsx'),
      output:
        "// Loams Studio (design §53): the projects, threads and run groups of loams-agentd…\nconst plugin: PluginModule = {\n  name: 'studio',\n  …",
    },
    reply:
      'Studio is the console plugin that drives `loams-agentd`: projects, threads, run groups, and the diff, files, terminal and preview panels. I have enough for a first draft.',
    followUp: 'Keep it short, and link the design doc instead of repeating it.',
    intro: 'Writing `web/plugins/studio/README.md`.',
    act: {
      call: write('web/plugins/studio/README.md'),
      output: 'Wrote web/plugins/studio/README.md (42 lines)',
    },
    done: 'The draft is in `web/plugins/studio/README.md`: three sections, 42 lines, linking design §53 for the details.',
  },
  'demo-r1': {
    prompt:
      'Fix the flaky studio e2e test. It fails about one run in ten on CI, waiting for the composer.',
    reasoning:
      'A fixed `waitForTimeout(2000)` before typing: on a slow runner the composer is not editable yet.',
    look: {
      call: read('web/apps/console/test/e2e/studio.spec.ts'),
      output:
        "    await page.goto('/#/studio');\n    await page.waitForTimeout(2000);\n    await page.getByRole('textbox').fill('hello');",
    },
    reply:
      'The test sleeps 2 seconds and hopes the composer is ready. It should wait for the composer instead.',
    followUp: 'Wait for it, then repeat the test 50 times to be sure.',
    intro: 'Replacing the sleep with a wait for an editable composer, then repeating the spec.',
    act: {
      call: exec(
        'pnpm --filter @loams/console exec playwright test studio.spec.ts --repeat-each=50',
      ),
      output: 'Running 50 tests using 4 workers\n  50 passed (2.1m)',
    },
    done: 'The test waited on a fixed timeout. It now waits for the composer to be ready, and passes 50 runs in a row.',
  },
  'demo-r2': {
    prompt:
      'Fix the flaky studio e2e test. It fails about one run in ten on CI, waiting for the composer.',
    reasoning: 'Check the Playwright config too: the retry and timeout settings may hide the race.',
    look: {
      call: read('web/apps/console/playwright.config.ts'),
      output: "  retries: process.env.CI ? 0 : 0,\n  timeout: 30_000,\n  use: { trace: 'off' },",
    },
    reply:
      'The spec sleeps instead of waiting, and CI has no retries or traces, so the failure leaves nothing to look at. I changed the spec to wait for the composer and turned traces on for the first retry.',
    followUp: 'Good. Check it on the full suite.',
    intro: 'I want to run the e2e suite to check the retry. May I?',
    act: {
      call: exec('pnpm --filter @loams/console exec playwright test studio.spec.ts'),
      output: '',
    },
    pending: {
      tool: 'Bash',
      command: 'pnpm --filter @loams/console exec playwright test studio.spec.ts',
    },
  },
  'demo-r3': {
    prompt:
      'Fix the flaky studio e2e test. It fails about one run in ten on CI, waiting for the composer.',
    reasoning: 'Start from the config and the spec, then reproduce with repeats.',
    look: {
      call: search('waitForTimeout', 'web/apps/console/test/e2e'),
      output: 'web/apps/console/test/e2e/studio.spec.ts:12:    await page.waitForTimeout(2000);',
    },
    reply: 'One fixed sleep in `studio.spec.ts`. Reading playwright.config.ts and the studio spec…',
    followUp: 'Try it with repeats once it is changed.',
    intro: 'Running the changed spec with repeats.',
    act: {
      call: exec('pnpm --filter @loams/console exec playwright test studio.spec.ts'),
      output: '',
    },
    pending: {
      tool: 'Bash',
      command: 'pnpm --filter @loams/console exec playwright test studio.spec.ts',
    },
  },
  'demo-hero': {
    prompt: 'Build a new hero section for the landing page: headline, one line, and two buttons.',
    reasoning:
      'Look at the current page and its tokens so the hero uses the same spacing and type.',
    look: {
      call: read('src/pages/index.astro'),
      output: '<Layout title="Acme">\n  <Header />\n  <Features />\n  <Footer />\n</Layout>',
    },
    reply:
      'There is no hero yet. I will add `Hero.astro` above `Features`, on the existing type scale.',
    followUp: 'Use "Start free" and "Book a demo" for the buttons.',
    intro: 'Adding the component and checking the build.',
    act: {
      call: exec('pnpm build'),
      output: '✓ 14 pages built in 3.21s',
    },
    done: 'The hero is in `src/components/Hero.astro` with "Start free" and "Book a demo"; the site builds.',
  },
  'demo-forms': {
    prompt:
      'Validate the contact form: required name and email, and a message of at least 20 characters.',
    reasoning:
      'The form posts straight to the API; validation belongs in the form and again on the server.',
    look: {
      call: read('src/components/ContactForm.tsx'),
      output:
        '<form action="/api/contact" method="post">\n  <input name="name" />\n  <input name="email" />',
    },
    reply: 'No validation on either side. I will add it to the form and to `/api/contact`.',
    followUp: 'Go ahead, and run the form tests.',
    intro: 'Adding the rules, then running the tests.',
    act: {
      call: exec('pnpm test contact'),
      output: 'FAIL  src/api/contact.test.ts\n  ✗ rejects a short message (expected 400, got 500)',
    },
    error: 'The turn stopped: the harness exited with code 1.',
  },
};

function text(id: string, value: string) {
  return { part: { case: 'text' as const, value: { id, text: value } } };
}

function tool(id: string, call: Call, output: string | undefined) {
  return {
    part: {
      case: 'tool' as const,
      value: {
        id,
        call: { call },
        resolved: output !== undefined,
        ...(output !== undefined ? { output } : {}),
      },
    },
  };
}

/** The frames one watch sends: a reset, then the deltas in order. */
export function transcriptFrames(chatId: string, nowMs: number = Date.now()): Frame[] {
  const s = DEMO_SCRIPTS[chatId];
  if (!s) return [{ update: { frame: { case: 'reset', value: { entries: [] } } } }];
  const at = (minutesAgo: number) => BigInt(nowMs - minutesAgo * 60_000);
  const id = (n: string) => `${chatId}-${n}`;
  const u1: Entry = {
    id: id('u1'),
    role: MessageRole.USER,
    createdAtMs: at(12),
    parts: [text('p1', s.prompt)],
  };
  const a1: Entry = {
    id: id('a1'),
    role: MessageRole.ASSISTANT,
    status: MessageStatus.COMPLETE,
    createdAtMs: at(11),
    durationMs: 41_000n,
    parts: [
      { part: { case: 'reasoning', value: { id: 'r1', text: s.reasoning } } },
      tool('t1', s.look.call, s.look.output),
      text('p2', s.reply),
    ],
  };
  const u2: Entry = {
    id: id('u2'),
    role: MessageRole.USER,
    createdAtMs: at(1),
    parts: [text('p1', s.followUp)],
  };
  const working: Entry = {
    id: id('a2'),
    role: MessageRole.ASSISTANT,
    status: MessageStatus.STREAMING,
    createdAtMs: at(1),
    parts: [text('p1', s.intro), tool('t2', s.act.call, undefined)],
  };
  const finished: Entry = {
    ...working,
    status: s.error ? MessageStatus.ABORTED : MessageStatus.COMPLETE,
    durationMs: 18_000n,
    parts: [
      text('p1', s.intro),
      tool('t2', s.act.call, s.act.output),
      ...(s.done ? [text('p3', s.done)] : []),
      ...(s.error
        ? [{ part: { case: 'error' as const, value: { id: 'e1', message: s.error } } }]
        : []),
    ],
  };
  const usage = { tokens: 18_400n, window: 200_000n };
  const delta = (after: string, entry: Entry, count: number): Frame => ({
    update: {
      contextUsage: usage,
      frame: { case: 'delta', value: { upsert: [{ after, entry }], count: BigInt(count) } },
    },
  });
  const frames: Frame[] = [
    { update: { contextUsage: usage, frame: { case: 'reset', value: { entries: [u1, a1] } } } },
    delta(id('a1'), u2, 3),
    delta(id('u2'), working, 4),
  ];
  if (!s.pending) frames.push(delta(id('u2'), finished, 4));
  return frames;
}

/** The demo threads whose last tool call waits for an approval, with that call. */
export function pendingDemoApprovals(): {
  chatId: string;
  turnId: string;
  tool: string;
  command: string;
}[] {
  return Object.entries(DEMO_SCRIPTS).flatMap(([chatId, s]) =>
    s.pending ? [{ chatId, turnId: `${chatId}-u2`, ...s.pending }] : [],
  );
}
