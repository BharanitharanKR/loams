// Inputs CodeQL's js/polynomial-redos query flagged: each helper keeps its
// output on ordinary text and stays linear on long adversarial strings (the
// previous patterns took seconds on these sizes).
import { describe, expect, it } from 'vitest';
import { shouldCollapseUserMessage } from '../src/chat/timeline.logic';
import { commandDisplayText } from '../src/chat/tools/commandLabel';
import { liveThoughtLine } from '../src/chat/tools/presentation';
import { formatWorktreePathForDisplay } from '../src/thread-list/worktreeCleanup';

const LONG = 200_000;
const BUDGET_MS = 1_000;

function timed(run: () => unknown): number {
  const start = performance.now();
  run();
  return performance.now() - start;
}

describe('linear-time text helpers', () => {
  it('shouldCollapseUserMessage counts link labels, not targets', () => {
    expect(shouldCollapseUserMessage(`[short](https://example.com/${'a'.repeat(700)})`)).toBe(
      false,
    );
    expect(shouldCollapseUserMessage(`[short](<https://example.com/${'a'.repeat(700)}>)`)).toBe(
      false,
    );
    expect(shouldCollapseUserMessage('a'.repeat(601))).toBe(true);
    for (const unit of ['[\\', '[](<', '[](!', '[](']) {
      expect(timed(() => shouldCollapseUserMessage(unit.repeat(LONG / unit.length)))).toBeLessThan(
        BUDGET_MS,
      );
    }
  });

  it('liveThoughtLine strips links and stays linear', () => {
    expect(liveThoughtLine('Read [the docs](https://example.com/x) first. Then more.')).toBe(
      'Read the docs first.',
    );
    for (const unit of ['[\\', '[](', '[](!']) {
      expect(timed(() => liveThoughtLine(unit.repeat(LONG / unit.length)))).toBeLessThan(BUDGET_MS);
    }
  });

  it('commandDisplayText unwraps shell -c and stays linear on long flags', () => {
    expect(commandDisplayText("bash -lc 'ls -la'")).toBe('ls -la');
    expect(commandDisplayText("sh -c 'echo hi'")).toBe('echo hi');
    const flag = `-${'c'.repeat(LONG)}!`;
    expect(timed(() => commandDisplayText(`bash ${flag} 'ls'`))).toBeLessThan(BUDGET_MS);
  });

  it('formatWorktreePathForDisplay trims trailing slashes and stays linear', () => {
    expect(formatWorktreePathForDisplay('/home/me/wt///')).toBe('wt');
    expect(formatWorktreePathForDisplay('///')).toBe('///');
    expect(timed(() => formatWorktreePathForDisplay(`${'/'.repeat(LONG)}x`))).toBeLessThan(
      BUDGET_MS,
    );
  });
});
