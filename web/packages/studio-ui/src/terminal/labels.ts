// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// packages/shared/src/terminalLabels.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// labels numbered from the tab's own label, since loams-agentd chooses terminal ids; the server-summary label dropped. Loams' changes are Apache-2.0.

// Tab labels. Upstream's client chooses `term-N` ids and labels a tab from its id;
// loams-agentd chooses the id (`OpenTerminal`), so a tab keeps its label beside the
// id and the number is the lowest one no open tab of the thread uses.

function terminalNumber(label: string): string | undefined {
  return /^Terminal (\d+)$/.exec(label)?.[1];
}

/** "Terminal N", N the lowest number none of `existingLabels` uses. */
export function nextTerminalLabel(existingLabels: ReadonlyArray<string>): string {
  const usedNumbers = new Set(existingLabels.map(terminalNumber));
  let nextIndex = 1;
  while (usedNumbers.has(String(nextIndex))) {
    nextIndex += 1;
  }
  return `Terminal ${nextIndex}`;
}
