// What the composer sends (SD1 Task 19): the draft's Markdown with its context chips
// projected for the agent. A context chip in the prompt is a reference link
// (`[label](t3-context://v1/<kind>/<id>)`); the vendored projection turns each into
// an in-place marker and puts its payload once in a trailing envelope, escaped, so
// captured text (a terminal line) cannot close the envelope or forge a record.
//
// Terminal selections wait outside the draft (in memory, T22-8) and join the prompt
// only here, as references appended after the text.

import { formatTerminalContextLabel, type TerminalContextSelection } from "../terminal/context";
import { ensureInlineContextReferences, toKindScopedComposerContextId } from "./contextReferences";
import { projectComposerContextForProvider } from "./shared/composerContextReferences";
import type { ComposerContextRecord, TerminalContextRecord } from "./shared/contracts";
import {
  COMPOSER_CONTEXT_LABEL_MAX_CHARS,
  COMPOSER_CONTEXT_TERMINAL_TEXT_MAX_CHARS,
} from "./shared/contracts";

/** A terminal selection as a context record, with a stable id per selection. */
export function terminalContextRecord(
  selection: TerminalContextSelection,
  index: number,
): TerminalContextRecord {
  const contextId = toKindScopedComposerContextId(
    "terminal",
    `${selection.terminalId}-${selection.lineStart}-${selection.lineEnd}-${index}`,
  );
  return {
    version: 1,
    kind: "terminal",
    contextId,
    label: formatTerminalContextLabel(selection).slice(0, COMPOSER_CONTEXT_LABEL_MAX_CHARS),
    terminalId: selection.terminalId,
    terminalLabel: selection.terminalLabel,
    lineStart: selection.lineStart,
    lineEnd: Math.max(selection.lineStart, selection.lineEnd),
    text: selection.text.slice(0, COMPOSER_CONTEXT_TERMINAL_TEXT_MAX_CHARS),
  };
}

/**
 * The prompt the daemon receives: the draft, the terminal selections appended as
 * references, every reference projected with its record.
 */
export function promptForSend(input: {
  prompt: string;
  terminalContexts?: readonly TerminalContextSelection[];
  records?: readonly ComposerContextRecord[];
}): string {
  const terminal = (input.terminalContexts ?? []).map(terminalContextRecord);
  const records = [...(input.records ?? []), ...terminal];
  const text = ensureInlineContextReferences(
    input.prompt.trimEnd(),
    terminal.map((record) => ({
      kind: record.kind,
      contextId: record.contextId,
      label: record.label,
    })),
  );
  return projectComposerContextForProvider({ text, records }).trim();
}

/** Whether there is anything to send: text, an attachment or a selection. */
export function hasSendableContent(input: {
  prompt: string;
  attachments: number;
  terminalContexts: number;
}): boolean {
  return input.prompt.trim().length > 0 || input.attachments > 0 || input.terminalContexts > 0;
}
