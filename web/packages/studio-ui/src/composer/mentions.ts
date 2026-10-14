// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/composer-editor-mentions.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// assistant citations are not collected (T3 Code citations are not in Studio); contracts types from ./shared/contracts. Loams' changes are Apache-2.0.

import { collectComposerContextReferences } from "./shared/composerContextReferences";
import {
  type ComposerInlineToken,
  collectComposerInlineTokens,
} from "./shared/composerInlineTokens";
import type { AssistantCitation } from "./shared/contracts";

export type ComposerPromptSegment =
  | {
      type: "text";
      text: string;
    }
  | {
      type: "mention";
      path: string;
      source: string;
    }
  | {
      type: "skill";
      name: string;
      source: string;
    }
  | {
      type: "citation";
      citation: AssistantCitation;
      source: string;
    }
  | {
      type: "context-reference";
      kind: string;
      contextId: string;
      label: string;
      source: string;
    };

function rangeIncludesIndex(start: number, end: number, index: number): boolean {
  return start <= index && index < end;
}

function pushTextSegment(segments: ComposerPromptSegment[], text: string): void {
  if (!text) return;
  const last = segments[segments.length - 1];
  if (last && last.type === "text") {
    last.text += text;
    return;
  }
  segments.push({ type: "text", text });
}

function forEachPromptTextSlice(
  prompt: string,
  visitor: (text: string, promptOffset: number) => boolean | void,
): boolean {
  return prompt.length > 0 && visitor(prompt, 0) === true;
}

function forEachMentionMatch(
  prompt: string,
  visitor: (
    match: Extract<ComposerInlineToken, { type: "mention" }>,
    promptOffset: number,
  ) => boolean | void,
): boolean {
  return forEachPromptTextSlice(prompt, (text, promptOffset) => {
    for (const match of collectComposerPromptInlineTokens(text)) {
      if (match.type !== "mention") {
        continue;
      }
      if (visitor(match, promptOffset) === true) {
        return true;
      }
    }
    return false;
  });
}

export function collectComposerPromptInlineTokens(text: string) {
  const tokens = collectComposerInlineTokens(text);
  // Studio has no assistant citations (they quote T3 Code's own threads), so only
  // context references become links here.
  const references = collectComposerContextReferences(text);
  if (references.length === 0) return tokens;

  // An unfinished @ mention can otherwise consume the start of a link label.
  const links = references.map((match) => ({ ...match, type: "context-reference" as const }));
  return [
    ...tokens.filter(
      (token) => !links.some((link) => token.start < link.end && token.end > link.start),
    ),
    ...links,
  ].sort((left, right) => left.start - right.start);
}

function splitPromptTextIntoComposerSegments(text: string): ComposerPromptSegment[] {
  const segments: ComposerPromptSegment[] = [];
  if (!text) {
    return segments;
  }

  const tokenMatches = collectComposerPromptInlineTokens(text);
  let cursor = 0;
  for (const match of tokenMatches) {
    if (match.start < cursor) {
      continue;
    }

    if (match.start > cursor) {
      pushTextSegment(segments, text.slice(cursor, match.start));
    }

    if (match.type === "context-reference") {
      segments.push({
        type: "context-reference",
        kind: match.kind,
        contextId: match.contextId,
        label: match.label,
        source: match.source,
      });
    } else if (match.type === "mention") {
      segments.push({
        type: "mention",
        path: match.value,
        source: match.source,
      });
    } else {
      segments.push({ type: "skill", name: match.value, source: match.source });
    }

    cursor = match.end;
  }

  if (cursor < text.length) {
    pushTextSegment(segments, text.slice(cursor));
  }

  return segments;
}

export function selectionTouchesMentionBoundary(
  prompt: string,
  start: number,
  end: number,
): boolean {
  if (!prompt || start >= end) {
    return false;
  }

  return forEachMentionMatch(prompt, (match, promptOffset) => {
    const mentionStart = promptOffset + match.start;
    const mentionEnd = promptOffset + match.end;
    const beforeMentionIndex = mentionStart - 1;
    const afterMentionIndex = mentionEnd;

    if (
      beforeMentionIndex >= 0 &&
      /\s/.test(prompt[beforeMentionIndex] ?? "") &&
      rangeIncludesIndex(start, end, beforeMentionIndex)
    ) {
      return true;
    }

    if (
      afterMentionIndex < prompt.length &&
      /\s/.test(prompt[afterMentionIndex] ?? "") &&
      rangeIncludesIndex(start, end, afterMentionIndex)
    ) {
      return true;
    }
    return false;
  });
}

export function splitPromptIntoComposerSegments(prompt: string): ComposerPromptSegment[] {
  return splitPromptTextIntoComposerSegments(prompt);
}
