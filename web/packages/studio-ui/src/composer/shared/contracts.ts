// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// packages/contracts/src/composerContext.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the composer context records restated as plain TypeScript types (no Effect Schema), with the skill and citation types the composer reads. Loams' changes are Apache-2.0.

/**
 * Inline context records: the typed payload behind every composer chip.
 * A message's `text` carries position through canonical reference links
 * (`[label](t3-context://v1/<kind>/<contextId>)`, see
 * `./composerContextReferences`); these records carry the payload,
 * keyed by `contextId`. Bytes never live here.
 *
 * Upstream validates these with Effect Schema; Studio builds them itself and
 * only ever projects them into the prompt text, so plain types are enough.
 */

export const COMPOSER_CONTEXT_KINDS = [
  "image",
  "file",
  "terminal",
  "element",
  "preview-annotation",
  "review-comment",
  "mention",
  "skill",
  "thread",
] as const;
export type KnownComposerContextKind = (typeof COMPOSER_CONTEXT_KINDS)[number];

export const COMPOSER_CONTEXT_LABEL_MAX_CHARS = 200;
export const COMPOSER_CONTEXT_TERMINAL_TEXT_MAX_CHARS = 64_000;
export const COMPOSER_CONTEXT_REVIEW_TEXT_MAX_CHARS = 16_000;
export const COMPOSER_CONTEXT_REVIEW_DIFF_MAX_CHARS = 32_000;

/** Durable identity of one payload: `[a-z0-9_-]{1,128}`, case-insensitive. */
export type ComposerContextId = string & { readonly __brand?: "ComposerContextId" };
/** Identity of one occurrence in a document. */
export type ComposerContextReferenceId = string & {
  readonly __brand?: "ComposerContextReferenceId";
};
/** Open kind: `[a-z][a-z0-9-]{0,39}`. */
export type ComposerContextKind = string;

export interface PullRequestContextMetadata {
  number: number;
  title: string;
  url: string;
  headBranch: string;
  baseBranch: string;
  state: "open" | "closed" | "merged";
  isDraft: boolean;
}

interface RecordBase {
  version: 1;
  contextId: ComposerContextId;
  label: string;
}

interface AttachmentBinding {
  attachmentId: string;
  name: string;
  mimeType: string;
  sizeBytes: number;
}

export interface ImageContextRecord extends RecordBase, AttachmentBinding {
  kind: "image";
}

export interface FileContextRecord extends RecordBase, AttachmentBinding {
  kind: "file";
}

export interface TerminalContextRecord extends RecordBase {
  kind: "terminal";
  terminalId: string;
  terminalLabel: string;
  lineStart: number;
  lineEnd: number;
  text: string;
}

export interface ElementContextSource {
  functionName: string | null;
  fileName: string | null;
  lineNumber: number | null;
  columnNumber: number | null;
}

/** What a picked page element looks like to the agent. */
export interface ElementContextDetails {
  pageUrl: string;
  pageTitle: string | null;
  tagName: string;
  selector: string | null;
  htmlPreview: string;
  componentName: string | null;
  source: ElementContextSource | null;
  styles: string;
}

export interface ElementContextRecord extends RecordBase, ElementContextDetails {
  kind: "element";
}

export interface PreviewAnnotationContextRecord extends RecordBase {
  kind: "preview-annotation";
  annotationId: string;
  pageUrl: string;
  pageTitle: string | null;
  comment: string;
  targetSummary: string;
  styleChanges: readonly string[];
  elements?: readonly ElementContextDetails[];
  elementIds?: readonly string[];
  regionCount?: number;
  strokeCount?: number;
  screenshotContextId?: ComposerContextId;
}

export interface ReviewCommentContextRecord extends RecordBase {
  kind: "review-comment";
  sectionId: string;
  sectionTitle: string;
  filePath: string;
  startIndex: number;
  endIndex: number;
  rangeLabel: string;
  text: string;
  diff: string;
  fenceLanguage?: string;
  pullRequest?: PullRequestContextMetadata;
}

export interface MentionContextRecord extends RecordBase {
  kind: "mention";
  path: string;
}

export interface SkillContextRecord extends RecordBase {
  kind: "skill";
  name: string;
}

/** Another thread, attached so the agent can read its history. */
export interface ThreadContextRecord extends RecordBase {
  kind: "thread";
  environmentId: string;
  threadId: string;
  title: string;
}

/** A kind this build does not know; its payload is kept as JSON. */
export interface UnknownContextRecord extends RecordBase {
  kind: ComposerContextKind;
  payload: unknown;
}

export type KnownComposerContextRecord =
  | ImageContextRecord
  | FileContextRecord
  | TerminalContextRecord
  | ElementContextRecord
  | PreviewAnnotationContextRecord
  | ReviewCommentContextRecord
  | MentionContextRecord
  | SkillContextRecord
  | ThreadContextRecord;

export type ComposerContextRecord = KnownComposerContextRecord | UnknownContextRecord;

export const COMPOSER_CONTEXT_MAX_RECORDS = 200;

/** Structured context riding on a user message. */
export interface OrchestrationMessageContext {
  version: 1;
  records: readonly ComposerContextRecord[];
}

/** From packages/contracts/src/server.ts: a provider skill, as the composer reads it. */
export interface ServerProviderSkill {
  name: string;
  description?: string;
  path: string;
  scope?: string;
  enabled: boolean;
  displayName?: string;
  shortDescription?: string;
  userInvocationOnly?: boolean;
  userInvocable?: boolean;
}

/**
 * From packages/contracts/src/assistantCitations.ts: a quoted span of an assistant
 * message. Studio does not insert citations; the type keeps the vendored doc model's
 * segment union whole.
 */
export interface AssistantCitation {
  version: 1;
  environmentId: string;
  threadId: string;
  messageId: string;
  text: string;
  comment?: string;
  start: number;
  end: number;
  prefix: string;
  suffix: string;
}
