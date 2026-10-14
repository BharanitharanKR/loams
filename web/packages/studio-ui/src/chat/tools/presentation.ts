// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// packages/client-runtime/src/work-log/presentation.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the tool presentation rules (action per call, present and past tense labels, group summaries of at most two actions plus a remainder, the live thought line) retargeted from T3's turn items to agentd's `ToolCall` kinds; no MCP-app, pull-request, image or source tables. Loams' changes are Apache-2.0.

// How a tool call reads in Studio's timeline (SD1 Task 18, inventory (b)): each agentd
// `ToolCall` kind maps to an action, a verb in two tenses and a one-line detail. A run
// of calls collapses into one summary line ("Ran 2 commands and read 3 files").

import type { MessagePart, ToolCall } from "@loams/agentd-client";
import { commandDisplayText } from "./commandLabel";

export type ToolPart = Extract<MessagePart, { kind: "tool" }>;

export type ToolGroupAction =
  | "read"
  | "edit"
  | "command"
  | "code-search"
  | "search"
  | "fetch"
  | "plan"
  | "delegate"
  | "other";

export type ToolLifecycleStatus = "inProgress" | "completed" | "failed";

/** One call as the timeline shows it. */
export interface ToolPresentation {
  action: ToolGroupAction;
  /** The verb for the call's status: "Running", "Ran", "Failed to run". */
  label: string;
  /** What it acted on: the command, the path, the pattern. Always plain text. */
  detail: string;
  status: ToolLifecycleStatus;
}

export function normalizeCompactToolLabel(value: string): string {
  return value.replace(/\s+(?:complete|completed)\s*$/i, "").trim();
}

export function toolLifecycleStatus(part: ToolPart): ToolLifecycleStatus {
  if (part.isError) return "failed";
  if (part.subagentStatus === "running") return "inProgress";
  if (part.subagentStatus === "failed") return "failed";
  return part.resolved ? "completed" : "inProgress";
}

/** [action, running, completed, base verb for "Failed to …"] */
type Verbs = readonly [ToolGroupAction, string, string, string];

function verbs(call: ToolCall, delegate: boolean): Verbs {
  if (delegate) return ["delegate", "Delegating", "Delegated", "delegate"];
  switch (call.kind) {
    case "exec":
      return ["command", "Running", "Ran", "run"];
    case "readFile":
      return ["read", "Reading", "Read", "read"];
    case "writeFile":
      return ["edit", "Writing", "Wrote", "write"];
    case "editFile":
      return ["edit", "Editing", "Edited", "edit"];
    case "applyPatch":
      return ["edit", "Applying a patch to", "Patched", "patch"];
    case "search":
      return ["code-search", "Searching for", "Searched for", "search for"];
    case "glob":
      return ["code-search", "Finding", "Found", "find"];
    case "webFetch":
      return ["fetch", "Fetching", "Fetched", "fetch"];
    case "webSearch":
      return ["search", "Searching the web for", "Searched the web for", "search the web for"];
    case "todo":
      return ["plan", "Updating the plan", "Updated the plan", "update the plan"];
    case "mcp":
      return ["other", "Calling", "Called", "call"];
    case "unknown":
      return ["other", "Using", "Used", "use"];
  }
}

function todoDetail(items: ReadonlyArray<{ done: boolean }>): string {
  const done = items.filter((item) => item.done).length;
  return `${done}/${items.length} done`;
}

/** The one-line detail of a call. Never markup: the row renders it as text. */
export function toolCallDetail(call: ToolCall): string {
  switch (call.kind) {
    case "exec":
      return commandDisplayText(call.command);
    case "readFile":
    case "writeFile":
    case "editFile":
      return call.path;
    case "applyPatch":
      return call.path ?? "files";
    case "search":
      return call.path ? `${call.pattern} in ${call.path}` : call.pattern;
    case "glob":
      return call.pattern;
    case "webFetch":
      return call.url;
    case "webSearch":
      return call.query;
    case "todo":
      return todoDetail(call.items);
    case "mcp":
      return `${call.server}.${call.tool}`;
    case "unknown":
      return call.name;
  }
}

/** The action a call counts under in a group summary. */
export function toolGroupAction(part: ToolPart): ToolGroupAction {
  return verbs(part.call, Boolean(part.subagentRef))[0];
}

/** Resolves a call's label, detail and status in one place for every row. */
export function resolveToolPresentation(part: ToolPart): ToolPresentation {
  const [action, running, completed, base] = verbs(part.call, Boolean(part.subagentRef));
  const status = toolLifecycleStatus(part);
  const label =
    status === "inProgress" ? running : status === "failed" ? `Failed to ${base}` : completed;
  return { action, label, detail: toolCallDetail(part.call), status };
}

/** The files a call changed, from its per-file stats (or its path for an edit). */
export function toolChangedFiles(part: ToolPart): string[] {
  if (part.diffStats && part.diffStats.length > 0) return part.diffStats.map((s) => s.path);
  const call = part.call;
  if (call.kind === "writeFile" || call.kind === "editFile") return [call.path];
  if (call.kind === "applyPatch" && call.path) return [call.path];
  return [];
}

function toolGroupActionCount(action: ToolGroupAction, parts: ReadonlyArray<ToolPart>): number {
  if (action !== "edit") return parts.length;
  const changedFiles = new Set<string>();
  let editsWithoutFileDetails = 0;
  for (const part of parts) {
    const files = toolChangedFiles(part);
    if (files.length === 0) editsWithoutFileDetails += 1;
    else for (const file of files) changedFiles.add(file);
  }
  return changedFiles.size + editsWithoutFileDetails;
}

function toolGroupActionLabel(action: ToolGroupAction, count: number): string {
  switch (action) {
    case "read":
      return `Read ${count} ${count === 1 ? "file" : "files"}`;
    case "edit":
      return `Changed ${count} ${count === 1 ? "file" : "files"}`;
    case "command":
      return `Ran ${count} ${count === 1 ? "command" : "commands"}`;
    case "search":
      return `Searched the web ${count} ${count === 1 ? "time" : "times"}`;
    case "code-search":
      return `Searched code ${count} ${count === 1 ? "time" : "times"}`;
    case "fetch":
      return `Fetched ${count} ${count === 1 ? "page" : "pages"}`;
    case "plan":
      return count === 1 ? "Updated the plan" : `Updated the plan ${count} times`;
    case "delegate":
      return `Delegated ${count} ${count === 1 ? "task" : "tasks"}`;
    case "other":
      return `Used ${count} ${count === 1 ? "tool" : "tools"}`;
  }
}

function summaryActionPriority(action: ToolGroupAction): number {
  switch (action) {
    case "command":
    case "edit":
    case "delegate":
      return 0;
    case "other":
    case "plan":
      return 2;
    default:
      return 1;
  }
}

/** Summarizes at most two action categories; every omitted call still counts in the remainder. */
export function summarizeToolGroup(parts: ReadonlyArray<ToolPart>): {
  summary: string;
  hasFailure: boolean;
} {
  const groups = new Map<ToolGroupAction, ToolPart[]>();
  for (const part of parts) {
    const action = toolGroupAction(part);
    const group = groups.get(action);
    if (group) group.push(part);
    else groups.set(action, [part]);
  }
  const summaries = [...groups].map(([action, entries], index) => ({
    index,
    count: entries.length,
    priority: summaryActionPriority(action),
    label: toolGroupActionLabel(action, toolGroupActionCount(action, entries)),
    failedCount: entries.filter((part) => toolLifecycleStatus(part) === "failed").length,
  }));
  const selected = [...summaries]
    .sort((a, b) => a.priority - b.priority || a.index - b.index)
    .slice(0, 2)
    .sort((a, b) => a.index - b.index);
  const labels = selected.map(({ label }) => label);
  const remainingCount = parts.length - selected.reduce((count, group) => count + group.count, 0);
  if (remainingCount > 0) {
    labels.push(`Performed ${remainingCount} other ${remainingCount === 1 ? "action" : "actions"}`);
  }
  const sentenceLabels = labels.map((label, index) =>
    index === 0 ? label : label.charAt(0).toLowerCase() + label.slice(1),
  );
  const summary =
    sentenceLabels.length < 3
      ? sentenceLabels.join(" and ")
      : `${sentenceLabels.slice(0, -1).join(", ")}, and ${sentenceLabels.at(-1)}`;
  return { summary, hasFailure: summaries.some((group) => group.failedCount > 0) };
}

/**
 * Plain-text line for the latest thought in the live activity row. A
 * bold-only opening line (the Codex summary heading) wins; otherwise this is
 * the first sentence of the reasoning text.
 */
export function liveThoughtLine(markdown: string): string {
  const heading = /^\s*\*\*([^*\r\n]+)\*\*[ \t]*\r?(?:\n|$)/.exec(markdown)?.[1];
  const text = (heading ?? markdown)
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, "$1")
    .replace(/^[ \t]*(?:#{1,6}|[-*+]|\d+\.)[ \t]+/gm, "")
    .replace(/`+|\*\*|~~/g, "")
    .replace(/(^|[^\w*])[*_]([^*_\n]+)[*_](?![\w*])/g, "$1$2")
    .replace(/\s+/g, " ")
    .trim();
  if (heading !== undefined) return text;
  // Cut after the first . ? or ! (plus a closing quote or paren) that a space follows.
  const end = /[.?!]["'”’)]?(?=\s)/.exec(text);
  return end ? text.slice(0, end.index + end[0].length) : text;
}
