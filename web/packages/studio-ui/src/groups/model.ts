// A run group as the group view shows it (SD1 Task 25; design §53 §5.5, D904). T3 Code has
// no parallel-run compare view (inventory (l)), so this is Loams' own code. The views are
// plain data: @loams/plugin-studio maps `loams.agentd.v1`'s RunGroup onto them, so this
// library stays free of the wire types.

export type MemberStatus =
  | "queued"
  | "running"
  | "awaiting-input"
  | "done"
  | "errored"
  | "cancelled";
export type CheckStatus = "none" | "pending" | "running" | "passed" | "failed";
export type GroupState = "running" | "ready" | "resolved";
export type ResolveActionName = "merge" | "pr" | "keep";

export interface MemberCheckView {
  readonly state: CheckStatus;
  readonly exitCode?: number;
  /** The output's last lines, as text. */
  readonly logTail: readonly string[];
}

export interface GroupMemberView {
  readonly chatId: string;
  /** The harness and model, as a person names the variant. */
  readonly label: string;
  readonly status: MemberStatus;
  readonly lastMessage?: string;
  readonly branch: string;
  readonly cwd: string;
  readonly filesChanged: number;
  readonly additions: number;
  readonly deletions: number;
  readonly elapsedMs: number;
  /** When the run started, for a live elapsed time. */
  readonly startedAtMs?: number;
  /** Unset until the harness reports usage (T12-10). */
  readonly tokens?: number;
  readonly check: MemberCheckView;
  /** Resolved with `keep`: an ordinary thread now. */
  readonly detached: boolean;
  readonly archived: boolean;
  readonly pendingApprovals: number;
}

export interface GroupResolutionView {
  readonly action: ResolveActionName;
  readonly winnerChatId: string;
  readonly result: string;
  readonly sha?: string;
  readonly url?: string;
}

export interface GroupView {
  readonly id: string;
  readonly prompt: string;
  readonly base: string;
  readonly repoPath: string;
  readonly state: GroupState;
  readonly createdAtMs: number;
  readonly hasCheck: boolean;
  readonly members: readonly GroupMemberView[];
  readonly resolution?: GroupResolutionView;
}

/** `GetRunGroupComparison`'s matrix: one path, and the members that changed it. */
export interface ComparisonRowView {
  readonly path: string;
  readonly perMember: readonly { chatId: string; additions: number; deletions: number }[];
}

export interface MatrixCell {
  readonly chatId: string;
  readonly touched: boolean;
  readonly additions: number;
  readonly deletions: number;
  /** 0 untouched; 1–3 small to large, relative to the matrix's largest change. */
  readonly level: 0 | 1 | 2 | 3;
}

export interface MatrixRow {
  readonly path: string;
  readonly cells: readonly MatrixCell[];
}

/** A change's size as one of three steps of the largest; 0 for none. */
export function changeLevel(lines: number, max: number): 0 | 1 | 2 | 3 {
  if (lines <= 0 || max <= 0) return 0;
  const share = lines / max;
  if (share > 2 / 3) return 3;
  if (share > 1 / 3) return 2;
  return 1;
}

/** The matrix with one cell per member, in the members' order. */
export function matrixCells(
  rows: readonly ComparisonRowView[],
  memberIds: readonly string[],
): MatrixRow[] {
  let max = 0;
  for (const row of rows) {
    for (const d of row.perMember) max = Math.max(max, d.additions + d.deletions);
  }
  return rows.map((row) => ({
    path: row.path,
    cells: memberIds.map((chatId) => {
      const d = row.perMember.find((m) => m.chatId === chatId);
      return {
        chatId,
        touched: d !== undefined,
        additions: d?.additions ?? 0,
        deletions: d?.deletions ?? 0,
        level: d ? (Math.max(1, changeLevel(d.additions + d.deletions, max)) as 1 | 2 | 3) : 0,
      };
    }),
  }));
}

export function isLive(status: MemberStatus): boolean {
  return status === "queued" || status === "running" || status === "awaiting-input";
}

/** The running time: on from the start while the run goes, else the daemon's figure. */
export function memberElapsedMs(member: GroupMemberView, nowMs: number): number {
  if (
    (member.status === "running" || member.status === "awaiting-input") &&
    member.startedAtMs !== undefined
  ) {
    return Math.max(member.elapsedMs, nowMs - member.startedAtMs);
  }
  return member.elapsedMs;
}

const pad = (n: number) => String(n).padStart(2, "0");

export function formatElapsed(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${pad(s % 60)}s`;
  return `${Math.floor(m / 60)}h ${pad(m % 60)}m`;
}

/** A value as JSON with its object keys sorted, so equal arguments compare equal. */
export function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stableJson(v)}`).join(",")}}`;
  }
  return JSON.stringify(value ?? null);
}

export interface PendingApprovalView {
  readonly id: string;
  readonly chatId: string;
  readonly tool: string;
  readonly args: unknown;
}

/** One tool call pending in several members: "Approve for all variants" answers each. */
export interface SharedApproval {
  readonly key: string;
  readonly tool: string;
  readonly argsText: string;
  readonly approvals: readonly { id: string; chatId: string }[];
}

/**
 * The members' pending approvals that share a tool and arguments with another member's
 * (§53 §8.1). Only the group's members count; one per member (its oldest).
 */
export function sharedApprovals(
  pending: readonly PendingApprovalView[],
  memberIds: readonly string[],
): SharedApproval[] {
  const members = new Set(memberIds);
  const byKey = new Map<
    string,
    { tool: string; argsText: string; approvals: { id: string; chatId: string }[] }
  >();
  for (const approval of pending) {
    if (!members.has(approval.chatId)) continue;
    const argsText = stableJson(approval.args);
    const key = `${approval.tool}\u0000${argsText}`;
    const entry = byKey.get(key) ?? { tool: approval.tool, argsText, approvals: [] };
    if (!entry.approvals.some((a) => a.chatId === approval.chatId)) {
      entry.approvals.push({ id: approval.id, chatId: approval.chatId });
    }
    byKey.set(key, entry);
  }
  return [...byKey.entries()]
    .filter(([, entry]) => entry.approvals.length > 1)
    .map(([key, entry]) => ({ key, ...entry }));
}

/** A fence longer than any run of backticks in `text`, so quoted text cannot close it. */
export function fenceFor(text: string): string {
  let longest = 0;
  for (const run of text.match(/`+/g) ?? []) longest = Math.max(longest, run.length);
  return "`".repeat(Math.max(3, longest + 1));
}

/** The judge's prompt is capped; each member's diff gets a share. */
export const JUDGE_PROMPT_DIFF_BUDGET = 60_000;

/**
 * The judge thread's prompt (Q811, T12-11): the task, each variant, the diff matrix and each
 * variant's diff from the base, quoted as data. The judge is an ordinary read-only thread:
 * it reads and recommends; a person resolves.
 */
export function judgePrompt(input: {
  prompt: string;
  base: string;
  members: readonly GroupMemberView[];
  matrix: readonly ComparisonRowView[];
  diffs: Readonly<Record<string, string>>;
}): string {
  const { members } = input;
  const share = Math.floor(JUDGE_PROMPT_DIFF_BUDGET / Math.max(1, members.length));
  const lines: string[] = [
    "You are judging a parallel run: several agents were given the same task, each in its own worktree from the same base. Compare their results and recommend which one to keep, and why. Point out bugs, missing tests and risky changes in each.",
    "",
    "You are read-only: do not change any file, run no command that writes, and do not try to merge anything. A person resolves the group.",
    "",
    `The task (base ${input.base}):`,
  ];
  const task = fenceFor(input.prompt);
  lines.push(task, input.prompt, task, "", "The variants:");
  for (const m of members) {
    const check =
      m.check.state === "none"
        ? "no check"
        : `check ${m.check.state}${m.check.exitCode !== undefined ? ` (exit ${m.check.exitCode})` : ""}`;
    lines.push(
      `- ${m.label} [${m.chatId}]: ${m.status}, ${m.filesChanged} files, +${m.additions} −${m.deletions}, ${check}, branch ${m.branch}`,
    );
  }
  lines.push("", "Files changed by each variant (+added −removed; blank: not changed):");
  for (const row of input.matrix) {
    const cells = members.map((m) => {
      const d = row.perMember.find((p) => p.chatId === m.chatId);
      return d ? `${m.label} +${d.additions} −${d.deletions}` : null;
    });
    lines.push(`- ${row.path}: ${cells.filter(Boolean).join("; ")}`);
  }
  for (const m of members) {
    const diff = input.diffs[m.chatId] ?? "";
    const cut =
      diff.length > share ? `${diff.slice(0, share)}\n[… cut at ${share} characters]` : diff;
    const fence = fenceFor(cut);
    lines.push(
      "",
      `The diff of ${m.label} [${m.chatId}] from the base:`,
      `${fence}diff`,
      cut || "(no changes)",
      fence,
    );
  }
  return lines.join("\n");
}
