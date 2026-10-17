// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/chat/WorktreeSetupCard.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the contracts snapshot replaced by WorktreeSetupSnapshot (what Studio assembles from loams-agentd's CreateWorktree and its setup action), times in milliseconds; @loams/ui primitives and token classes in place of T3's components, the shimmer and the work-log row; "Work locally" kept, the embedded timeline variant dropped. Loams' changes are Apache-2.0.

// A new worktree's progress (§53 §5.7; SD1 Task 23): creating it, then the project's
// setup action, with the setup output's last lines. Everything shown is text.

import { Button, cx, MiddleTruncate, Spinner } from "@loams/ui";
import {
  Check,
  ChevronDown,
  ChevronRight,
  Circle,
  CircleAlert,
  Laptop,
  Minus,
  Terminal,
  X,
} from "lucide-react";
import { useEffect, useState } from "react";
import { formatGitActionElapsed } from "./actions.logic";

/** Milliseconds as "12s" or "1m 3s". */
const formatDuration = (ms: number) => formatGitActionElapsed(0, ms) ?? "";

export type WorktreeSetupStageStatus =
  | "pending"
  | "running"
  | "done"
  | "failed"
  | "warning"
  | "skipped";

export type WorktreeSetupStageId = "worktree" | "setup-script";

export interface WorktreeSetupStage {
  id: WorktreeSetupStageId;
  status: WorktreeSetupStageStatus;
  /** Milliseconds since the epoch. */
  startedAt?: number | null;
  endedAt?: number | null;
  detail?: string | null;
  /** The setup action's last output lines (setup-script only). */
  tail: readonly string[];
}

export interface WorktreeSetupSnapshot {
  phase: "running" | "done" | "failed" | "cancelled";
  startedAt: number;
  endedAt?: number | null;
  branch?: string | null;
  baseRef?: string | null;
  worktreePath?: string | null;
  setupScript?: { name: string; command?: string } | null;
  error?: string | null;
  stages: readonly WorktreeSetupStage[];
}

export const WORKTREE_SETUP_STAGE_LABEL: Record<WorktreeSetupStageId, string> = {
  worktree: "Create worktree",
  "setup-script": "Run setup action",
};

interface WorktreeSetupCardProps {
  snapshot: WorktreeSetupSnapshot;
  /** Stops the setup. Hidden once the setup has settled. */
  onCancel?: (() => void) | null;
  /** Goes back to working in the project's own checkout instead. */
  onWorkLocally?: (() => void) | null;
  /** Shows the setup action's terminal. Null when no action ran. */
  onOpenTerminal?: (() => void) | null;
}

function stageElapsedMs(stage: WorktreeSetupStage, nowMs: number): number | null {
  if (!stage.startedAt) return null;
  const end = stage.endedAt ?? nowMs;
  return Math.max(0, end - stage.startedAt);
}

/** Ticks once a second while any stage runs, so elapsed labels stay live. */
function useNowWhile(active: boolean): number {
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    if (!active) return;
    const id = setInterval(() => setNowMs(Date.now()), 1_000);
    return () => clearInterval(id);
  }, [active]);
  return nowMs;
}

function StageIcon({ status }: { status: WorktreeSetupStageStatus }) {
  const size = 14;
  switch (status) {
    case "done":
      return <Check size={size} aria-hidden="true" className="shrink-0" />;
    case "running":
      return <Spinner size="sm" label="Running" className="shrink-0" />;
    case "failed":
      return <X size={size} aria-hidden="true" className="shrink-0" />;
    case "warning":
      return <CircleAlert size={size} aria-hidden="true" className="shrink-0" />;
    case "skipped":
      return <Minus size={size} aria-hidden="true" className="shrink-0" />;
    case "pending":
      return <Circle size={size} aria-hidden="true" className="shrink-0" />;
  }
}

function stageRowClassName(status: WorktreeSetupStageStatus): string {
  switch (status) {
    case "failed":
      return "text-danger";
    case "warning":
      return "text-accent-fg";
    case "pending":
      return "text-faint";
    case "running":
    case "skipped":
    case "done":
      return "text-muted";
  }
}

function headerLabel(snapshot: WorktreeSetupSnapshot): string {
  switch (snapshot.phase) {
    case "running":
      return "Setting up worktree…";
    case "done":
      return snapshot.stages.some((stage) => stage.status === "failed")
        ? "Worktree ready, setup action failed"
        : "Worktree ready";
    case "failed":
      return "Worktree setup failed";
    case "cancelled":
      return "Worktree setup cancelled";
  }
}

/** One stage, as a row: icon, label, detail, elapsed time. */
function StageRow({
  stage,
  nowMs,
  scriptName,
}: {
  stage: WorktreeSetupStage;
  nowMs: number;
  scriptName: string | null;
}) {
  const elapsed = stageElapsedMs(stage, nowMs);
  const label =
    stage.id === "setup-script" && scriptName ? scriptName : WORKTREE_SETUP_STAGE_LABEL[stage.id];
  const trailing =
    stage.status === "pending"
      ? null
      : stage.status === "skipped"
        ? (stage.detail ?? "skipped")
        : stage.detail;
  return (
    <div
      className={cx(
        "flex min-w-0 items-center gap-2 py-1 text-[0.84rem]",
        stageRowClassName(stage.status),
      )}
      data-worktree-setup-stage={stage.id}
      data-worktree-setup-status={stage.status}
    >
      <StageIcon status={stage.status} />
      <span className="min-w-0 flex-1 truncate">{label}</span>
      {trailing ? (
        <span className="min-w-0 truncate text-[0.76rem] text-faint tabular-nums">{trailing}</span>
      ) : null}
      {elapsed !== null && stage.status !== "skipped" && stage.status !== "pending" ? (
        <span className="shrink-0 text-[0.76rem] text-faint tabular-nums">
          {formatDuration(elapsed)}
        </span>
      ) : null}
    </div>
  );
}

/** The daemon keeps this many trailing lines; the box is sized for exactly that. */
const OUTPUT_TAIL_LINES = 4;
const OUTPUT_TAIL_SLOTS = Array.from({ length: OUTPUT_TAIL_LINES }, (_, slot) => slot);

/**
 * A fixed-height window onto the action's last lines. Rows never wrap and the box
 * never grows or shrinks, so streaming output cannot push the layout around.
 */
function OutputTail({ lines, failed }: { lines: readonly string[]; failed: boolean }) {
  const rows = OUTPUT_TAIL_SLOTS.map((slot) => ({
    slot,
    line: lines[lines.length - OUTPUT_TAIL_LINES + slot] ?? "",
  }));
  return (
    <pre
      className={cx(
        "m-0 mb-1 ml-6 overflow-hidden border px-2.5 py-1.5 font-mono text-[0.72rem] leading-relaxed",
        failed
          ? "border-danger bg-danger-soft text-danger"
          : "border-rule-soft bg-raised text-muted",
      )}
    >
      {rows.map(({ slot, line }) => (
        <div key={slot} className="truncate whitespace-pre">
          {line.length === 0 ? " " : line}
        </div>
      ))}
    </pre>
  );
}

function SetupDetails({ snapshot }: { snapshot: WorktreeSetupSnapshot }) {
  const rows: [string, string][] = [];
  if (snapshot.branch) rows.push(["Branch", snapshot.branch]);
  if (snapshot.baseRef) rows.push(["Base", snapshot.baseRef]);
  if (snapshot.worktreePath) rows.push(["Path", snapshot.worktreePath]);
  if (snapshot.setupScript?.command) rows.push(["Setup", snapshot.setupScript.command]);
  return (
    <dl className="m-0 mt-1 mb-1.5 ml-6 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-[0.76rem] text-muted">
      {rows.map(([term, value]) => (
        <div key={term} className="contents">
          <dt className="text-ink">{term}</dt>
          <dd className="m-0 min-w-0 font-mono">
            <MiddleTruncate value={value} className="flex" />
          </dd>
        </div>
      ))}
    </dl>
  );
}

export function WorktreeSetupCard({
  snapshot,
  onCancel = null,
  onWorkLocally = null,
  onOpenTerminal = null,
}: WorktreeSetupCardProps) {
  const running = snapshot.phase === "running";
  const nowMs = useNowWhile(running);
  const [detailsOpen, setDetailsOpen] = useState(false);
  const totalElapsed = Math.max(0, (snapshot.endedAt ?? nowMs) - snapshot.startedAt);
  const setupStage = snapshot.stages.find((stage) => stage.id === "setup-script");
  const showTerminal = onOpenTerminal && setupStage && setupStage.status !== "pending";
  const failed = snapshot.phase === "failed";
  const finishedWithFailedStage =
    snapshot.phase === "done" && snapshot.stages.some((stage) => stage.status === "failed");
  // The tail is part of the action's row while it runs, and after it failed, so the
  // last lines explain the failure. An action that printed nothing shows no box.
  const showTail =
    setupStage !== undefined &&
    (setupStage.status === "failed" ||
      (setupStage.status === "running" && setupStage.tail.length > 0));

  return (
    <section
      aria-label="Worktree setup"
      data-worktree-setup-phase={snapshot.phase}
      className="flex flex-col gap-1"
    >
      <div
        className={cx(
          "flex min-w-0 items-baseline gap-2 border-b border-rule-soft pb-2 text-[0.88rem] tabular-nums",
          failed ? "text-danger" : finishedWithFailedStage ? "text-accent-fg" : "text-ink",
        )}
        role="status"
        aria-live="polite"
      >
        <span className="min-w-0 flex-1 truncate">{headerLabel(snapshot)}</span>
        <span className="shrink-0 text-[0.76rem] text-faint">{formatDuration(totalElapsed)}</span>
      </div>
      <div>
        {snapshot.stages.map((stage) => (
          <div key={stage.id}>
            <StageRow stage={stage} nowMs={nowMs} scriptName={snapshot.setupScript?.name ?? null} />
            {stage.id === "setup-script" && showTail ? (
              <OutputTail lines={stage.tail} failed={stage.status === "failed"} />
            ) : null}
          </div>
        ))}
      </div>

      {failed && snapshot.error ? (
        <p className="m-0 ml-6 text-[0.78rem] text-danger">{snapshot.error}</p>
      ) : null}

      {detailsOpen ? <SetupDetails snapshot={snapshot} /> : null}

      <div className="flex flex-wrap items-center gap-1">
        <Button
          size="sm"
          variant="quiet"
          aria-expanded={detailsOpen}
          onClick={() => setDetailsOpen((open) => !open)}
        >
          {detailsOpen ? (
            <ChevronDown size={13} aria-hidden="true" />
          ) : (
            <ChevronRight size={13} aria-hidden="true" />
          )}
          Details
        </Button>
        {showTerminal ? (
          <Button size="sm" variant="quiet" onClick={onOpenTerminal}>
            <Terminal size={13} aria-hidden="true" />
            Open terminal
          </Button>
        ) : null}
        {onWorkLocally ? (
          <Button size="sm" variant="quiet" onClick={onWorkLocally}>
            <Laptop size={13} aria-hidden="true" />
            Work locally
          </Button>
        ) : null}
        {onCancel && running ? (
          <Button size="sm" variant="quiet" onClick={onCancel}>
            <X size={13} aria-hidden="true" />
            Cancel
          </Button>
        ) : null}
      </div>
    </section>
  );
}
