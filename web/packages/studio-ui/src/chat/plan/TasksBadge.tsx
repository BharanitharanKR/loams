// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/chat/ComposerTasksBadge.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the tasks summary (current step, done/total, progress segments) and its expandable step list, read from an agentd todo call's items and drawn as a timeline plan card with @loams/ui tokens instead of a composer banner. Loams' changes are Apache-2.0.

import type { TodoItem } from "@loams/agentd-client";
import { cx } from "@loams/ui";
import { CheckIcon, ChevronDown, CircleDotIcon, CircleIcon, ListTodoIcon } from "lucide-react";
import { memo, useId, useState } from "react";

export interface TasksProgress {
  readonly step: string;
  readonly completedSteps: number;
  readonly totalSteps: number;
}

export interface TaskStep {
  readonly step: string;
  readonly status: "pending" | "inProgress" | "completed";
}

const MAX_TASK_SEGMENTS = 10;

const taskStatusLabels = {
  pending: "Pending",
  inProgress: "Next",
  completed: "Completed",
} satisfies Record<TaskStep["status"], string>;

/** A todo call's items as steps: done ones completed, the first open one next. */
export function taskStepsOf(items: ReadonlyArray<TodoItem>): TaskStep[] {
  let next = false;
  return items.map((item) => {
    if (item.done) return { step: item.text, status: "completed" as const };
    if (!next) {
      next = true;
      return { step: item.text, status: "inProgress" as const };
    }
    return { step: item.text, status: "pending" as const };
  });
}

export function tasksProgressOf(steps: ReadonlyArray<TaskStep>): TasksProgress {
  const completedSteps = steps.filter((s) => s.status === "completed").length;
  const current =
    steps.find((s) => s.status === "inProgress") ?? steps.at(-1) ?? ({ step: "" } as TaskStep);
  return { step: current.step, completedSteps, totalSteps: steps.length };
}

function keyedTaskSteps(steps: readonly TaskStep[]) {
  const occurrences = new Map<string, number>();
  return steps.map((step) => {
    const occurrence = occurrences.get(step.step) ?? 0;
    occurrences.set(step.step, occurrence + 1);
    return { key: `${step.step}:${occurrence}`, step };
  });
}

function TaskSegments({ steps }: { readonly steps: readonly TaskStep[] }) {
  if (steps.length <= 1 || steps.length > MAX_TASK_SEGMENTS) return null;
  return (
    <span aria-hidden className="flex w-20 shrink-0 items-center gap-0.5">
      {keyedTaskSteps(steps).map(({ key, step }) => (
        <span
          key={key}
          className={cx(
            "h-[3px] min-w-0 flex-1 rounded-pill",
            step.status === "completed"
              ? "bg-grow"
              : step.status === "inProgress"
                ? "bg-accent"
                : "bg-rule",
          )}
        />
      ))}
    </span>
  );
}

function StepIcon({ status }: { status: TaskStep["status"] }) {
  if (status === "completed") return <CheckIcon className="size-3.5 text-grow" aria-hidden />;
  if (status === "inProgress")
    return <CircleDotIcon className="size-3.5 text-accent-fg" aria-hidden />;
  return <CircleIcon className="size-3.5 text-faint" aria-hidden />;
}

/** The plan card: what the agent is doing next, and the list behind it. */
export const TasksBadge = memo(function TasksBadge({ items }: { items: ReadonlyArray<TodoItem> }) {
  const [expanded, setExpanded] = useState(false);
  const listId = useId();
  const steps = taskStepsOf(items);
  const progress = tasksProgressOf(steps);
  if (progress.totalSteps <= 0) return null;
  const done = progress.completedSteps >= progress.totalSteps;
  return (
    <div className="border border-rule-soft bg-surface" data-plan-card="">
      <button
        type="button"
        aria-expanded={expanded}
        aria-controls={listId}
        onClick={() => setExpanded((v) => !v)}
        className="flex w-full cursor-pointer items-center gap-2 border-0 bg-transparent px-3 py-1.5 text-left text-sm text-ink"
      >
        <ListTodoIcon className="size-3.5 shrink-0 text-muted" aria-hidden />
        <span className="shrink-0 text-muted">Tasks</span>
        <span className="min-w-0 flex-1 truncate font-medium">{progress.step}</span>
        <span
          className={cx("shrink-0 text-xs tabular-nums", done ? "text-grow" : "text-muted")}
          data-tasks-progress=""
        >
          {progress.completedSteps}/{progress.totalSteps}
        </span>
        <TaskSegments steps={steps} />
        <ChevronDown
          className={cx("size-3.5 shrink-0 text-faint", expanded && "rotate-180")}
          aria-hidden
        />
      </button>
      {expanded ? (
        <ol id={listId} className="m-0 list-none border-t border-rule-soft px-3 py-1.5">
          {keyedTaskSteps(steps).map(({ key, step }) => (
            <li key={key} className="flex items-center gap-2 py-0.5 text-sm">
              <StepIcon status={step.status} />
              <span className={cx("min-w-0 flex-1", step.status === "completed" && "text-muted")}>
                {step.step}
              </span>
              <span className="sr-only">{taskStatusLabels[step.status]}</span>
            </li>
          ))}
        </ol>
      ) : null}
    </div>
  );
});
