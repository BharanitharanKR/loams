// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// an inline timeline card for an agentd `input` part: one question at a time with its progress, option buttons with 1–9 shortcuts (scoped to the card), single-select auto-advance, free text, and Send through `AnswerQuestion`; @loams/ui tokens, no composer banner, collapse or dismiss. Loams' changes are Apache-2.0.

// A question the agent asked (SD1 Task 18; Q802 keeps DD1e's question card's job, here
// drawn from upstream's panel). An answered question shows its state; an open one takes
// answers until it is sent.

import type { UserInputAnswer, UserInputQuestion } from "@loams/agentd-client";
import { Button, cx } from "@loams/ui";
import { CheckIcon, MessageCircleQuestion } from "lucide-react";
import { memo, useCallback, useEffect, useRef, useState } from "react";
import {
  buildPendingUserInputAnswers,
  derivePendingUserInputProgress,
  initialDraftAnswer,
  type PendingUserInputDraftAnswer,
  toggleDraftOption,
} from "./pendingUserInput";

const AUTO_ADVANCE_MS = 150;

export interface QuestionCardProps {
  requestId: string;
  questions: UserInputQuestion[];
  /** The daemon marked the request answered. */
  resolved: boolean;
  disabled?: boolean;
  onAnswer?: (requestId: string, answers: UserInputAnswer[]) => Promise<unknown>;
}

function initialDrafts(questions: ReadonlyArray<UserInputQuestion>) {
  return Object.fromEntries(questions.map((q) => [q.id, initialDraftAnswer(q)]));
}

export const QuestionCard = memo(function QuestionCard({
  requestId,
  questions,
  resolved,
  disabled = false,
  onAnswer,
}: QuestionCardProps) {
  const [answers, setAnswers] = useState<Record<string, PendingUserInputDraftAnswer>>(() =>
    initialDrafts(questions),
  );
  const [questionIndex, setQuestionIndex] = useState(0);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const autoAdvanceTimerRef = useRef<number | null>(null);
  const progress = derivePendingUserInputProgress(questions, answers, questionIndex);
  const activeQuestion = progress.activeQuestion;
  const responseDisabled = disabled || sending || resolved || !onAnswer;

  useEffect(
    () => () => {
      if (autoAdvanceTimerRef.current !== null) window.clearTimeout(autoAdvanceTimerRef.current);
    },
    [],
  );

  const send = useCallback(
    async (drafts: Record<string, PendingUserInputDraftAnswer>) => {
      const built = buildPendingUserInputAnswers(questions, drafts);
      if (!built || !onAnswer) return;
      setSending(true);
      setError(null);
      try {
        await onAnswer(requestId, built);
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : String(cause));
      } finally {
        setSending(false);
      }
    },
    [onAnswer, questions, requestId],
  );

  const advance = useCallback(
    (drafts: Record<string, PendingUserInputDraftAnswer>) => {
      const next = derivePendingUserInputProgress(questions, drafts, questionIndex);
      if (!next.canAdvance) return;
      if (next.isLastQuestion) void send(drafts);
      else setQuestionIndex(next.questionIndex + 1);
    },
    [questionIndex, questions, send],
  );

  const select = useCallback(
    (question: UserInputQuestion, option: string) => {
      if (responseDisabled) return;
      const nextDrafts = {
        ...answers,
        [question.id]: toggleDraftOption(question, answers[question.id], option),
      };
      setAnswers(nextDrafts);
      if (question.multiSelect) return;
      if (autoAdvanceTimerRef.current !== null) window.clearTimeout(autoAdvanceTimerRef.current);
      autoAdvanceTimerRef.current = window.setTimeout(() => {
        autoAdvanceTimerRef.current = null;
        advance(nextDrafts);
      }, AUTO_ADVANCE_MS);
    },
    [advance, answers, responseDisabled],
  );

  if (resolved || !activeQuestion) {
    return (
      <div
        className="flex items-center gap-2 border border-rule-soft bg-surface px-3 py-1.5 text-sm text-muted"
        data-question-card="answered"
      >
        <CheckIcon className="size-3.5 text-grow" aria-hidden />
        <span className="min-w-0 truncate">{questions[0]?.header ?? "Question"}: answered</span>
      </div>
    );
  }

  return (
    <fieldset
      aria-label={`Question: ${activeQuestion.header}`}
      className="m-0 min-w-0 border border-accent bg-surface p-0"
      data-question-card="open"
      onKeyDown={(event) => {
        if (event.metaKey || event.ctrlKey || event.altKey) return;
        const target = event.target;
        if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) return;
        const digit = Number.parseInt(event.key, 10);
        if (Number.isNaN(digit) || digit < 1 || digit > 9) return;
        const option = activeQuestion.options[digit - 1];
        if (option === undefined) return;
        event.preventDefault();
        select(activeQuestion, option);
      }}
    >
      <div className="flex items-center gap-2 border-b border-rule-soft px-3 py-1.5 text-xs text-muted">
        <MessageCircleQuestion className="size-3.5 text-accent-fg" aria-hidden />
        <span className="font-medium text-ink">{activeQuestion.header}</span>
        {questions.length > 1 ? (
          <span className="ml-auto tabular-nums">
            {progress.questionIndex + 1}/{questions.length}
          </span>
        ) : null}
      </div>
      <div className="px-3 py-2">
        <p className="m-0 text-sm text-ink">{activeQuestion.question}</p>
        {activeQuestion.multiSelect ? (
          <p className="m-0 mt-1 text-xs text-muted">Select one or more options.</p>
        ) : null}
        {activeQuestion.options.length > 0 ? (
          <div className="mt-2 flex flex-col gap-0.5">
            {activeQuestion.options.map((option, index) => {
              const isSelected =
                !progress.usingCustomAnswer && progress.selectedOptionValues.includes(option);
              const shortcutKey = index < 9 ? index + 1 : null;
              return (
                <button
                  key={`${activeQuestion.id}:${option}`}
                  type="button"
                  disabled={responseDisabled}
                  aria-pressed={isSelected}
                  onClick={() => select(activeQuestion, option)}
                  className={cx(
                    "flex w-full items-center gap-2 rounded-md border-0 px-2.5 py-1.5 text-left text-sm",
                    isSelected
                      ? "bg-accent-soft text-ink"
                      : "bg-transparent text-ink hover:bg-raised",
                    responseDisabled ? "cursor-not-allowed opacity-60" : "cursor-pointer",
                  )}
                >
                  <span className="min-w-0 flex-1 font-medium">{option}</span>
                  {isSelected ? (
                    <CheckIcon className="size-3.5 shrink-0 text-accent-fg" aria-hidden />
                  ) : shortcutKey !== null ? (
                    <kbd className="text-xs text-faint tabular-nums">{shortcutKey}</kbd>
                  ) : null}
                </button>
              );
            })}
          </div>
        ) : null}
        <div className="mt-2 flex items-end gap-2">
          {activeQuestion.multiline ? (
            <textarea
              aria-label={activeQuestion.options.length > 0 ? "Other answer" : "Answer"}
              placeholder={
                activeQuestion.options.length > 0 ? "Or type an answer" : "Type an answer"
              }
              rows={3}
              disabled={responseDisabled}
              value={progress.customAnswer}
              onChange={(event) =>
                setAnswers({
                  ...answers,
                  [activeQuestion.id]: { customAnswer: event.target.value },
                })
              }
              className="min-w-0 flex-1 rounded-md border border-input bg-bg px-2 py-1 text-sm text-ink"
            />
          ) : (
            <input
              type="text"
              aria-label={activeQuestion.options.length > 0 ? "Other answer" : "Answer"}
              placeholder={
                activeQuestion.options.length > 0 ? "Or type an answer" : "Type an answer"
              }
              disabled={responseDisabled}
              value={progress.customAnswer}
              onChange={(event) =>
                setAnswers({
                  ...answers,
                  [activeQuestion.id]: { customAnswer: event.target.value },
                })
              }
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  advance(answers);
                }
              }}
              className="min-w-0 flex-1 rounded-md border border-input bg-bg px-2 py-1 text-sm text-ink"
            />
          )}
          <Button
            size="sm"
            variant="primary"
            disabled={responseDisabled || !progress.canAdvance}
            onClick={() => advance(answers)}
          >
            {progress.isLastQuestion ? (sending ? "Sending…" : "Send answer") : "Next"}
          </Button>
        </div>
        {error ? (
          <p role="alert" className="m-0 mt-1 text-xs text-danger">
            {error}
          </p>
        ) : null}
      </div>
    </fieldset>
  );
});
