// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/pendingUserInput.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the draft-answer rules (normalising selections and custom text, the active question, progress and completeness) over agentd's `UserInputQuestion` (string options, `prefill`) and `UserInputAnswer` (labels); no attachments, thread links or context references. Loams' changes are Apache-2.0.

import type { UserInputAnswer, UserInputQuestion } from "@loams/agentd-client";

export interface PendingUserInputDraftAnswer {
  selectedOptionValues?: string[];
  customAnswer?: string;
}

export interface PendingUserInputProgress {
  questionIndex: number;
  activeQuestion: UserInputQuestion | null;
  activeDraft: PendingUserInputDraftAnswer | undefined;
  selectedOptionValues: string[];
  customAnswer: string;
  resolvedAnswer: string[] | null;
  usingCustomAnswer: boolean;
  answeredQuestionCount: number;
  isLastQuestion: boolean;
  isComplete: boolean;
  canAdvance: boolean;
}

function normalizeDraftAnswer(value: string | undefined): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeSelectedOptionValues(value: string[] | undefined): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  // Option labels must stay unchanged, including whitespace.
  return Array.from(new Set(value.filter((entry) => typeof entry === "string")));
}

/** The draft for `question`, starting from its prefill. */
export function initialDraftAnswer(question: UserInputQuestion): PendingUserInputDraftAnswer {
  return question.prefill ? { customAnswer: question.prefill } : {};
}

/** The labels a draft answers `question` with, or null when it does not answer it yet. */
export function resolvePendingUserInputAnswer(
  question: UserInputQuestion,
  draft: PendingUserInputDraftAnswer | undefined,
): string[] | null {
  const customAnswer = normalizeDraftAnswer(draft?.customAnswer);
  if (customAnswer !== null) {
    return [customAnswer];
  }
  const selectedOptionValues = normalizeSelectedOptionValues(draft?.selectedOptionValues).filter(
    (value) => question.options.includes(value),
  );
  if (selectedOptionValues.length === 0) return null;
  return question.multiSelect ? selectedOptionValues : selectedOptionValues.slice(0, 1);
}

/** Selecting an option: multi-select toggles it; single select replaces the choice. */
export function toggleDraftOption(
  question: UserInputQuestion,
  draft: PendingUserInputDraftAnswer | undefined,
  option: string,
): PendingUserInputDraftAnswer {
  const selected = normalizeSelectedOptionValues(draft?.selectedOptionValues);
  if (!question.multiSelect) return { selectedOptionValues: [option] };
  return {
    selectedOptionValues: selected.includes(option)
      ? selected.filter((value) => value !== option)
      : [...selected, option],
  };
}

export function derivePendingUserInputProgress(
  questions: ReadonlyArray<UserInputQuestion>,
  draftAnswers: Record<string, PendingUserInputDraftAnswer>,
  questionIndex: number,
): PendingUserInputProgress {
  const normalizedQuestionIndex =
    questions.length === 0 ? 0 : Math.max(0, Math.min(questionIndex, questions.length - 1));
  const activeQuestion = questions[normalizedQuestionIndex] ?? null;
  const activeDraft = activeQuestion ? draftAnswers[activeQuestion.id] : undefined;
  const resolvedAnswer = activeQuestion
    ? resolvePendingUserInputAnswer(activeQuestion, activeDraft)
    : null;
  const customAnswer = activeDraft?.customAnswer ?? "";
  const answeredQuestionCount = questions.filter(
    (question) => resolvePendingUserInputAnswer(question, draftAnswers[question.id]) !== null,
  ).length;

  return {
    questionIndex: normalizedQuestionIndex,
    activeQuestion,
    activeDraft,
    selectedOptionValues: normalizeSelectedOptionValues(activeDraft?.selectedOptionValues),
    customAnswer,
    resolvedAnswer,
    usingCustomAnswer: normalizeDraftAnswer(customAnswer) !== null,
    answeredQuestionCount,
    isLastQuestion: questions.length === 0 ? true : normalizedQuestionIndex >= questions.length - 1,
    isComplete: questions.length > 0 && answeredQuestionCount === questions.length,
    canAdvance: resolvedAnswer !== null,
  };
}

/** Every question's answer for `AnswerQuestion`, or null while one is unanswered. */
export function buildPendingUserInputAnswers(
  questions: ReadonlyArray<UserInputQuestion>,
  draftAnswers: Record<string, PendingUserInputDraftAnswer>,
): UserInputAnswer[] | null {
  const answers: UserInputAnswer[] = [];
  for (const question of questions) {
    const labels = resolvePendingUserInputAnswer(question, draftAnswers[question.id]);
    if (labels === null) return null;
    answers.push({ questionId: question.id, labels });
  }
  return answers;
}
