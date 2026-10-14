// The composer's suggestion menu (SD1 Task 19; §53 §5.6): what `@`, `/` and `$` offer
// at the caret, ranked with T3 Code's vendored search ranking. The scoring tiers follow
// upstream's composerSlashCommandSearch.ts (name before description; exact, prefix,
// boundary, substring, then fuzzy).

import { rankFileMatches } from "../files/filePickerLogic";
import type { FileMatch } from "../files/source";
import type { ComposerTrigger } from "./logic";
import { serializeComposerFileLink } from "./shared/composerTrigger";
import {
  insertRankedSearchResult,
  normalizeSearchQuery,
  scoreQueryMatch,
} from "./shared/searchRanking";

/** A file the daemon matched for an `@` query (Task 21's quick-open search). */
export type ComposerFileMatch = FileMatch;

/** A harness slash command (`ListCommands`). */
export interface ComposerSlashCommandInfo {
  name: string;
  description: string;
  inputHint?: string;
}

/** A harness skill (`ListSkills`). */
export interface ComposerSkillInfo {
  name: string;
  description: string;
  path: string;
}

export type ComposerMenuItem =
  | { type: "path"; id: string; path: string; isDir: boolean }
  | { type: "slash-command"; id: string; command: ComposerSlashCommandInfo }
  | { type: "skill"; id: string; skill: ComposerSkillInfo };

/** At most this many rows; the daemon's file search is already bounded. */
export const COMPOSER_MENU_LIMIT = 50;

function scoreNameAndDescription(name: string, description: string, query: string) {
  const scores = [
    scoreQueryMatch({
      value: name.toLowerCase(),
      query,
      exactBase: 0,
      prefixBase: 2,
      boundaryBase: 4,
      includesBase: 6,
      fuzzyBase: 100,
      boundaryMarkers: ["-", "_", "/", ":"],
    }),
    scoreQueryMatch({
      value: description.toLowerCase(),
      query,
      exactBase: 20,
      prefixBase: 22,
      boundaryBase: 24,
      includesBase: 26,
    }),
  ].filter((score): score is number => score !== null);
  return scores.length === 0 ? null : Math.min(...scores);
}

function rank<T extends ComposerMenuItem>(
  items: readonly T[],
  query: string,
  score: (item: T, query: string) => number | null,
  tieBreaker: (item: T) => string,
): T[] {
  if (!query) return items.slice(0, COMPOSER_MENU_LIMIT);
  const ranked: { item: T; score: number; tieBreaker: string }[] = [];
  for (const item of items) {
    const value = score(item, query);
    if (value === null) continue;
    insertRankedSearchResult(
      ranked,
      { item, score: value, tieBreaker: tieBreaker(item) },
      COMPOSER_MENU_LIMIT,
    );
  }
  return ranked.map((entry) => entry.item);
}

/** `/` at the start of a line: the harness's commands, then its skills. */
export function searchSlashMenu(
  query: string,
  commands: readonly ComposerSlashCommandInfo[],
  skills: readonly ComposerSkillInfo[],
): ComposerMenuItem[] {
  const normalized = normalizeSearchQuery(query, { trimLeadingPattern: /^\/+/ });
  const items: ComposerMenuItem[] = [
    ...commands.map((command) => ({
      type: "slash-command" as const,
      id: `command:${command.name}`,
      command,
    })),
    ...skills.map((skill) => ({ type: "skill" as const, id: `skill:${skill.name}`, skill })),
  ];
  return rank(
    items,
    normalized,
    (item, q) =>
      item.type === "slash-command"
        ? scoreNameAndDescription(item.command.name, item.command.description, q)
        : item.type === "skill"
          ? scoreNameAndDescription(item.skill.name, item.skill.description, q)
          : null,
    (item) =>
      item.type === "slash-command"
        ? `0\u0000${item.command.name}`
        : item.type === "skill"
          ? `1\u0000${item.skill.name}`
          : `2\u0000${item.id}`,
  );
}

/** `$` anywhere: skills only. */
export function searchSkillMenu(
  query: string,
  skills: readonly ComposerSkillInfo[],
): ComposerMenuItem[] {
  return searchSlashMenu(query, [], skills);
}

/**
 * `@`: the daemon's matches as menu rows, ranked as Task 21's quick open ranks them
 * (`rankFileMatches`: files only, best score first, the daemon's order on ties).
 */
export function fileMenuItems(
  matches: readonly ComposerFileMatch[],
  query: string,
): ComposerMenuItem[] {
  return rankFileMatches(matches, query, COMPOSER_MENU_LIMIT).map((match) => ({
    type: "path",
    id: `path:${match.path}`,
    path: match.path,
    isDir: false,
  }));
}

/** The text a chosen row puts in place of the trigger, with the trailing space. */
export function composerMenuReplacement(item: ComposerMenuItem): string {
  switch (item.type) {
    case "path":
      return `${serializeComposerFileLink(item.path)} `;
    case "slash-command":
      return `/${item.command.name} `;
    case "skill":
      return `$${item.skill.name} `;
  }
}

/** The trigger kinds Studio offers a menu for (`#` pull requests are not one yet). */
export function isMenuTrigger(trigger: ComposerTrigger | null): trigger is ComposerTrigger {
  return (
    trigger !== null &&
    (trigger.kind === "path" || trigger.kind === "slash-command" || trigger.kind === "skill")
  );
}
