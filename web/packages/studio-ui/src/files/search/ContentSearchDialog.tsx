// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/search/ProjectContentSearchDialog.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// streams matches from a ContentSearchSource (agentd's SearchWorkspaceContent, SD1 Task 11) into @loams/ui's Dialog; no whole-word option (the daemon has none), no access scopes or right-panel store. Loams' changes are Apache-2.0.

// Search in files (SD1 Task 21): ripgrep-style content search across the checkout. Matches
// stream in as the daemon finds them and are grouped by file; Up and Down move, Enter (or
// a click) opens the file at the match's line. A newer query aborts the one in flight.

import { cx, Dialog, InputGroup, InputGroupInput, Spinner, Toggle, Tooltip } from "@loams/ui";
import { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";

import { useDebouncedValue } from "../FileBrowser";
import type { ContentMatch, ContentSearchQuery, ContentSearchSource } from "../source";
import { HighlightedSearchLine } from "./HighlightedSearchLine";

/** Rows render in windows that grow as the end of the list scrolls into view. */
const VISIBLE_MATCH_WINDOW = 100;
const MAX_RESULTS = 500;

interface MatchGroup {
  readonly path: string;
  readonly matches: ReadonlyArray<ContentMatch & { readonly resultIndex: number }>;
}

function splitPath(path: string): { readonly name: string; readonly directory: string } {
  const separator = path.lastIndexOf("/");
  return separator === -1
    ? { name: path, directory: "" }
    : { name: path.slice(separator + 1), directory: path.slice(0, separator) };
}

function groupMatches(matches: ReadonlyArray<ContentMatch>): MatchGroup[] {
  const groups = new Map<string, Array<ContentMatch & { readonly resultIndex: number }>>();
  matches.forEach((match, resultIndex) => {
    const group = groups.get(match.path);
    const indexedMatch = { ...match, resultIndex };
    if (group) group.push(indexedMatch);
    else groups.set(match.path, [indexedMatch]);
  });
  return [...groups].map(([path, groupedMatches]) => ({ path, matches: groupedMatches }));
}

function SearchOptionButton(props: {
  readonly active: boolean;
  readonly disabled: boolean;
  readonly label: string;
  readonly onClick: () => void;
  readonly children: ReactNode;
}) {
  return (
    <Tooltip content={props.label}>
      <Toggle
        size="sm"
        aria-label={props.label}
        disabled={props.disabled}
        pressed={props.active}
        onPressedChange={props.onClick}
      >
        <span className={/* mono-ok: code */ "font-mono"}>{props.children}</span>
      </Toggle>
    </Tooltip>
  );
}

type SearchState =
  | { readonly phase: "idle" }
  | { readonly phase: "searching" }
  | { readonly phase: "done"; readonly truncated: boolean }
  | { readonly phase: "error"; readonly message: string };

export function ContentSearchDialog(props: {
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
  readonly source: ContentSearchSource;
  readonly projectName: string;
  readonly onOpenMatch: (match: ContentMatch) => void;
}) {
  const { source } = props;
  const available = source.availability.available;
  const [query, setQuery] = useState("");
  const [caseSensitive, setCaseSensitive] = useState(false);
  const [useRegex, setUseRegex] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [visibleCount, setVisibleCount] = useState(VISIBLE_MATCH_WINDOW);
  const [matches, setMatches] = useState<readonly ContentMatch[]>([]);
  const [state, setState] = useState<SearchState>({ phase: "idle" });
  const debounced = useDebouncedValue(query, 250);
  const request = useMemo<ContentSearchQuery>(
    () => ({ query: debounced, caseSensitive, regex: useRegex, maxResults: MAX_RESULTS }),
    [debounced, caseSensitive, useRegex],
  );
  const invalidRegex = useMemo(() => {
    if (!useRegex || !query) return false;
    try {
      new RegExp(query);
      return false;
    } catch {
      return true;
    }
  }, [query, useRegex]);

  useEffect(() => {
    setMatches([]);
    setSelectedIndex(0);
    setVisibleCount(VISIBLE_MATCH_WINDOW);
    if (!props.open || !available || !request.query.trim() || invalidRegex) {
      setState({ phase: "idle" });
      return;
    }
    const controller = new AbortController();
    setState({ phase: "searching" });
    source
      .search(
        request,
        (match) => {
          if (!controller.signal.aborted) setMatches((previous) => [...previous, match]);
        },
        controller.signal,
      )
      .then(
        (result) => {
          if (!controller.signal.aborted) setState({ phase: "done", truncated: result.truncated });
        },
        (error: unknown) => {
          if (controller.signal.aborted) return;
          setState({
            phase: "error",
            message: error instanceof Error ? error.message : "The search failed.",
          });
        },
      );
    return () => controller.abort();
  }, [available, invalidRegex, props.open, request, source]);

  const visibleMatches = useMemo(() => matches.slice(0, visibleCount), [matches, visibleCount]);
  const groups = useMemo(() => groupMatches(visibleMatches), [visibleMatches]);
  const fileCount = useMemo(() => new Set(matches.map((match) => match.path)).size, [matches]);

  const listRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (selectedIndex >= visibleCount) {
      setVisibleCount(selectedIndex + VISIBLE_MATCH_WINDOW);
      return;
    }
    listRef.current
      ?.querySelector<HTMLElement>(`[data-content-search-result="${selectedIndex}"]`)
      ?.scrollIntoView?.({ block: "nearest" });
  }, [selectedIndex, visibleCount]);

  const observeLoadMoreSentinel = useCallback((sentinel: HTMLElement | null) => {
    if (!sentinel || typeof IntersectionObserver === "undefined") return;
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        setVisibleCount((current) => current + VISIBLE_MATCH_WINDOW);
      }
    });
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, []);

  const close = () => props.onOpenChange(false);
  const openMatch = (match: ContentMatch) => {
    close();
    props.onOpenMatch(match);
  };
  const plural = (n: number, one: string) =>
    `${n.toLocaleString("en-US")} ${one}${n === 1 ? "" : "s"}`;

  const status: ReactNode =
    state.phase === "searching" ? (
      <span className="flex items-center gap-2">
        <Spinner size="sm" tone="muted" label="Searching" /> Searching…
      </span>
    ) : state.phase === "error" ? (
      <span className="text-danger">{state.message}</span>
    ) : invalidRegex ? (
      <span className="text-danger">Invalid regular expression</span>
    ) : state.phase === "done" ? (
      `${plural(matches.length, "result")}${state.truncated ? "+" : ""} in ${plural(fileCount, "file")}`
    ) : null;

  return (
    <Dialog open={props.open} onClose={close} title="Search in files">
      <div className="flex min-h-0 flex-col gap-2" data-content-search>
        <div className="flex items-center gap-1">
          <InputGroup className="min-w-0 flex-1">
            <InputGroupInput
              type="search"
              autoFocus
              aria-label={`Search in ${props.projectName}`}
              placeholder={`Search in ${props.projectName}`}
              spellCheck={false}
              value={query}
              disabled={!available}
              onChange={(event) => setQuery(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "ArrowDown" && matches.length > 0) {
                  event.preventDefault();
                  setSelectedIndex((current) => (current + 1) % matches.length);
                } else if (event.key === "ArrowUp" && matches.length > 0) {
                  event.preventDefault();
                  setSelectedIndex((current) => (current - 1 + matches.length) % matches.length);
                } else if (event.key === "Enter") {
                  // While a newer query is debouncing, the matches belong to the previous one.
                  if (debounced !== query) {
                    event.preventDefault();
                    return;
                  }
                  const match = matches[selectedIndex];
                  if (match) {
                    event.preventDefault();
                    openMatch(match);
                  }
                }
              }}
            />
          </InputGroup>
          <SearchOptionButton
            active={caseSensitive}
            disabled={!available}
            label="Match case"
            onClick={() => setCaseSensitive((current) => !current)}
          >
            Aa
          </SearchOptionButton>
          <SearchOptionButton
            active={useRegex}
            disabled={!available}
            label="Use regular expression"
            onClick={() => setUseRegex((current) => !current)}
          >
            .*
          </SearchOptionButton>
        </div>
        {!source.availability.available ? (
          <p className="py-6 text-center text-[0.86rem] text-muted">{source.availability.reason}</p>
        ) : (
          <>
            {status ? (
              <div role="status" className="text-[0.78rem] text-muted">
                {status}
              </div>
            ) : null}
            {matches.length === 0 ? (
              <p className="py-6 text-center text-[0.86rem] text-muted">
                {state.phase === "done"
                  ? "No results found."
                  : "Type to search across the project."}
              </p>
            ) : (
              <div ref={listRef} className="max-h-[55vh] min-h-0 overflow-auto">
                {groups.map((group) => {
                  const path = splitPath(group.path);
                  return (
                    <section className="pb-2" key={group.path} aria-label={group.path}>
                      <div className="sticky top-0 z-10 flex h-8 items-center gap-2 bg-bg px-1 text-[0.78rem]">
                        <span className="font-medium text-ink">{path.name}</span>
                        {path.directory ? (
                          <span className="min-w-0 truncate text-muted">{path.directory}</span>
                        ) : null}
                        <span className="ml-auto rounded-pill bg-raised px-1.5 py-0.5 text-[0.7rem] text-muted tabular-nums">
                          {group.matches.length}
                        </span>
                      </div>
                      {group.matches.map((match) => (
                        <button
                          type="button"
                          key={`${match.path}:${match.line}:${match.resultIndex}`}
                          data-content-search-result={match.resultIndex}
                          aria-label={`${match.path}, line ${match.line}`}
                          className={cx(
                            /* mono-ok: path */ "flex h-7 w-full min-w-0 cursor-pointer items-center gap-3 border-0 bg-transparent px-1 text-left font-mono text-[0.78rem] text-ink hover:bg-raised",
                            match.resultIndex === selectedIndex && "bg-raised",
                          )}
                          onMouseEnter={() => setSelectedIndex(match.resultIndex)}
                          onClick={() => openMatch(match)}
                        >
                          <span className="w-10 shrink-0 text-right text-faint tabular-nums">
                            {match.line}
                          </span>
                          <span className="min-w-0 flex-1 truncate whitespace-pre">
                            <HighlightedSearchLine line={match.preview} query={request} />
                          </span>
                        </button>
                      ))}
                    </section>
                  );
                })}
                {matches.length > visibleCount ? (
                  <div ref={observeLoadMoreSentinel} className="h-8" aria-hidden="true" />
                ) : null}
              </div>
            )}
          </>
        )}
      </div>
    </Dialog>
  );
}
