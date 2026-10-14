// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/chat/ThreadFindBar.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the find bar (query, "n/total", previous, next, close; Enter and Shift+Enter, Esc) on @loams/ui's InputGroup, with `formatThreadFindCount` from threadFind.ts; no remote search status or details-card layout. Loams' changes are Apache-2.0.

import { InputGroup, InputGroupAddon, InputGroupInput } from "@loams/ui";
import { ChevronDownIcon, ChevronUpIcon, XIcon } from "lucide-react";
import { type KeyboardEvent, useEffect, useRef } from "react";

interface ThreadFindBarProps {
  readonly query: string;
  readonly matchCount: number;
  readonly activeIndex: number;
  readonly onQueryChange: (query: string) => void;
  readonly onNext: () => void;
  readonly onPrevious: () => void;
  readonly onClose: () => void;
}

export function clampThreadFindIndex(index: number, total: number): number {
  if (total <= 0) return 0;
  return ((index % total) + total) % total;
}

export function formatThreadFindCount(index: number, total: number): string {
  return total <= 0 ? "0/0" : `${clampThreadFindIndex(index, total) + 1}/${total}`;
}

function IconButton(props: {
  label: string;
  disabled?: boolean;
  onClick: () => void;
  icon: typeof XIcon;
}) {
  const Icon = props.icon;
  return (
    <button
      type="button"
      aria-label={props.label}
      title={props.label}
      disabled={props.disabled}
      onClick={props.onClick}
      className="inline-flex size-6 cursor-pointer items-center justify-center rounded-pill border-0 bg-transparent text-muted hover:bg-raised hover:text-ink disabled:cursor-not-allowed disabled:opacity-40"
    >
      <Icon className="size-3.5" aria-hidden />
    </button>
  );
}

export function ThreadFindBar(props: ThreadFindBarProps) {
  const wrapper = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const input = wrapper.current?.querySelector("input");
    input?.focus();
    input?.select();
  }, []);

  const hasQuery = props.query.trim().length > 0;
  const noResults = hasQuery && props.matchCount === 0;
  const label = hasQuery ? formatThreadFindCount(props.activeIndex, props.matchCount) : "";
  const navigationDisabled = props.matchCount === 0;
  const handleKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.nativeEvent.isComposing) return;
    if (event.key === "Enter") {
      event.preventDefault();
      event.stopPropagation();
      if (event.shiftKey) props.onPrevious();
      else props.onNext();
    } else if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      props.onClose();
    }
  };

  return (
    <div ref={wrapper} data-thread-find-bar="" className="w-full max-w-sm">
      <InputGroup role="search" aria-label="Find in thread" className="bg-surface shadow-pop">
        <InputGroupInput
          type="text"
          value={props.query}
          aria-label="Find in thread"
          placeholder="Find in thread"
          maxLength={200}
          spellCheck={false}
          autoComplete="off"
          onChange={(event) => props.onQueryChange(event.target.value)}
          onKeyDown={handleKeyDown}
        />
        <InputGroupAddon align="end">
          <span
            aria-live="polite"
            className={
              noResults
                ? "min-w-10 text-center text-xs tabular-nums text-danger"
                : "min-w-10 text-center text-xs tabular-nums text-muted"
            }
          >
            {label}
          </span>
          <IconButton
            label="Previous match"
            disabled={navigationDisabled}
            onClick={props.onPrevious}
            icon={ChevronUpIcon}
          />
          <IconButton
            label="Next match"
            disabled={navigationDisabled}
            onClick={props.onNext}
            icon={ChevronDownIcon}
          />
          <IconButton label="Close find" onClick={props.onClose} icon={XIcon} />
        </InputGroupAddon>
      </InputGroup>
    </div>
  );
}
