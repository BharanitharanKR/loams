// The file tree's rows (SD1 Task 21; inventory (e), "the fallback"): a WAI-ARIA tree on
// TanStack Virtual, in place of @pierre/trees (ruling T21-1). Only the rows in view are in
// the DOM; a row is a `treeitem` with its level, its expanded state for a folder, and a
// roving tab stop. The keys are the tree pattern's: Up and Down move, Right opens a folder
// or moves into it, Left closes it or moves to its parent, Home and End jump, Enter opens.
// "@" on a row mentions it in the composer.

import { cx, Spinner } from "@loams/ui";
import { useVirtualizer } from "@tanstack/react-virtual";
import { AtSignIcon, ChevronRightIcon, FileIcon, FolderIcon, FolderOpenIcon } from "lucide-react";
import { type KeyboardEvent, useEffect, useRef, useState } from "react";

import type { EntryKind } from "./source";

export interface TreeRow {
  readonly path: string;
  readonly name: string;
  readonly kind: EntryKind;
  /** 1 for the root's children. */
  readonly level: number;
  readonly expanded: boolean;
  readonly loading: boolean;
  readonly ignored: boolean;
  /** The folder holding this row; "" for the root. */
  readonly parent: string;
}

const ROW_HEIGHT = 26;
const INDENT = 12;

/** jsdom and hidden panels have no layout: render a screenful instead of nothing. */
const FALLBACK_RECT = { width: 320, height: 800 };

export function VirtualTree(props: {
  readonly label: string;
  readonly rows: readonly TreeRow[];
  readonly selectedPath: string | null;
  /** A file row was chosen. */
  readonly onOpen: (row: TreeRow) => void;
  /** A folder row was opened or closed. */
  readonly onToggle: (row: TreeRow, expanded: boolean) => void;
  readonly onMention?: (row: TreeRow) => void;
  /** Scroll this path into view when it changes. */
  readonly revealPath?: string | null;
}) {
  const { rows } = props;
  const scroller = useRef<HTMLDivElement>(null);
  const [focusPath, setFocusPath] = useState<string | null>(null);
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scroller.current,
    estimateSize: () => ROW_HEIGHT,
    getItemKey: (index) => rows[index]?.path ?? index,
    overscan: 12,
    initialRect: FALLBACK_RECT,
    observeElementRect: (instance, callback) => {
      const element = instance.scrollElement;
      if (!element) return;
      const report = () => {
        const rect = element.getBoundingClientRect();
        callback(rect.height > 0 ? { width: rect.width, height: rect.height } : FALLBACK_RECT);
      };
      report();
      if (typeof ResizeObserver === "undefined") return;
      const observer = new ResizeObserver(report);
      observer.observe(element);
      return () => observer.disconnect();
    },
  });

  const focusIndex = Math.max(
    0,
    rows.findIndex((row) => row.path === (focusPath ?? props.selectedPath)),
  );

  const revealIndex = props.revealPath
    ? rows.findIndex((row) => row.path === props.revealPath)
    : -1;
  useEffect(() => {
    if (revealIndex >= 0) virtualizer.scrollToIndex(revealIndex, { align: "auto" });
  }, [revealIndex, virtualizer]);

  const moveTo = (index: number) => {
    const row = rows[index];
    if (!row) return;
    setFocusPath(row.path);
    virtualizer.scrollToIndex(index, { align: "auto" });
    requestAnimationFrame(() => {
      const rendered = scroller.current?.querySelectorAll<HTMLElement>("[data-path]") ?? [];
      for (const element of rendered) if (element.dataset.path === row.path) element.focus();
    });
  };

  const activate = (row: TreeRow) => {
    setFocusPath(row.path);
    if (row.kind === "directory") props.onToggle(row, !row.expanded);
    else props.onOpen(row);
  };

  const onKeyDown = (event: KeyboardEvent<HTMLElement>, row: TreeRow, index: number) => {
    switch (event.key) {
      case "ArrowDown":
        moveTo(Math.min(rows.length - 1, index + 1));
        break;
      case "ArrowUp":
        moveTo(Math.max(0, index - 1));
        break;
      case "Home":
        moveTo(0);
        break;
      case "End":
        moveTo(rows.length - 1);
        break;
      case "ArrowRight":
        if (row.kind !== "directory") return;
        if (!row.expanded) props.onToggle(row, true);
        else moveTo(index + 1);
        break;
      case "ArrowLeft":
        if (row.kind === "directory" && row.expanded) props.onToggle(row, false);
        else if (row.parent) moveTo(rows.findIndex((r) => r.path === row.parent));
        else return;
        break;
      case "Enter":
      case " ":
        activate(row);
        break;
      case "@":
        if (!props.onMention) return;
        props.onMention(row);
        break;
      default:
        return;
    }
    event.preventDefault();
  };

  return (
    <div
      ref={scroller}
      role="tree"
      aria-label={props.label}
      className="min-h-0 flex-1 overflow-auto py-1"
      data-file-tree
    >
      <div className="relative w-full" style={{ height: virtualizer.getTotalSize() }}>
        {virtualizer.getVirtualItems().map((item) => {
          const row = rows[item.index];
          if (!row) return null;
          const selected = row.path === props.selectedPath;
          const isDir = row.kind === "directory";
          const Icon = isDir ? (row.expanded ? FolderOpenIcon : FolderIcon) : FileIcon;
          return (
            <div
              key={item.key}
              role="treeitem"
              aria-label={row.name}
              aria-level={row.level}
              aria-selected={selected}
              aria-expanded={isDir ? row.expanded : undefined}
              aria-busy={row.loading || undefined}
              data-path={row.path}
              tabIndex={item.index === focusIndex ? 0 : -1}
              onClick={() => activate(row)}
              onKeyDown={(event) => onKeyDown(event, row, item.index)}
              onFocus={() => setFocusPath(row.path)}
              className={cx(
                "group absolute top-0 left-0 flex w-full cursor-pointer items-center gap-1.5 pr-1 text-[0.82rem] outline-none select-none",
                "hover:bg-raised focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent",
                selected ? "bg-accent-soft text-ink" : "text-ink",
                row.ignored && "text-faint",
              )}
              style={{
                height: ROW_HEIGHT,
                transform: `translateY(${item.start}px)`,
                paddingLeft: 6 + (row.level - 1) * INDENT,
              }}
            >
              <ChevronRightIcon
                aria-hidden
                className={cx(
                  "size-3.5 shrink-0 text-faint transition-transform duration-150 ease-op",
                  isDir ? (row.expanded ? "rotate-90" : "") : "invisible",
                )}
              />
              <Icon
                aria-hidden
                className={cx("size-3.5 shrink-0", isDir ? "text-accent" : "text-muted")}
              />
              <span className="min-w-0 flex-1 truncate">{row.name}</span>
              {row.loading ? (
                <Spinner size="xs" tone="muted" label={`Loading ${row.name}`} />
              ) : null}
              {props.onMention ? (
                <button
                  type="button"
                  tabIndex={-1}
                  aria-label={`Mention ${row.path} in composer`}
                  title="Mention in composer"
                  onClick={(event) => {
                    event.stopPropagation();
                    props.onMention?.(row);
                  }}
                  className="invisible inline-flex size-5 shrink-0 cursor-pointer items-center justify-center rounded-pill border-0 bg-transparent p-0 text-muted group-hover:visible group-focus-visible:visible hover:bg-bg hover:text-ink"
                >
                  <AtSignIcon aria-hidden className="size-3" />
                </button>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}
