// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/ReadOnlySourcePreview.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// read-only and on the shadow-root path only, through @pierre/diffs' File class in an effect (each mount gets its own container), with a token theme, no worker pool, virtualizer, settings store or editor, and a line to reveal. Loams' changes are Apache-2.0.

// Highlighted source for the file viewer (SD1 Task 21; design §53 D919, ruling T0-4).
// @pierre/diffs tokenises the text with Shiki and writes escaped text nodes into an open
// shadow root; the file's text is never parsed as HTML. Studio uses only this path: no
// `prerenderedHTML`, no SSR, no marker `{html}` messages, no `allowDangerousHtml`.
//
// The File class is driven from an effect rather than through `@pierre/diffs/react`: the
// React wrapper reuses its container across React's development remount (Strict Mode), and
// the second instance then finds the first one's empty <pre> and never renders (T21-2).

import { type FileOptions, File as PierreFile } from "@pierre/diffs";
import { useEffect, useMemo, useRef } from "react";

import { CODE_SURFACE_CSS, ensureCodeTheme, useColourScheme } from "./codeTheme";

export default function SourceView(props: {
  readonly name: string;
  readonly text: string;
  readonly cacheKey?: string;
  readonly wrap: boolean;
  /** 1-based line to select and scroll to (a content search result). */
  readonly revealLine?: number | null;
}) {
  const scheme = useColourScheme();
  const theme = ensureCodeTheme();
  const revealLine = props.revealLine ?? null;
  const host = useRef<HTMLDivElement>(null);
  const options = useMemo<FileOptions<unknown, undefined>>(
    () => ({
      disableFileHeader: true,
      overflow: props.wrap ? "wrap" : "scroll",
      theme,
      themeType: scheme,
      // The WASM engine: Shiki's JS regex engine can backtrack without end on hostile input.
      preferredHighlighter: "shiki-wasm",
      unsafeCSS: CODE_SURFACE_CSS,
      ...(revealLine !== null
        ? {
            onPostRender: (fileContainer: HTMLElement) => {
              const row = fileContainer.shadowRoot?.querySelector<HTMLElement>(
                `[data-line="${revealLine}"]`,
              );
              row?.scrollIntoView?.({ block: "center" });
            },
          }
        : {}),
    }),
    [props.wrap, revealLine, scheme, theme],
  );

  useEffect(() => {
    const wrapper = host.current;
    if (!wrapper) return;
    const instance = new PierreFile<unknown, undefined>(options);
    instance.render({
      file: {
        name: props.name,
        contents: props.text,
        ...(props.cacheKey ? { cacheKey: props.cacheKey } : {}),
      },
      containerWrapper: wrapper,
    });
    if (revealLine !== null) instance.setSelectedLines({ start: revealLine, end: revealLine });
    return () => instance.cleanUp();
  }, [options, props.name, props.text, props.cacheKey, revealLine]);

  return <div ref={host} className="min-h-0 flex-1 overflow-auto" data-source-view />;
}
