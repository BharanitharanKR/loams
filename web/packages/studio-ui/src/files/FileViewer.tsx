// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/FilePreviewPanel.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// read-only (no editor, saves, comments or attachments), over a FilesSource; images as blob: URLs from agentd's bytes; markdown through a renderer the caller passes; no browser, PDF, video or audio preview. Loams' changes are Apache-2.0.

// The file viewer (SD1 Task 21; design §53 §2.2, §5.5, D919). Studio has no editor: a file
// is shown read-only as highlighted text (@pierre/diffs on its shadow-root path), as
// rendered markdown when the caller passes Task 18's renderer, as a table for CSV and TSV,
// or, for a checkout image, from its bytes. Breadcrumbs lead back to the tree, which sits
// beside the file or fills the panel when no file is open. The daemon's change stream
// keeps both current.

import { cx } from "@loams/ui";
import { Code2, Eye, FolderTree, Table2, WrapTextIcon } from "lucide-react";
import { type ReactNode, useState } from "react";

import { DelimitedTablePreview } from "./DelimitedTablePreview";
import { FileBreadcrumbs } from "./FileBreadcrumbs";
import { FileBrowser } from "./FileBrowser";
import { filePreviewMode, readOnlyReasonMessage } from "./filePreviewMode";
import {
  FILE_SURFACE_SUBHEADER_CLASS,
  FileSurfaceAction,
  FileSurfaceFailure,
  FileSurfaceLoading,
  FileSurfaceNotice,
} from "./fileSurfaceChrome";
import SourceView from "./SourceView";
import type { FilesSource } from "./source";
import { useFilesWatch, useFileText, useImageUrl } from "./useDirectory";

export interface FileViewerProps {
  readonly source: FilesSource;
  readonly projectName: string;
  /** The open file, relative to the checkout; null shows the tree alone. */
  readonly path: string | null;
  /** 1-based line to select and scroll to. */
  readonly revealLine?: number | null;
  readonly onOpenFile: (relativePath: string) => void;
  /** "Mention in composer" from the tree. */
  readonly onMention?: (relativePath: string) => void;
  /**
   * Renders markdown under SD1 Task 18's rules (no raw HTML, no remote images, http(s)
   * links only). Without it markdown is shown as its source.
   */
  readonly renderMarkdown?: (text: string, path: string) => ReactNode;
  /** More header actions (Go to file, Search in files). */
  readonly actions?: ReactNode;
}

function renderedToggleLabel(mode: "markdown" | "table", rendered: boolean): string {
  if (mode === "markdown") return rendered ? "Show markdown source" : "Show rendered markdown";
  return rendered ? "Show source" : "Show table";
}

function WorkspaceImagePreview(props: { readonly source: FilesSource; readonly path: string }) {
  const image = useImageUrl(props.source, props.path);
  if (image.state === "error") {
    return <FileSurfaceFailure message={image.message} onRetry={image.retry} />;
  }
  if (image.state === "loading") return <FileSurfaceLoading label="Loading image" />;
  return (
    <div className="flex min-h-0 flex-1 items-center justify-center overflow-auto p-4">
      <img className="max-h-full max-w-full object-contain" src={image.url} alt={props.path} />
    </div>
  );
}

function TextSurface(props: {
  readonly source: FilesSource;
  readonly path: string;
  readonly mode: "markdown" | "table" | "source";
  readonly delimiter: "," | "\t" | null;
  readonly rendered: boolean;
  readonly wrap: boolean;
  readonly revealLine: number | null;
  readonly renderMarkdown: FileViewerProps["renderMarkdown"];
}) {
  const file = useFileText(props.source, props.path);
  if (file.error && !file.data) {
    return (
      <FileSurfaceFailure
        message={file.error instanceof Error ? file.error.message : "The file could not be read."}
        onRetry={() => void file.refetch()}
      />
    );
  }
  if (!file.data) return <FileSurfaceLoading />;
  const { text } = file.data;
  if (text === null)
    return <FileSurfaceFailure message={readOnlyReasonMessage(file.data.readOnlyReason)} />;
  return (
    <>
      {file.data.truncated ? (
        <FileSurfaceNotice>
          Preview limited to the first part of a {file.data.size.toLocaleString("en-US")} byte file.
        </FileSurfaceNotice>
      ) : null}
      {props.mode === "markdown" && props.rendered && props.renderMarkdown ? (
        <div className="min-h-0 flex-1 overflow-auto" data-markdown-preview>
          <div className="mx-auto max-w-4xl px-6 py-5">
            {props.renderMarkdown(text, props.path)}
          </div>
        </div>
      ) : props.mode === "table" && props.rendered && props.delimiter ? (
        <DelimitedTablePreview name={props.path} text={text} delimiter={props.delimiter} />
      ) : (
        <SourceView
          name={props.path}
          text={text}
          wrap={props.wrap}
          revealLine={props.revealLine}
          {...(file.data.contentHash ? { cacheKey: `${props.path}:${file.data.contentHash}` } : {})}
        />
      )}
    </>
  );
}

export function FileViewer({
  source,
  projectName,
  path,
  revealLine = null,
  onOpenFile,
  onMention,
  renderMarkdown,
  actions,
}: FileViewerProps) {
  useFilesWatch(source);
  const [explorerOpen, setExplorerOpen] = useState(true);
  const [wrap, setWrap] = useState(false);
  // Reading markdown rendered, and tables as tables, are preferences, not per file.
  const [renderMarkdownPreferred, setRenderMarkdownPreferred] = useState(true);
  const [renderTablePreferred, setRenderTablePreferred] = useState(true);
  const [revealDirectory, setRevealDirectory] = useState<{ path: string; id: number } | null>(null);

  const mode = path ? filePreviewMode(path) : null;
  const renderedMode =
    mode?.kind === "markdown" && renderMarkdown
      ? ("markdown" as const)
      : mode?.kind === "table"
        ? ("table" as const)
        : null;
  // A line to reveal only exists in the source, so it wins over the preference.
  const rendered =
    revealLine === null &&
    (renderedMode === "markdown"
      ? renderMarkdownPreferred
      : renderedMode === "table"
        ? renderTablePreferred
        : false);
  const showsRawText = path !== null && mode?.kind !== "image" && !rendered;
  const showExplorer = explorerOpen || path === null;

  const showDirectory = (directory: string) => {
    setExplorerOpen(true);
    setRevealDirectory((previous) => ({ path: directory, id: (previous?.id ?? 0) + 1 }));
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden bg-bg" data-file-viewer>
      <div className={FILE_SURFACE_SUBHEADER_CLASS} data-surface-subheader>
        <div className="min-w-0 flex-1 overflow-x-auto">
          {path ? (
            <FileBreadcrumbs
              projectName={projectName}
              relativePath={path}
              onRevealDirectory={showDirectory}
            />
          ) : (
            <span className="px-1 text-[0.78rem] font-medium text-ink">{projectName}</span>
          )}
        </div>
        {actions}
        {renderedMode ? (
          <FileSurfaceAction
            label={renderedToggleLabel(renderedMode, rendered)}
            pressed={rendered}
            onPress={() =>
              renderedMode === "markdown"
                ? setRenderMarkdownPreferred(!rendered)
                : setRenderTablePreferred(!rendered)
            }
          >
            {rendered ? (
              <Code2 aria-hidden className="size-3.5" />
            ) : renderedMode === "table" ? (
              <Table2 aria-hidden className="size-3.5" />
            ) : (
              <Eye aria-hidden className="size-3.5" />
            )}
          </FileSurfaceAction>
        ) : null}
        {showsRawText ? (
          <FileSurfaceAction
            label={wrap ? "Disable word wrap" : "Enable word wrap"}
            pressed={wrap}
            onPress={() => setWrap(!wrap)}
          >
            <WrapTextIcon aria-hidden className="size-3.5" />
          </FileSurfaceAction>
        ) : null}
        {path !== null ? (
          <FileSurfaceAction
            label={explorerOpen ? "Hide file explorer" : "Show file explorer"}
            pressed={explorerOpen}
            onPress={() => setExplorerOpen(!explorerOpen)}
          >
            <FolderTree aria-hidden className="size-3.5" />
          </FileSurfaceAction>
        ) : null}
      </div>
      <div className="flex min-h-0 flex-1 overflow-hidden">
        {path && mode ? (
          <div className="flex min-w-0 flex-1 flex-col overflow-hidden" data-file-surface>
            {mode.kind === "image" ? (
              <WorkspaceImagePreview key={path} source={source} path={path} />
            ) : (
              <TextSurface
                key={path}
                source={source}
                path={path}
                mode={mode.kind}
                delimiter={mode.kind === "table" ? mode.delimiter : null}
                rendered={rendered}
                wrap={wrap}
                revealLine={revealLine}
                renderMarkdown={renderMarkdown}
              />
            )}
          </div>
        ) : null}
        {showExplorer ? (
          <aside
            aria-label="File explorer"
            className={cx(
              "flex min-h-0 shrink-0 overflow-hidden bg-bg",
              path ? "w-[min(16rem,42%)] min-w-36 border-l border-rule-soft" : "min-w-0 flex-1",
            )}
          >
            <FileBrowser
              source={source}
              projectName={projectName}
              selectedPath={path}
              revealDirectory={revealDirectory}
              onOpenFile={onOpenFile}
              {...(onMention ? { onMention } : {})}
            />
          </aside>
        ) : null}
      </div>
    </div>
  );
}
