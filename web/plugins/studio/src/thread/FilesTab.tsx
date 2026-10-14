// The thread panel's Files tab (SD1 Task 21; §53 §5.5, Shared contracts `?panel=files`):
// the thread's worktree as a tree and a read-only viewer, quick open ("Go to file") and
// search in files. Data comes from the daemon over the agentd transport of the nearest
// `AgentdScope`; the tree's "Mention in composer" queues the path for the thread's
// composer (thread/mentions.tsx).

import { useAgentdTransport } from '@loams/console-host/query';
import {
  type ContentMatch,
  ContentSearchDialog,
  type ContentSearchSource,
  FilePicker,
  FileViewer,
  type FileViewerProps,
  pendingContentSearch,
} from '@loams/studio-ui/files';
import { createMarkdownFileRenderer } from '@loams/studio-ui/markdown';
import { Button, Empty, Tooltip } from '@loams/ui';
import { FileSearch, TextSearch } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import type { FileLink } from '../routes.js';
import { agentdFilesSource } from './files-source.js';
import { type ComposerMentions, MentionChips, mentionsFor } from './mentions.js';

const PENDING_CONTENT_SEARCH = pendingContentSearch();

export function FilesTab({
  threadId,
  projectName = 'Worktree',
  mentions = mentionsFor(threadId),
  contentSearch = PENDING_CONTENT_SEARCH,
  renderMarkdown,
  link,
}: {
  threadId: string;
  /** A file the route names (`?panel=files&path=…&line=…`): a terminal's file link. */
  link?: FileLink;
  /** The root crumb's name: the thread's project. */
  projectName?: string;
  mentions?: ComposerMentions;
  /** SD1 Task 11's `SearchWorkspaceContent`; says it is unavailable until then. */
  contentSearch?: ContentSearchSource;
  /** The markdown renderer; defaults to SD1 Task 18's sanitised one (T21-3). */
  renderMarkdown?: FileViewerProps['renderMarkdown'];
}) {
  const transport = useAgentdTransport();
  const source = useMemo(
    () => (transport ? agentdFilesSource(transport, { chatId: threadId }) : undefined),
    [transport, threadId],
  );
  const [open, setOpen] = useState<{ path: string; line: number | null } | null>(null);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const linkPath = link?.path;
  const linkLine = link?.line;
  // A link opens its file at its line. The viewer is read-only, so a column only says
  // where on the line the match was; the whole line is selected.
  useEffect(() => {
    if (linkPath) setOpen({ path: linkPath, line: linkLine ?? null });
  }, [linkPath, linkLine]);

  if (!source) {
    return (
      <div className="p-3">
        <Empty title="No files to show" seed={2}>
          The daemon is not reachable, so this thread's files cannot be read.
        </Empty>
      </div>
    );
  }

  const openFile = (path: string) => setOpen({ path, line: null });
  // SD1 Task 18's renderer (T21-3): a document's relative links open here.
  const markdown = useMemo(
    () =>
      renderMarkdown ??
      createMarkdownFileRenderer({ onOpenFile: (path) => setOpen({ path, line: null }) }),
    [renderMarkdown],
  );
  const openMatch = (match: ContentMatch) => setOpen({ path: match.path, line: match.line });

  return (
    <div className="-mx-3 flex min-h-0 flex-1 flex-col border-t border-rule-soft" data-files-tab>
      <FileViewer
        key={source.key}
        source={source}
        projectName={projectName}
        path={open?.path ?? null}
        revealLine={open?.line ?? null}
        onOpenFile={openFile}
        onMention={mentions.mention}
        renderMarkdown={markdown}
        actions={
          <>
            <Tooltip content="Go to file">
              <Button
                variant="quiet"
                size="icon"
                aria-label="Go to file"
                onClick={() => setPickerOpen(true)}
              >
                <FileSearch aria-hidden className="size-3.5" />
              </Button>
            </Tooltip>
            <Tooltip content="Search in files">
              <Button
                variant="quiet"
                size="icon"
                aria-label="Search in files"
                onClick={() => setSearchOpen(true)}
              >
                <TextSearch aria-hidden className="size-3.5" />
              </Button>
            </Tooltip>
          </>
        }
      />
      <MentionChips mentions={mentions} />
      <FilePicker
        open={pickerOpen}
        onOpenChange={setPickerOpen}
        source={source}
        projectName={projectName}
        onOpenFile={openFile}
      />
      <ContentSearchDialog
        open={searchOpen}
        onOpenChange={setSearchOpen}
        source={contentSearch}
        projectName={projectName}
        onOpenMatch={openMatch}
      />
    </div>
  );
}
