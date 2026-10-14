// The thread panel's Preview tab (SD1 Task 26; §53 §5.8, Shared contracts `?panel=preview`):
// the dev servers loams-agentd discovered under the thread's project (`WatchPreviews`),
// a URL bar, the project's actions to start one, and the preview itself: on the desktop
// a WebContentsView placed by Electron main (D678), in a browser the remote agentd's
// `/preview/…` in a sandboxed frame (Q806), or, where neither exists, the command that
// forwards the preview proxy's port.

import { createClient, type Transport } from '@connectrpc/connect';
import { skipToken } from '@connectrpc/connect-query';
import { useAgentdQuery, useAgentdTransport } from '@loams/console-host/query';
import { PreviewService, type PreviewSnapshot, ProjectService } from '@loams/proto/agentd';
import {
  type DiscoveredServer,
  forwardCommand,
  mergeServers,
  openDiscoveredPort,
  type PreviewableServer,
  PreviewChromeRow,
  PreviewEmptyState,
  type PreviewReach,
  type PreviewStartAction,
  type PreviewTarget,
  PreviewUnreachable,
  parsePreviewUrl,
} from '@loams/studio-ui/preview';
import { Button, Empty, Notice, Snippet } from '@loams/ui';
import { ExternalLink } from 'lucide-react';
import { useCallback, useMemo, useState } from 'react';
import { useWatch } from '../sidebar/data.js';
import { type DesktopPreviewState, PreviewUnsupported, type StudioPreview } from './access.js';
import { DesktopSurface } from './DesktopSurface.js';
import { RemoteFrame } from './RemoteFrame.js';

/** The preview proxy's port when a snapshot does not say (`PREVIEW_PROXY_PORT`). */
export const DEFAULT_PROXY_PORT = 7331;

/** The thread's previews: each frame is the whole list (ndjson `WatchPreviews`). */
export function watchPreviews(threadId: string) {
  return async function* (transport: Transport, signal: AbortSignal) {
    const client = createClient(PreviewService, transport);
    for await (const frame of client.watchPreviews({ chatId: threadId }, { signal })) {
      if (frame.snapshot) yield frame.snapshot;
    }
  };
}

export function serversOf(snapshot: PreviewSnapshot | undefined): ReadonlyArray<PreviewableServer> {
  if (!snapshot) return [];
  const servers: DiscoveredServer[] = snapshot.services.map((s) => ({
    id: s.id,
    name: s.name,
    hostname: s.hostname,
    port: s.port,
    pid: s.pid,
    cwd: s.cwd,
    projectName: s.projectName,
    startedAtMs: Number(s.startedAtMs),
  }));
  return mergeServers({ servers, proxyPort: snapshot.proxyPort || DEFAULT_PROXY_PORT });
}

/** Where the open preview is, and what the URL bar shows. */
type Open = PreviewTarget & { display: string; server?: PreviewableServer };

const NO_STATE: DesktopPreviewState = {
  url: '',
  canGoBack: false,
  canGoForward: false,
  loading: false,
};

export function PreviewTab({
  threadId,
  preview,
  openExternal,
  spaceId,
}: {
  threadId: string;
  /** How previews are reached; absent on the web means the agentd serves none (Q806). */
  preview?: StudioPreview;
  /** Opens an http(s) link in the person's browser. */
  openExternal?: (url: string) => Promise<void> | void;
  /** The thread's project (agentd space), for its actions; none hides "start". */
  spaceId?: string;
}) {
  const transport = useAgentdTransport();
  const watch = useWatch(transport ? watchPreviews(threadId) : null, `previews:${threadId}`);
  const snapshot = watch.data;
  const servers = useMemo(() => serversOf(snapshot), [snapshot]);
  const proxyPort = snapshot?.proxyPort || DEFAULT_PROXY_PORT;
  const [unsupported, setUnsupported] = useState(false);
  const reach: PreviewReach = preview && !unsupported ? preview.reach() : { kind: 'forward' };
  const [open, setOpen] = useState<Open | null>(null);
  const [opening, setOpening] = useState(false);
  const [problem, setProblem] = useState<string>();
  const [viewState, setViewState] = useState<DesktopPreviewState>(NO_STATE);
  const [reloadKey, setReloadKey] = useState(0);

  const external = useCallback(
    (url: string) => {
      if (!/^https?:\/\//i.test(url)) return;
      if (openExternal) void Promise.resolve(openExternal(url)).catch(() => {});
      else window.open(url, '_blank', 'noopener,noreferrer');
    },
    [openExternal],
  );

  const openServer = useCallback(
    async (server: PreviewableServer, path = '/') => {
      setProblem(undefined);
      setOpening(true);
      try {
        const target = await openDiscoveredPort({ server, reach, proxyPort });
        const tail = path.replace(/^\/+/, '');
        const display = new URL(tail, server.url).href;
        if (target.kind === 'remote') {
          setOpen({ ...target, url: `${target.url}${tail}`, display, server });
        } else {
          setOpen({ ...target, url: display, display, server });
        }
        setViewState(NO_STATE);
      } catch (e) {
        if (e instanceof PreviewUnsupported) setUnsupported(true);
        else setProblem(e instanceof Error ? e.message : String(e));
      } finally {
        setOpening(false);
      }
    },
    [reach, proxyPort],
  );

  const submit = (raw: string) => {
    setProblem(undefined);
    const url = parsePreviewUrl(raw);
    if (!url) {
      setProblem('Only addresses on the daemon’s machine open here, such as localhost:5173.');
      return;
    }
    const parsed = new URL(url);
    const server = servers.find((s) => s.hostname.toLowerCase() === parsed.hostname.toLowerCase());
    if (reach.kind === 'loopback') {
      setOpen({ kind: 'loopback', url, display: url, ...(server ? { server } : {}) });
      setViewState(NO_STATE);
      return;
    }
    if (!server) {
      setProblem('In a browser, only the servers listed here can be opened.');
      return;
    }
    void openServer(server, `${parsed.pathname}${parsed.search}${parsed.hash}`);
  };

  const actions = useAgentdQuery(
    ProjectService.method.listProjectActions,
    spaceId ? { spaceId } : skipToken,
  );
  const startActions: PreviewStartAction[] =
    actions.data?.snapshot?.actions.map((a) => ({ id: a.id, name: a.name })) ?? [];
  const [starting, setStarting] = useState(false);
  const start = async (action: PreviewStartAction) => {
    if (!transport || !spaceId) return;
    setStarting(true);
    setProblem(undefined);
    try {
      await createClient(ProjectService, transport).runProjectAction({
        spaceId,
        chatId: threadId,
        actionId: action.id,
        cols: 120,
        rows: 32,
      });
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    } finally {
      setStarting(false);
    }
  };

  if (!transport) {
    return (
      <div className="p-3">
        <Empty title="No previews to show" seed={4}>
          The daemon is not reachable, so this thread&rsquo;s dev servers cannot be found.
        </Empty>
      </div>
    );
  }
  if (watch.unsupported) {
    return (
      <div className="p-3">
        <Empty title="Previews need a newer loams-agentd" seed={4}>
          This daemon does not discover dev servers.
        </Empty>
      </div>
    );
  }

  const desktopView = open?.kind === 'loopback' ? preview?.view : undefined;
  const chromeUrl = desktopView && viewState.url ? viewState.url : (open?.display ?? '');
  const forwardHint =
    reach.kind === 'forward' ? (
      <ForwardHint proxyPort={proxyPort} {...(reach.sshHost ? { sshHost: reach.sshHost } : {})} />
    ) : undefined;

  return (
    <div className="-mx-3 flex min-h-0 flex-1 flex-col border-t border-rule-soft" data-preview-tab>
      <PreviewChromeRow
        url={chromeUrl}
        loading={opening || viewState.loading}
        canGoBack={!!desktopView && viewState.canGoBack}
        canGoForward={!!desktopView && viewState.canGoForward}
        refreshDisabled={!open}
        onBack={() => void desktopView?.back()}
        onForward={() => void desktopView?.forward()}
        onRefresh={() => {
          if (desktopView) void desktopView.reload();
          // An expired ticket is replaced by a new one, at the same path.
          else if (open?.kind === 'remote' && open.server && open.expiresAtMs <= Date.now()) {
            const display = new URL(open.display);
            void openServer(open.server, `${display.pathname}${display.search}`);
          } else setReloadKey((k) => k + 1);
        }}
        onSubmit={submit}
        {...(open
          ? { onOpenInBrowser: () => external(chromeUrl), onClose: () => setOpen(null) }
          : {})}
      />
      <div className="flex min-h-0 flex-1 flex-col px-3 pb-3">
        {snapshot?.error && (
          <Notice tone="warn" title="The preview proxy reports a problem">
            {snapshot.error}
          </Notice>
        )}
        {watch.error && !snapshot && (
          <Notice tone="danger" title="The dev server list could not be read">
            {watch.error.message}
          </Notice>
        )}
        {problem && <Notice tone="danger" title={problem} />}
        {!open ? (
          <PreviewEmptyState
            servers={servers}
            onOpenServer={(server) => void openServer(server)}
            startActions={startActions}
            {...(spaceId ? { onStart: (a: PreviewStartAction) => void start(a) } : {})}
            starting={starting}
            {...(forwardHint ? { footer: forwardHint } : {})}
          />
        ) : open.kind === 'remote' ? (
          <div className="flex min-h-0 flex-1 flex-col pt-3">
            <RemoteFrame url={open.url} reloadKey={reloadKey} />
          </div>
        ) : open.kind === 'forward' ? (
          <div className="flex flex-col gap-3 py-4">
            <ForwardHint proxyPort={proxyPort} />
            <div>
              <Button size="sm" onClick={() => external(open.url)}>
                <ExternalLink aria-hidden className="size-3.5" /> Open {open.url}
              </Button>
            </div>
          </div>
        ) : desktopView && viewState.error ? (
          <PreviewUnreachable
            url={viewState.error.url || open.url}
            code={viewState.error.code}
            description={viewState.error.description}
            onReload={() => void desktopView.reload()}
            onOpenInBrowser={() => external(open.url)}
          />
        ) : desktopView ? (
          <div className="flex min-h-0 flex-1 flex-col pt-3">
            <DesktopSurface view={desktopView} url={open.url} onState={setViewState} />
          </div>
        ) : (
          <div className="py-4">
            <Empty
              title="Open this preview in your browser"
              seed={5}
              actions={
                <Button size="sm" onClick={() => external(open.url)}>
                  <ExternalLink aria-hidden className="size-3.5" /> Open in browser
                </Button>
              }
            >
              This version of Loams Desktop cannot show previews inside Studio.
            </Empty>
          </div>
        )}
      </div>
    </div>
  );
}

/** Before Q806, the port has to be forwarded by hand (§53 §5.8). */
export function ForwardHint({ proxyPort, sshHost }: { proxyPort: number; sshHost?: string }) {
  return (
    <div className="flex flex-col gap-2 text-sm text-muted" data-preview-forward-hint>
      <p className="m-0">
        This agentd does not serve previews to the browser. Forward its preview port to this
        computer, then open a server&rsquo;s address in a new tab:
      </p>
      <Snippet>{forwardCommand(proxyPort, sshHost)}</Snippet>
    </div>
  );
}
