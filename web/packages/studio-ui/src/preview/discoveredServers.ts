// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/preview/useDiscoveredLocalServers.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the server list is loams-agentd's WatchPreviews snapshot (stable *.localhost URLs through its preview proxy) instead of T3's port-scanner state and configured URLs; the hook became pure functions. Loams' changes are Apache-2.0.

/** One dev server as loams-agentd discovered it (`PreviewServer`). */
export interface DiscoveredServer {
  id: string;
  /** The process's role, such as "web" or "api". */
  name: string;
  /** `<device>.<service>.localhost`, served by the daemon's preview proxy. */
  hostname: string;
  /** The process's own port. */
  port: number;
  pid: number;
  cwd: string;
  projectName: string;
  startedAtMs: number;
}

export interface PreviewableServer extends DiscoveredServer {
  /** The stable loopback URL through the preview proxy. */
  url: string;
  /** `<device>.<service>`: the remote listener's `/preview/<name>/` segment. */
  previewName: string;
}

const LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

/** `<a>.<b>` for a preview hostname `<a>.<b>.localhost`, else undefined. */
export function previewNameOf(hostname: string): string | undefined {
  const labels = hostname.toLowerCase().split(".");
  if (labels.length !== 3 || labels[2] !== "localhost") return undefined;
  const [a, b] = labels;
  return a && b && LABEL.test(a) && LABEL.test(b) ? `${a}.${b}` : undefined;
}

/**
 * The servers a snapshot lists, each with its proxy URL, sorted by name and then by
 * port. A server whose hostname is not the proxy's shape is left out: it could not be
 * opened through the proxy anyway.
 */
export function mergeServers(input: {
  servers: ReadonlyArray<DiscoveredServer>;
  proxyPort: number;
}): ReadonlyArray<PreviewableServer> {
  const live: PreviewableServer[] = [];
  const seen = new Set<string>();
  for (const server of input.servers) {
    const previewName = previewNameOf(server.hostname);
    if (!previewName || seen.has(previewName)) continue;
    seen.add(previewName);
    live.push({
      ...server,
      previewName,
      url: `http://${server.hostname.toLowerCase()}:${input.proxyPort}/`,
    });
  }
  return live.toSorted((a, b) => a.name.localeCompare(b.name) || a.port - b.port);
}

/** Whether `host` (no port) names this machine. */
export function isLoopbackHost(host: string): boolean {
  const h = host.toLowerCase().replace(/^\[|\]$/g, "");
  return (
    h === "localhost" || h.endsWith(".localhost") || h === "::1" || /^127(?:\.\d{1,3}){3}$/.test(h)
  );
}

/**
 * The URL bar's input as a loopback http(s) URL, or undefined. A bare `host:port` or
 * `:port` gets `http://`. Nothing else is opened in the preview: other sites go to the
 * person's browser.
 */
export function parsePreviewUrl(raw: string): string | undefined {
  const text = raw.trim();
  if (!text) return undefined;
  const withScheme = /^[a-z][a-z0-9+.-]*:\/\//i.test(text)
    ? text
    : `http://${text.startsWith(":") ? `localhost${text}` : text}`;
  try {
    const url = new URL(withScheme);
    if (url.protocol !== "http:" && url.protocol !== "https:") return undefined;
    if (url.username || url.password) return undefined;
    return isLoopbackHost(url.hostname) ? url.href : undefined;
  } catch {
    return undefined;
  }
}
