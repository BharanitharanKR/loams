// The web's preview surface (SD1 Task 26, Q806): the remote agentd's
// `/preview/pt_<ticket>/` in a sandboxed iframe.
//
// The page is a dev server's: untrusted. The sandbox drops top navigation, popups that
// escape it, plugins and the pointer lock; `allow` grants no device permission; no
// referrer leaves. `allow-same-origin` is kept only while the frame's origin is not
// the console's own, so the page keeps its storage on the agentd's origin and can
// never reach the console's (were they one origin, scripts plus same-origin would let
// the frame lift its own sandbox, so the frame gets an opaque origin instead). The
// listener answers every preview with `frame-ancestors` naming the console origins
// only, so no other site can frame it.

const BASE_SANDBOX = 'allow-scripts allow-forms allow-modals allow-downloads';

/** The sandbox for a frame of `url` inside a page at `consoleOrigin`. */
export function frameSandbox(url: string, consoleOrigin: string): string {
  let origin: string | undefined;
  try {
    origin = new URL(url).origin;
  } catch {
    origin = undefined;
  }
  return origin && origin !== 'null' && origin !== consoleOrigin
    ? `${BASE_SANDBOX} allow-same-origin`
    : BASE_SANDBOX;
}

/** Only an https (or loopback http) URL on the agentd host is framed. */
export function frameable(url: string): boolean {
  try {
    const u = new URL(url);
    if (u.username || u.password) return false;
    if (u.protocol === 'https:') return true;
    return u.protocol === 'http:' && /^(localhost|127\.0\.0\.1|\[::1\])$/.test(u.hostname);
  } catch {
    return false;
  }
}

export function RemoteFrame({ url, reloadKey }: { url: string; reloadKey: number }) {
  if (!frameable(url)) return null;
  return (
    <iframe
      key={reloadKey}
      src={url}
      title="Preview"
      sandbox={frameSandbox(url, window.location.origin)}
      allow=""
      referrerPolicy="no-referrer"
      loading="eager"
      data-preview-surface="web"
      className="min-h-40 w-full flex-1 border border-rule-soft bg-bg"
    />
  );
}
