// Pure policy for Studio's preview view (design §53 §5.8; plan SD1 Task 26): no
// electron import, unit-testable. The preview is a WebContentsView placed by main, as
// D678's factory views are, with the same hardening shape: its own persistent
// partition, no preload, sandboxed, and navigation, permission and download rules.
// Dev servers are untrusted content: they run whatever the project's code serves.
import type { NavDecision } from "../security/policy";

/** One partition for every preview, apart from the console and the factory apps. */
export const PREVIEW_PARTITION = "persist:studio-preview";

/** Web preferences for the preview view: sandboxed, no preload, no bridge. */
export function previewWebPreferences() {
	return {
		partition: PREVIEW_PARTITION,
		preload: undefined,
		sandbox: true,
		contextIsolation: true,
		nodeIntegration: false,
		nodeIntegrationInSubFrames: false,
		webSecurity: true,
		webviewTag: false,
		safeDialogs: true,
		spellcheck: false,
	} as const;
}

function parse(raw: string): URL | null {
	try {
		return new URL(raw);
	} catch {
		return null;
	}
}

/** This machine: `localhost`, `*.localhost` (the daemon's preview proxy), 127/8, ::1. */
export function isLoopbackHost(host: string): boolean {
	const h = host.toLowerCase().replace(/^\[|\]$/g, "");
	return (
		h === "localhost" ||
		h.endsWith(".localhost") ||
		h === "::1" ||
		/^127(?:\.\d{1,3}){3}$/.test(h)
	);
}

/**
 * What the preview may show: an http(s) URL on this machine, without userinfo (a
 * spoofing vector). The daemon's discovered servers are `http://<a>.<b>.localhost:7331`.
 */
export function isPreviewUrl(raw: unknown): raw is string {
	if (typeof raw !== "string" || raw.length > 4096) return false;
	const u = parse(raw);
	return (
		!!u &&
		(u.protocol === "http:" || u.protocol === "https:") &&
		u.username === "" &&
		u.password === "" &&
		isLoopbackHost(u.hostname)
	);
}

/**
 * Navigation per frame. The main frame stays in the view on this machine (a dev app may
 * move between its own servers, such as its API's sign-in page); any other http(s) URL
 * opens in the person's browser; everything else (file:, javascript:, custom schemes)
 * is denied. A subframe may load only this machine; it is never handed to the browser.
 */
export function previewNavigation(
	to: string,
	isMainFrame: boolean,
): NavDecision {
	if (isPreviewUrl(to)) return "allow";
	if (!isMainFrame) return "deny";
	const u = parse(to);
	return u && (u.protocol === "http:" || u.protocol === "https:")
		? "external"
		: "deny";
}

/** Only sanitized clipboard writes, from a page on this machine; nothing else. */
export function previewPermission(
	permission: string,
	requestingUrl: string,
): boolean {
	return (
		permission === "clipboard-sanitized-write" && isPreviewUrl(requestingUrl)
	);
}

/** Downloads only from this machine; Electron's own save dialog asks where. */
export function previewDownload(url: string): "allow" | "deny" {
	return isPreviewUrl(url) ? "allow" : "deny";
}
