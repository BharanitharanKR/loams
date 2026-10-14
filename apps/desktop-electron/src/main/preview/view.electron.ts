import {
	type BrowserWindow,
	shell,
	type WebContents,
	WebContentsView,
} from "electron";
import {
	type IpcResult,
	PREVIEW_CH,
	type PreviewViewState,
} from "../../shared/contracts";
import { clampBounds, isFocusConsoleKey } from "../factory/embed-model";
import {
	isPreviewUrl,
	previewDownload,
	previewNavigation,
	previewPermission,
	previewWebPreferences,
} from "./policy";

const bad = (message: string): IpcResult<never> => ({
	ok: false,
	code: "bad_request",
	message,
});

/**
 * Studio's Preview tab on the desktop (design §53 §5.8; SD1 Task 26): one
 * WebContentsView inside the console's window, placed over the rect the tab reports,
 * as D678 places the factory apps. It runs in `persist:studio-preview` with no preload,
 * sandboxed, and the policy in ./policy: the console's origin, bridge and token are
 * never in reach of a dev server's page.
 */
export class StudioPreviewView {
	#win: BrowserWindow | undefined;
	#view: WebContentsView | undefined;
	#requested = "";
	#error: PreviewViewState["error"];
	readonly #sessions = new WeakSet<object>();

	show(win: BrowserWindow, url: unknown, rect: unknown): IpcResult<void> {
		if (!isPreviewUrl(url))
			return bad("Only http(s) addresses on this computer open in the preview");
		this.#attach(win);
		const [width, height] = win.getContentSize();
		const bounds = clampBounds(
			rect,
			{ width: width ?? 0, height: height ?? 0 },
			win.webContents.getZoomFactor(),
		);
		if (bounds === undefined) {
			this.hide();
			return bad("Invalid bounds");
		}
		if (bounds === null) {
			this.hide();
			return { ok: true, value: undefined };
		}
		const view = this.#view ?? this.#create(win);
		if (url !== this.#requested) {
			this.#requested = url;
			this.#error = undefined;
			void view.webContents.loadURL(url).catch(() => undefined);
		}
		view.setBounds(bounds);
		view.setVisible(true);
		return { ok: true, value: undefined };
	}

	hide(): void {
		const view = this.#view;
		if (!view) return;
		if (!view.webContents.isDestroyed() && view.webContents.isFocused())
			this.#win?.webContents.focus();
		view.setVisible(false);
	}

	reload(): void {
		const wc = this.#wc();
		if (!wc) return;
		this.#error = undefined;
		// After a failed load the page is Chromium's error page: load the address again.
		if (wc.getURL() === "" || wc.getURL().startsWith("chrome-error:"))
			void wc.loadURL(this.#requested).catch(() => undefined);
		else wc.reload();
	}

	back(): void {
		const wc = this.#wc();
		if (wc?.navigationHistory.canGoBack()) wc.navigationHistory.goBack();
	}

	forward(): void {
		const wc = this.#wc();
		if (wc?.navigationHistory.canGoForward()) wc.navigationHistory.goForward();
	}

	/** Quit, a new console window, or the window closing. */
	destroy(): void {
		const view = this.#view;
		this.#view = undefined;
		this.#requested = "";
		if (!view) return;
		const win = this.#win;
		if (win && !win.isDestroyed()) win.contentView.removeChildView(view);
		if (!view.webContents.isDestroyed()) view.webContents.close();
	}

	#wc(): WebContents | undefined {
		const wc = this.#view?.webContents;
		return wc && !wc.isDestroyed() ? wc : undefined;
	}

	#attach(win: BrowserWindow): void {
		if (this.#win === win) return;
		this.destroy();
		this.#win = win;
		// A crashed or navigating console can no longer hide the view: hide it here.
		win.webContents.on("render-process-gone", () => {
			if (this.#win === win) this.hide();
		});
		win.webContents.on(
			"did-start-navigation",
			(d: { isMainFrame?: boolean; isSameDocument?: boolean }) => {
				if (this.#win === win && d?.isMainFrame && !d.isSameDocument)
					this.hide();
			},
		);
		win.once("closed", () => {
			if (this.#win === win) {
				this.destroy();
				this.#win = undefined;
			}
		});
	}

	#create(win: BrowserWindow): WebContentsView {
		const view = new WebContentsView({
			webPreferences: previewWebPreferences(),
		});
		const wc = view.webContents;
		harden(wc, this.#sessions);
		wc.on("before-input-event", (event, input) => {
			if (!isFocusConsoleKey(input)) return;
			event.preventDefault();
			if (!win.isDestroyed()) win.webContents.focus();
		});
		const push = () => this.#push();
		wc.on("did-start-loading", push);
		wc.on("did-stop-loading", push);
		wc.on("did-navigate", () => {
			this.#error = undefined;
			push();
		});
		wc.on("did-navigate-in-page", push);
		wc.on(
			"did-fail-load",
			(
				_e,
				code: number,
				description: string,
				url: string,
				isMainFrame: boolean,
			) => {
				// -3 is ERR_ABORTED: a navigation replaced by another, not a failure.
				if (!isMainFrame || code === -3) return;
				this.#error = { code, description, url };
				push();
			},
		);
		view.setVisible(false);
		win.contentView.addChildView(view);
		this.#view = view;
		return view;
	}

	#push(): void {
		const win = this.#win;
		const wc = this.#wc();
		if (!win || win.isDestroyed() || !wc) return;
		const url = wc.getURL();
		const state: PreviewViewState = {
			url: url.startsWith("chrome-error:") ? this.#requested : url,
			canGoBack: wc.navigationHistory.canGoBack(),
			canGoForward: wc.navigationHistory.canGoForward(),
			loading: wc.isLoading(),
			...(this.#error ? { error: this.#error } : {}),
		};
		win.webContents.send(PREVIEW_CH.state, state);
	}
}

/**
 * The preview's guards: per webContents for navigation, popups and webviews; once per
 * session for downloads and permissions.
 */
export function harden(wc: WebContents, sessions: WeakSet<object>): void {
	const guard = (
		event: { preventDefault(): void },
		url: string,
		isMainFrame: boolean,
	): void => {
		const d = previewNavigation(url, isMainFrame);
		if (d === "allow") return;
		event.preventDefault();
		if (d === "external") void shell.openExternal(url);
	};
	wc.on("will-frame-navigate", (e) => guard(e, e.url, e.isMainFrame));
	wc.on("will-redirect", (e) => guard(e, e.url, e.isMainFrame));
	wc.setWindowOpenHandler(({ url }) => {
		const d = previewNavigation(url, true);
		// A popup on this machine opens in the preview itself; never a new window.
		if (d === "allow") void wc.loadURL(url).catch(() => undefined);
		else if (d === "external") void shell.openExternal(url);
		return { action: "deny" };
	});
	wc.on("will-attach-webview", (event) => event.preventDefault());
	const ses = wc.session;
	if (sessions.has(ses)) return;
	sessions.add(ses);
	ses.on("will-download", (event, item) => {
		if (previewDownload(item.getURL()) !== "allow") event.preventDefault();
	});
	ses.setPermissionCheckHandler(
		(_wc, permission, requestingOrigin, details) =>
			details.isMainFrame !== false &&
			previewPermission(permission, details.requestingUrl ?? requestingOrigin),
	);
	ses.setPermissionRequestHandler((_wc, permission, callback, details) =>
		callback(
			details.isMainFrame !== false &&
				previewPermission(permission, details.requestingUrl),
		),
	);
}
