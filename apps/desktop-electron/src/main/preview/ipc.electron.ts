import { BrowserWindow, ipcMain } from "electron";
import { type IpcResult, PREVIEW_CH } from "../../shared/contracts";
import { assertTrustedSender } from "../security/policy";
import type { StudioPreviewView } from "./view.electron";

/** Studio's preview view, for the console's top-level frame only (SD1 Task 26). */
export function registerPreviewIpc(preview: StudioPreviewView): void {
	ipcMain.handle(
		PREVIEW_CH.show,
		async (e, url: unknown, rect: unknown): Promise<IpcResult<void>> => {
			assertTrustedSender(e);
			const win = BrowserWindow.fromWebContents(e.sender);
			if (!win) return { ok: false, code: "bad_request", message: "No window" };
			return preview.show(win, url, rect);
		},
	);
	ipcMain.handle(PREVIEW_CH.hide, async (e) => {
		assertTrustedSender(e);
		preview.hide();
	});
	ipcMain.handle(PREVIEW_CH.reload, async (e) => {
		assertTrustedSender(e);
		preview.reload();
	});
	ipcMain.handle(PREVIEW_CH.back, async (e) => {
		assertTrustedSender(e);
		preview.back();
	});
	ipcMain.handle(PREVIEW_CH.forward, async (e) => {
		assertTrustedSender(e);
		preview.forward();
	});
}
