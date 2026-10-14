// Loams Studio's desktop channels (SD1 Task 28): "Open in editor" (Q810) and the
// notification choices. Every value is checked here; the editor's URL or argv is
// built by editor.ts from a checked path, and run without a shell.
import { spawn } from "node:child_process";
import { statSync } from "node:fs";
import {
	BrowserWindow,
	dialog,
	ipcMain,
	type OpenDialogOptions,
	shell,
} from "electron";
import { CH, type IpcResult, type StudioEditor } from "../../shared/contracts";
import { assertTrustedSender } from "../security/policy";
import {
	parseNotifyPrefs,
	readSetting,
	STUDIO_NOTIFY_KEY,
	writeSetting,
} from "../settings";
import {
	EDITOR_KEY,
	editorLaunch,
	editorView,
	isLocalPath,
	launchEditor,
	readEditor,
} from "./editor";

const fail = (code: string, message: string) =>
	({ ok: false, code, message }) as const;
const ok = { ok: true, value: undefined } as const;

function positionOf(
	at: unknown,
): { line?: number; column?: number } | undefined {
	if (!at || typeof at !== "object") return undefined;
	const { line, column } = at as { line?: unknown; column?: unknown };
	return {
		...(typeof line === "number" ? { line } : {}),
		...(typeof column === "number" ? { column } : {}),
	};
}

function exists(p: string): boolean {
	try {
		statSync(p);
		return true;
	} catch {
		return false;
	}
}

export function registerStudioIpc(o: { settingsFile: string }): void {
	const choice = () =>
		readEditor(() =>
			readSetting<unknown>(o.settingsFile, EDITOR_KEY, undefined),
		);

	ipcMain.handle(
		CH.studioOpenInEditor,
		async (e, path: unknown, at: unknown): Promise<IpcResult<void>> => {
			assertTrustedSender(e);
			if (!isLocalPath(path))
				return fail("bad_params", "Expected an absolute local path");
			if (!exists(path))
				return fail("not_found", "That folder is not on this computer");
			const launch = editorLaunch(choice(), path, positionOf(at));
			if (!launch)
				return fail("no_editor", "Choose an editor in Settings › Studio first");
			try {
				await launchEditor(launch, {
					spawn: (file, args, opts) => spawn(file, args, opts),
					openExternal: (url) => shell.openExternal(url),
				});
				return ok;
			} catch {
				return fail("launch_failed", "The editor did not open");
			}
		},
	);
	ipcMain.handle(CH.studioEditorGet, (e): StudioEditor => {
		assertTrustedSender(e);
		return editorView(choice());
	});
	ipcMain.handle(CH.studioEditorSet, (e, editor: unknown): IpcResult<void> => {
		assertTrustedSender(e);
		if (editor !== "vscode" && editor !== "zed" && editor !== "cursor")
			return fail("bad_params", "Expected vscode, zed or cursor");
		writeSetting(o.settingsFile, EDITOR_KEY, { editor });
		return ok;
	});
	// The program comes from the OS file dialog only: the renderer can choose that a
	// program is used, never which one.
	ipcMain.handle(
		CH.studioEditorChooseCommand,
		async (e): Promise<IpcResult<StudioEditor>> => {
			assertTrustedSender(e);
			const opts: OpenDialogOptions = {
				title: "Choose your editor",
				properties: ["openFile"],
			};
			const w = BrowserWindow.fromWebContents(e.sender);
			const r = w
				? await dialog.showOpenDialog(w, opts)
				: await dialog.showOpenDialog(opts);
			const picked = r.canceled ? undefined : r.filePaths[0];
			if (!picked) return fail("cancelled", "No program was chosen");
			if (!isLocalPath(picked))
				return fail("bad_params", "Expected a program on this computer");
			writeSetting(o.settingsFile, EDITOR_KEY, {
				editor: "command",
				command: picked,
			});
			return {
				ok: true,
				value: editorView({ editor: "command", command: picked }),
			};
		},
	);
	ipcMain.handle(CH.studioNotifySet, (e, prefs: unknown): IpcResult<void> => {
		assertTrustedSender(e);
		const p = parseNotifyPrefs(prefs);
		if (!p)
			return fail("bad_params", "Expected { turnDone, approval } booleans");
		writeSetting(o.settingsFile, STUDIO_NOTIFY_KEY, p);
		return ok;
	});
}
