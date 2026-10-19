// Adapted from dataelement/dsh-desktop (MIT), src/main/update/update-manager.ts.
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { rm } from "node:fs/promises";
import {
	app,
	BrowserWindow,
	ipcMain,
	net,
	powerMonitor,
	shell,
} from "electron";
import { autoUpdater } from "electron-updater";
import { CH } from "../../shared/contracts";
import { assertTrustedSender } from "../security/policy";
import { readSetting, writeSetting } from "../settings";
import { createController, type State, type UpdaterLike } from "./controller";
import { fetchFeedFiles, manualGet, type NetRequestLike } from "./feed";
import { verifyManifest } from "./manifest";
import {
	autoCheckEnabled,
	channelFile,
	decideUpdateMode,
	releaseUrl,
	UPDATE_AUTO_KEY,
	type UpdateMode,
	updateCheckForcedOff,
} from "./policy";
import { createCheckScheduler } from "./scheduler";
import { UPDATE_FEED, UPDATE_PUBKEY_HEX } from "./pubkey";

export interface UpdaterHandle {
	state(): State;
	hasVerifiedDownload(): boolean;
	installOnQuit(): Promise<boolean>;
	/** Help › Check for Updates… (CF1 Task 14); its result reaches the renderer as state. */
	check(): Promise<void>;
	stop(): void;
}

/** net.request with manual redirects; see manualGet. */
const netGetManual = (url: string): Promise<Response> =>
	manualGet(
		() =>
			net.request({
				url,
				method: "GET",
				redirect: "manual",
				credentials: "omit",
				useSessionCookies: false,
			}) as unknown as NetRequestLike,
	);

async function sha512Base64(file: string): Promise<string> {
	const h = createHash("sha512");
	for await (const chunk of createReadStream(file)) h.update(chunk as Buffer);
	return h.digest("base64");
}

export function startUpdater(opts: {
	/** userData/settings.json, where `update.autoCheck` lives. */
	settingsFile: string;
	/** Called before quitAndInstall so the engine can stop. */
	prepareToInstall?: () => Promise<void>;
	restartEngine?: () => Promise<void>;
}): UpdaterHandle {
	const mode: UpdateMode = decideUpdateMode({
		packaged: app.isPackaged,
		feed: UPDATE_FEED,
		pubkeyHex: UPDATE_PUBKEY_HEX,
		platform: process.platform,
		appImage: process.env.APPIMAGE,
		noUpdateCheckEnv: process.env.LOAMS_DESKTOP_NO_UPDATE_CHECK,
	});
	const forcedOff = updateCheckForcedOff(
		process.env.LOAMS_DESKTOP_NO_UPDATE_CHECK,
	);
	const autoOn = (): boolean =>
		autoCheckEnabled(
			readSetting<unknown>(opts.settingsFile, UPDATE_AUTO_KEY, true),
			process.env.LOAMS_DESKTOP_NO_UPDATE_CHECK,
		);
	if (mode === "self") {
		autoUpdater.setFeedURL({ provider: "generic", url: UPDATE_FEED });
		autoUpdater.allowDowngrade = false;
		autoUpdater.allowPrerelease = false;
	}
	const ctl = createController({
		mode,
		appVersion: app.getVersion(),
		updater: autoUpdater as unknown as UpdaterLike,
		fetchFeed: () =>
			fetchFeedFiles(
				UPDATE_FEED,
				channelFile(process.platform, process.arch),
				(url) => netGetManual(url),
			),
		verify: (yml, sig) => verifyManifest(yml, sig, UPDATE_PUBKEY_HEX),
		hashFile: sha512Base64,
		deleteFile: (f) => rm(f, { force: true }),
		openRelease: (v) => shell.openExternal(releaseUrl(v)),
		prepareToInstall: opts.prepareToInstall,
		restartEngine: opts.restartEngine,
		onState: (s) => {
			for (const w of BrowserWindow.getAllWindows())
				if (!w.isDestroyed()) w.webContents.send(CH.updateState, s);
		},
		log: (...a) => console.error("[updater]", ...a),
	});

	ipcMain.handle(CH.updateState, (e) => {
		assertTrustedSender(e);
		return ctl.state();
	});
	ipcMain.handle(CH.updateCheck, async (e) => {
		assertTrustedSender(e);
		await ctl.check();
	});
	ipcMain.handle(CH.updateDownload, async (e) => {
		assertTrustedSender(e);
		await ctl.download();
	});
	ipcMain.handle(CH.updateInstall, async (e) => {
		assertTrustedSender(e);
		await ctl.install();
	});

	ipcMain.handle(CH.updateAutoGet, (e) => {
		assertTrustedSender(e);
		return { enabled: autoOn(), locked: forcedOff };
	});
	ipcMain.handle(CH.updateAutoSet, (e, enabled: unknown) => {
		assertTrustedSender(e);
		if (typeof enabled !== "boolean" || forcedOff) return;
		writeSetting(opts.settingsFile, UPDATE_AUTO_KEY, enabled);
		scheduler.setEnabled(mode !== "disabled" && autoOn());
	});

	const scheduler = createCheckScheduler({
		check: () => void ctl.check(),
		onResume: (cb) => {
			powerMonitor.on("resume", cb);
			return () => void powerMonitor.removeListener("resume", cb);
		},
	});
	scheduler.setEnabled(mode !== "disabled" && autoOn());
	return {
		state: () => ctl.state(),
		hasVerifiedDownload: () => ctl.hasVerifiedDownload(),
		installOnQuit: () => ctl.installOnQuit(),
		check: () => ctl.check(),
		stop: () => scheduler.setEnabled(false),
	};
}
