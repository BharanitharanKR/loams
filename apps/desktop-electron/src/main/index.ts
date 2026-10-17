import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { userInfo } from "node:os";
import { join } from "node:path";
import {
	app,
	BrowserWindow,
	crashReporter,
	Notification,
	nativeTheme,
	safeStorage,
	screen,
	session,
	shell,
} from "electron";
import { CH, type EngineState } from "../shared/contracts";
import { THEME_ARG, VERSION_ARG } from "../shared/version";
import { registerChatIpc } from "./agent/ipc.electron";
import type { ChatService } from "./agent/service";
import { registerTools } from "./agent/tools";
import { agentdDir, readAgentdTarget } from "./agentd/discover";
import { registerAgentdIpc } from "./agentd/ipc.electron";
import { createAgentdService } from "./agentd/setup";
import { type StudioWatch, watchStudio } from "./agentd/studio-watch";
import { appPaths } from "./app-paths";
import { ConnectorCatalog, catalogPath } from "./connectors/catalog";
import { registerConnectorsIpc } from "./connectors/ipc.electron";
import {
	findEngineBinary,
	probeLiveOnMainPort,
	probeLiveSupport,
} from "./engine/binary";
import { registerEngineIpc } from "./engine/ipc.electron";
import { EngineSupervisor } from "./engine/supervisor";
import { FactoryEmbed } from "./factory/embed.electron";
import { FactoryHardening } from "./factory/hardening.electron";
import { FactoryHost } from "./factory/host";
import { registerFactoryIpc } from "./factory/ipc.electron";
import { Vault } from "./factory/vault";
import { FactoryViews } from "./factory/views.electron";
import { registerPreviewIpc } from "./preview/ipc.electron";
import { StudioPreviewView } from "./preview/view.electron";
import {
	installAppProtocol,
	registerAppScheme,
} from "./protocol/handler.electron";
import { createLocalShim } from "./protocol/local-shim";
import { secureWindow } from "./security/install.electron";
import { registerServerIpc } from "./servers/ipc.electron";
import { ServerRegistry } from "./servers/registry";
import {
	LIVE_STORE_KEY,
	readLiveStore,
	readNotifyPrefs,
	readSetting,
	readThemeChoice,
	writeSetting,
} from "./settings";
import { getMainWindow, setMainWindow } from "./shell/main-window";
import { installAppMenu } from "./shell/menu.electron";
import { DOCS_URL, menuCommand } from "./shell/menu-model";
import { quitSequence } from "./shell/quit";
import { reveal } from "./shell/reveal";
import { registerShellIpc } from "./shell/shell-ipc.electron";
import { initSingleInstance } from "./shell/single-instance.electron";
import {
	EMPTY_STUDIO,
	notificationContent,
	notificationRoute,
	type StudioSnapshot,
	shouldNotify,
	studioTrayRoute,
} from "./shell/studio-tray-model";
import { registerThemeIpc } from "./shell/theme-ipc.electron";
import { createTray, type TrayHandle } from "./shell/tray.electron";
import { closeAction, TRAY_ROUTES } from "./shell/tray-model";
import {
	loadWindowState,
	MIN_HEIGHT,
	MIN_WIDTH,
	saveWindowState,
	type WindowState,
} from "./shell/window-state";
import { createSqlServices, registerSqlIpc } from "./sql/ipc.electron";
import { sqlAgentTools } from "./sql/tools";
import { createStackManager, registerStacksIpc } from "./stacks/ipc.electron";
import { bindLiveToTikv } from "./stacks/stacks";
import { registerStudioIpc } from "./studio/ipc.electron";
import { startUpdater, type UpdaterHandle } from "./update/updater.electron";

// Test hook (unpackaged builds only): the e2e smoke runs against a scratch profile, which
// also gives it its own single-instance lock.
if (!app.isPackaged && process.env.LOAMS_DESKTOP_USER_DATA)
	app.setPath("userData", process.env.LOAMS_DESKTOP_USER_DATA);

registerAppScheme();
// D663: crash dumps stay on this machine (userData/Crashpad); nothing is uploaded.
crashReporter.start({ uploadToServer: false });

let registry: ServerRegistry;
let engine: EngineSupervisor | undefined;
let updater: UpdaterHandle | undefined;
let factory: FactoryHost | undefined;
let factoryViews: FactoryViews | undefined;
let factoryEmbed: FactoryEmbed | undefined;
let studioPreview: StudioPreviewView | undefined;
let chat: ChatService | undefined;
let tray: TrayHandle | undefined;
let studioWatch: StudioWatch | undefined;
let studio: StudioSnapshot = EMPTY_STUDIO;
let isQuitting = false;
const settingsFile = (): string =>
	join(app.getPath("userData"), "settings.json");

/** `engine.autoStart` in userData/settings.json; default true. */
function engineAutoStart(): boolean {
	return (
		readSetting<unknown>(settingsFile(), "engine.autoStart", true) !== false
	);
}

/** `shell.closeToTray` in settings.json; default: true on Windows, false elsewhere. */
function closeToTray(): boolean | undefined {
	const v = readSetting<unknown>(
		settingsFile(),
		"shell.closeToTray",
		undefined,
	);
	return typeof v === "boolean" ? v : undefined;
}

function showMainWindow(create: () => BrowserWindow): void {
	const win = getMainWindow() ?? create();
	reveal(win);
}

function createWindow(): BrowserWindow {
	const stateFile = join(app.getPath("userData"), "window-state.json");
	const state = loadWindowState(
		stateFile,
		screen.getAllDisplays().map((d) => d.workArea),
	);
	const win = new BrowserWindow({
		width: state.width,
		height: state.height,
		x: state.x,
		y: state.y,
		minWidth: MIN_WIDTH,
		minHeight: MIN_HEIGHT,
		show: false,
		title: "Loams Desktop",
		webPreferences: {
			preload: join(__dirname, "../preload/index.cjs"),
			contextIsolation: true,
			sandbox: true,
			nodeIntegration: false,
			webSecurity: true,
			webviewTag: false,
			additionalArguments: [
				`${VERSION_ARG}${app.getVersion()}`,
				`${THEME_ARG}${readThemeChoice(settingsFile())}`,
			],
		},
	});
	secureWindow(win);
	// The console sets document.title per page; the native title stays the product name.
	win.on("page-title-updated", (e) => e.preventDefault());
	setMainWindow(win);
	win.on("closed", () => setMainWindow(undefined));
	if (state.isMaximized) win.maximize();
	let last: WindowState = state;
	const persist = (): void => {
		if (win.isDestroyed()) return;
		const maximized = win.isMaximized();
		if (!maximized && !win.isMinimized())
			last = { ...win.getBounds(), isMaximized: false };
		else last = { ...last, isMaximized: maximized };
		saveWindowState(stateFile, last);
	};
	let timer: NodeJS.Timeout | undefined;
	const schedule = (): void => {
		clearTimeout(timer);
		timer = setTimeout(persist, 400);
	};
	win.on("resize", schedule);
	win.on("move", schedule);
	win.on("maximize", schedule);
	win.on("unmaximize", schedule);
	win.on("close", (e) => {
		clearTimeout(timer);
		persist();
		const action = closeAction({
			platform: process.platform,
			hasTray: tray !== undefined,
			closeToTray: closeToTray(),
			quitting: isQuitting,
		});
		if (action === "hide") {
			e.preventDefault();
			win.hide();
		}
	});
	win.once("ready-to-show", () => win.show());
	void win.loadURL("loams-app://console/ui/");
	return win;
}

const singleInstance = initSingleInstance({
	getWindow: getMainWindow,
	onPrimary: () => {
		void app.whenReady().then(async () => {
			app.setAppUserModelId("dev.loams.desktop");
			// The theme choice (D1074): OS dialogs and menus follow it; Task 13 syncs the chrome.
			nativeTheme.themeSource = readThemeChoice(settingsFile());
			registerThemeIpc({ settingsFile, onTheme: () => {} });
			registry = new ServerRegistry(
				join(app.getPath("userData"), "servers.json"),
				{ devDemo: !app.isPackaged },
			);
			registerServerIpc(registry);
			const connectorsFile = catalogPath({
				isPackaged: app.isPackaged,
				resourcesPath: process.resourcesPath,
				appRoot: app.getAppPath(),
			});
			const connectors = new ConnectorCatalog(() =>
				readFileSync(connectorsFile, "utf8"),
			);
			registerConnectorsIpc(connectors);
			const vault = new Vault(
				join(app.getPath("userData"), "factory", "credentials.bin"),
				{
					// On Linux, basic_text means no keyring: the "encryption" is a fixed key.
					available: () =>
						safeStorage.isEncryptionAvailable() &&
						(process.platform !== "linux" ||
							safeStorage.getSelectedStorageBackend() !== "basic_text"),
					encrypt: (v) => safeStorage.encryptString(v),
					decrypt: (b) => safeStorage.decryptString(b),
				},
			);
			factory = new FactoryHost(vault);
			// Postgres and WeSQL services: the pages (IPC) and the agent tools share them.
			const sql = createSqlServices(app.getPath("userData"));
			registerTools(sqlAgentTools(sql));
			chat = registerChatIpc({
				session: session.defaultSession,
				vault,
				factory,
				connectors,
			});
			const hardening = new FactoryHardening();
			const views = new FactoryViews(factory, hardening);
			factoryViews = views;
			factoryEmbed = new FactoryEmbed(factory, hardening, (a) => views.open(a));
			registerFactoryIpc(factory, views, factoryEmbed);
			// Studio's Preview tab (SD1 Task 26): one sandboxed view placed by main.
			studioPreview = new StudioPreviewView();
			registerPreviewIpc(studioPreview);
			const paths = await appPaths();
			const logFile = join(paths.logs, "engine.log");
			engine = new EngineSupervisor({
				spawn,
				binary: () => findEngineBinary(paths.engineBin, existsSync),
				dataDir: paths.engineData,
				logFile,
				fetch,
				now: Date.now,
				sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
				liveSupported: probeLiveSupport,
				liveOnMainPort: probeLiveOnMainPort,
			});
			engine.on("state", (st: EngineState) => {
				registry.setLocalUrl(st.phase === "ready" ? st.url : "");
				tray?.refresh();
			});
			// Compose runs from a per-user copy: no bind mount points into the app resources.
			const stacks = createStackManager({
				sourceDir: paths.stacksDir,
				stacksDir: paths.stacksRunDir,
				version: app.getVersion(),
				alwaysCopy: !app.isPackaged,
				logsDir: paths.logs,
			});
			// Live's store follows the user's choice in Settings, never the
			// stack alone (ruling T23-8).
			const liveStore = bindLiveToTikv(
				stacks,
				engine,
				() => readLiveStore(settingsFile()),
				(e) => console.error("[stacks] setLivePd failed:", e),
			);
			registerEngineIpc(engine, logFile, {
				get: () => readLiveStore(settingsFile()),
				set: (choice) => {
					writeSetting(settingsFile(), LIVE_STORE_KEY, choice);
					liveStore.refresh();
				},
			});
			registerStacksIpc(stacks, paths.logs);
			registerSqlIpc(sql);
			// DD1: the agent daemon's status panel (mock daemon in development builds).
			const agentd = createAgentdService({
				userData: app.getPath("userData"),
				settingsFile: settingsFile(),
				version: app.getVersion(),
				mock: !app.isPackaged,
			});
			registerAgentdIpc(agentd);
			void agentd.attach();
			// SD1 Task 28: Studio's editor hand-off and notification choices.
			registerStudioIpc({ settingsFile: settingsFile() });
			// With the TiKV stack chosen, a stack left running from last time starts the engine
			// with Live on it directly, but a slow or absent runtime must not hold the engine
			// back for long.
			await Promise.race([
				stacks.state("tikv"),
				new Promise((r) => setTimeout(r, 2000)),
			]);
			if (engineAutoStart()) engine.start();
			installAppProtocol(session.defaultSession, {
				distRoot: paths.consoleDist,
				activeServer: () => registry.active(),
				engineState: () => engine?.state() ?? { phase: "stopped" },
				localShim: createLocalShim(
					{ version: app.getVersion(), username: userInfo().username },
					() => registry.active().kind,
				),
				// SD1 Task 5: the daemon's loams.agentd.v1 listener; main alone reads its token.
				agentd: () => readAgentdTarget(agentdDir(app.getPath("userData"))),
			});
			const open = (): BrowserWindow => {
				const w = createWindow();
				singleInstance.watchMainWindow(w);
				return w;
			};
			registerShellIpc({
				onBadge: (n) => tray?.setBadge(n),
				onNotifyClick: (route) => {
					showMainWindow(open);
					if (route) singleInstance.navigate(route);
				},
			});
			updater = startUpdater({
				settingsFile: settingsFile(),
				prepareToInstall: async () => {
					await engine?.stop();
				},
				restartEngine: async () => {
					engine?.start();
				},
			});
			app.setAboutPanelOptions({
				applicationName: "Loams Desktop",
				applicationVersion: app.getVersion(),
				website: "https://loams.dev",
			});
			const openLogs = (): void => void shell.openPath(paths.logs);
			installAppMenu({
				dev: !app.isPackaged,
				onAction: (id) => {
					// Mode switches and New thread are the renderer's (SD1 Task 28).
					const command = menuCommand(id);
					if (command) {
						showMainWindow(open);
						getMainWindow()?.webContents.send(CH.shellCommand, command);
					} else if (id === "quit") app.quit();
					else if (id === "docs") void shell.openExternal(DOCS_URL);
					else if (id === "logs") openLogs();
					else if (id === "about") app.showAboutPanel();
				},
			});
			const eng = engine;
			tray = createTray({
				getEngine: () => eng.state(),
				getServer: () => registry.active(),
				getStudio: () => studio,
				onAction: (id) => {
					const studioRoute = studioTrayRoute(id);
					if (studioRoute) {
						showMainWindow(open);
						singleInstance.navigate(studioRoute);
					} else if (id === "open") showMainWindow(open);
					else if (id === "engine")
						void (eng.state().phase === "ready" ? eng.stop() : eng.start());
					else if (id === "approvals") {
						showMainWindow(open);
						singleInstance.navigate(TRAY_ROUTES.approvals);
					} else if (id === "servers") {
						showMainWindow(open);
						singleInstance.navigate(TRAY_ROUTES.servers);
					} else if (id === "quit") app.quit();
				},
			});
			// SD1 Task 28 (§53 §7, §50 §5.5): the daemon's threads and approvals in the tray,
			// its notifications as OS notifications that open Studio links.
			studioWatch = watchStudio({
				target: () => readAgentdTarget(agentdDir(app.getPath("userData"))),
				now: Date.now,
				retryMs: 5_000,
				onSnapshot: (s) => {
					studio = s;
					tray?.refresh();
				},
				onNotice: (n) => {
					const focused = getMainWindow()?.isFocused() === true;
					const prefs = readNotifyPrefs(settingsFile());
					if (
						!Notification.isSupported() ||
						!shouldNotify(n, prefs, { focused, since: 0 })
					)
						return;
					const note = new Notification(notificationContent(n));
					const route = notificationRoute(n);
					note.on("click", () => {
						showMainWindow(open);
						singleInstance.navigate(route);
					});
					note.show();
				},
			});
			open();
			app.on("activate", () => showMainWindow(open));
		});
		let quitting = false;
		let quitDone = false;
		app.on("before-quit", (e) => {
			isQuitting = true;
			if (quitDone) return;
			e.preventDefault();
			if (quitting) return;
			quitting = true;
			studioWatch?.stop();
			tray?.destroy();
			factoryEmbed?.closeAll();
			factoryViews?.closeAll();
			studioPreview?.destroy();
			// State first (agent turns stopped, chat files written), then the quit-time install of
			// a hash-verified download (D661; it stops the engine itself) or a plain engine stop.
			void quitSequence({
				flush: async () => chat?.dispose(),
				stopEngine: async () => engine?.dispose(),
				hasVerifiedDownload: () => updater?.hasVerifiedDownload() === true,
				installOnQuit: () => updater?.installOnQuit() ?? Promise.resolve(false),
			}).finally(() => {
				quitDone = true;
				app.quit();
			});
		});
		app.on("window-all-closed", () => {
			if (process.platform !== "darwin") app.quit();
		});
	},
});
