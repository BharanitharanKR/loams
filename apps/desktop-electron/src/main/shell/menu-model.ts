// Pure application-menu template (ids and roles); menu.electron.ts maps it to Electron.
import type { ShellCommand } from "../../shared/contracts";

export const DOCS_URL = "https://loams.dev/docs";

export interface MenuNode {
	id?: string;
	label?: string;
	role?: string;
	type?: "separator";
	accelerator?: string;
	/**
	 * False: the accelerator is shown but not registered, so the key press reaches the
	 * renderer, which owns the chord (Mod+Shift+M is the shell's, Mod+N Studio's).
	 */
	registerAccelerator?: boolean;
	submenu?: MenuNode[];
}

/**
 * The menu items the renderer acts on (SD1 Task 28, §53 §4.2): main forwards them on
 * `shell:command`; every other id is main's own.
 */
export const MENU_COMMANDS: Readonly<Record<string, ShellCommand>> = {
	"mode-studio": "mode.studio",
	"mode-console": "mode.console",
	"mode-toggle": "mode.toggle",
	"new-thread": "chat.new",
};

export function menuCommand(id: string): ShellCommand | undefined {
	return Object.hasOwn(MENU_COMMANDS, id) ? MENU_COMMANDS[id] : undefined;
}

export function menuModel(o: { platform: string; dev: boolean }): MenuNode[] {
	const sep: MenuNode = { type: "separator" };
	const mac = o.platform === "darwin";
	const menus: MenuNode[] = [];
	const newThread: MenuNode = {
		id: "new-thread",
		label: "New thread",
		accelerator: "CmdOrCtrl+N",
		registerAccelerator: false,
	};
	if (mac) {
		menus.push({
			label: "Loams Desktop",
			submenu: [
				{ id: "about", label: "About Loams Desktop" },
				sep,
				{ role: "hide" },
				{ role: "hideOthers" },
				{ role: "unhide" },
				sep,
				{ id: "quit", label: "Quit Loams Desktop" },
			],
		});
		menus.push({ label: "File", submenu: [newThread] });
	} else {
		menus.push({
			label: "File",
			submenu: [newThread, sep, { id: "quit", label: "Quit" }],
		});
	}
	menus.push({
		label: "Edit",
		submenu: [
			{ role: "undo" },
			{ role: "redo" },
			sep,
			{ role: "cut" },
			{ role: "copy" },
			{ role: "paste" },
			{ role: "selectAll" },
		],
	});
	const view: MenuNode[] = [
		// Loams Studio and Console (§53 §4.2): one window, two modes.
		{ id: "mode-studio", label: "Studio" },
		{ id: "mode-console", label: "Console" },
		{
			id: "mode-toggle",
			label: "Switch mode",
			accelerator: "CmdOrCtrl+Shift+M",
			registerAccelerator: false,
		},
		sep,
		{ role: "resetZoom" },
		{ role: "zoomIn" },
		{ role: "zoomOut" },
		sep,
		{ role: "togglefullscreen" },
	];
	if (o.dev) view.unshift({ role: "reload" }, { role: "toggleDevTools" }, sep);
	menus.push({ label: "View", submenu: view });
	menus.push({
		label: "Window",
		submenu: [{ role: "minimize" }, { role: "zoom" }, { role: "close" }],
	});
	menus.push({
		label: "Help",
		submenu: [
			{ id: "docs", label: "Documentation" },
			{ id: "logs", label: "Open logs folder" },
			...(mac ? [] : [sep, { id: "about", label: "About Loams Desktop" }]),
		],
	});
	return menus;
}
