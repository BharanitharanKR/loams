import { describe, expect, it } from "vitest";
import {
	MENU_COMMANDS,
	type MenuNode,
	menuCommand,
	menuModel,
} from "../src/main/shell/menu-model";

function flat(nodes: MenuNode[]): MenuNode[] {
	return nodes.flatMap((n) => [n, ...flat(n.submenu ?? [])]);
}

function item(platform: string, id: string): MenuNode | undefined {
	return flat(menuModel({ platform, dev: false })).find((n) => n.id === id);
}

describe("menu model", () => {
	it.each(["linux", "darwin", "win32"])(
		"menu_switches_mode (%s)",
		(platform) => {
			// "Studio" and "Console" switch modes, and the chord's item shows Mod+Shift+M (§53 §4.2).
			expect(item(platform, "mode-studio")).toMatchObject({ label: "Studio" });
			expect(item(platform, "mode-console")).toMatchObject({
				label: "Console",
			});
			expect(item(platform, "mode-toggle")).toMatchObject({
				accelerator: "CmdOrCtrl+Shift+M",
				// The renderer's shell owns the chord in both modes (ruling T24-4): the menu
				// only shows it, so one key press never runs twice.
				registerAccelerator: false,
			});
			expect(menuCommand("mode-studio")).toBe("mode.studio");
			expect(menuCommand("mode-console")).toBe("mode.console");
			expect(menuCommand("mode-toggle")).toBe("mode.toggle");
		},
	);

	it("new thread is Mod+N, handled by Studio's chat.new", () => {
		for (const platform of ["linux", "darwin", "win32"]) {
			expect(item(platform, "new-thread")).toMatchObject({
				label: "New thread",
				accelerator: "CmdOrCtrl+N",
				registerAccelerator: false,
			});
		}
		expect(menuCommand("new-thread")).toBe("chat.new");
	});

	it("only the renderer commands are forwarded", () => {
		expect(menuCommand("quit")).toBeUndefined();
		expect(menuCommand("docs")).toBeUndefined();
		expect(new Set(Object.values(MENU_COMMANDS))).toEqual(
			new Set(["mode.studio", "mode.console", "mode.toggle", "chat.new"]),
		);
	});

	it("keeps quit, docs, logs and about", () => {
		for (const id of ["quit", "docs", "logs", "about"])
			expect(item("linux", id)).toBeDefined();
	});
});
