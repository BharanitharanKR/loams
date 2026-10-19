import { ipcMain, nativeTheme } from "electron";
import { CH, type ThemeChoice } from "../../shared/contracts";
import { assertTrustedSender } from "../security/policy";
import { THEME_SETTING, writeSetting } from "../settings";

/** `theme:set` (D1074): save the choice, and let the OS dialogs and menus follow it. */
export function registerThemeIpc(o: {
	settingsFile: () => string;
	onTheme: (dark: boolean) => void;
}): void {
	ipcMain.handle(CH.themeSet, (e, choice: unknown, dark: unknown) => {
		assertTrustedSender(e);
		if (choice !== "system" && choice !== "dark" && choice !== "light") return;
		if (typeof dark !== "boolean") return;
		writeSetting(o.settingsFile(), THEME_SETTING, choice satisfies ThemeChoice);
		nativeTheme.themeSource = choice;
		o.onTheme(dark);
	});
}
