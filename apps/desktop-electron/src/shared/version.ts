import type { ThemeChoice } from "./contracts";

/** The renderer is told the app version through a `webPreferences.additionalArguments` flag. */
export const VERSION_ARG = "--loams-desktop-version=";

/** `--loams-desktop-version=1.2.3` from a process argv, or "0.0.0" when absent or empty. */
export function versionFromArgv(argv: readonly string[]): string {
	const hit = argv.find((a) => a.startsWith(VERSION_ARG));
	const v = hit?.slice(VERSION_ARG.length).trim();
	return v ? v : "0.0.0";
}

/** The saved theme choice reaches the preload the same way (D1074). */
export const THEME_ARG = "--loams-theme=";

/** `--loams-theme=dark` from a process argv; "system" when absent or unknown. */
export function themeFromArgv(argv: readonly string[]): ThemeChoice {
	const v = argv.find((a) => a.startsWith(THEME_ARG))?.slice(THEME_ARG.length);
	return v === "dark" || v === "light" ? v : "system";
}
