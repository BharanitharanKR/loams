// Where the Postgres control plane is (PG2 R9.1): `loams pg-control` serves
// loams.postgres.v1 on its own listener (127.0.0.1:7680 in the release compose
// stack, profile postgres-control), not on the engine's API port, and checks
// `authorization: Bearer lk_…` against its keys file. Main alone holds the key;
// the renderer reaches pg-control through loams-app://console/pg-control/.
// Pure: no electron import.
import { readFileSync } from "node:fs";
import { readSetting } from "../settings";

/** pg-control's API listener as the release compose stack publishes it. */
export const DEFAULT_PG_CONTROL_URL = "http://127.0.0.1:7680";

/** settings.json: pg-control's base URL. */
export const PG_CONTROL_URL_KEY = "postgres.control.url";
/** settings.json: a file holding the API key (`lk_…`), as `pg-api-key` printed it. */
export const PG_CONTROL_KEY_FILE_KEY = "postgres.control.apiKeyFile";

export interface PgControlTarget {
	/** The listener's origin, `http://127.0.0.1:7680`. */
	url: string;
	/** The API key, or null (a `--pg-authz dev-loopback` listener needs none). */
	apiKey: string | null;
}

const httpUrl = (v: unknown): string | null => {
	if (typeof v !== "string" || v.trim() === "") return null;
	try {
		const u = new URL(v.trim());
		return u.protocol === "http:" || u.protocol === "https:" ? u.origin : null;
	} catch {
		return null;
	}
};

const readKeyFile = (path: unknown): string | null => {
	if (typeof path !== "string" || path.trim() === "") return null;
	try {
		return readFileSync(path.trim(), "utf8").trim() || null;
	} catch {
		return null;
	}
};

/**
 * The URL from `LOAMS_PG_CONTROL_URL`, else settings.json's
 * `postgres.control.url`, else the release stack's 127.0.0.1:7680. The key from
 * `LOAMS_PG_CONTROL_API_KEY`, else the file `LOAMS_PG_CONTROL_API_KEY_FILE` or
 * settings.json's `postgres.control.apiKeyFile` names. Read per request, so a
 * key minted after the app started is picked up.
 */
export function readPgControlTarget(
	settingsFile: string,
	env: NodeJS.ProcessEnv = process.env,
): PgControlTarget {
	const url =
		httpUrl(env.LOAMS_PG_CONTROL_URL) ??
		httpUrl(readSetting<unknown>(settingsFile, PG_CONTROL_URL_KEY, null)) ??
		DEFAULT_PG_CONTROL_URL;
	const apiKey =
		env.LOAMS_PG_CONTROL_API_KEY?.trim() ||
		readKeyFile(env.LOAMS_PG_CONTROL_API_KEY_FILE) ||
		readKeyFile(readSetting<unknown>(settingsFile, PG_CONTROL_KEY_FILE_KEY, null));
	return { url, apiKey: apiKey || null };
}
