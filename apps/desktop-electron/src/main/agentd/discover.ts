// Discovery (§50 §5.4): `<userData>/agentd/agentd.json` names the daemon's port
// and pid. Pure parsing plus an injectable reader; a stale or malformed file is
// treated as "no daemon".
import { join } from "node:path";
import { AGENTD_PROTOCOL, type DaemonMode, type Discovery } from "./types";

export const AGENTD_DIR = "agentd";
export const DISCOVERY_FILE = "agentd.json";
export const TOKEN_FILE = "agentd.token";

/** `<userData>/agentd`. */
export function agentdDir(userData: string): string {
	return join(userData, AGENTD_DIR);
}

const MODES: readonly DaemonMode[] = ["service", "child"];

function isInt(v: unknown, min: number, max: number): v is number {
	return typeof v === "number" && Number.isInteger(v) && v >= min && v <= max;
}

/** Validates the discovery JSON; null for anything malformed. */
export function parseDiscovery(text: string): Discovery | null {
	let raw: unknown;
	try {
		raw = JSON.parse(text);
	} catch {
		return null;
	}
	if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
	const r = raw as Record<string, unknown>;
	if (!isInt(r.protocol, 1, 0xffff_ffff)) return null;
	if (typeof r.version !== "string" || r.version.length === 0 || r.version.length > 64)
		return null;
	if (!isInt(r.pid, 1, 0x7fff_ffff)) return null;
	if (!isInt(r.port, 1, 65535)) return null;
	if (!isInt(r.startedAtMs, 0, Number.MAX_SAFE_INTEGER)) return null;
	if (typeof r.mode !== "string" || !MODES.includes(r.mode as DaemonMode))
		return null;
	return {
		protocol: r.protocol,
		version: r.version,
		pid: r.pid,
		port: r.port,
		startedAtMs: r.startedAtMs,
		mode: r.mode as DaemonMode,
	};
}

export type DiscoveryResult =
	| { kind: "found"; discovery: Discovery }
	| { kind: "absent" }
	| { kind: "stale"; pid: number }
	| { kind: "protocol_mismatch"; protocol: number };

/** Reads and checks the discovery file; `pidAlive` is injected (process.kill(pid, 0) in main). */
export function readDiscovery(
	dir: string,
	deps: {
		readFile: (path: string) => string;
		pidAlive: (pid: number) => boolean;
	},
): DiscoveryResult {
	let text: string;
	try {
		text = deps.readFile(join(dir, DISCOVERY_FILE));
	} catch {
		return { kind: "absent" };
	}
	const d = parseDiscovery(text);
	if (!d) return { kind: "absent" };
	if (!deps.pidAlive(d.pid)) return { kind: "stale", pid: d.pid };
	if (d.protocol !== AGENTD_PROTOCOL)
		return { kind: "protocol_mismatch", protocol: d.protocol };
	return { kind: "found", discovery: d };
}

/** `process.kill(pid, 0)`: true when the process exists (EPERM still means it exists). */
export function pidAliveDefault(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (e) {
		return (e as NodeJS.ErrnoException).code === "EPERM";
	}
}
