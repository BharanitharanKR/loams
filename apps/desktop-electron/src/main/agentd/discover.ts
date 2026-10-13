// Discovery (§50 §5.4): `<userData>/agentd/agentd.json` names the daemon's port
// and pid. Pure parsing plus an injectable reader; a stale or malformed file is
// treated as "no daemon".
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { AgentdToken } from "./client";
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
	if (
		typeof r.version !== "string" ||
		r.version.length === 0 ||
		r.version.length > 64
	)
		return null;
	if (!isInt(r.pid, 1, 0x7fff_ffff)) return null;
	if (!isInt(r.port, 1, 65535)) return null;
	if (!isInt(r.startedAtMs, 0, Number.MAX_SAFE_INTEGER)) return null;
	if (typeof r.mode !== "string" || !MODES.includes(r.mode as DaemonMode))
		return null;
	const connectPort = r.connectPort;
	if (connectPort !== undefined && !isInt(connectPort, 1, 65535)) return null;
	return {
		protocol: r.protocol,
		version: r.version,
		pid: r.pid,
		port: r.port,
		startedAtMs: r.startedAtMs,
		mode: r.mode as DaemonMode,
		...(isInt(connectPort, 1, 65535) ? { connectPort } : {}),
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

/** Where the daemon's Connect listener is (SD1 Task 5), and the token it wants. */
export interface AgentdTarget {
	/** `http://127.0.0.1:<connectPort>`. */
	url: string;
	token: AgentdToken;
}

/**
 * The running daemon's `loams.agentd.v1` listener, from `<dir>/agentd.json`
 * (`connectPort`) and `<dir>/agentd.token`. Null when no daemon runs, its
 * file is stale, or it is too old to have a Connect listener. Read on each
 * request, so a restarted daemon (new port, rotated token) is picked up.
 */
export function readAgentdTarget(
	dir: string,
	deps: {
		readFile: (path: string) => string;
		pidAlive: (pid: number) => boolean;
	} = {
		readFile: (path) => readFileSync(path, "utf8"),
		pidAlive: pidAliveDefault,
	},
): AgentdTarget | null {
	const found = readDiscovery(dir, deps);
	if (found.kind !== "found" || found.discovery.connectPort === undefined)
		return null;
	let token: string;
	try {
		token = deps.readFile(join(dir, TOKEN_FILE)).trim();
	} catch {
		return null;
	}
	if (!token) return null;
	return {
		url: `http://127.0.0.1:${found.discovery.connectPort}`,
		token: new AgentdToken(token),
	};
}
