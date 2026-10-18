// Starting loams-agentd from the desktop (0.1.0 release fix B9). No electron import;
// setup.ts wires it.
//
//   child    `loams-agentd run --child --config <userData>/agentd/config.toml`, a child
//            of this process: its output goes to `<logs>/agentd.log`, and it is stopped
//            (SIGTERM) when the app quits.
//   service  `install-runtime --from <bundle> --to <userData>/agentd/runtime`, then
//            `service install --config …`, which installs the per-user service and
//            starts it (design §50 §5.2). The bundle is `resources/bin`, which holds
//            loams-agentd, loams, SHA256SUMS and VERSION (scripts/fetch-engine.mjs).
// Both wait for the daemon's discovery file to name a live process.
import { type ChildProcess, execFile, spawn } from "node:child_process";
import {
	chmodSync,
	closeSync,
	existsSync,
	mkdirSync,
	openSync,
	readFileSync,
	writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { pidAliveDefault, readDiscovery } from "./discover";
import { AgentdError } from "./types";

export interface LauncherDeps {
	/** `<userData>/agentd`. */
	dir: string;
	logsDir: string;
	/** Where the binary may be, in order (paths.ts `agentdBin`). */
	candidates: string[];
	exists?: (p: string) => boolean;
	spawnChild?: typeof spawn;
	run?: (file: string, args: string[]) => Promise<void>;
	/** How long to wait for the discovery file (ms). */
	readyTimeoutMs?: number;
}

export interface AgentdLauncher {
	launch(mode: "service" | "child"): Promise<void>;
	stopService(): Promise<void>;
	/** Stops a child this launcher started (on quit). */
	killChild(): void;
	/** The binary, or null when none is bundled. */
	binary(): string | null;
}

function execOnce(file: string, args: string[]): Promise<void> {
	return new Promise((resolve, reject) => {
		execFile(file, args, { timeout: 60_000, windowsHide: true }, (err, _out, stderr) => {
			if (err) reject(new Error(`${args[0]} failed: ${String(stderr).trim() || err.message}`));
			else resolve();
		});
	});
}

export function createAgentdLauncher(deps: LauncherDeps): AgentdLauncher {
	const exists = deps.exists ?? existsSync;
	const doSpawn = deps.spawnChild ?? spawn;
	const run = deps.run ?? execOnce;
	const readyTimeout = deps.readyTimeoutMs ?? 20_000;
	const config = join(deps.dir, "config.toml");
	let child: ChildProcess | null = null;

	const binary = (): string | null => deps.candidates.find((c) => exists(c)) ?? null;

	const prepare = (): string => {
		const bin = binary();
		if (!bin) throw new AgentdError("unavailable", "loams-agentd is not bundled with this build");
		mkdirSync(deps.dir, { recursive: true });
		if (process.platform !== "win32") chmodSync(deps.dir, 0o700);
		// Every key is optional (crates/loams-agentd/src/config.rs); the file must exist.
		if (!exists(config)) writeFileSync(config, "", { mode: 0o600 });
		return bin;
	};

	const waitReady = async (proc: ChildProcess | null = null): Promise<void> => {
		const deadline = Date.now() + readyTimeout;
		while (Date.now() < deadline) {
			const d = readDiscovery(deps.dir, {
				readFile: (p) => readFileSync(p, "utf8"),
				pidAlive: pidAliveDefault,
			});
			if (d.kind === "found" && d.discovery.connectPort !== undefined) return;
			if (proc && (proc.exitCode !== null || proc.signalCode !== null))
				throw new AgentdError("unavailable", "loams-agentd exited while starting");
			await new Promise((r) => setTimeout(r, 200));
		}
		throw new AgentdError("unavailable", "loams-agentd did not start in time");
	};

	return {
		binary,
		async launch(mode) {
			const bin = prepare();
			if (mode === "service") {
				await run(bin, ["install-runtime", "--from", dirname(bin), "--to", join(deps.dir, "runtime")]);
				await run(bin, ["service", "install", "--config", config]);
				await waitReady();
				return;
			}
			if (child && child.exitCode === null && child.signalCode === null) {
				await waitReady(child);
				return;
			}
			mkdirSync(deps.logsDir, { recursive: true });
			const log = openSync(join(deps.logsDir, "agentd.log"), "a", 0o600);
			try {
				const c = doSpawn(bin, ["run", "--child", "--config", config], {
					stdio: ["ignore", log, log],
					windowsHide: true,
				});
				c.on("error", () => {});
				c.on("exit", () => {
					if (child === c) child = null;
				});
				child = c;
			} finally {
				closeSync(log);
			}
			await waitReady(child);
		},
		async stopService() {
			const bin = binary();
			if (!bin) return;
			await run(bin, ["service", "stop", "--config", config]);
		},
		killChild() {
			const c = child;
			child = null;
			if (c && c.exitCode === null) c.kill("SIGTERM");
		},
	};
}
