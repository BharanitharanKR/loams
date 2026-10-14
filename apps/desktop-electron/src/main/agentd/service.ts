// The daemon as main sees it: connects (or not), tracks status, and answers the
// renderer's named calls. No electron import; ipc.electron.ts wires it.
// Nothing here returns or logs the token: the client holds it.
import type {
	AgentdBackground,
	AgentdSession,
	AgentdStatus,
	IpcResult,
} from "../../shared/contracts";
import type { AgentdClient, ConnectFn, Subscription } from "./client";
import {
	modeFor,
	needsConsent,
	parseAnswer,
	stopConfirmed,
} from "./consent";
import { AgentdError, type DaemonStatus, type HelloReply } from "./types";

const SESSION_ID = /^[A-Za-z0-9._-]{1,128}$/;
const DRAIN_TIMEOUT_MS = 20_000;

export interface AgentdServiceDeps {
	connect: ConnectFn;
	/** `<userData>`; the daemon's directory is `<userData>/agentd`. */
	userData: string;
	/** Shown as `dataDir`. */
	agentdDir: string;
	clientVersion: string;
	readBackground(): AgentdBackground;
	writeBackground(v: "on" | "off"): void;
	/**
	 * Start the daemon in a mode (child: spawn `run --child`; service:
	 * `service install` and start). Task 11/12 supply the real runner.
	 */
	launch(mode: "service" | "child"): Promise<void>;
	/** `service stop` in service mode (after the drain); a no-op in child mode. */
	stopService(): Promise<void>;
}

const fail = (code: string, message: string) =>
	({ ok: false, code, message }) as const;
const ok = { ok: true, value: undefined } as const;

function errorResult(e: unknown): { ok: false; code: string; message: string } {
	if (e instanceof AgentdError) return fail(e.code, e.message);
	return fail("internal", "The daemon call failed");
}

export class AgentdService {
	#client: AgentdClient | null = null;
	#watch: Subscription | null = null;
	#last: DaemonStatus | null = null;
	#status: AgentdStatus;
	readonly #listeners = new Set<(s: AgentdStatus) => void>();

	constructor(private readonly deps: AgentdServiceDeps) {
		this.#status = { state: "stopped", dataDir: deps.agentdDir };
	}

	status(): AgentdStatus {
		return this.#status;
	}

	onStatus(cb: (s: AgentdStatus) => void): () => void {
		this.#listeners.add(cb);
		return () => void this.#listeners.delete(cb);
	}

	#set(s: AgentdStatus): void {
		this.#status = s;
		for (const l of this.#listeners) l(s);
	}

	#fromHello(h: HelloReply, d: DaemonStatus | null): AgentdStatus {
		return {
			state: "running",
			mode: d?.mode ?? h.mode,
			version: d?.version ?? h.version,
			protocol: h.protocol,
			pid: h.pid,
			startedAtMs: h.startedAtMs,
			dataDir: this.deps.agentdDir,
			draining: d?.draining ?? false,
			runningTurns: d?.runningTurns ?? 0,
			pendingApprovals: d?.pendingApprovals ?? 0,
		};
	}

	/** Connect to a running daemon; leaves the status `stopped` or `unavailable` when none answers. */
	async attach(): Promise<void> {
		if (this.#client) return;
		try {
			const c = await this.deps.connect({
				dataDir: this.deps.userData,
				clientVersion: this.deps.clientVersion,
			});
			this.#client = c;
			this.#last = null;
			this.#set(this.#fromHello(c.hello, null));
			this.#watch = c.subscribe("WatchDaemonStatus", {}, (d) => {
				this.#last = d;
				this.#set(this.#fromHello(c.hello, d));
			});
			c.on("disconnected", () =>
				this.#set({ state: "unavailable", reason: "disconnected" }),
			);
			c.on("reconnected", () =>
				this.#set(this.#fromHello(c.hello, this.#last)),
			);
		} catch (e) {
			this.#client = null;
			if (e instanceof AgentdError && e.code === "unavailable") {
				this.#set({ state: "stopped", dataDir: this.deps.agentdDir });
			} else if (e instanceof AgentdError) {
				this.#set({ state: "unavailable", reason: e.code });
			} else {
				this.#set({ state: "unavailable", reason: "connect_failed" });
			}
		}
	}

	async sessions(): Promise<IpcResult<AgentdSession[]>> {
		const c = this.#client;
		if (!c) return fail("not_running", "Background agents are not running");
		try {
			const rows = await c.call("ListSessions", {});
			return { ok: true, value: rows.map((r) => ({ ...r })) };
		} catch (e) {
			return errorResult(e);
		}
	}

	async reattach(sessionId: unknown): Promise<IpcResult<{ route: string }>> {
		if (typeof sessionId !== "string" || !SESSION_ID.test(sessionId))
			return fail("bad_params", "Invalid session id");
		const c = this.#client;
		if (!c) return fail("not_running", "Background agents are not running");
		try {
			const r = await c.call("AttachSession", { sessionId });
			return { ok: true, value: { route: `/agent/${encodeURIComponent(r.sessionId)}` } };
		} catch (e) {
			return errorResult(e);
		}
	}

	background(): AgentdBackground {
		return this.deps.readBackground();
	}

	setBackground(v: unknown): IpcResult<void> {
		const a = parseAnswer(v);
		if (!a) return fail("bad_params", "Expected on or off");
		this.deps.writeBackground(a);
		return ok;
	}

	async start(): Promise<IpcResult<void>> {
		const bg = this.deps.readBackground();
		if (needsConsent(bg))
			return fail(
				"consent_required",
				"Choose whether agents keep running in the background first",
			);
		if (this.#client) return ok;
		const mode = modeFor(bg);
		if (!mode) return fail("consent_required", "No background choice yet");
		try {
			await this.deps.launch(mode);
		} catch {
			return fail("launch_failed", "Could not start the daemon");
		}
		await this.attach();
		return this.#client
			? ok
			: fail("launch_failed", "The daemon did not answer after starting");
	}

	/** Explicit stop (D785): drain first, then shut down (child) or stop the service. */
	async stop(opts: unknown): Promise<IpcResult<void>> {
		if (!stopConfirmed(opts))
			return fail("consent_required", "Stopping needs the user's confirmation");
		const c = this.#client;
		if (!c) return ok;
		const mode = c.hello.mode;
		try {
			await c.call("Drain", { reason: "stop", timeoutMs: DRAIN_TIMEOUT_MS });
			if (mode === "child") await c.call("Shutdown", {});
		} catch (e) {
			return errorResult(e);
		}
		// Stop the service before letting go of the client, so a failed stop
		// leaves the daemon attached for a confirmed retry.
		if (mode === "service") {
			try {
				await this.deps.stopService();
			} catch {
				return fail("service_stop_failed", "The background service did not stop");
			}
		}
		this.#watch?.cancel();
		this.#watch = null;
		this.#client = null;
		await c.close().catch(() => {});
		this.#set({ state: "stopped", dataDir: this.deps.agentdDir, version: c.hello.version });
		return ok;
	}

	async dispose(): Promise<void> {
		this.#watch?.cancel();
		await this.#client?.close().catch(() => {});
		this.#client = null;
		this.#listeners.clear();
	}
}
