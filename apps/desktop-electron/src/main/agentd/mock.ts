// A fixture-backed AgentdClient for development and tests, until the daemon and
// `@loams/agentd-client` land (DD1 Tasks 6, 10). It holds no token.
import type { AgentdClient, Subscription } from "./client";
import {
	AGENTD_PROTOCOL,
	AgentdError,
	type DaemonMode,
	type DaemonStatus,
	type HelloReply,
	type Repo,
	type SessionSummary,
	type UnaryMethod,
	type UnaryMethods,
} from "./types";

export interface MockFixture {
	mode: DaemonMode;
	version: string;
	pid: number;
	startedAtMs: number;
	sessions: SessionSummary[];
	repos?: Repo[];
}

export function defaultFixture(now: number = Date.now()): MockFixture {
	return {
		mode: "child",
		version: "0.2.1",
		pid: 41234,
		startedAtMs: now - 42 * 60_000,
		sessions: [
			{
				id: "s_01hq7x",
				title: "Summarise yesterday's failed connector runs",
				harness: "loams-agent",
				state: "waiting_approval",
				updatedAtMs: now - 90_000,
				pendingApprovals: 1,
				resumable: true,
			},
			{
				id: "s_01hq7y",
				title: "Refactor the ingest pipeline tests",
				harness: "claude-code",
				state: "running",
				updatedAtMs: now - 12_000,
				pendingApprovals: 0,
				resumable: true,
			},
			{
				id: "s_01hq6a",
				title: "Branch Postgres for the migration dry run",
				harness: "loams-agent",
				state: "interrupted",
				updatedAtMs: now - 3_600_000,
				pendingApprovals: 0,
				resumable: true,
			},
			{
				id: "s_01hq5k",
				title: "Old exploration",
				harness: "codex",
				state: "failed",
				updatedAtMs: now - 86_400_000,
				pendingApprovals: 0,
				resumable: false,
			},
		],
	};
}

export interface MockAgentdClient extends AgentdClient {
	/** Test hook: replace a session list and push a status. */
	setSessions(s: SessionSummary[]): void;
	/** Every method called, in order. */
	readonly calls: string[];
	readonly closed: boolean;
}

export function createMockAgentdClient(
	fixture: MockFixture = defaultFixture(),
): MockAgentdClient {
	let sessions = fixture.sessions.map((s) => ({ ...s }));
	const repos: Repo[] = (
		fixture.repos ?? [{ path: "/home/omar/src/loams", name: "loams", defaultBranch: "main" }]
	).map((r) => ({ ...r }));
	const baseName = (p: string) =>
		p.replace(/\/+$/, "").split(/[/:]/).pop()?.replace(/\.git$/, "") || p;
	const addRepo = (path: string): Repo => {
		const known = repos.find((r) => r.path === path);
		if (known) return { ...known };
		const repo = { path, name: baseName(path), defaultBranch: "main" };
		repos.push(repo);
		return { ...repo };
	};
	let draining = false;
	let closed = false;
	const calls: string[] = [];
	const watchers = new Set<(s: DaemonStatus) => void>();
	const hello: HelloReply = {
		protocol: AGENTD_PROTOCOL,
		version: fixture.version,
		buildSha: "0000000",
		mode: fixture.mode,
		pid: fixture.pid,
		startedAtMs: fixture.startedAtMs,
		role: "owner",
		features: ["sessions", "approvals"],
	};
	const status = (): DaemonStatus => ({
		mode: fixture.mode,
		draining,
		runningTurns: sessions.filter((s) => s.state === "running").length,
		pendingApprovals: sessions.reduce((n, s) => n + s.pendingApprovals, 0),
		engine: { state: "stopped" },
		version: fixture.version,
	});
	const push = () => {
		const s = status();
		for (const w of watchers) w(s);
	};
	const handlers: {
		[M in UnaryMethod]: (
			p: UnaryMethods[M]["params"],
		) => UnaryMethods[M]["result"];
	} = {
		DaemonStatus: () => status(),
		ListSessions: () => sessions.map((s) => ({ ...s })),
		AttachSession: ({ sessionId }) => {
			const s = sessions.find((x) => x.id === sessionId);
			if (!s) throw new AgentdError("not_found", "No such session");
			if (!s.resumable)
				throw new AgentdError("bad_params", "This session cannot be resumed");
			return { sessionId };
		},
		Drain: () => {
			draining = true;
			push();
			return null;
		},
		Shutdown: () => {
			closed = true;
			return null;
		},
		ListRepos: () => repos.map((r) => ({ ...r })),
		AddRepo: ({ path }) => addRepo(path),
		CloneRepo: ({ url }) => addRepo(`/home/omar/src/${baseName(url)}`),
	};
	return {
		hello,
		calls,
		get closed() {
			return closed;
		},
		async call(method, params) {
			calls.push(method);
			if (closed) throw new AgentdError("unavailable", "The daemon is not running");
			if (draining && method === "AttachSession")
				throw new AgentdError("agentd_draining", "The daemon is stopping");
			const h = handlers[method] as (p: unknown) => unknown;
			return h(params) as never;
		},
		subscribe(method, _params, onItem): Subscription {
			calls.push(method);
			const cb = onItem as (s: DaemonStatus) => void;
			watchers.add(cb);
			let end: () => void = () => {};
			const done = new Promise<void>((r) => {
				end = r;
			});
			queueMicrotask(() => cb(status()));
			return {
				done,
				cancel: () => {
					watchers.delete(cb);
					end();
				},
			};
		},
		on() {
			return () => {};
		},
		async close() {
			watchers.clear();
		},
		setSessions(s) {
			sessions = s.map((x) => ({ ...x }));
			push();
		},
	};
}
